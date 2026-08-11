//! Networked DFS service state, outcome, event, and pending-state types.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::metadata_keys::normalize_posix_path as normalize_metadata_posix_path;
use crate::posix_path::parent_posix_path;
use crate::transfer::{PendingNetworkedRead, PendingNetworkedWrite};
use crate::{
    DfsBulkReadItemResponse, DfsConflictRecord, DfsConflictStatus, DfsControlPlaneKvClient,
    DfsDistributedWriteCommand, DfsDistributedWriteResult, DfsError, DfsFileSystemConfig,
    DfsFileSystemId, DfsMessageResponse, DfsMetadataBackend, DfsMetadataShardConfig,
    DfsMetadataShardId, DfsMetadataStore, DfsNamespaceRequestRecord, DfsNamespaceResult,
    DfsObjectId, DfsObjectMetadata, DfsOutboundAttachment, DfsPendingNamespaceRequestPageCursor,
    DfsPosixCoordinationResult, DfsReadData, DfsReadRangeData, DfsReadResult,
    DfsRepairAttemptRecord, DfsStableNamespaceOperationReconciliation,
    DfsStableWriteOperationReconciliation, DfsVersion, DistributedEventualCoordinator,
    DistributedRepairJob, LocalContentStore, ObjectVersionReader, ResidentLocalMetadataShardLease,
    ResidentLocalShardedMetadataBackendLease,
};

pub(crate) const DFS_REMOTE_READ_RETRY_DELAY: Duration = Duration::from_secs(1);
pub(crate) const DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE: usize = 4096;
pub(crate) const DFS_METADATA_OBJECT_VERSION_PAGE_SIZE: usize = 1024;
pub(crate) const DFS_METADATA_OBJECT_INTENT_PAGE_SIZE: usize = 1024;
pub(crate) const DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE: usize = 1024;
#[cfg(not(test))]
pub(crate) const DFS_CONFLICT_SCAN_PAGE_SIZE: usize = 1024;
#[cfg(test)]
pub(crate) const DFS_CONFLICT_SCAN_PAGE_SIZE: usize = 2;

pub(crate) const DFS_DEFERRED_PARENT_TOUCH_DRAIN_LIMIT: usize = 8;

/// Transient namespace objects indexed by their direct parent directory.
///
/// Membership is established only after `add_transient_object_metadata` has
/// compared the object with durable metadata. Directory reads can therefore
/// decide whether an overlay merge is needed without scanning every in-flight
/// object or issuing another backend lookup for each candidate.
#[derive(Debug, Default)]
pub(crate) struct TransientMetadataObjectIndex {
    object_ids: BTreeSet<DfsObjectId>,
    parent_by_object_id: BTreeMap<DfsObjectId, (DfsFileSystemId, String)>,
    object_ids_by_parent: BTreeMap<DfsFileSystemId, BTreeMap<String, BTreeSet<DfsObjectId>>>,
}

impl TransientMetadataObjectIndex {
    pub(crate) fn insert_object(&mut self, object: &DfsObjectMetadata) {
        self.remove(&object.object_id);
        self.object_ids.insert(object.object_id.clone());
        let normalized_path = normalize_metadata_posix_path(&object.path);
        let Some(parent) = parent_posix_path(&normalized_path) else {
            return;
        };
        self.parent_by_object_id.insert(
            object.object_id.clone(),
            (object.file_system_id.clone(), parent.clone()),
        );
        self.object_ids_by_parent
            .entry(object.file_system_id.clone())
            .or_default()
            .entry(parent)
            .or_default()
            .insert(object.object_id.clone());
    }

    pub(crate) fn remove(&mut self, object_id: &DfsObjectId) -> bool {
        let removed = self.object_ids.remove(object_id);
        let Some((file_system_id, parent)) = self.parent_by_object_id.remove(object_id) else {
            return removed;
        };
        let remove_file_system = self
            .object_ids_by_parent
            .get_mut(&file_system_id)
            .is_some_and(|parents| {
                let remove_parent = parents.get_mut(&parent).is_some_and(|object_ids| {
                    object_ids.remove(object_id);
                    object_ids.is_empty()
                });
                if remove_parent {
                    parents.remove(&parent);
                }
                parents.is_empty()
            });
        if remove_file_system {
            self.object_ids_by_parent.remove(&file_system_id);
        }
        removed
    }

    pub(crate) fn clear(&mut self) {
        self.object_ids.clear();
        self.parent_by_object_id.clear();
        self.object_ids_by_parent.clear();
    }

    pub(crate) fn contains(&self, object_id: &DfsObjectId) -> bool {
        self.object_ids.contains(object_id)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.object_ids.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.object_ids.len()
    }

    pub(crate) fn child_object_ids(
        &self,
        file_system_id: &DfsFileSystemId,
        parent: &str,
    ) -> Option<&BTreeSet<DfsObjectId>> {
        let parent = normalize_metadata_posix_path(parent);
        self.object_ids_by_parent.get(file_system_id)?.get(&parent)
    }
}

// Namespace RPCs are on the app-visible critical path and may include a
// durable WAN metadata mutation plus bounded journal replay. The authority's
// durable request ledger prevents blind re-execution after this deadline.
pub const DFS_ROUTED_NAMESPACE_OP_TIMEOUT: Duration = Duration::from_secs(60);
/// Nested coordination depth (serving a namespace request while settling
/// another) beyond which new coordination rejects as Busy instead of growing
/// the stack.
pub(crate) const DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH: usize = 8;
/// Journal batches a primary ships in one routed-operation response.
pub const DFS_ROUTED_RESPONSE_JOURNAL_BATCH_LIMIT: usize = 16;
/// Journal batches a primary ships in one normal routed-write response.
///
/// Normal SIFS writes are on the app-visible critical path and must not turn a
/// commit response into a historical replica catch-up sweep. The batch remains
/// contiguous from the requester's cursor so replay stays correct; background
/// replication and explicit sync operations continue catch-up beyond it.
pub(crate) const DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT: usize = 1;
/// Journal batches a namespace primary ships in one foreground namespace
/// response. Namespace mutations also carry synced path objects and can pull
/// additional journal batches with bounded SyncPath rounds, so keep the first
/// response small enough to return promptly on deep real-cluster journals.
pub const DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT: usize = 1;
/// Maximum journal entries packed into one foreground namespace response
/// batch. Namespace catch-up is latency-sensitive and benefits from denser
/// packing than ordinary replication, while the encoded-byte cap below keeps
/// the response bounded.
pub(crate) const DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT: usize = 2048;
/// Maximum encoded bytes in one foreground namespace response journal batch.
pub(crate) const DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BYTE_LIMIT: usize =
    crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT;
pub(crate) const DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT: usize = 3;
pub(crate) const DFS_ROUTED_NAMESPACE_IDLE_WAIT: Duration = Duration::from_millis(10);

pub(crate) const DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES: usize = 64;

/// Process-local guard registry for paths owned by running cross-shard jobs.
///
/// Metadata backends remain the durable source of truth. This registry is the
/// live admission projection shared by every request shell borrowing the same
/// resident backend generation, so a global rename/hardlink job cannot become
/// invisible to an already-open exact-shard lane.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsCrossShardNamespaceJobLock {
    shard_id: Option<DfsMetadataShardId>,
    file_system_id: DfsFileSystemId,
    paths: Vec<String>,
}

pub(crate) trait IntoCrossShardNamespaceLockPaths {
    fn into_lock_paths(self) -> Vec<String>;
}

impl IntoCrossShardNamespaceLockPaths for String {
    fn into_lock_paths(self) -> Vec<String> {
        vec![self]
    }
}

impl IntoCrossShardNamespaceLockPaths for Vec<String> {
    fn into_lock_paths(self) -> Vec<String> {
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct DfsCrossShardNamespaceLockRegistry {
    /// Job identity is the ownership boundary. A delayed terminal update for
    /// one job must not remove a newer job's guard merely because both jobs
    /// use the same path.
    locks_by_job: Arc<Mutex<BTreeMap<String, DfsCrossShardNamespaceJobLock>>>,
}

impl DfsCrossShardNamespaceLockRegistry {
    pub(crate) fn replace_jobs<P>(
        &self,
        locks: impl IntoIterator<Item = (Option<DfsMetadataShardId>, String, DfsFileSystemId, P)>,
    ) where
        P: IntoCrossShardNamespaceLockPaths,
    {
        let mut current = self
            .locks_by_job
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        current.clear();
        current.extend(
            locks
                .into_iter()
                .map(|(shard_id, job_id, file_system_id, paths)| {
                    (
                        job_id,
                        DfsCrossShardNamespaceJobLock {
                            shard_id,
                            file_system_id,
                            paths: paths.into_lock_paths(),
                        },
                    )
                }),
        );
    }

    pub(crate) fn replace_shard_jobs<P>(
        &self,
        shard_id: &DfsMetadataShardId,
        locks: impl IntoIterator<Item = (String, DfsFileSystemId, P)>,
    ) where
        P: IntoCrossShardNamespaceLockPaths,
    {
        let mut current = self
            .locks_by_job
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        current.retain(|_, lock| lock.shard_id.as_ref() != Some(shard_id));
        current.extend(locks.into_iter().map(|(job_id, file_system_id, paths)| {
            (
                job_id,
                DfsCrossShardNamespaceJobLock {
                    shard_id: Some(shard_id.clone()),
                    file_system_id,
                    paths: paths.into_lock_paths(),
                },
            )
        }));
    }

    pub(crate) fn upsert_job<P>(
        &self,
        shard_id: Option<DfsMetadataShardId>,
        job_id: String,
        file_system_id: DfsFileSystemId,
        paths: P,
    ) where
        P: IntoCrossShardNamespaceLockPaths,
    {
        self.locks_by_job
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                job_id,
                DfsCrossShardNamespaceJobLock {
                    shard_id,
                    file_system_id,
                    paths: paths.into_lock_paths(),
                },
            );
    }

    pub(crate) fn remove_job(&self, job_id: &str) {
        self.locks_by_job
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(job_id);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.locks_by_job
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        let current = self
            .locks_by_job
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        current
            .values()
            .flat_map(|lock| {
                lock.paths
                    .iter()
                    .map(|path| (&lock.file_system_id, path.as_str()))
            })
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub(crate) fn blocks(&self, file_system_id: &DfsFileSystemId, path: &str) -> bool {
        self.locks_by_job
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(|lock| {
                lock.file_system_id == *file_system_id
                    && lock.paths.iter().any(|locked_path| {
                        crate::posix_path::posix_path_is_at_or_under_prefix(path, locked_path)
                            || crate::posix_path::posix_path_is_at_or_under_prefix(
                                locked_path,
                                path,
                            )
                    })
            })
    }

    #[cfg(test)]
    pub(crate) fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.locks_by_job, &other.locks_by_job)
    }
}

/// Immutable control-plane inputs reused by the local planning portion of one
/// routed namespace command.
///
/// The request has already compared its sender's shard stamps with the
/// receiver's current committed view. Keeping that exact view through path
/// planning, placement resolution, and prepare avoids repeatedly collecting
/// and decoding cluster-KV state during the same operation. General authority
/// lookups deliberately do not consult this context: explicit refresh and
/// reentrant routed requests must always begin from a fresh committed view.
#[derive(Clone, Debug)]
pub(crate) struct DfsNamespaceOperationContext {
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) shards: Vec<DfsMetadataShardConfig>,
    pub(crate) placement_config: Option<DfsFileSystemConfig>,
}

/// Cooperative cancellation handle for a [`NetworkedDfsService`]. Cloneable and
/// thread-safe: a caller keeps one clone, hands the service the other, and flips
/// it when a budgeted operation gives up (e.g. a degraded cluster link on a
/// mobile device — a normal operating state, not an exceptional one). The
/// service's distributed wait/retry loops observe the flag and return
/// [`DfsError::Cancelled`] instead of running out their full network timeouts.
#[derive(Clone, Debug, Default)]
pub struct DfsCancellationFlag {
    local: Arc<AtomicBool>,
    inherited: Arc<[DfsCancellationFlag]>,
}

impl DfsCancellationFlag {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a cancellation source which also observes every supplied source.
    ///
    /// The returned handle retains its own independently cancellable bit. This
    /// lets a mount add lifecycle cancellation without discarding a caller's
    /// pre-existing operation cancellation source.
    pub fn linked(inherited: impl IntoIterator<Item = DfsCancellationFlag>) -> DfsCancellationFlag {
        Self {
            local: Arc::new(AtomicBool::new(false)),
            inherited: inherited.into_iter().collect::<Vec<_>>().into(),
        }
    }

    /// Request cancellation. Idempotent; already-running local work finishes,
    /// distributed waits and retries stop at their next flag check.
    pub fn cancel(&self) {
        self.local.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.local.load(Ordering::Acquire)
            || self.inherited.iter().any(DfsCancellationFlag::is_cancelled)
    }
}

/// One metadata capability temporarily attached to a DFS service.
///
/// Ordinary callers retain an owned backend. Daemon request lanes instead
/// attach a lease from the process-resident shard pool for the bounded request
/// which is about to run. Keeping that distinction behind `Deref` preserves
/// the existing backend call sites while making it impossible to open a second
/// authority-bearing backend merely to execute another shard concurrently.
pub(crate) enum DfsMetadataBackendOwner {
    Owned(Box<dyn DfsMetadataBackend>),
    ResidentShard(ResidentLocalMetadataShardLease),
    ResidentSharded(ResidentLocalShardedMetadataBackendLease),
}

impl std::fmt::Debug for DfsMetadataBackendOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(backend) => formatter
                .debug_tuple("OwnedMetadataBackend")
                .field(backend)
                .finish(),
            Self::ResidentShard(lease) => lease.fmt(formatter),
            Self::ResidentSharded(lease) => lease.fmt(formatter),
        }
    }
}

impl Deref for DfsMetadataBackendOwner {
    type Target = dyn DfsMetadataBackend;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(backend) => backend.as_ref(),
            Self::ResidentShard(lease) => &**lease,
            Self::ResidentSharded(lease) => &**lease,
        }
    }
}

impl DerefMut for DfsMetadataBackendOwner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Owned(backend) => backend.as_mut(),
            Self::ResidentShard(lease) => &mut **lease,
            Self::ResidentSharded(lease) => &mut **lease,
        }
    }
}

impl AsRef<dyn DfsMetadataBackend> for DfsMetadataBackendOwner {
    fn as_ref(&self) -> &(dyn DfsMetadataBackend + 'static) {
        &**self
    }
}

impl AsMut<dyn DfsMetadataBackend> for DfsMetadataBackendOwner {
    fn as_mut(&mut self) -> &mut (dyn DfsMetadataBackend + 'static) {
        &mut **self
    }
}

impl DfsMetadataBackendOwner {
    pub(crate) fn resident_exact_shard_id(&self) -> Option<&DfsMetadataShardId> {
        match self {
            Self::ResidentShard(lease) => Some(lease.shard_id()),
            Self::Owned(_) | Self::ResidentSharded(_) => None,
        }
    }
}

/// Stateful networked DFS data-plane and metadata coordinator.
#[derive(Debug)]
pub struct NetworkedDfsService<T> {
    pub(crate) node_id: NodeId,
    pub(crate) metadata: DfsMetadataStore,
    pub(crate) tombstones_by_metadata_path: BTreeMap<(DfsFileSystemId, String), DfsObjectMetadata>,
    pub(crate) transient_metadata_object_ids: TransientMetadataObjectIndex,
    pub(crate) transient_metadata_predecessor_object_ids: BTreeMap<DfsObjectId, DfsObjectId>,
    pub(crate) content: LocalContentStore,
    pub(crate) coordinator: DistributedEventualCoordinator,
    pub(crate) control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    pub(crate) metadata_backend: Option<DfsMetadataBackendOwner>,
    pub(crate) metadata_lookup_cache:
        Mutex<BTreeMap<(DfsFileSystemId, String), Option<DfsObjectMetadata>>>,
    pub(crate) metadata_lookup_cache_depth: Mutex<usize>,
    pub(crate) active_replica_stages: BTreeMap<DfsObjectId, ActiveReplicaStage>,
    pub(crate) pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    pub(crate) pending_reads: BTreeMap<String, PendingNetworkedRead>,
    pub(crate) repair_attempt_records: BTreeMap<(String, NodeId), DfsRepairAttemptRecord>,
    pub(crate) transient_remote_read_pruned_locations:
        BTreeMap<(DfsObjectId, DfsVersion, Option<String>), BTreeSet<NodeId>>,
    pub(crate) local_metadata_journal_cursors: BTreeMap<DfsMetadataShardId, u64>,
    pub(crate) metadata_replication_cursors: BTreeMap<(NodeId, DfsMetadataShardId), u64>,
    pub(crate) recent_namespace_freshness_syncs:
        BTreeMap<(DfsFileSystemId, DfsMetadataShardId, NodeId), Instant>,
    /// Whether a failed read-only authority lookup may fall back to applying
    /// the primary's journal locally. Mounted clients disable this and bracket
    /// their explicit directory refreshes with a durable host handoff.
    pub(crate) implicit_read_replica_ingest_enabled: bool,
    pub(crate) pending_metadata_replications: BTreeMap<String, PendingMetadataReplication>,
    pub(crate) pending_metadata_authority_probes: BTreeMap<String, PendingMetadataAuthorityProbe>,
    pub(crate) metadata_authority_probe_results: BTreeMap<String, bool>,
    pub(crate) pending_routed_writes: BTreeMap<String, PendingRoutedWrite>,
    pub(crate) pending_remote_write_responses: BTreeMap<String, PendingRemoteWriteResponse>,
    pub(crate) deferred_routed_create_parent_touches: BTreeSet<String>,
    pub(crate) deferred_untracked_routed_write_responses: BTreeMap<String, DfsObjectId>,
    pub(crate) pending_routed_namespace_ops: BTreeMap<String, PendingRoutedNamespaceOp>,
    pub(crate) stable_write_reconciliation_results:
        BTreeMap<String, DfsStableWriteOperationReconciliation>,
    pub(crate) stable_write_reconciliation_targets: BTreeMap<String, NodeId>,
    pub(crate) stable_namespace_reconciliation_results:
        BTreeMap<String, DfsStableNamespaceOperationReconciliation>,
    pub(crate) stable_namespace_reconciliation_targets: BTreeMap<String, NodeId>,
    pub(crate) active_namespace_operation_context: Option<DfsNamespaceOperationContext>,
    /// In-memory round-robin cursor for bounded pending-request maintenance.
    /// Durable request status remains the recovery source of truth.
    pub(crate) pending_namespace_request_recovery_cursor:
        Option<DfsPendingNamespaceRequestPageCursor>,
    /// Routed request whose exact transaction is currently being planned.
    /// `apply_namespace_transaction_with_selected_shard` uses this context to
    /// place the terminal request result in the same durable prepare commit.
    pub(crate) active_durable_namespace_request: Option<DfsNamespaceRequestRecord>,
    /// Set once the active request has written a hidden request-owned prepare.
    /// Any later error is outcome-ambiguous: recovery owns that exact prepare,
    /// so the request admission must remain pending rather than recording a
    /// contradictory terminal rejection.
    pub(crate) active_durable_namespace_prepare_recorded: bool,
    pub(crate) routed_namespace_settle_depth: usize,
    pub(crate) cross_shard_namespace_locks: DfsCrossShardNamespaceLockRegistry,
    pub(crate) last_events: Vec<NetworkedDfsServiceEvent>,
    pub(crate) message_committed_work: DfsMessageCommittedWork,
    pub(crate) committed_work_tracker: Option<DfsCommittedWorkTracker>,
    pub(crate) transport: T,
    pub(crate) repair_retry_tick: u64,
    pub(crate) next_frame_id: u64,
    pub(crate) next_message_id: u64,
    pub(crate) next_request_id: u64,
    pub(crate) cancellation: Option<DfsCancellationFlag>,
}

/// Result from serving one DFS message command through a networked service.
#[derive(Debug)]
pub struct DfsMessageProcessResult {
    pub response: DfsMessageResponse,
    pub attachment: Option<DfsOutboundAttachment>,
    pub events: Vec<NetworkedDfsServiceEvent>,
}

/// Durable work created while processing a DFS mutation scope.
///
/// This receipt is intentionally separate from response/observation events:
/// metadata may already be committed when response journal construction or
/// delivery fails. The daemon consumes these coalesced bits before unwrapping
/// an operation outcome, so every durable edge retains an owner.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct DfsCommittedWork {
    pub publish_metadata: bool,
    pub repair_maintenance: bool,
    pub namespace_maintenance: bool,
    pub metadata_replica_progress: bool,
    /// A replica ACK advanced the durable journal beyond its rebuildable table
    /// cursor. The daemon consumes this edge on the same resident shard and
    /// coalesces asynchronous materialization behind the response.
    pub metadata_materialization: bool,
}

impl DfsCommittedWork {
    pub(crate) fn merge(&mut self, work: Self) {
        self.publish_metadata |= work.publish_metadata;
        self.repair_maintenance |= work.repair_maintenance;
        self.namespace_maintenance |= work.namespace_maintenance;
        self.metadata_replica_progress |= work.metadata_replica_progress;
        self.metadata_materialization |= work.metadata_materialization;
    }
}

/// Backwards-compatible name for committed work produced by a served message.
pub type DfsMessageCommittedWork = DfsCommittedWork;

const DFS_COMMITTED_WORK_PUBLISH_METADATA: u8 = 1 << 0;
const DFS_COMMITTED_WORK_REPAIR_MAINTENANCE: u8 = 1 << 1;
const DFS_COMMITTED_WORK_NAMESPACE_MAINTENANCE: u8 = 1 << 2;
const DFS_COMMITTED_WORK_METADATA_REPLICA_PROGRESS: u8 = 1 << 3;
const DFS_COMMITTED_WORK_METADATA_MATERIALIZATION: u8 = 1 << 4;

/// Caller-retained committed-work receipt for cancellable DFS operations.
///
/// A maintenance future may be dropped after a synchronous backend mutation
/// reaches durability but before the future returns. Recording each edge in
/// this shared tracker at the mutation boundary lets the caller transfer the
/// exact coalesced work after the backend-bearing service generation is gone.
#[derive(Clone, Debug, Default)]
pub struct DfsCommittedWorkTracker {
    bits: Arc<AtomicU8>,
}

impl DfsCommittedWorkTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record(&self, work: DfsCommittedWork) {
        let mut bits = 0;
        if work.publish_metadata {
            bits |= DFS_COMMITTED_WORK_PUBLISH_METADATA;
        }
        if work.repair_maintenance {
            bits |= DFS_COMMITTED_WORK_REPAIR_MAINTENANCE;
        }
        if work.namespace_maintenance {
            bits |= DFS_COMMITTED_WORK_NAMESPACE_MAINTENANCE;
        }
        if work.metadata_replica_progress {
            bits |= DFS_COMMITTED_WORK_METADATA_REPLICA_PROGRESS;
        }
        if work.metadata_materialization {
            bits |= DFS_COMMITTED_WORK_METADATA_MATERIALIZATION;
        }
        if bits != 0 {
            self.bits.fetch_or(bits, Ordering::Release);
        }
    }

    pub fn take(&self) -> DfsCommittedWork {
        let bits = self.bits.swap(0, Ordering::AcqRel);
        DfsCommittedWork {
            publish_metadata: bits & DFS_COMMITTED_WORK_PUBLISH_METADATA != 0,
            repair_maintenance: bits & DFS_COMMITTED_WORK_REPAIR_MAINTENANCE != 0,
            namespace_maintenance: bits & DFS_COMMITTED_WORK_NAMESPACE_MAINTENANCE != 0,
            metadata_replica_progress: bits & DFS_COMMITTED_WORK_METADATA_REPLICA_PROGRESS != 0,
            metadata_materialization: bits & DFS_COMMITTED_WORK_METADATA_MATERIALIZATION != 0,
        }
    }
}

/// Outcome plus the durable-work receipt for one served DFS message.
#[derive(Debug)]
pub struct DfsMessageProcessReport {
    pub outcome: Result<DfsMessageProcessResult, DfsError>,
    pub committed_work: DfsMessageCommittedWork,
    /// Exact zero-attempt repair queue entries created by this message.
    ///
    /// This is deliberately a delta rather than a snapshot of every pending
    /// repair owned by the service lane.
    pub committed_repair_records: Vec<DfsRepairAttemptRecord>,
}

/// Result of a networked DFS write after the service has applied routing,
/// policy, and replica-receipt rules.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NetworkedDfsWriteOutcome {
    Committed {
        intent_id: String,
        version: DfsVersion,
        content_hash: String,
        repair_jobs: Vec<DistributedRepairJob>,
    },
    Pending {
        intent_id: String,
        durable_receipts: usize,
        required_receipts: usize,
    },
    Conflict {
        conflict: DfsConflictRecord,
    },
}

/// Lightweight conflict view returned by read paths when the authoritative
/// version cannot be selected without explicit conflict resolution.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictReadView {
    pub conflict_id: String,
    pub base_version: Option<DfsVersion>,
    pub local_version: DfsVersion,
    pub remote_version: DfsVersion,
    pub detected_by_node_id: NodeId,
    pub reason: String,
    pub local_content_hash: Option<String>,
    pub remote_content_hash: Option<String>,
    pub status: DfsConflictStatus,
}

/// Result of a full-object networked read, including freshness information for
/// confirmed or stale replicas.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NetworkedDfsReadOutcome {
    Found {
        data: DfsReadData,
        freshness: NetworkedDfsReadFreshness,
    },
    NotConfirmed {
        object_id: DfsObjectId,
        version: DfsVersion,
        confirmed_locations: Vec<NodeId>,
        required_locations: Vec<NodeId>,
    },
    PendingReplication {
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: Option<String>,
        current_locations: Vec<NodeId>,
    },
    Conflicted {
        object_id: DfsObjectId,
        visible_version: DfsVersion,
        visible_content_hash: Option<String>,
        conflicts: Vec<DfsConflictReadView>,
    },
    NotFound,
}

/// Result of a ranged networked read, including freshness information for
/// confirmed or stale replicas.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NetworkedDfsRangeReadOutcome {
    Found {
        data: DfsReadRangeData,
        freshness: NetworkedDfsReadFreshness,
    },
    NotConfirmed {
        object_id: DfsObjectId,
        version: DfsVersion,
        confirmed_locations: Vec<NodeId>,
        required_locations: Vec<NodeId>,
    },
    PendingReplication {
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: Option<String>,
        current_locations: Vec<NodeId>,
    },
    Conflicted {
        object_id: DfsObjectId,
        visible_version: DfsVersion,
        visible_content_hash: Option<String>,
        conflicts: Vec<DfsConflictReadView>,
    },
    NotFound,
}

pub(crate) struct NetworkedDfsReadStream {
    pub(crate) data: DfsReadData,
    pub(crate) total_size_bytes: u64,
    pub(crate) reader: ObjectVersionReader,
}

pub(crate) struct NetworkedDfsRangeReadStream {
    pub(crate) data: DfsReadRangeData,
    pub(crate) range_size_bytes: u64,
    pub(crate) reader: ObjectVersionReader,
}

pub(crate) enum NetworkedDfsReadStreamOutcome {
    Found(NetworkedDfsReadStream),
    PendingReplication,
    Conflicted { conflicts: Vec<DfsConflictReadView> },
    NotFound,
}

pub(crate) enum NetworkedDfsRangeReadStreamOutcome {
    Found(NetworkedDfsRangeReadStream),
    PendingReplication,
    Conflicted { conflicts: Vec<DfsConflictReadView> },
    NotFound,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StageBlobSendOutcome {
    pub(crate) sent: bool,
    pub(crate) last_error: Option<String>,
}

/// Freshness classification for read data returned by a networked DFS service.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkedDfsReadFreshness {
    Current,
    Confirmed {
        replica_node_ids: Vec<NodeId>,
    },
    Stale {
        current_version: DfsVersion,
        current_content_hash: Option<String>,
    },
}

/// Policy for handling a replicated-confirmed write that did not collect enough
/// receipts before its caller's timeout.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsConfirmedTimeoutAction {
    KeepPending,
    Abort,
    DowngradeToEventual { min_commit_replicas: usize },
}

/// Result of applying a timeout policy to a replicated-confirmed write.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsConfirmedTimeoutOutcome {
    Committed(NetworkedDfsWriteOutcome),
    StillPending {
        intent_id: String,
        durable_receipts: usize,
        required_receipts: usize,
    },
    Aborted {
        intent_id: String,
    },
    Conflict {
        conflict: DfsConflictRecord,
    },
    Downgraded(NetworkedDfsWriteOutcome),
}

/// Summary of state refreshed while reconciling a reconnected DFS node.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReconnectReconciliation {
    pub object_id: DfsObjectId,
    pub refreshed_receipts: usize,
    #[serde(default)]
    pub ignored_divergent_manifests: usize,
    pub conflicts: Vec<DfsConflictRecord>,
    pub missing_current: bool,
}

/// Result of trying to transfer local-owned object authority to a new owner.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsOwnerHandoffOutcome {
    Transferred {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        previous_owner_node_id: NodeId,
        new_owner_node_id: NodeId,
        previous_epoch: u64,
        new_epoch: u64,
    },
    Rejected {
        reason: String,
    },
}

/// Per-target result from sending metadata journal updates to configured shard
/// replicas.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsMetadataReplicationOutcome {
    Sent {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        entries: usize,
        next_after_sequence: u64,
    },
    InFlight {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
    },
    NoChanges {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    Unroutable {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PendingMetadataReplicationResponseKind {
    JournalBatch,
    SnapshotInstall,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingMetadataReplication {
    pub(crate) target_node_id: NodeId,
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) next_after_sequence: u64,
    pub(crate) expected_response_kind: PendingMetadataReplicationResponseKind,
    pub(crate) verify_prefix_only: bool,
    /// Optional authority identity that a rolling-upgrade-safe response must
    /// echo before an exact-prefix acknowledgement can count.
    pub(crate) required_authority_confirmation_id: Option<String>,
    pub(crate) created_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingMetadataAuthorityProbe {
    pub(crate) target_node_id: NodeId,
    pub(crate) confirmation_id: String,
    pub(crate) expected_shard: DfsMetadataShardConfig,
    pub(crate) expected_next_after_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingRoutedWrite {
    /// Stable requester identity for this routed write. Remote intent aliases
    /// point back to this key without cloning the full wire command.
    pub(crate) canonical_request_id: String,
    pub(crate) object_id: DfsObjectId,
    pub(crate) primary_node_id: NodeId,
    /// Exact committed shard assignment used to choose `primary_node_id` for
    /// this dispatch. Response journals are admitted only while this remains
    /// the current assignment, including its authority epoch.
    pub(crate) authority_shard: DfsMetadataShardConfig,
    /// Full retry command while the request is unresolved. Terminal outcomes
    /// are compact polling receipts; durable metadata owns exact replay after
    /// this body is released.
    pub(crate) command: Option<DfsDistributedWriteCommand>,
    pub(crate) authority_redirects_remaining: usize,
    pub(crate) remote_intent_id: Option<String>,
    pub(crate) durable_receipts: usize,
    pub(crate) required_receipts: usize,
    pub(crate) result: Option<Result<NetworkedDfsWriteOutcome, DfsError>>,
    /// When the full retry body was released. The requester accepts late
    /// authoritative completion only for a bounded recovery window; durable
    /// intent receipts own reconciliation after this compact state expires.
    pub(crate) compact_retained_at: Option<Instant>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingRemoteWriteResponse {
    pub(crate) requester_node_id: NodeId,
    pub(crate) request_id: String,
    pub(crate) object_id: DfsObjectId,
    pub(crate) metadata_journal_after_sequence: Option<u64>,
    pub(crate) created_object_path: Option<String>,
    pub(crate) foreground_replica_repair: bool,
    pub(crate) created_at: Instant,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRoutedNamespaceResult {
    pub(crate) result: DfsNamespaceResult,
    pub(crate) posix_coordination_result: Option<DfsPosixCoordinationResult>,
    pub(crate) synced_path_object: Option<DfsObjectMetadata>,
    pub(crate) synced_path_objects: Vec<DfsObjectMetadata>,
    pub(crate) metadata_journal_has_more: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRoutedNamespaceOp {
    pub(crate) primary_node_id: NodeId,
    /// Exact immutable assignment whose routing path selected
    /// `primary_node_id`. Redirects and zero-batch responses bind to this
    /// authority rather than to any unrelated shard touched by coordination.
    pub(crate) dispatch_authority_shard: Option<DfsMetadataShardConfig>,
    /// One immutable authority snapshot for all shards touched by the
    /// operation. Each response batch must still match its dispatch snapshot
    /// and the receiver's current committed assignment.
    pub(crate) authority_shards: Vec<DfsMetadataShardConfig>,
    pub(crate) replay_journal_batches: bool,
    pub(crate) apply_synced_path_object: bool,
    pub(crate) result: Option<PendingRoutedNamespaceResult>,
}

/// Event emitted by the networked DFS service while handling data-plane,
/// metadata, repair, and ownership workflows.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NetworkedDfsServiceEvent {
    BlobStaged {
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
        content_hash: String,
    },
    BlobStageRejected {
        intent_id: String,
        object_id: DfsObjectId,
        replica_node_id: NodeId,
        reason: String,
    },
    VersionCommitted {
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
    },
    ReplicaRepaired {
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
        content_hash: String,
    },
    BlobFetched {
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        source_node_id: NodeId,
        content_hash: String,
    },
    BlobFetchRejected {
        intent_id: String,
        object_id: DfsObjectId,
        source_node_id: NodeId,
        reason: String,
    },
    RemoteReadServed {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsReadResult,
    },
    RemoteBulkReadServed {
        request_id: String,
        requester_node_id: NodeId,
        result_count: usize,
    },
    RemoteReadCompleted {
        request_id: String,
        object_id: DfsObjectId,
        source_node_id: NodeId,
        result: DfsReadResult,
    },
    RemoteBulkReadCompleted {
        request_id: String,
        source_node_id: NodeId,
        results: Vec<DfsBulkReadItemResponse>,
    },
    RemoteWriteServed {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsDistributedWriteResult,
    },
    RemoteWriteCompleted {
        request_id: String,
        object_id: DfsObjectId,
        primary_node_id: NodeId,
        result: DfsDistributedWriteResult,
    },
    RemoteNamespaceServed {
        request_id: String,
        requester_node_id: NodeId,
        result: DfsNamespaceResult,
        mutates_metadata: bool,
    },
    RemoteNamespaceCompleted {
        request_id: String,
        primary_node_id: NodeId,
        result: DfsNamespaceResult,
    },
    StableReconciliationServed {
        request_id: String,
        requester_node_id: NodeId,
        kind: String,
    },
    StableReconciliationCompleted {
        request_id: String,
        authority_node_id: NodeId,
        kind: String,
    },
    /// A routed-operation response arrived for a request this node no longer
    /// tracks (settled, timed out, or retried elsewhere) or did not match the
    /// pending request. The frame is discarded; it must never fail the event
    /// loop.
    RemoteResponseDiscarded {
        request_id: String,
        source_node_id: NodeId,
        reason: String,
    },
    RepairRejected {
        intent_id: String,
        object_id: DfsObjectId,
        replica_node_id: NodeId,
        reason: String,
    },
    RepairRetryFailed {
        reason: String,
    },
    WriteConflictCreated {
        conflict: DfsConflictRecord,
    },
    MetadataJournalBatchApplied {
        request_id: String,
        source_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        applied_entries: usize,
        next_after_sequence: u64,
        verified_prefix_only: bool,
    },
    MetadataJournalBatchRejected {
        request_id: String,
        source_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    MetadataSnapshotInstalled {
        request_id: String,
        source_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
    },
    ContentRootCatalogPageServed {
        requester_node_id: NodeId,
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        entry_count: usize,
        complete: bool,
    },
    ContentRootCatalogPageReceived {
        primary_node_id: NodeId,
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        entry_count: usize,
        complete: bool,
    },
    OwnerHandoffCompleted {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        previous_owner_node_id: NodeId,
        new_owner_node_id: NodeId,
        previous_epoch: u64,
        new_epoch: u64,
    },
}

pub(crate) fn retained_networked_dfs_service_events(
    events: &[NetworkedDfsServiceEvent],
) -> Vec<NetworkedDfsServiceEvent> {
    events
        .iter()
        .map(retained_networked_dfs_service_event)
        .collect()
}

fn retained_networked_dfs_service_event(
    event: &NetworkedDfsServiceEvent,
) -> NetworkedDfsServiceEvent {
    match event {
        NetworkedDfsServiceEvent::RemoteReadServed {
            request_id,
            object_id,
            requester_node_id,
            result,
        } => NetworkedDfsServiceEvent::RemoteReadServed {
            request_id: request_id.clone(),
            object_id: object_id.clone(),
            requester_node_id: requester_node_id.clone(),
            result: retained_dfs_read_result(result),
        },
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id,
            object_id,
            source_node_id,
            result,
        } => NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: request_id.clone(),
            object_id: object_id.clone(),
            source_node_id: source_node_id.clone(),
            result: retained_dfs_read_result(result),
        },
        NetworkedDfsServiceEvent::RemoteBulkReadCompleted {
            request_id,
            source_node_id,
            results,
        } => NetworkedDfsServiceEvent::RemoteBulkReadCompleted {
            request_id: request_id.clone(),
            source_node_id: source_node_id.clone(),
            results: results
                .iter()
                .map(retained_dfs_bulk_read_item_response)
                .collect(),
        },
        _ => event.clone(),
    }
}

fn retained_dfs_bulk_read_item_response(item: &DfsBulkReadItemResponse) -> DfsBulkReadItemResponse {
    DfsBulkReadItemResponse {
        request_id: item.request_id.clone(),
        object_id: item.object_id.clone(),
        result: retained_dfs_read_result(&item.result),
    }
}

fn retained_dfs_read_result(result: &DfsReadResult) -> DfsReadResult {
    match result {
        DfsReadResult::Found(data) => DfsReadResult::Found(DfsReadData {
            object_id: data.object_id.clone(),
            version: data.version.clone(),
            content_hash: data.content_hash.clone(),
            bytes: Vec::new(),
        }),
        DfsReadResult::RangeFound(data) => DfsReadResult::RangeFound(DfsReadRangeData {
            object_id: data.object_id.clone(),
            version: data.version.clone(),
            content_hash: data.content_hash.clone(),
            offset: data.offset,
            total_size_bytes: data.total_size_bytes,
            bytes: Vec::new(),
        }),
        DfsReadResult::NotFound => DfsReadResult::NotFound,
        DfsReadResult::Rejected { reason } => DfsReadResult::Rejected {
            reason: reason.clone(),
        },
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveReplicaStage {
    pub(crate) intent_id: String,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) requester_node_id: NodeId,
}
