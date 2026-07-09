//! Networked DFS service state, outcome, event, and pending-state types.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::transfer::{
    InProgressBlobTransfer, InProgressFetchBlobTransfer, InProgressPatchTransfer,
    InProgressReadMemoryTransfer, InProgressReadTransfer, PendingNetworkedRead,
    PendingNetworkedWrite,
};
use crate::{
    DfsBulkReadItemResponse, DfsConflictRecord, DfsConflictStatus, DfsControlPlaneKvClient,
    DfsDistributedWriteCommand, DfsDistributedWriteResult, DfsError, DfsFileSystemId,
    DfsMessageResponse, DfsMetadataBackend, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataStore, DfsNamespaceResult, DfsObjectId, DfsObjectMetadata, DfsReadData,
    DfsReadRangeData, DfsReadResult, DfsRepairAttemptRecord, DfsVersion,
    DistributedEventualCoordinator, DistributedRepairJob, LocalContentStore, ObjectVersionReader,
};

pub(crate) const DFS_BLOB_INLINE_LIMIT_BYTES: usize = 1024 * 1024;
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
// Cross-shard coordination (rename/hardlink sagas) does several primary-to-
// primary roundtrips plus journal publication before the requester's answer;
// over bridge-relayed segments that legitimately exceeds a small budget.
pub const DFS_ROUTED_NAMESPACE_OP_TIMEOUT: Duration = Duration::from_secs(30);
/// Nested coordination depth (serving a namespace request while settling
/// another) beyond which new coordination rejects as Busy instead of growing
/// the stack.
pub(crate) const DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH: usize = 8;
/// Journal batches a primary ships in one routed-operation response.
pub const DFS_ROUTED_RESPONSE_JOURNAL_BATCH_LIMIT: usize = 16;
/// Journal batches a namespace primary ships in one foreground namespace
/// response. Namespace mutations also carry synced path objects and can pull
/// additional journal batches with bounded SyncPath rounds, so keep the first
/// response small enough to return promptly on deep real-cluster journals.
pub const DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT: usize = 1;
pub(crate) const DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT: usize = 3;
pub(crate) const DFS_ROUTED_WRITE_INLINE_BYTES_LIMIT: u64 = DFS_BLOB_INLINE_LIMIT_BYTES as u64;
pub(crate) const DFS_ROUTED_NAMESPACE_IDLE_WAIT: Duration = Duration::from_millis(10);

pub(crate) const DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES: usize = 64;

/// Cooperative cancellation handle for a [`NetworkedDfsService`]. Cloneable and
/// thread-safe: a caller keeps one clone, hands the service the other, and flips
/// it when a budgeted operation gives up (e.g. a degraded cluster link on a
/// mobile device — a normal operating state, not an exceptional one). The
/// service's distributed wait/retry loops observe the flag and return
/// [`DfsError::Cancelled`] instead of running out their full network timeouts.
#[derive(Clone, Debug, Default)]
pub struct DfsCancellationFlag(Arc<AtomicBool>);

impl DfsCancellationFlag {
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation. Idempotent; already-running local work finishes,
    /// distributed waits and retries stop at their next flag check.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Stateful networked DFS data-plane and metadata coordinator.
#[derive(Debug)]
pub struct NetworkedDfsService<T> {
    pub(crate) node_id: NodeId,
    pub(crate) metadata: DfsMetadataStore,
    pub(crate) tombstones_by_metadata_path: BTreeMap<(DfsFileSystemId, String), DfsObjectMetadata>,
    pub(crate) content: LocalContentStore,
    pub(crate) coordinator: DistributedEventualCoordinator,
    pub(crate) control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    pub(crate) cached_sifs_metadata_shards: Option<CachedSifsMetadataShards>,
    pub(crate) metadata_backend: Option<Box<dyn DfsMetadataBackend>>,
    pub(crate) active_replica_stages: BTreeMap<DfsObjectId, ActiveReplicaStage>,
    pub(crate) pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    pub(crate) pending_reads: BTreeMap<String, PendingNetworkedRead>,
    pub(crate) repair_attempt_records: BTreeMap<(String, NodeId), DfsRepairAttemptRecord>,
    pub(crate) transient_remote_read_pruned_locations:
        BTreeMap<(DfsObjectId, DfsVersion, Option<String>), BTreeSet<NodeId>>,
    pub(crate) incoming_read_transfers: BTreeMap<String, InProgressReadTransfer>,
    pub(crate) incoming_read_memory_transfers: BTreeMap<String, InProgressReadMemoryTransfer>,
    pub(crate) incoming_fetch_transfers: BTreeMap<String, InProgressFetchBlobTransfer>,
    pub(crate) incoming_blob_transfers: BTreeMap<String, InProgressBlobTransfer>,
    pub(crate) incoming_patch_transfers: BTreeMap<String, InProgressPatchTransfer>,
    pub(crate) metadata_replication_cursors: BTreeMap<(NodeId, DfsMetadataShardId), u64>,
    pub(crate) recent_namespace_freshness_syncs:
        BTreeMap<(DfsFileSystemId, DfsMetadataShardId, NodeId), Instant>,
    pub(crate) pending_metadata_replications: BTreeMap<String, PendingMetadataReplication>,
    pub(crate) pending_routed_writes: BTreeMap<String, PendingRoutedWrite>,
    pub(crate) pending_remote_write_responses: BTreeMap<String, PendingRemoteWriteResponse>,
    pub(crate) deferred_routed_create_parent_touches: BTreeSet<String>,
    pub(crate) deferred_untracked_routed_write_responses: BTreeMap<String, DfsObjectId>,
    pub(crate) pending_routed_namespace_ops: BTreeMap<String, PendingRoutedNamespaceOp>,
    pub(crate) routed_namespace_settle_depth: usize,
    pub(crate) cross_shard_namespace_locks: BTreeSet<(DfsFileSystemId, String)>,
    pub(crate) last_events: Vec<NetworkedDfsServiceEvent>,
    pub(crate) transport: T,
    pub(crate) repair_retry_tick: u64,
    pub(crate) next_frame_id: u64,
    pub(crate) next_message_id: u64,
    pub(crate) next_request_id: u64,
    pub(crate) cancellation: Option<DfsCancellationFlag>,
}

/// Result from serving one DFS message command through a networked service.
#[derive(Clone, Debug)]
pub struct DfsMessageProcessResult {
    pub response: DfsMessageResponse,
    pub events: Vec<NetworkedDfsServiceEvent>,
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
    pub(crate) complete: bool,
    pub(crate) next_chunk_index: u32,
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
pub(crate) struct PendingMetadataReplication {
    pub(crate) target_node_id: NodeId,
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) next_after_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingRoutedWrite {
    pub(crate) object_id: DfsObjectId,
    pub(crate) primary_node_id: NodeId,
    pub(crate) command: DfsDistributedWriteCommand,
    pub(crate) authority_redirects_remaining: usize,
    pub(crate) remote_intent_id: Option<String>,
    pub(crate) durable_receipts: usize,
    pub(crate) required_receipts: usize,
    pub(crate) result: Option<Result<NetworkedDfsWriteOutcome, DfsError>>,
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
    pub(crate) synced_path_object: Option<DfsObjectMetadata>,
    pub(crate) synced_path_objects: Vec<DfsObjectMetadata>,
    pub(crate) journal_batch_limit_reached: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRoutedNamespaceOp {
    pub(crate) primary_node_id: NodeId,
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
    RemoteRangeReadChunkReceived {
        request_id: String,
        object_id: DfsObjectId,
        source_node_id: NodeId,
        version: DfsVersion,
        content_hash: Option<String>,
        range_offset: u64,
        range_size_bytes: u64,
        object_total_size_bytes: u64,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
        complete: bool,
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
        NetworkedDfsServiceEvent::RemoteRangeReadChunkReceived {
            request_id,
            object_id,
            source_node_id,
            version,
            content_hash,
            range_offset,
            range_size_bytes,
            object_total_size_bytes,
            chunk_index,
            total_chunks,
            complete,
            ..
        } => NetworkedDfsServiceEvent::RemoteRangeReadChunkReceived {
            request_id: request_id.clone(),
            object_id: object_id.clone(),
            source_node_id: source_node_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            range_offset: *range_offset,
            range_size_bytes: *range_size_bytes,
            object_total_size_bytes: *object_total_size_bytes,
            chunk_index: *chunk_index,
            total_chunks: *total_chunks,
            bytes: Vec::new(),
            complete: *complete,
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

#[derive(Clone, Debug)]
pub(crate) struct CachedSifsMetadataShards {
    pub(crate) recorded_at: Instant,
    pub(crate) shards: Vec<DfsMetadataShardConfig>,
}
