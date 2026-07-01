//! Networked DFS service outcome, event, and pending-state types.

use std::time::Instant;

use serde::{Deserialize, Serialize};
use sinomo_node::NodeId;

use crate::{
    DfsBulkReadItemResponse, DfsConflictRecord, DfsConflictStatus, DfsDistributedWriteCommand,
    DfsDistributedWriteResult, DfsError, DfsFileSystemId, DfsMetadataShardConfig,
    DfsMetadataShardId, DfsNamespaceResult, DfsObjectId, DfsObjectMetadata, DfsReadData,
    DfsReadRangeData, DfsReadResult, DfsVersion, DistributedRepairJob, ObjectVersionReader,
};

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
