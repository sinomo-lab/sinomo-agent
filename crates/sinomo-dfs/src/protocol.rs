use crate::{
    DfsFileSystemId, DfsMetadataJournalBatch, DfsMetadataShardId, DfsMetadataTableSnapshot,
    DfsObjectId, DfsObjectMetadata, DfsReplicationPolicy, DfsVersion, NetworkedDfsWriteOutcome,
};
use serde::{Deserialize, Serialize};
use sinomo_core::{DfsWriteReceipt, DfsWriteRequest};
use sinomo_network::NetworkFrame;
use sinomo_node::NodeId;

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadData {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadRangeData {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub offset: u64,
    pub total_size_bytes: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadRequest {
    pub request_id: String,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_max_bytes: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsReadResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadResponseChunk {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsReadResult {
    Found(DfsReadData),
    RangeFound(DfsReadRangeData),
    NotFound,
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteCommand {
    pub request_id: String,
    pub request: DfsWriteRequest,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsWriteResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteResult {
    Committed {
        receipt: DfsWriteReceipt,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
    },
    NotFound,
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDistributedWriteCommand {
    pub request_id: String,
    pub request: DfsWriteRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    pub desired_replica_nodes: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDistributedWriteResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsDistributedWriteResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsDistributedWriteResult {
    Outcome { outcome: NetworkedDfsWriteOutcome },
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobChunkCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStagePatchCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub base_version: DfsVersion,
    pub version: DfsVersion,
    pub content_hash: String,
    pub offset: u64,
    pub patch_hash: String,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsCommitVersionCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsCommitVersionResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaChunkCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFetchBlobCommand {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub start_chunk_index: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFetchBlobResponse {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub source_node_id: NodeId,
    pub result: DfsFetchBlobResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFetchBlobResponseChunk {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub source_node_id: NodeId,
    pub content_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsFetchBlobResult {
    Found {
        content_hash: String,
        bytes: Vec<u8>,
    },
    NotFound,
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalBatchCommand {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<NodeId>,
    pub batch: DfsMetadataJournalBatch,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalBatchResponse {
    pub request_id: String,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub applied_entries: usize,
    pub result: DfsMetadataJournalBatchResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSnapshotInstallCommand {
    pub request_id: String,
    pub source_node_id: NodeId,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub snapshot: DfsMetadataTableSnapshot,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSnapshotInstallResponse {
    pub request_id: String,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub result: DfsMetadataSnapshotInstallResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DfsProtocolFrameCommand {
    pub request_id: String,
    pub frame: NetworkFrame,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DfsProtocolFrameResponse {
    pub request_id: String,
    pub outbound_frames: Vec<NetworkFrame>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataJournalBatchResult {
    Applied,
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataSnapshotInstallResult {
    Installed { archived_existing: bool },
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsBlobTransferResult {
    Durable { content_hash: String },
    Committed { content_hash: String },
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNetworkMessage {
    ReadRequest(DfsReadRequest),
    ReadResponse(DfsReadResponse),
    ReadResponseChunk(DfsReadResponseChunk),
    WriteRequest(DfsWriteCommand),
    WriteResponse(DfsWriteResponse),
    DistributedWriteRequest(DfsDistributedWriteCommand),
    DistributedWriteResponse(DfsDistributedWriteResponse),
    StageBlob(DfsStageBlobCommand),
    StageBlobChunk(DfsStageBlobChunkCommand),
    StagePatch(DfsStagePatchCommand),
    StageBlobResponse(DfsStageBlobResponse),
    CommitVersion(DfsCommitVersionCommand),
    CommitVersionResponse(DfsCommitVersionResponse),
    RepairReplica(DfsRepairReplicaCommand),
    RepairReplicaChunk(DfsRepairReplicaChunkCommand),
    RepairReplicaResponse(DfsRepairReplicaResponse),
    FetchBlob(DfsFetchBlobCommand),
    FetchBlobResponse(DfsFetchBlobResponse),
    FetchBlobResponseChunk(DfsFetchBlobResponseChunk),
    MetadataJournalBatch(DfsMetadataJournalBatchCommand),
    MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse),
    MetadataSnapshotInstall(DfsMetadataSnapshotInstallCommand),
    MetadataSnapshotInstallResponse(DfsMetadataSnapshotInstallResponse),
}
