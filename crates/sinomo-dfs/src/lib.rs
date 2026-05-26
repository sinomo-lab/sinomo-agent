//! Local-first distributed filesystem metadata skeleton.

pub mod content_store;
pub mod control_plane;
pub mod discovery;
pub mod distributed;
pub mod local_owned;
pub mod metadata_backend;
pub mod namespace;
pub mod operator;
pub mod posix;
pub mod protocol;
mod transfer;

pub use content_store::{ContentManifest, ContentStoreError, LocalContentStore};
pub use control_plane::{
    DfsControlPlaneKvClient, DfsControlPlaneStore, DfsFileSystemConfig, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, SharedDfsControlPlaneKvClient, file_system_config_key,
    metadata_shard_key, metadata_shards_prefix,
};
pub use discovery::{
    DfsClusterView, DfsNodeAdvertisement, content_service_key, content_services_prefix,
    metadata_service_key, metadata_services_prefix,
};
pub use distributed::{
    DfsDistributedError, DfsReplicaCandidate, DfsReplicaPlacementPlan, DfsReplicaPlacementRequest,
    DfsReplicaReceiptState, DfsReplicationPolicy, DfsWriteIntentState,
    DistributedEventualCoordinator, DistributedRepairJob, DistributedReplicaReceipt,
    DistributedWriteIntent, plan_replicas, validate_replication_plan,
};
pub use local_owned::{
    DfsExposeAddRequest, DfsExposeConfig, DfsExposeEntry, DfsExposeFileInfo, DfsExposeStore,
    DfsExposeStoreError, DfsExposedDirectory, LocalOwnedDfsError,
};
pub use metadata_backend::{
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, DfsConflictRecord, DfsConflictStatus,
    DfsDirFragmentId, DfsDirFragmentRecord, DfsInodeId, DfsInodeRecord, DfsMetadataBackend,
    DfsMetadataJournalBatch, DfsMetadataJournalEntry, DfsMetadataMutation, DfsMetadataShardId,
    DfsMetadataSubscription, DfsNamespaceTransaction, DfsObjectVersionRecord, DfsPathRecord,
    DfsReplicaReceiptRecord, DfsWriteIntentRecord, DfsWriteIntentStatus, LocalJsonMetadataBackend,
    MetadataBackendError,
};
use namespace::metadata_path;
pub use namespace::{
    SIFS_NODES_PATH, SIFS_ROOT_PATH, SIFS_SERVICES_PATH, SIFS_SINOMO_PATH, SIFS_USERS_PATH,
    directory_object, distributed_sifs, distributed_sifs_bootstrap_objects, sifs_node_files_path,
    sifs_node_path,
};
pub use operator::{
    DfsAgentFileSystemSummary, DfsAgentObjectState, DfsAgentObjectSummary,
    DfsOperatorFileSystemView, DfsOperatorInspector, DfsOperatorObjectView, DfsOperatorRepairView,
    DfsOperatorStatus, DfsPinAddRequest, DfsPinMode, DfsPinRecord, DfsPinStoreError,
    LocalDfsPinStore,
};
pub use posix::{DfsDirEntry, DfsPosixError, DfsStat, DfsWriteOptions};
pub use protocol::{
    DfsBlobTransferResult, DfsCommitVersionCommand, DfsCommitVersionResponse, DfsFetchBlobCommand,
    DfsFetchBlobResponse, DfsFetchBlobResponseChunk, DfsFetchBlobResult,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
    DfsNetworkMessage, DfsProtocolFrameCommand, DfsProtocolFrameResponse, DfsReadData,
    DfsReadRequest, DfsReadResponse, DfsReadResponseChunk, DfsReadResult,
    DfsRepairReplicaChunkCommand, DfsRepairReplicaCommand, DfsRepairReplicaResponse,
    DfsStageBlobChunkCommand, DfsStageBlobCommand, DfsStageBlobResponse, DfsWriteCommand,
    DfsWriteResponse, DfsWriteResult,
};

pub type DfsFileSystemId = sinomo_core::Id;
pub type DfsObjectId = sinomo_core::Id;
pub type DfsVersion = sinomo_core::Id;

use serde::{Deserialize, Serialize};
use sinomo_core::{
    Confidence, DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest, EventEnvelope, EventKind,
    LatencyClass, PrivacyClass, RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_events::EventId;
use sinomo_kv::KvError;
use sinomo_local::{LocalDataError, SinomoDataDir};
use sinomo_network::{
    MeshMessage, MeshMessageBody, MeshMessageId, NetworkError, NetworkFrame, NetworkFrameId,
    NetworkTransport, ProtocolMessage,
};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use thiserror::Error;
use time::OffsetDateTime;
#[cfg(test)]
use transfer::RepairRetryState;
use transfer::{
    InProgressBlobTransfer, InProgressFetchBlobTransfer, InProgressReadTransfer,
    PendingNetworkedRead, PendingNetworkedWrite, blob_chunk_count, blob_transfer_key,
    validate_chunk_bytes,
};

pub const DFS_PROTOCOL: &str = "sinomo.dfs.v1";
pub const DEFAULT_DFS_NODE_ID: &str = "node_local";
pub const SIFS_FILE_SYSTEM_ID: &str = "sifs";
const DFS_BLOB_INLINE_LIMIT_BYTES: usize = 64 * 1024;
const DFS_BLOB_CHUNK_BYTES: usize = 64 * 1024;
const DFS_REMOTE_READ_RETRY_DELAY: Duration = Duration::from_secs(1);
const DFS_CHUNK_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::StorageRing;

    for (relative_path, reason) in [
        ("dfs/content/blobs", "content-addressed DFS blob bytes"),
        (
            "dfs/content/objects",
            "dynamic DFS object manifests stored as sinomo-object files",
        ),
        ("dfs/content/staging", "temporary DFS staging bytes"),
        (
            "dfs/metadata/records",
            "dynamic DFS metadata records stored as sinomo-object files",
        ),
        (
            "dfs/metadata/journal",
            "dynamic DFS metadata journal entries stored as sinomo-object files",
        ),
        (
            "dfs/metadata/root",
            "default DFS root metadata shard with object records and journal entries",
        ),
        (
            "dfs/metadata-replication-cursors",
            "durable per-replica DFS metadata journal replication cursors",
        ),
        (
            "dfs/operator",
            "dynamic DFS operator intent records stored as sinomo-object files",
        ),
        (
            "dfs/pins",
            "dynamic DFS pin records stored as sinomo-object files",
        ),
        ("files", "local-owned user files exposed into DFS"),
    ] {
        layout.register_escape_path("sinomo-dfs", StorageRing::R1, relative_path, reason)?;
    }
    Ok(())
}

pub fn fallback_dfs_node_id() -> NodeId {
    NodeId::from_legacy_named(DEFAULT_DFS_NODE_ID).expect("valid fallback DFS node id")
}

pub fn sifs_file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).expect("valid sifs filesystem id")
}

pub fn default_dfs_metadata_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("metadata").join("root")
}

pub fn resolve_dfs_metadata_root(explicit: Option<PathBuf>) -> Result<PathBuf, LocalDataError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(default_dfs_metadata_root(&data_dir))
}

pub fn default_dfs_pin_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("pins")
}

pub fn resolve_dfs_pin_root(explicit: Option<PathBuf>) -> Result<PathBuf, LocalDataError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(default_dfs_pin_root(&data_dir))
}

pub fn default_dfs_expose_config_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_expose_config_file()
}

pub fn dfs_expose_config_path(explicit: Option<PathBuf>, data_dir: &SinomoDataDir) -> PathBuf {
    explicit.unwrap_or_else(|| default_dfs_expose_config_path(data_dir))
}

pub fn ensure_default_dfs_expose_config(
    config_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
    node_id: NodeId,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf())
        .ensure_default(data_dir.as_ref(), node_id)
}

pub fn load_dfs_expose_config(
    config_path: impl AsRef<Path>,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).load()
}

pub fn add_dfs_expose_entry(
    config_path: impl AsRef<Path>,
    request: DfsExposeAddRequest,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).add_entry(request)
}

pub fn remove_dfs_expose_entry(
    config_path: impl AsRef<Path>,
    id: &str,
) -> Result<(DfsExposeConfig, bool), DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).remove_entry(id)
}

pub fn list_local_dfs_pins(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
) -> Result<Vec<DfsPinRecord>, DfsPinStoreError> {
    LocalDfsPinStore::new(root).list_pins(file_system_id)
}

pub fn add_local_dfs_pin(
    root: impl Into<PathBuf>,
    request: DfsPinAddRequest,
) -> Result<DfsPinRecord, DfsPinStoreError> {
    LocalDfsPinStore::new(root).add_pin(request)
}

pub fn remove_local_dfs_pin(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
    path: &str,
    node_id: Option<&NodeId>,
) -> Result<bool, DfsPinStoreError> {
    LocalDfsPinStore::new(root).remove_pin(file_system_id, path, node_id)
}

pub fn open_root_local_metadata_backend(
    root: impl Into<PathBuf>,
) -> Result<LocalJsonMetadataBackend, MetadataBackendError> {
    LocalJsonMetadataBackend::new(DfsMetadataShardId::new("root"), root.into())
}

pub fn inspect_dfs_status(
    root: impl Into<PathBuf>,
) -> Result<DfsOperatorStatus, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).status()
}

pub fn inspect_dfs_file_system(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
) -> Result<Option<DfsOperatorFileSystemView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).file_system(file_system_id)
}

pub fn inspect_dfs_object(
    root: impl Into<PathBuf>,
    object_id: &DfsObjectId,
) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).object(object_id)
}

pub fn inspect_dfs_path(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).object_by_path(file_system_id, path)
}

pub fn inspect_dfs_conflicts(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
    object_id: Option<&DfsObjectId>,
) -> Result<Vec<DfsConflictRecord>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).conflicts(file_system_id, object_id)
}

pub fn inspect_dfs_repairs(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
    object_id: Option<&DfsObjectId>,
) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).repairs(file_system_id, object_id)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileSystem {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsSubtreePolicy {
    pub path_prefix: String,
    pub policy: DfsObjectPolicy,
}

impl DfsFileSystem {
    pub fn sifs(owner_node_id: NodeId) -> Self {
        Self {
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id,
            subtree_policies: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectMetadata {
    pub object_id: DfsObjectId,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub kind: DfsObjectKind,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_target_object_id: Option<DfsObjectId>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub mode: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub uid: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub gid: u32,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub accessed_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub modified_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub created_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub changed_at_unix_nanos: u64,
    pub version: DfsVersion,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectKind {
    File,
    Directory,
    Symlink,
    Hardlink,
    Artifact,
    Mount,
    Tombstone,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteIntent {
    pub request: DfsWriteRequest,
    pub target_path: String,
    pub status: WriteIntentStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WriteIntentStatus {
    PendingOwner,
    Committed,
    Rejected,
    Conflict,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPolicyViolation {
    pub requesting_node_id: NodeId,
    pub executing_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<DfsObjectId>,
    pub reason: String,
}

impl DfsPolicyViolation {
    pub fn event(&self) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.executing_node_id.clone(),
            Source::new("dfs.policy").expect("valid source"),
            EventKind::new("dfs.policy.violation").expect("valid event kind"),
            Salience::new(0.9).expect("valid salience"),
            Confidence::new(0.96).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(self.executing_node_id.clone())
        .with_payload(serde_json::json!({
            "requesting_node_id": self.requesting_node_id,
            "executing_node_id": self.executing_node_id,
            "file_system_id": self.file_system_id,
            "object_id": self.object_id,
            "reason": self.reason,
        }))
    }
}

#[derive(Clone, Debug, Default)]
pub struct DfsMetadataStore {
    filesystems: BTreeMap<DfsFileSystemId, DfsFileSystem>,
    objects: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    conflicts: BTreeMap<DfsObjectId, BTreeMap<String, DfsConflictRecord>>,
}

impl DfsMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) {
        self.filesystems
            .insert(file_system.file_system_id.clone(), file_system);
    }

    pub fn add_object(&mut self, mut object: DfsObjectMetadata) -> Result<(), DfsError> {
        if !self.filesystems.contains_key(&object.file_system_id) {
            return Err(DfsError::UnknownFileSystem {
                file_system_id: object.file_system_id,
            });
        }
        normalize_current_locations(&mut object.current_locations);
        self.objects.insert(object.object_id.clone(), object);
        Ok(())
    }

    pub fn object(&self, object_id: &DfsObjectId) -> Option<&DfsObjectMetadata> {
        self.objects.get(object_id)
    }

    pub fn add_conflict(&mut self, conflict: DfsConflictRecord) {
        self.conflicts
            .entry(conflict.object_id.clone())
            .or_default()
            .insert(conflict.conflict_id.clone(), conflict);
    }

    pub fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Vec<DfsConflictRecord> {
        self.conflicts
            .get(object_id)
            .map(|conflicts| conflicts.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn open_conflicts_for_object(&self, object_id: &DfsObjectId) -> Vec<DfsConflictRecord> {
        self.conflicts_for_object(object_id)
            .into_iter()
            .filter(|conflict| conflict.status == DfsConflictStatus::Open)
            .collect()
    }

    pub fn route_read(
        &self,
        object_id: &DfsObjectId,
        requester_node_id: &NodeId,
    ) -> Result<ReadRoute, DfsError> {
        let object = self
            .objects
            .get(object_id)
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;

        let action = match &object.policy {
            DfsObjectPolicy::LocalOwned => {
                if &object.owner_node_id == requester_node_id {
                    ReadRouteAction::ReadLocally
                } else {
                    ReadRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            DfsObjectPolicy::ReplicatedConfirmed | DfsObjectPolicy::ReplicatedEventual => {
                if object.current_locations.contains(requester_node_id) {
                    ReadRouteAction::ReadLocally
                } else {
                    ReadRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            _ => {
                return Err(DfsError::UnsupportedPolicy {
                    policy: object.policy.clone(),
                });
            }
        };

        Ok(ReadRoute {
            object_id: object.object_id.clone(),
            action,
        })
    }

    pub fn route_write(
        &self,
        request: DfsWriteRequest,
        requester_node_id: &NodeId,
    ) -> Result<WriteRoute, DfsError> {
        let object =
            self.objects
                .get(&request.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: request.object_id.clone(),
                })?;

        let action = match &object.policy {
            DfsObjectPolicy::LocalOwned => {
                if &object.owner_node_id == requester_node_id {
                    WriteRouteAction::CommitLocally
                } else {
                    WriteRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            DfsObjectPolicy::ReplicatedConfirmed => WriteRouteAction::RequireReplicaReceipts,
            DfsObjectPolicy::ReplicatedEventual => WriteRouteAction::CommitThenReplicate,
            DfsObjectPolicy::AppendOnly => WriteRouteAction::AppendNewVersion,
            DfsObjectPolicy::EphemeralLocal => WriteRouteAction::CommitLocally,
        };

        Ok(WriteRoute {
            request,
            object_id: object.object_id.clone(),
            action,
        })
    }

    pub fn commit_write(
        &mut self,
        receipt: DfsWriteReceipt,
        content_hash: Option<String>,
    ) -> Result<(), DfsError> {
        let object =
            self.objects
                .get_mut(&receipt.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: receipt.object_id.clone(),
                })?;
        object.version = receipt.committed_version;
        object.last_modified_by_event_id = Some(receipt.event_id);
        object.content_hash = content_hash;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReadRoute {
    pub object_id: DfsObjectId,
    pub action: ReadRouteAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ReadRouteAction {
    ReadLocally,
    RouteToOwner { owner_node_id: NodeId },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteRoute {
    pub request: DfsWriteRequest,
    pub object_id: DfsObjectId,
    pub action: WriteRouteAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WriteRouteAction {
    CommitLocally,
    RouteToOwner { owner_node_id: NodeId },
    RequireReplicaReceipts,
    CommitThenReplicate,
    AppendNewVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsReadOutcome {
    Local {
        object_id: DfsObjectId,
        data: Option<DfsReadData>,
    },
    Routed {
        request_id: String,
        owner_node_id: NodeId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsWriteOutcome {
    Local {
        receipt: DfsWriteReceipt,
    },
    Routed {
        request_id: String,
        owner_node_id: NodeId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsServiceEvent {
    ReadRequestHandled {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsReadResult,
    },
    ReadResponseReceived {
        request_id: String,
        object_id: DfsObjectId,
        result: DfsReadResult,
    },
    WriteRequestHandled {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsWriteResult,
    },
    WriteResponseReceived {
        request_id: String,
        object_id: DfsObjectId,
        result: DfsWriteResult,
    },
}

#[derive(Clone, Debug)]
pub struct LocalOwnedDfsService<T> {
    node_id: NodeId,
    metadata: DfsMetadataStore,
    content: BTreeMap<DfsObjectId, Vec<u8>>,
    pending_reads: BTreeMap<String, DfsObjectId>,
    pending_writes: BTreeMap<String, DfsObjectId>,
    transport: T,
    next_frame_id: u64,
    next_message_id: u64,
    next_request_id: u64,
}

impl<T: NetworkTransport> LocalOwnedDfsService<T> {
    pub fn new(transport: T) -> Self {
        let node_id = transport.local_node_id().clone();
        let mut metadata = DfsMetadataStore::new();
        metadata.add_file_system(DfsFileSystem::sifs(node_id.clone()));
        Self {
            node_id,
            metadata,
            content: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            transport,
            next_frame_id: 1,
            next_message_id: 1,
            next_request_id: 1,
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn metadata(&self) -> &DfsMetadataStore {
        &self.metadata
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) {
        self.metadata.add_file_system(file_system);
    }

    pub fn add_object_metadata(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        self.metadata.add_object(object)
    }

    pub fn put_local_object(
        &mut self,
        mut object: DfsObjectMetadata,
        bytes: Vec<u8>,
    ) -> Result<(), DfsError> {
        if object.owner_node_id != self.node_id {
            return Err(DfsError::NotObjectOwner {
                object_id: object.object_id,
                owner_node_id: object.owner_node_id,
                local_node_id: self.node_id.clone(),
            });
        }

        object.content_hash = Some(content_hash(&bytes));
        object.content_size_bytes = Some(bytes.len() as u64);
        object.current_locations = vec![self.node_id.clone()];
        let object_id = object.object_id.clone();
        self.metadata.add_object(object)?;
        self.content.insert(object_id, bytes);
        Ok(())
    }

    pub fn read_object(&mut self, object_id: &DfsObjectId) -> Result<DfsReadOutcome, DfsError> {
        let route = self.metadata.route_read(object_id, &self.node_id)?;
        match route.action {
            ReadRouteAction::ReadLocally => Ok(DfsReadOutcome::Local {
                object_id: object_id.clone(),
                data: self.local_read(object_id)?,
            }),
            ReadRouteAction::RouteToOwner { owner_node_id } => {
                let request_id = self.next_request_id();
                self.pending_reads
                    .insert(request_id.clone(), object_id.clone());
                self.send_dfs_message(
                    &owner_node_id,
                    DfsNetworkMessage::ReadRequest(DfsReadRequest {
                        request_id: request_id.clone(),
                        object_id: object_id.clone(),
                    }),
                )?;
                Ok(DfsReadOutcome::Routed {
                    request_id,
                    owner_node_id,
                })
            }
        }
    }

    pub fn write_object(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
    ) -> Result<DfsWriteOutcome, DfsError> {
        let route = self.metadata.route_write(request.clone(), &self.node_id)?;
        match route.action {
            WriteRouteAction::CommitLocally => {
                let receipt = self.commit_local_write(request, bytes)?;
                Ok(DfsWriteOutcome::Local { receipt })
            }
            WriteRouteAction::RouteToOwner { owner_node_id } => {
                let request_id = self.next_request_id();
                self.pending_writes
                    .insert(request_id.clone(), request.object_id.clone());
                self.send_dfs_message(
                    &owner_node_id,
                    DfsNetworkMessage::WriteRequest(DfsWriteCommand {
                        request_id: request_id.clone(),
                        request,
                        bytes,
                    }),
                )?;
                Ok(DfsWriteOutcome::Routed {
                    request_id,
                    owner_node_id,
                })
            }
            _ => Err(DfsError::UnsupportedPolicy {
                policy: self
                    .metadata
                    .object(&route.object_id)
                    .map(|object| object.policy.clone())
                    .unwrap_or(DfsObjectPolicy::LocalOwned),
            }),
        }
    }

    pub fn process_incoming(&mut self) -> Result<Vec<DfsServiceEvent>, DfsError> {
        let inbound_frames = self
            .transport
            .drain_incoming()
            .map_err(DfsError::from_network)?;
        let mut events = Vec::new();

        for inbound in inbound_frames {
            let source_node_id = inbound.frame.source_node_id.clone();
            let MeshMessageBody::Protocol(protocol_message) = inbound.frame.message.body else {
                continue;
            };

            if protocol_message.protocol != DFS_PROTOCOL {
                continue;
            }

            let Ok(message) = protocol_message.decode::<DfsNetworkMessage>() else {
                continue;
            };

            match message {
                DfsNetworkMessage::ReadRequest(request) => {
                    let event = self.handle_read_request(source_node_id, request)?;
                    events.push(event);
                }
                DfsNetworkMessage::ReadResponse(response) => {
                    self.pending_reads.remove(&response.request_id);
                    events.push(DfsServiceEvent::ReadResponseReceived {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        result: response.result,
                    });
                }
                DfsNetworkMessage::WriteRequest(command) => {
                    let event = self.handle_write_request(source_node_id, command)?;
                    events.push(event);
                }
                DfsNetworkMessage::WriteResponse(response) => {
                    self.pending_writes.remove(&response.request_id);
                    if let DfsWriteResult::Committed {
                        receipt,
                        content_hash,
                    } = &response.result
                    {
                        self.metadata
                            .commit_write(receipt.clone(), content_hash.clone())?;
                    }
                    events.push(DfsServiceEvent::WriteResponseReceived {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        result: response.result,
                    });
                }
                DfsNetworkMessage::StageBlob(_)
                | DfsNetworkMessage::StageBlobChunk(_)
                | DfsNetworkMessage::StageBlobResponse(_)
                | DfsNetworkMessage::CommitVersion(_)
                | DfsNetworkMessage::CommitVersionResponse(_)
                | DfsNetworkMessage::RepairReplica(_)
                | DfsNetworkMessage::RepairReplicaChunk(_)
                | DfsNetworkMessage::RepairReplicaResponse(_)
                | DfsNetworkMessage::FetchBlob(_)
                | DfsNetworkMessage::FetchBlobResponse(_)
                | DfsNetworkMessage::FetchBlobResponseChunk(_)
                | DfsNetworkMessage::MetadataJournalBatch(_)
                | DfsNetworkMessage::MetadataJournalBatchResponse(_)
                | DfsNetworkMessage::ReadResponseChunk(_) => {}
            }
        }

        Ok(events)
    }

    fn handle_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsReadRequest,
    ) -> Result<DfsServiceEvent, DfsError> {
        let result = match self.metadata.object(&request.object_id) {
            Some(object) if object.owner_node_id != self.node_id => DfsReadResult::Rejected {
                reason: format!(
                    "node {} does not own object {}",
                    self.node_id, request.object_id
                ),
            },
            Some(_) => self
                .local_read(&request.object_id)?
                .map(DfsReadResult::Found)
                .unwrap_or(DfsReadResult::NotFound),
            None => DfsReadResult::NotFound,
        };

        self.send_dfs_message(
            &requester_node_id,
            DfsNetworkMessage::ReadResponse(DfsReadResponse {
                request_id: request.request_id.clone(),
                object_id: request.object_id.clone(),
                result: result.clone(),
            }),
        )?;

        Ok(DfsServiceEvent::ReadRequestHandled {
            request_id: request.request_id,
            object_id: request.object_id,
            requester_node_id,
            result,
        })
    }

    fn handle_write_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsWriteCommand,
    ) -> Result<DfsServiceEvent, DfsError> {
        let object_id = command.request.object_id.clone();
        let result = match self.commit_local_write(command.request, command.bytes) {
            Ok(receipt) => DfsWriteResult::Committed {
                content_hash: self
                    .metadata
                    .object(&object_id)
                    .and_then(|object| object.content_hash.clone()),
                receipt,
            },
            Err(DfsError::UnknownObject { .. }) => DfsWriteResult::NotFound,
            Err(error) => DfsWriteResult::Rejected {
                reason: error.to_string(),
            },
        };

        self.send_dfs_message(
            &requester_node_id,
            DfsNetworkMessage::WriteResponse(DfsWriteResponse {
                request_id: command.request_id.clone(),
                object_id: object_id.clone(),
                result: result.clone(),
            }),
        )?;

        Ok(DfsServiceEvent::WriteRequestHandled {
            request_id: command.request_id,
            object_id,
            requester_node_id,
            result,
        })
    }

    fn local_read(&self, object_id: &DfsObjectId) -> Result<Option<DfsReadData>, DfsError> {
        let object = self
            .metadata
            .object(object_id)
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;

        Ok(self.content.get(object_id).map(|bytes| DfsReadData {
            object_id: object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            bytes: bytes.clone(),
        }))
    }

    fn commit_local_write(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
    ) -> Result<DfsWriteReceipt, DfsError> {
        let object =
            self.metadata
                .object(&request.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: request.object_id.clone(),
                })?;

        if object.policy != DfsObjectPolicy::LocalOwned {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }

        if object.owner_node_id != self.node_id {
            return Err(DfsError::NotObjectOwner {
                object_id: object.object_id.clone(),
                owner_node_id: object.owner_node_id.clone(),
                local_node_id: self.node_id.clone(),
            });
        }

        let content_hash = Some(content_hash(&bytes));
        let receipt = DfsWriteReceipt {
            file_system_id: request.file_system_id,
            object_id: request.object_id.clone(),
            committed_version: request.target_version,
            owner_node_id: self.node_id.clone(),
            event_id: EventId::generate(),
        };
        self.content.insert(request.object_id, bytes);
        self.metadata
            .commit_write(receipt.clone(), content_hash.clone())?;
        Ok(receipt)
    }

    fn send_dfs_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<(), DfsError> {
        let protocol_message =
            ProtocolMessage::encode(DFS_PROTOCOL, &message).map_err(DfsError::from_network)?;
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: self.node_id.clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: self.node_id.clone(),
                target_node_id: target_node_id.clone(),
                body: MeshMessageBody::Protocol(protocol_message),
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;

        self.transport
            .send(target_node_id, frame)
            .map_err(DfsError::from_network)
    }

    fn next_request_id(&mut self) -> String {
        let request_id = format!("dfs_req_{}", self.next_request_id);
        self.next_request_id += 1;
        request_id
    }
}

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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsConfirmedTimeoutAction {
    KeepPending,
    Abort,
    DowngradeToEventual { min_commit_replicas: usize },
}

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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReconnectReconciliation {
    pub object_id: DfsObjectId,
    pub refreshed_receipts: usize,
    pub conflicts: Vec<DfsConflictRecord>,
    pub missing_current: bool,
}

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
struct PendingMetadataReplication {
    target_node_id: NodeId,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
}

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
    RemoteReadCompleted {
        request_id: String,
        object_id: DfsObjectId,
        source_node_id: NodeId,
        result: DfsReadResult,
    },
    RepairRejected {
        intent_id: String,
        object_id: DfsObjectId,
        replica_node_id: NodeId,
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
    OwnerHandoffCompleted {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        previous_owner_node_id: NodeId,
        new_owner_node_id: NodeId,
        previous_epoch: u64,
        new_epoch: u64,
    },
}

#[derive(Debug)]
pub struct NetworkedDfsService<T> {
    node_id: NodeId,
    metadata: DfsMetadataStore,
    tombstones_by_metadata_path: BTreeMap<(DfsFileSystemId, String), DfsObjectMetadata>,
    content: LocalContentStore,
    coordinator: DistributedEventualCoordinator,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    metadata_backend: Option<Box<dyn DfsMetadataBackend>>,
    pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    pending_reads: BTreeMap<String, PendingNetworkedRead>,
    incoming_read_transfers: BTreeMap<String, InProgressReadTransfer>,
    incoming_fetch_transfers: BTreeMap<String, InProgressFetchBlobTransfer>,
    incoming_blob_transfers: BTreeMap<String, InProgressBlobTransfer>,
    metadata_replication_cursors: BTreeMap<(NodeId, DfsMetadataShardId), u64>,
    pending_metadata_replications: BTreeMap<String, PendingMetadataReplication>,
    last_events: Vec<NetworkedDfsServiceEvent>,
    transport: T,
    repair_retry_tick: u64,
    next_frame_id: u64,
    next_message_id: u64,
    next_request_id: u64,
}

impl<T: NetworkTransport> NetworkedDfsService<T> {
    pub fn new(transport: T, content: LocalContentStore) -> Self {
        let node_id = transport.local_node_id().clone();
        let mut metadata = DfsMetadataStore::new();
        metadata.add_file_system(DfsFileSystem::sifs(node_id.clone()));
        Self {
            node_id,
            metadata,
            tombstones_by_metadata_path: BTreeMap::new(),
            content,
            coordinator: DistributedEventualCoordinator::new(),
            control_plane: None,
            metadata_backend: None,
            pending_writes: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            incoming_read_transfers: BTreeMap::new(),
            incoming_fetch_transfers: BTreeMap::new(),
            incoming_blob_transfers: BTreeMap::new(),
            metadata_replication_cursors: BTreeMap::new(),
            pending_metadata_replications: BTreeMap::new(),
            last_events: Vec::new(),
            transport,
            repair_retry_tick: 0,
            next_frame_id: 1,
            next_message_id: 1,
            next_request_id: 1,
        }
    }

    pub fn with_control_plane(mut self, kv: impl DfsControlPlaneKvClient + Send + 'static) -> Self {
        self.control_plane = Some(Box::new(kv));
        self
    }

    pub fn with_metadata_backend(
        mut self,
        backend: impl DfsMetadataBackend + Send + 'static,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(Box::new(backend));
        self.hydrate_from_metadata_backend()?;
        Ok(self)
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn metadata(&self) -> &DfsMetadataStore {
        &self.metadata
    }

    pub fn content(&self) -> &LocalContentStore {
        &self.content
    }

    pub fn last_events(&self) -> &[NetworkedDfsServiceEvent] {
        &self.last_events
    }

    pub fn into_transport(self) -> T {
        self.transport
    }

    pub fn coordinator(&self) -> &DistributedEventualCoordinator {
        &self.coordinator
    }

    pub fn metadata_backend(&self) -> Option<&dyn DfsMetadataBackend> {
        self.metadata_backend.as_deref()
    }

    pub fn send_metadata_journal_since(
        &mut self,
        target_node_id: &NodeId,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(batch) = self.metadata_journal_batch_since(after_sequence)? else {
            return Ok(None);
        };
        let request_id = self.next_request_id();
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                request_id,
                batch: batch.clone(),
            }),
        )?;
        Ok(sent.then_some(batch))
    }

    pub fn replicate_metadata_shard_to_configured_replicas(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Vec<DfsMetadataReplicationOutcome>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(Vec::new());
        };
        let Some(shard) = DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(file_system_id, shard_id)?
        else {
            return Ok(Vec::new());
        };
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id,
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }

        let shard_id = DfsMetadataShardId::new(shard.shard_id);
        let local_node_id = self.node_id.clone();
        let mut outcomes = Vec::new();
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != &local_node_id)
        {
            let cursor_key = (target_node_id.clone(), shard_id.clone());
            let after_sequence =
                if let Some(after_sequence) = self.metadata_replication_cursors.get(&cursor_key) {
                    *after_sequence
                } else if let Some(backend) = self.metadata_backend.as_ref() {
                    let after_sequence =
                        backend.metadata_replication_cursor(&target_node_id, &shard_id)?;
                    self.metadata_replication_cursors
                        .insert(cursor_key.clone(), after_sequence);
                    after_sequence
                } else {
                    0
                };
            let Some(batch) = self.metadata_journal_batch_since(after_sequence)? else {
                outcomes.push(DfsMetadataReplicationOutcome::NoChanges {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
                continue;
            };
            let pending_next_after_sequence = self
                .pending_metadata_replications
                .values()
                .filter(|pending| {
                    pending.target_node_id == target_node_id && pending.shard_id == shard_id
                })
                .map(|pending| pending.next_after_sequence)
                .max();
            if let Some(pending_next_after_sequence) = pending_next_after_sequence
                && pending_next_after_sequence >= batch.next_after_sequence
            {
                outcomes.push(DfsMetadataReplicationOutcome::InFlight {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    next_after_sequence: pending_next_after_sequence,
                });
                continue;
            }
            let entries = batch.entries.len();
            let next_after_sequence = batch.next_after_sequence;
            let request_id = self.next_request_id();
            self.pending_metadata_replications.insert(
                request_id.clone(),
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                },
            );
            let sent = self.send_dfs_message_if_routable(
                &target_node_id,
                DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                    request_id: request_id.clone(),
                    batch,
                }),
            )?;
            if sent {
                outcomes.push(DfsMetadataReplicationOutcome::Sent {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    entries,
                    next_after_sequence,
                });
            } else {
                self.pending_metadata_replications.remove(&request_id);
                outcomes.push(DfsMetadataReplicationOutcome::Unroutable {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
            }
        }
        Ok(outcomes)
    }

    pub fn replicate_configured_metadata_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataReplicationOutcome>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(Vec::new());
        };
        let shard_ids = DfsControlPlaneStore::new(control_plane.as_mut())
            .list_metadata_shards(file_system_id)?
            .into_iter()
            .filter(|shard| shard.primary_node_id == self.node_id)
            .map(|shard| shard.shard_id)
            .collect::<Vec<_>>();

        let mut outcomes = Vec::new();
        for shard_id in shard_ids {
            outcomes.extend(
                self.replicate_metadata_shard_to_configured_replicas(file_system_id, &shard_id)?,
            );
        }
        Ok(outcomes)
    }

    pub fn hydrate_from_metadata_backend(&mut self) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };

        let file_systems = backend.list_file_systems()?;
        let mut objects = Vec::new();
        let mut conflicts = Vec::new();
        let mut intents = Vec::new();
        let mut receipts_by_intent = BTreeMap::<String, Vec<DfsReplicaReceiptRecord>>::new();

        for file_system in &file_systems {
            for object in backend.objects_for_file_system(&file_system.file_system_id)? {
                conflicts.extend(backend.conflicts_for_object(&object.object_id)?);
                for intent in backend.intents_for_object(&object.object_id)? {
                    let receipts =
                        backend.receipts_for_version(&object.object_id, &intent.target_version)?;
                    receipts_by_intent.insert(intent.intent_id.clone(), receipts);
                    intents.push(intent);
                }
                objects.push(object);
            }
        }

        self.metadata = DfsMetadataStore::new();
        self.tombstones_by_metadata_path.clear();
        self.coordinator = DistributedEventualCoordinator::new();
        self.pending_writes.clear();
        self.pending_reads.clear();
        self.incoming_read_transfers.clear();
        self.incoming_fetch_transfers.clear();
        self.incoming_blob_transfers.clear();

        for file_system in file_systems {
            self.metadata.add_file_system(file_system);
        }
        if !self
            .metadata
            .filesystems
            .contains_key(&sifs_file_system_id())
        {
            self.metadata
                .add_file_system(DfsFileSystem::sifs(self.node_id.clone()));
        }

        for object in objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
            .chain(
                objects
                    .iter()
                    .filter(|object| object.kind != DfsObjectKind::Tombstone),
            )
            .cloned()
        {
            self.coordinator.set_committed_version(
                object.file_system_id.clone(),
                object.object_id.clone(),
                object.version.clone(),
            );
            self.track_metadata_tombstone(&object);
            self.metadata.add_object(object)?;
        }

        for conflict in conflicts {
            self.metadata.add_conflict(conflict);
        }

        intents.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.intent_id.cmp(&right.intent_id))
        });
        let mut pending_write_records = Vec::new();
        for intent_record in intents {
            let intent = distributed_intent_from_record(&intent_record);
            match self.coordinator.restore_intent(intent) {
                Ok(()) => {}
                Err(error @ DfsDistributedError::PendingIntentConflict { .. })
                    if matches!(
                        intent_record.status,
                        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                    ) =>
                {
                    let object = self.current_read_object_metadata(&intent_record.object_id)?;
                    self.create_write_conflict(
                        &object,
                        intent_record.base_version.clone(),
                        intent_record.target_version.clone(),
                        intent_record.coordinator_node_id.clone(),
                        intent_record.content_hash.clone(),
                        error.to_string(),
                        Some(intent_record.intent_id.clone()),
                    )?;
                    let mut conflict_record = intent_record.clone();
                    conflict_record.status = DfsWriteIntentStatus::Conflict;
                    conflict_record.updated_at = Some(OffsetDateTime::now_utc());
                    if let Some(backend) = self.metadata_backend.as_mut() {
                        backend.put_intent(&conflict_record)?;
                    }
                    self.coordinator
                        .restore_intent(distributed_intent_from_record(&conflict_record))?;
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
            if let Some(receipts) = receipts_by_intent.get(&intent_record.intent_id) {
                for receipt in receipts {
                    if let Some(expected_hash) = intent_record.content_hash.as_deref()
                        && receipt.content_hash.as_deref() != Some(expected_hash)
                    {
                        continue;
                    }
                    match self.coordinator.restore_durable_receipt(
                        &intent_record.intent_id,
                        receipt.replica_node_id.clone(),
                    ) {
                        Ok(()) => {}
                        Err(
                            DfsDistributedError::UnexpectedReplica { .. }
                            | DfsDistributedError::IntentClosed { .. },
                        ) => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            if matches!(
                intent_record.status,
                DfsWriteIntentStatus::Staging
                    | DfsWriteIntentStatus::ReceiptsPending
                    | DfsWriteIntentStatus::Committed
            ) {
                pending_write_records.push(intent_record);
            }
        }

        let repair_intent_ids: BTreeSet<String> = self
            .coordinator
            .repair_jobs()
            .iter()
            .map(|job| job.intent_id.clone())
            .collect();
        let mut recovered_commit_ready_intent_ids = Vec::new();
        for intent_record in pending_write_records {
            let needs_pending_write =
                matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) || (matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                    && repair_intent_ids.contains(&intent_record.intent_id));
            if needs_pending_write && let Some(content_hash) = intent_record.content_hash.clone() {
                let size_bytes = self
                    .pending_write_size_hint(
                        &intent_record.object_id,
                        &intent_record.target_version,
                        &content_hash,
                    )?
                    .unwrap_or(0);
                self.pending_writes.insert(
                    intent_record.intent_id.clone(),
                    PendingNetworkedWrite {
                        object_id: intent_record.object_id.clone(),
                        version: intent_record.target_version.clone(),
                        content_hash,
                        size_bytes,
                        stage_retries: BTreeMap::new(),
                        repair_retries: BTreeMap::new(),
                        stage_inflight_request_ids: BTreeMap::new(),
                        repair_inflight_request_ids: BTreeMap::new(),
                        fetch_inflight_from: BTreeSet::new(),
                        fetch_inflight_request_ids: BTreeMap::new(),
                        fetch_inflight_started_ticks: BTreeMap::new(),
                        fetch_failed_from: BTreeSet::new(),
                    },
                );
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) && self
                    .coordinator
                    .intent(&intent_record.intent_id)
                    .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed))
                {
                    recovered_commit_ready_intent_ids.push(intent_record.intent_id.clone());
                }
            }
        }
        for intent_id in recovered_commit_ready_intent_ids {
            self.apply_committed_intent(&intent_id)?;
            self.cleanup_committed_pending_write_if_repaired(&intent_id);
        }

        Ok(())
    }

    fn pending_write_size_hint(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<Option<u64>, DfsError> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash.as_deref() == Some(content_hash)
            && version_record.size_bytes.is_some()
        {
            return Ok(version_record.size_bytes);
        }
        match self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
        {
            Ok(manifest) if manifest.content_hash == content_hash => Ok(Some(manifest.size_bytes)),
            Ok(_) => Ok(None),
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) -> Result<(), DfsError> {
        if let Some(kv) = self.control_plane.as_mut() {
            DfsControlPlaneStore::new(kv.as_mut())
                .put_file_system_config(&DfsFileSystemConfig::from(&file_system))?;
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_file_system(&file_system)?;
        }
        self.metadata.add_file_system(file_system);
        Ok(())
    }

    pub fn add_object_metadata(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        self.put_visible_object_in_metadata_backend(&object)?;
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)
    }

    pub fn add_transient_object_metadata(
        &mut self,
        object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.metadata.add_object(object)
    }

    fn track_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        let key = (object.file_system_id.clone(), metadata_path(&object.path));
        if object.kind == DfsObjectKind::Tombstone {
            let replace = self
                .tombstones_by_metadata_path
                .get(&key)
                .is_none_or(|existing| existing.version < object.version);
            if replace {
                self.tombstones_by_metadata_path.insert(key, object.clone());
            }
            return;
        }
        self.tombstones_by_metadata_path.remove(&key);
    }

    pub fn remove_transient_object_metadata(&mut self, object_id: &DfsObjectId) {
        self.metadata.objects.remove(object_id);
    }

    pub fn apply_namespace_transaction(
        &mut self,
        transaction: DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        if let Some(backend) = self.metadata_backend.as_mut() {
            if backend
                .get_file_system(&transaction.file_system_id)?
                .is_none()
            {
                backend.put_file_system(&DfsFileSystem {
                    file_system_id: transaction.file_system_id.clone(),
                    display_name: transaction.file_system_id.to_string(),
                    owner_node_id: self.node_id.clone(),
                    subtree_policies: Vec::new(),
                })?;
            }
            backend.apply_namespace_transaction(&transaction)?;
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
            .chain(
                transaction
                    .objects
                    .iter()
                    .filter(|object| object.kind != DfsObjectKind::Tombstone),
            )
            .cloned()
        {
            self.coordinator.set_committed_version(
                object.file_system_id.clone(),
                object.object_id.clone(),
                object.version.clone(),
            );
            self.track_metadata_tombstone(&object);
            self.metadata.add_object(object)?;
        }
        Ok(())
    }

    pub fn put_local_object_version(
        &mut self,
        mut object: DfsObjectMetadata,
        bytes: &[u8],
    ) -> Result<String, DfsError> {
        let manifest = self.content.put_object_version(
            object.object_id.as_str(),
            object.version.as_str(),
            bytes,
        )?;
        object.content_hash = Some(manifest.content_hash.clone());
        object.content_size_bytes = Some(manifest.size_bytes);
        if !object.current_locations.contains(&self.node_id) {
            object.current_locations.push(self.node_id.clone());
        }
        self.put_visible_object_in_metadata_backend(&object)?;
        self.put_object_version_size_in_metadata_backend(&object, Some(manifest.size_bytes))?;
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)?;
        Ok(manifest.content_hash)
    }

    pub fn read_local_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsReadData>, DfsError> {
        Ok(match self.read_local_object_with_state(object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data,
                freshness:
                    NetworkedDfsReadFreshness::Current | NetworkedDfsReadFreshness::Confirmed { .. },
            } => Some(data),
            NetworkedDfsReadOutcome::Found { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::Conflicted { .. }
            | NetworkedDfsReadOutcome::NotFound => None,
        })
    }

    pub fn request_remote_object_read(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<NodeId>, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.prune_stale_pending_remote_reads_for_object(&object);
        if object.content_hash.is_none() {
            return Ok(Vec::new());
        }

        let mut requested = Vec::new();
        let now = Instant::now();
        let mut seen_locations = BTreeSet::new();
        let mut expired_candidates = Vec::new();
        for node_id in &object.current_locations {
            if node_id == &self.node_id {
                continue;
            }
            if !seen_locations.insert(node_id.clone()) {
                continue;
            }
            if let Some((request_id, pending)) =
                self.pending_remote_read_for_object_target(&object, node_id)
            {
                if pending.next_retry_at > now {
                    requested.push(node_id.clone());
                    break;
                }
                self.pending_reads.remove(&request_id);
                self.incoming_read_transfers.remove(&request_id);
                expired_candidates.push(node_id.clone());
                continue;
            }
            if self.send_remote_read_request_to(&object, node_id, now)? {
                requested.push(node_id.clone());
                break;
            }
        }
        if requested.is_empty() {
            for node_id in expired_candidates {
                if self.send_remote_read_request_to(&object, &node_id, now)? {
                    requested.push(node_id);
                    break;
                }
            }
        }
        Ok(requested)
    }

    fn send_remote_read_request_to(
        &mut self,
        object: &DfsObjectMetadata,
        target_node_id: &NodeId,
        now: Instant,
    ) -> Result<bool, DfsError> {
        let request_id = self.next_request_id();
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: request_id.clone(),
                object_id: object.object_id.clone(),
            }),
        )?;
        if sent {
            self.pending_reads.insert(
                request_id,
                PendingNetworkedRead {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: object.content_hash.clone(),
                    content_size_bytes: object.content_size_bytes,
                    target_node_id: target_node_id.clone(),
                    next_retry_at: now + DFS_REMOTE_READ_RETRY_DELAY,
                },
            );
        }
        Ok(sent)
    }

    fn prune_stale_pending_remote_reads_for_object(&mut self, object: &DfsObjectMetadata) {
        let request_ids = self
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == object.object_id
                    && (pending.version != object.version
                        || pending.content_hash != object.content_hash
                        || pending.content_size_bytes != object.content_size_bytes
                        || !object.current_locations.contains(&pending.target_node_id))
            })
            .map(|(request_id, _)| request_id.clone())
            .collect::<Vec<_>>();
        for request_id in request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
        }
    }

    pub(crate) fn has_pending_remote_read_for_current_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        Ok(self.pending_reads.values().any(|pending| {
            pending.object_id == *object_id
                && pending.version == object.version
                && pending.content_hash == object.content_hash
                && pending.content_size_bytes == object.content_size_bytes
                && object.current_locations.contains(&pending.target_node_id)
                && pending.next_retry_at > Instant::now()
        }))
    }

    fn pending_remote_read_for_object_target(
        &self,
        object: &DfsObjectMetadata,
        target_node_id: &NodeId,
    ) -> Option<(String, PendingNetworkedRead)> {
        self.pending_reads
            .iter()
            .find(|(_, pending)| {
                pending.object_id == object.object_id
                    && pending.target_node_id == *target_node_id
                    && pending.version == object.version
                    && pending.content_hash == object.content_hash
            })
            .map(|(request_id, pending)| (request_id.clone(), pending.clone()))
    }

    fn current_read_object_metadata(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let memory_object = self.metadata.object(object_id).cloned();
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            None
        };
        match (memory_object, backend_object) {
            (_, Some(backend_object)) => Ok(backend_object),
            (Some(memory_object), None) => Ok(memory_object),
            (None, None) => Err(DfsError::UnknownObject {
                object_id: object_id.clone(),
            }),
        }
    }

    pub fn read_local_object_with_state(
        &self,
        object_id: &DfsObjectId,
        allow_stale: bool,
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let open_conflicts = self.open_conflicts_for_object(object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsReadOutcome::Conflicted {
                object_id: object.object_id.clone(),
                visible_version: object.version.clone(),
                visible_content_hash: object.content_hash.clone(),
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok(manifest) = self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            if object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
                && let Ok(bytes) = self.content.read_blob(&manifest.content_hash)
            {
                return Ok(NetworkedDfsReadOutcome::Found {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(manifest.content_hash),
                        bytes,
                    },
                    freshness: NetworkedDfsReadFreshness::Current,
                });
            }
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(bytes) = self.content.read_blob(expected_hash)
        {
            return Ok(NetworkedDfsReadOutcome::Found {
                data: DfsReadData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(expected_hash.clone()),
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if allow_stale {
            let stale_read = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .filter(|manifest| manifest.version != object.version.as_str())
                .find_map(|manifest| {
                    self.content
                        .read_blob(&manifest.content_hash)
                        .ok()
                        .map(|bytes| (manifest, bytes))
                });
            if let Some((manifest, bytes)) = stale_read {
                return Ok(NetworkedDfsReadOutcome::Found {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: DfsVersion::deserialize_migrating_legacy(manifest.version.clone())
                            .map_err(|error| DfsError::ContentStore {
                                message: error.to_string(),
                            })?,
                        content_hash: Some(manifest.content_hash.clone()),
                        bytes,
                    },
                    freshness: NetworkedDfsReadFreshness::Stale {
                        current_version: object.version.clone(),
                        current_content_hash: object.content_hash.clone(),
                    },
                });
            }
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsReadOutcome::PendingReplication {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: object.current_locations.clone(),
            })
        } else {
            Ok(NetworkedDfsReadOutcome::NotFound)
        }
    }

    pub fn read_local_object_confirmed(
        &self,
        object_id: &DfsObjectId,
        required_replica_node_ids: &[NodeId],
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedConfirmed {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }

        let confirmed_locations =
            self.confirmed_locations_for_object(&object, required_replica_node_ids)?;
        let confirmed_set: BTreeSet<&NodeId> = confirmed_locations.iter().collect();
        let missing_required = required_replica_node_ids
            .iter()
            .any(|node_id| !confirmed_set.contains(node_id));
        if missing_required {
            return Ok(NetworkedDfsReadOutcome::NotConfirmed {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                confirmed_locations,
                required_locations: required_replica_node_ids.to_vec(),
            });
        }

        match self.read_local_object_with_state(object_id, false)? {
            NetworkedDfsReadOutcome::Found { data, .. } => Ok(NetworkedDfsReadOutcome::Found {
                data,
                freshness: NetworkedDfsReadFreshness::Confirmed {
                    replica_node_ids: confirmed_locations,
                },
            }),
            other => Ok(other),
        }
    }

    pub fn plan_distributed_eventual_replicas(
        &self,
        request: &DfsWriteRequest,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<DfsReplicaPlacementPlan, DfsError> {
        let candidates = candidate_nodes
            .into_iter()
            .map(DfsReplicaCandidate::new)
            .collect();
        Ok(plan_replicas(&DfsReplicaPlacementRequest {
            file_system_id: request.file_system_id.clone(),
            object_id: request.object_id.clone(),
            target_version: request.target_version.clone(),
            coordinator_node_id: self.node_id.clone(),
            policy,
            candidates,
        })?)
    }

    pub fn write_distributed_eventual_with_candidates(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_eventual(request, bytes, plan.desired_replica_nodes, policy)
    }

    pub fn write_replicated_confirmed(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.ensure_replicated_confirmed_object(&request.object_id)?;
        let policy = DfsReplicationPolicy::confirmed(desired_replica_nodes.len())?;
        self.write_distributed_eventual(request, bytes, desired_replica_nodes, policy)
    }

    pub fn write_replicated_confirmed_with_candidates(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        candidate_nodes: Vec<NodeId>,
        desired_replicas: usize,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.ensure_replicated_confirmed_object(&request.object_id)?;
        let policy = DfsReplicationPolicy::confirmed(desired_replicas)?;
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_eventual(request, bytes, plan.desired_replica_nodes, policy)
    }

    pub fn write_distributed_eventual(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        self.write_distributed_eventual_with_base(
            request,
            Some(object.version),
            bytes,
            desired_replica_nodes,
            policy,
        )
    }

    pub fn write_distributed_eventual_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let owner_epoch = self.ensure_file_system_write_authority(&request.file_system_id)?;

        if expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &object.version)
        {
            let local_content_hash = self.store_conflicting_write_content_hash(&request, &bytes)?;
            let conflict = self.create_write_conflict(
                &object,
                expected_base_version,
                request.target_version.clone(),
                request.requesting_node_id.clone(),
                local_content_hash,
                "base version conflict for distributed write".to_string(),
                None,
            )?;
            return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
        }

        if let Err(error) = self.coordinator.validate_new_intent(
            &request.file_system_id,
            &request.object_id,
            expected_base_version.as_ref(),
            &request.target_version,
            &desired_replica_nodes,
            &policy,
        ) {
            if object.policy == DfsObjectPolicy::ReplicatedEventual
                && distributed_error_creates_conflict(&error)
            {
                let local_content_hash =
                    self.store_conflicting_write_content_hash(&request, &bytes)?;
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    local_content_hash,
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            return Err(error.into());
        }

        if object.policy == DfsObjectPolicy::ReplicatedEventual
            && policy.min_commit_replicas == 1
            && desired_replica_nodes.len() == 1
            && desired_replica_nodes[0] == self.node_id
        {
            let mut committed_object = object.clone();
            let manifest = self.content.put_object_version(
                request.object_id.as_str(),
                request.target_version.as_str(),
                &bytes,
            )?;
            let write_time = dfs_now_unix_nanos();
            committed_object.version = request.target_version.clone();
            committed_object.content_hash = Some(manifest.content_hash.clone());
            committed_object.content_size_bytes = Some(manifest.size_bytes);
            committed_object.modified_at_unix_nanos = write_time;
            committed_object.changed_at_unix_nanos = write_time;
            committed_object.current_locations = vec![self.node_id.clone()];
            committed_object.last_modified_by_event_id = Some(EventId::generate());
            self.put_visible_object_in_metadata_backend(&committed_object)?;
            self.put_object_version_size_in_metadata_backend(
                &committed_object,
                Some(manifest.size_bytes),
            )?;
            self.coordinator.set_committed_version(
                committed_object.file_system_id.clone(),
                committed_object.object_id.clone(),
                committed_object.version.clone(),
            );
            self.track_metadata_tombstone(&committed_object);
            self.metadata.add_object(committed_object)?;
            return Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: format!(
                    "local-eventual:{}:{}",
                    request.object_id, request.target_version
                ),
                version: request.target_version,
                content_hash: manifest.content_hash,
                repair_jobs: Vec::new(),
            });
        }

        let manifest = self.content.put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            &bytes,
        )?;
        let intent = match self.coordinator.create_intent_at_epoch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            expected_base_version.clone(),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_creates_conflict(&error) =>
            {
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    Some(manifest.content_hash.clone()),
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            Err(error) => return Err(error.into()),
        };
        self.put_metadata_intent(&intent, Some(&manifest.content_hash))?;
        self.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite {
                object_id: request.object_id.clone(),
                version: request.target_version.clone(),
                content_hash: manifest.content_hash.clone(),
                size_bytes: manifest.size_bytes,
                stage_retries: BTreeMap::new(),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
                fetch_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_started_ticks: BTreeMap::new(),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        let updated_intent = if desired_replica_nodes.contains(&self.node_id) {
            let local_node_id = self.node_id.clone();
            self.put_metadata_receipt(
                &request.file_system_id,
                &request.object_id,
                &request.target_version,
                &local_node_id,
                &manifest.content_hash,
            )?;
            self.coordinator
                .record_durable_receipt(&intent.intent_id, self.node_id.clone())?
        } else {
            intent
        };

        if matches!(updated_intent.state, DfsWriteIntentState::Committed) {
            self.apply_committed_intent(&updated_intent.intent_id)?;
        } else {
            let stage_targets: Vec<NodeId> = desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id)
                .cloned()
                .collect();
            for replica_node_id in stage_targets {
                let request_id = self.next_request_id();
                let sent = self.send_stage_blob_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &updated_intent.intent_id,
                    &request.file_system_id,
                    &request.object_id,
                    updated_intent.base_version.clone(),
                    &request.target_version,
                    &manifest.content_hash,
                    &bytes,
                    Some(object.clone()),
                )?;
                if sent {
                    self.record_stage_inflight(
                        &updated_intent.intent_id,
                        &replica_node_id,
                        request_id,
                    );
                } else {
                    self.record_stage_retry_scheduled(&updated_intent.intent_id, &replica_node_id);
                }
            }
        }

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(outcome)
    }

    pub fn process_incoming(&mut self) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.prune_idle_chunk_transfers();
        let inbound_frames = self
            .transport
            .drain_incoming()
            .map_err(DfsError::from_network)?;
        let mut events = Vec::new();

        for inbound in inbound_frames {
            let source_node_id = inbound.frame.source_node_id.clone();
            let MeshMessageBody::Protocol(protocol_message) = inbound.frame.message.body else {
                continue;
            };
            if protocol_message.protocol != DFS_PROTOCOL {
                continue;
            }
            let Ok(message) = protocol_message.decode::<DfsNetworkMessage>() else {
                continue;
            };

            match message {
                DfsNetworkMessage::StageBlob(command) => {
                    events.push(self.handle_stage_blob(source_node_id, command)?);
                }
                DfsNetworkMessage::StageBlobChunk(command) => {
                    if let Some(event) = self.handle_stage_blob_chunk(source_node_id, command)? {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::StageBlobResponse(response) => {
                    if let Some(event) =
                        self.handle_stage_blob_response(source_node_id, response)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::CommitVersion(command) => {
                    events.push(self.handle_commit_version(source_node_id, command)?);
                }
                DfsNetworkMessage::RepairReplica(command) => {
                    events.push(self.handle_repair_replica(source_node_id, command)?);
                }
                DfsNetworkMessage::RepairReplicaChunk(command) => {
                    if let Some(event) =
                        self.handle_repair_replica_chunk(source_node_id, command)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::RepairReplicaResponse(response) => {
                    events.push(self.handle_repair_replica_response(source_node_id, response)?);
                }
                DfsNetworkMessage::FetchBlob(command) => {
                    events.push(self.handle_fetch_blob(source_node_id, command)?);
                }
                DfsNetworkMessage::FetchBlobResponse(response) => {
                    events.push(self.handle_fetch_blob_response(source_node_id.clone(), response)?);
                }
                DfsNetworkMessage::FetchBlobResponseChunk(chunk) => {
                    if let Some(event) =
                        self.handle_fetch_blob_response_chunk(source_node_id.clone(), chunk)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::ReadRequest(request) => {
                    events.push(self.handle_networked_read_request(source_node_id, request)?);
                }
                DfsNetworkMessage::ReadResponse(response) => {
                    events.push(self.handle_networked_read_response(source_node_id, response)?);
                }
                DfsNetworkMessage::ReadResponseChunk(chunk) => {
                    if let Some(event) =
                        self.handle_networked_read_response_chunk(source_node_id, chunk)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::MetadataJournalBatch(command) => {
                    events.push(self.handle_metadata_journal_batch(source_node_id, command)?);
                }
                DfsNetworkMessage::MetadataJournalBatchResponse(response) => {
                    if let Some(event) =
                        self.handle_metadata_journal_batch_response(source_node_id, response)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::CommitVersionResponse(_)
                | DfsNetworkMessage::WriteRequest(_)
                | DfsNetworkMessage::WriteResponse(_) => {}
            }
        }

        self.prune_expired_pending_remote_reads();
        self.last_events = events.clone();
        Ok(events)
    }

    fn prune_expired_pending_remote_reads(&mut self) {
        if self.pending_reads.is_empty() {
            return;
        }
        let now = Instant::now();
        let expired_request_ids = self
            .pending_reads
            .iter()
            .filter_map(|(request_id, pending)| {
                let prune_after = pending
                    .next_retry_at
                    .checked_add(DFS_CHUNK_TRANSFER_IDLE_TIMEOUT)
                    .unwrap_or(pending.next_retry_at);
                (prune_after <= now).then_some(request_id.clone())
            })
            .collect::<Vec<_>>();
        for request_id in expired_request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
        }
    }

    fn handle_metadata_journal_batch_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsMetadataJournalBatchResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let pending = self
            .pending_metadata_replications
            .remove(&response.request_id);
        let Some(pending) = pending else {
            return Ok(None);
        };
        if pending.target_node_id != source_node_id || pending.shard_id != response.shard_id {
            return Ok(Some(
                NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason: "metadata journal batch response does not match pending replication"
                        .to_string(),
                },
            ));
        }

        Ok(match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                let cursor_key = (source_node_id.clone(), response.shard_id.clone());
                let cursor = self
                    .metadata_replication_cursors
                    .entry(cursor_key)
                    .or_insert(0);
                *cursor = (*cursor)
                    .max(response.next_after_sequence)
                    .max(pending.next_after_sequence);
                if let Some(backend) = self.metadata_backend.as_mut() {
                    backend.put_metadata_replication_cursor(
                        &source_node_id,
                        &response.shard_id,
                        *cursor,
                    )?;
                }
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    applied_entries: response.applied_entries,
                    next_after_sequence: *cursor,
                })
            }
            DfsMetadataJournalBatchResult::Rejected { reason } => {
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason,
                })
            }
        })
    }

    fn metadata_journal_batch_since(
        &self,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let entries = backend
            .journal_since_limited(after_sequence, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT)?;
        let Some(last_entry) = entries.last() else {
            return Ok(None);
        };
        Ok(Some(DfsMetadataJournalBatch {
            shard_id: last_entry.shard_id.clone(),
            next_after_sequence: last_entry.sequence,
            entries,
        }))
    }

    fn handle_metadata_journal_batch(
        &mut self,
        source_node_id: NodeId,
        command: DfsMetadataJournalBatchCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let shard_id = command.batch.shard_id.clone();
        let next_after_sequence = command.batch.next_after_sequence;
        let Some(backend) = self.metadata_backend.as_mut() else {
            let reason = "local DFS metadata backend is not configured".to_string();
            let _ = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    applied_entries: 0,
                    result: DfsMetadataJournalBatchResult::Rejected {
                        reason: reason.clone(),
                    },
                }),
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason,
            });
        };

        match backend.apply_journal_batch(&command.batch) {
            Ok(applied_entries) => {
                self.hydrate_from_metadata_backend()?;
                let _ = self.send_dfs_message_if_routable(
                    &source_node_id,
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: shard_id.clone(),
                            next_after_sequence,
                            applied_entries,
                            result: DfsMetadataJournalBatchResult::Applied,
                        },
                    ),
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    applied_entries,
                    next_after_sequence,
                })
            }
            Err(error) => {
                let reason = error.to_string();
                let _ = self.send_dfs_message_if_routable(
                    &source_node_id,
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: shard_id.clone(),
                            next_after_sequence,
                            applied_entries: 0,
                            result: DfsMetadataJournalBatchResult::Rejected {
                                reason: reason.clone(),
                            },
                        },
                    ),
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    reason,
                })
            }
        }
    }

    fn prune_idle_chunk_transfers(&mut self) {
        if !self.has_incoming_chunk_transfer_state() {
            return;
        }
        let now = Instant::now();
        self.incoming_blob_transfers.retain(|_, transfer| {
            now.duration_since(transfer.last_updated_at) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT
        });
        self.incoming_read_transfers.retain(|request_id, transfer| {
            let keep =
                now.duration_since(transfer.last_updated_at) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT;
            if !keep {
                self.pending_reads.remove(request_id);
            }
            keep
        });

        let mut expired_fetches = Vec::new();
        self.incoming_fetch_transfers
            .retain(|request_id, transfer| {
                let keep =
                    now.duration_since(transfer.last_updated_at) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT;
                if !keep {
                    expired_fetches.push(request_id.clone());
                }
                keep
            });
        if expired_fetches.is_empty() {
            return;
        }
        let expired = expired_fetches.into_iter().collect::<BTreeSet<_>>();
        for pending in self.pending_writes.values_mut() {
            let expired_sources = pending
                .fetch_inflight_request_ids
                .iter()
                .filter_map(|(source, request_id)| {
                    expired.contains(request_id).then_some(source.clone())
                })
                .collect::<Vec<_>>();
            let has_expired_source = !expired_sources.is_empty();
            for source in expired_sources {
                pending.fetch_inflight_from.remove(&source);
                pending.fetch_inflight_request_ids.remove(&source);
                pending.fetch_inflight_started_ticks.remove(&source);
                pending.fetch_failed_from.insert(source);
            }
            if has_expired_source {
                for state in pending.stage_retries.values_mut() {
                    state.next_retry_tick = 0;
                    state.inflight = false;
                }
                for state in pending.repair_retries.values_mut() {
                    state.next_retry_tick = 0;
                    state.inflight = false;
                }
            }
        }
    }

    fn has_incoming_chunk_transfer_state(&self) -> bool {
        !self.incoming_blob_transfers.is_empty()
            || !self.incoming_read_transfers.is_empty()
            || !self.incoming_fetch_transfers.is_empty()
    }

    pub fn retry_pending_repairs(&mut self) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.prune_idle_chunk_transfers();
        if self.pending_writes.is_empty() {
            return Ok(Vec::new());
        }
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        self.retry_pending_stages()?;
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        let mut scheduled = Vec::new();

        for job in repair_jobs {
            if job.missing_replica_node_id == self.node_id {
                continue;
            }
            let Some(pending) = self.pending_writes.get(&job.intent_id).cloned() else {
                continue;
            };
            if !self.repair_retry_is_due(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }

            let bytes = match self.content.read_blob(&pending.content_hash) {
                Ok(bytes) => bytes,
                Err(_) => {
                    let fetch_requested =
                        self.request_repair_blob_from_alternate(&job, &pending)?;
                    if !fetch_requested {
                        self.record_repair_retry_scheduled(
                            &job.intent_id,
                            &job.missing_replica_node_id,
                        );
                    }
                    continue;
                }
            };
            let request_id = self.next_request_id();
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let sent = self.send_repair_replica_if_routable(
                &job.missing_replica_node_id,
                request_id.clone(),
                &job.intent_id,
                &job.file_system_id,
                &pending.object_id,
                self.coordinator
                    .intent(&job.intent_id)
                    .and_then(|intent| intent.base_version.clone()),
                &pending.version,
                &pending.content_hash,
                &bytes,
                object_metadata,
            )?;

            if sent {
                self.record_repair_inflight(
                    &job.intent_id,
                    &job.missing_replica_node_id,
                    request_id,
                );
                scheduled.push(job);
            } else {
                self.record_repair_retry_scheduled(&job.intent_id, &job.missing_replica_node_id);
            }
        }

        Ok(scheduled)
    }

    pub fn handle_confirmed_write_timeout(
        &mut self,
        intent_id: &str,
        action: DfsConfirmedTimeoutAction,
    ) -> Result<DfsConfirmedTimeoutOutcome, DfsError> {
        match action {
            DfsConfirmedTimeoutAction::KeepPending => match self.write_outcome(intent_id)? {
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } => Ok(DfsConfirmedTimeoutOutcome::StillPending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                }),
                outcome @ NetworkedDfsWriteOutcome::Committed { .. } => {
                    Ok(DfsConfirmedTimeoutOutcome::Committed(outcome))
                }
                NetworkedDfsWriteOutcome::Conflict { conflict } => {
                    Ok(DfsConfirmedTimeoutOutcome::Conflict { conflict })
                }
            },
            DfsConfirmedTimeoutAction::Abort => {
                self.abort_pending_write(intent_id)?;
                Ok(DfsConfirmedTimeoutOutcome::Aborted {
                    intent_id: intent_id.to_string(),
                })
            }
            DfsConfirmedTimeoutAction::DowngradeToEventual {
                min_commit_replicas,
            } => {
                let intent = self.coordinator.intent(intent_id).cloned().ok_or_else(|| {
                    DfsError::Distributed {
                        message: format!("unknown intent: {intent_id}"),
                    }
                })?;
                let policy =
                    DfsReplicationPolicy::new(min_commit_replicas, intent.policy.desired_replicas)?;
                let updated_intent = self
                    .coordinator
                    .downgrade_intent_policy(intent_id, policy)?;
                let content_hash = self
                    .pending_writes
                    .get(intent_id)
                    .map(|pending| pending.content_hash.clone());
                self.put_metadata_intent(&updated_intent, content_hash.as_deref())?;
                if matches!(updated_intent.state, DfsWriteIntentState::Committed) {
                    self.apply_committed_intent(intent_id)?;
                }
                Ok(DfsConfirmedTimeoutOutcome::Downgraded(
                    self.write_outcome(intent_id)?,
                ))
            }
        }
    }

    fn current_object_for_write(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let memory_object = self.metadata.object(object_id).cloned();
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(object) = backend.get_object(object_id)?
        {
            if object.kind == DfsObjectKind::Tombstone
                && let Some(memory_object) = memory_object
                && memory_object.kind != DfsObjectKind::Tombstone
                && memory_object.version >= object.version
            {
                return Ok(memory_object);
            }
            self.metadata.add_object(object.clone())?;
            return Ok(object);
        }
        self.metadata
            .object(object_id)
            .cloned()
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })
    }

    pub fn abort_pending_write(
        &mut self,
        intent_id: &str,
    ) -> Result<DistributedWriteIntent, DfsError> {
        let content_hash = self
            .pending_writes
            .get(intent_id)
            .map(|pending| pending.content_hash.clone());
        let intent = self.coordinator.abort_intent(intent_id)?;
        self.put_metadata_intent(&intent, content_hash.as_deref())?;
        self.remove_pending_write_state(intent_id);
        Ok(intent)
    }

    fn remove_pending_write_state(&mut self, intent_id: &str) {
        let Some(pending) = self.pending_writes.remove(intent_id) else {
            return;
        };
        for request_id in pending.fetch_inflight_request_ids.values() {
            self.incoming_fetch_transfers.remove(request_id);
        }
        self.incoming_blob_transfers
            .retain(|_, transfer| transfer.intent_id != intent_id);
    }

    fn cleanup_committed_pending_write_if_repaired(&mut self, intent_id: &str) {
        let is_committed = self
            .coordinator
            .intent(intent_id)
            .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed));
        if !is_committed {
            return;
        }
        let has_repair_jobs = self
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.intent_id == intent_id);
        if has_repair_jobs {
            return;
        }
        self.remove_pending_write_state(intent_id);
    }

    pub fn conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        let mut conflicts: BTreeMap<String, DfsConflictRecord> = self
            .metadata
            .conflicts_for_object(object_id)
            .into_iter()
            .map(|conflict| (conflict.conflict_id.clone(), conflict))
            .collect();
        if let Some(backend) = self.metadata_backend.as_ref() {
            for conflict in backend.conflicts_for_object(object_id)? {
                conflicts.insert(conflict.conflict_id.clone(), conflict);
            }
        }
        Ok(conflicts.into_values().collect())
    }

    pub fn open_conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        Ok(self
            .conflicts_for_object(object_id)?
            .into_iter()
            .filter(|conflict| conflict.status == DfsConflictStatus::Open)
            .collect())
    }

    pub fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>, DfsError> {
        let updated = if let Some(backend) = self.metadata_backend.as_mut() {
            backend.update_conflict_status(object_id, conflict_id, status.clone())?
        } else {
            self.metadata
                .conflicts_for_object(object_id)
                .into_iter()
                .find(|conflict| conflict.conflict_id == conflict_id)
                .map(|mut conflict| {
                    conflict.status = status;
                    conflict
                })
        };
        if let Some(conflict) = updated.clone() {
            self.metadata.add_conflict(conflict);
        }
        Ok(updated)
    }

    pub fn reconcile_local_manifests(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsReconnectReconciliation, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let manifests = self
            .content
            .manifests_for_object(object.object_id.as_str())?;
        let mut refreshed_receipts = 0;
        let mut conflicts = Vec::new();
        let mut has_current_manifest = false;

        for manifest in manifests {
            let manifest_version = DfsVersion::deserialize_migrating_legacy(
                manifest.version.clone(),
            )
            .map_err(|error| DfsError::ContentStore {
                message: error.to_string(),
            })?;
            if manifest_version == object.version {
                if object
                    .content_hash
                    .as_ref()
                    .is_some_and(|hash| hash == &manifest.content_hash)
                {
                    if self
                        .content
                        .has_blob(&manifest.content_hash)
                        .unwrap_or(false)
                    {
                        has_current_manifest = true;
                        let local_node_id = self.node_id.clone();
                        let receipt_inserted = self.put_metadata_receipt(
                            &object.file_system_id,
                            &object.object_id,
                            &object.version,
                            &local_node_id,
                            &manifest.content_hash,
                        )?;
                        self.mark_repaired_location(
                            &object.object_id,
                            &object.version,
                            &local_node_id,
                            &manifest.content_hash,
                        )?;
                        if receipt_inserted {
                            refreshed_receipts += 1;
                        }
                    }
                } else {
                    conflicts.push(self.create_write_conflict(
                        &object,
                        Some(object.version.clone()),
                        manifest_version,
                        self.node_id.clone(),
                        Some(manifest.content_hash),
                        "local current-version manifest hash diverged during reconnect".to_string(),
                        None,
                    )?);
                }
            } else if !self.local_manifest_is_known_historical_version(
                &object.object_id,
                &manifest_version,
                &manifest.content_hash,
            )? {
                conflicts.push(self.create_write_conflict(
                    &object,
                    Some(object.version.clone()),
                    manifest_version,
                    self.node_id.clone(),
                    Some(manifest.content_hash),
                    "local non-visible manifest diverged during reconnect".to_string(),
                    None,
                )?);
            }
        }

        Ok(DfsReconnectReconciliation {
            object_id: object.object_id.clone(),
            refreshed_receipts,
            conflicts,
            missing_current: object.content_hash.is_some() && !has_current_manifest,
        })
    }

    fn local_manifest_is_known_historical_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        Ok(backend
            .get_version(object_id, version)?
            .is_some_and(|record| record.content_hash.as_deref() == Some(content_hash)))
    }

    pub fn handoff_object_owner(
        &mut self,
        object_id: &DfsObjectId,
        shard_id: &str,
        expected_epoch: u64,
    ) -> Result<DfsOwnerHandoffOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let previous_owner_node_id = object.owner_node_id.clone();
        let Some(expected_hash) = object.content_hash.as_deref() else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason:
                    "candidate cannot prove latest content for an object without a visible hash"
                        .to_string(),
            });
        };
        let manifest = match self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return Ok(DfsOwnerHandoffOutcome::Rejected {
                    reason: format!("candidate is missing latest manifest: {error}"),
                });
            }
        };
        if manifest.content_hash != expected_hash {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "candidate manifest hash does not match visible object hash".to_string(),
            });
        }

        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "owner handoff requires control-plane shard fencing".to_string(),
            });
        };
        let transferred = DfsControlPlaneStore::new(kv.as_mut()).transfer_metadata_shard_primary(
            &object.file_system_id,
            shard_id,
            expected_epoch,
            self.node_id.clone(),
        )?;
        let Some(transferred) = transferred else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "control-plane epoch compare failed".to_string(),
            });
        };

        let file_system_to_persist =
            if let Some(file_system) = self.metadata.filesystems.get_mut(&object.file_system_id) {
                file_system.owner_node_id = self.node_id.clone();
                Some(file_system.clone())
            } else {
                None
            };
        if let Some(file_system) = file_system_to_persist.as_ref() {
            if let Some(backend) = self.metadata_backend.as_mut() {
                backend.put_file_system(file_system)?;
            }
            if let Some(kv) = self.control_plane.as_mut() {
                DfsControlPlaneStore::new(kv.as_mut())
                    .put_file_system_config(&DfsFileSystemConfig::from(file_system))?;
            }
        }

        let mut updated_object = object.clone();
        updated_object.owner_node_id = self.node_id.clone();
        if !updated_object.current_locations.contains(&self.node_id) {
            updated_object.current_locations.push(self.node_id.clone());
        }
        self.metadata
            .objects
            .insert(updated_object.object_id.clone(), updated_object.clone());
        self.put_visible_object_in_metadata_backend(&updated_object)?;

        Ok(DfsOwnerHandoffOutcome::Transferred {
            file_system_id: updated_object.file_system_id,
            object_id: updated_object.object_id,
            previous_owner_node_id,
            new_owner_node_id: self.node_id.clone(),
            previous_epoch: expected_epoch,
            new_epoch: transferred.epoch,
        })
    }

    fn ensure_file_system_write_authority(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<u64, DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(0);
        };
        let Some(shard) =
            DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard(file_system_id, "root")?
        else {
            return Ok(0);
        };
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        Ok(shard.epoch)
    }

    fn ensure_intent_epoch_current(
        &mut self,
        intent: &DistributedWriteIntent,
    ) -> Result<(), DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(());
        };
        let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
            .get_metadata_shard(&intent.file_system_id, "root")?
        else {
            return Ok(());
        };
        if shard.primary_node_id != self.node_id || shard.epoch != intent.owner_epoch {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: intent.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        Ok(())
    }

    fn create_write_conflict(
        &mut self,
        object: &DfsObjectMetadata,
        base_version: Option<DfsVersion>,
        local_version: DfsVersion,
        detected_by_node_id: NodeId,
        local_content_hash: Option<String>,
        reason: String,
        created_by_intent_id: Option<String>,
    ) -> Result<DfsConflictRecord, DfsError> {
        let conflict_id = conflict_id_for(
            &object.object_id,
            &local_version,
            local_content_hash.as_deref(),
            &detected_by_node_id,
        );
        if let Some(existing) = self
            .conflicts_for_object(&object.object_id)?
            .into_iter()
            .find(|conflict| conflict.conflict_id == conflict_id)
        {
            return Ok(existing);
        }
        let conflict = DfsConflictRecord {
            conflict_id,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            base_version,
            local_version,
            remote_version: object.version.clone(),
            detected_by_node_id,
            reason,
            local_content_hash,
            remote_content_hash: object.content_hash.clone(),
            created_by_intent_id,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::now_utc(),
        };
        self.put_conflict_record(&conflict)?;
        Ok(conflict)
    }

    fn put_conflict_record(&mut self, conflict: &DfsConflictRecord) -> Result<(), DfsError> {
        if let Some(backend) = self.metadata_backend.as_mut() {
            if backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)?
                .as_ref()
                == Some(conflict)
            {
                self.metadata.add_conflict(conflict.clone());
                return Ok(());
            }
            backend.put_conflict(conflict)?;
        }
        self.metadata.add_conflict(conflict.clone());
        Ok(())
    }

    fn handle_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "stage",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "stage",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
                )
            }) {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                self.put_metadata_receipt(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )?;
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id.clone(),
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Committed { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobStageRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    fn handle_stage_blob_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            let reason = format!(
                "stage blob chunk transfer {} has no chunks",
                command.intent_id
            );
            return self.reject_stage_blob_chunk(requester_node_id, command, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS blob transfer",
            command.total_size_bytes,
            command.chunk_index,
            command.total_chunks,
            &command.bytes,
        ) {
            return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
        }
        let transfer_key = blob_transfer_key(
            "stage",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        if !self.incoming_blob_transfers.contains_key(&transfer_key) {
            if let Err(error) = self.ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) =
                self.ensure_replica_object_metadata(command.object_metadata.as_ref())
            {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) = self.ensure_command_base_matches_current(
                "stage",
                &command.object_id,
                command.expected_base_version.as_ref(),
                &command.version,
                &command.content_hash,
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) = self.ensure_no_conflicting_pending_replica_write(
                "stage",
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                &command.version,
                &command.content_hash,
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if self.local_object_version_manifest_matches(
                &command.object_id,
                &command.version,
                &command.content_hash,
                command.total_size_bytes,
            ) {
                let local_node_id = self.node_id.clone();
                if let Err(error) = self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                ) {
                    return self.reject_stage_blob_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
                self.put_metadata_receipt(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )?;
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        file_system_id: command.file_system_id,
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        replica_node_id: self.node_id.clone(),
                        result: DfsBlobTransferResult::Durable {
                            content_hash: command.content_hash.clone(),
                        },
                    }),
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash: command.content_hash,
                }));
            }
            let transfer = InProgressBlobTransfer::new(
                command.request_id.clone(),
                command.intent_id.clone(),
                command.file_system_id.clone(),
                command.object_id.clone(),
                command.expected_base_version.clone(),
                command.version.clone(),
                command.content_hash.clone(),
                command.total_size_bytes,
                command.total_chunks,
                command.object_metadata.clone(),
            )?;
            self.incoming_blob_transfers
                .insert(transfer_key.clone(), transfer);
        }
        let transfer = self
            .incoming_blob_transfers
            .get_mut(&transfer_key)
            .expect("transfer inserted");
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.expected_base_version != command.expected_base_version
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
        {
            self.incoming_blob_transfers.remove(&transfer_key);
            let reason = format!(
                "conflicting stage blob chunk metadata for {}",
                command.intent_id
            );
            return self.reject_stage_blob_chunk(requester_node_id, command, reason);
        }
        let bytes = match transfer.insert_chunk(
            command.chunk_index,
            command.total_chunks,
            command.bytes.clone(),
        ) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(error) => {
                self.incoming_blob_transfers.remove(&transfer_key);
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
        };
        let transfer = self
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        Ok(Some(self.handle_stage_blob(
            requester_node_id,
            DfsStageBlobCommand {
                request_id: transfer.request_id,
                intent_id: command.intent_id,
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id,
                expected_base_version: transfer.expected_base_version,
                version: transfer.version,
                content_hash: transfer.content_hash,
                bytes,
                object_metadata: transfer.object_metadata,
            },
        )?))
    }

    fn reject_stage_blob_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobChunkCommand,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let transfer_key = blob_transfer_key(
            "stage",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        self.incoming_blob_transfers.remove(&transfer_key);
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version,
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: reason.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
            intent_id: command.intent_id,
            object_id: command.object_id,
            replica_node_id: self.node_id.clone(),
            reason,
        }))
    }

    fn handle_stage_blob_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsStageBlobResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS stage response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            }));
        }
        match response.result {
            DfsBlobTransferResult::Durable { ref content_hash }
            | DfsBlobTransferResult::Committed { ref content_hash } => {
                if let Err(error) =
                    self.ensure_stage_response_matches_pending(&response, content_hash)
                {
                    return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }));
                }
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                let updated_intent = self.coordinator.record_durable_receipt(
                    &response.intent_id,
                    response.replica_node_id.clone(),
                )?;
                let event = NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: response.intent_id.clone(),
                    object_id: response.object_id.clone(),
                    version: response.version.clone(),
                    replica_node_id: response.replica_node_id.clone(),
                    content_hash: content_hash.clone(),
                };
                if matches!(updated_intent.state, DfsWriteIntentState::Committed) {
                    self.apply_committed_intent(&updated_intent.intent_id)?;
                }
                if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
                    pending
                        .stage_inflight_request_ids
                        .remove(&response.replica_node_id);
                    pending.stage_retries.remove(&response.replica_node_id);
                }
                self.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                Ok(Some(event))
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                match self.ensure_stage_response_request_matches_pending(&response) {
                    Ok(()) => {
                        self.record_stage_retry_scheduled(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        }))
                    }
                    Err(error) => Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    })),
                }
            }
        }
    }

    fn handle_commit_version(
        &mut self,
        requester_node_id: NodeId,
        command: DfsCommitVersionCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "commit",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.content
                    .read_manifest(command.object_id.as_str(), command.version.as_str())
                    .map_err(DfsError::from)
            })
            .and_then(|manifest| {
                if manifest.content_hash != command.content_hash {
                    return Err(DfsError::ContentHashMismatch {
                        expected: command.content_hash.clone(),
                        actual: manifest.content_hash,
                    });
                }
                if !self.content.has_blob(&command.content_hash)? {
                    return Err(DfsError::ContentStore {
                        message: format!(
                            "committed blob bytes are missing for {} {}",
                            command.object_id, command.version
                        ),
                    });
                }
                Ok(())
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "commit",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.put_metadata_receipt(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|_| {
                let write_time = dfs_now_unix_nanos();
                self.commit_local_metadata(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    write_time,
                )
                .and_then(|object| {
                    self.put_visible_object_in_metadata_backend(&object)?;
                    Ok(())
                })
            }) {
            Ok(()) => DfsBlobTransferResult::Committed {
                content_hash: command.content_hash.clone(),
            },
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::CommitVersionResponse(DfsCommitVersionResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::VersionCommitted {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    fn ensure_command_base_matches_current(
        &self,
        operation: &str,
        object_id: &DfsObjectId,
        expected_base_version: Option<&DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let current = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            self.metadata.object(object_id).cloned()
        };
        let Some(current) = current else {
            return Ok(());
        };
        if current.version == *version
            && let Some(current_hash) = current.content_hash.as_deref()
            && current_hash == content_hash
        {
            return Ok(());
        }
        if let Some(expected_base_version) = expected_base_version
            && &current.version != expected_base_version
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "stale DFS {operation} base for {object_id}: expected {}, current {}, command {}",
                    expected_base_version, current.version, version
                ),
            });
        }
        if current.version == *version
            && let Some(current_hash) = current.content_hash.as_deref()
            && current_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: current_hash.to_string(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    fn ensure_no_conflicting_pending_replica_write(
        &self,
        operation: &str,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        for (pending_intent_id, pending) in &self.pending_writes {
            if pending.object_id != *object_id {
                continue;
            }
            let Some(intent) = self.coordinator.intent(pending_intent_id) else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            if pending_intent_id == intent_id && intent.requesting_node_id == *requester_node_id {
                continue;
            }
            if pending.version == *version && pending.content_hash == content_hash {
                continue;
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "conflicting pending DFS {operation} for {object_id}: local intent {} from {} targets {} ({}) while remote intent {intent_id} from {} targets {} ({})",
                    pending_intent_id,
                    intent.requesting_node_id,
                    pending.version,
                    pending.content_hash,
                    requester_node_id,
                    version,
                    content_hash
                ),
            });
        }
        Ok(())
    }

    fn ensure_stage_response_request_matches_pending(
        &self,
        response: &DfsStageBlobResponse,
    ) -> Result<(), DfsError> {
        let Some(pending) = self.pending_writes.get(&response.intent_id) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "missing pending write for stage response: {}",
                    response.intent_id
                ),
            });
        };
        let expected_request_id = pending
            .stage_inflight_request_ids
            .get(&response.replica_node_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "unexpected DFS stage response source for intent {}: {}",
                    response.intent_id, response.replica_node_id
                ),
            })?;
        if expected_request_id != &response.request_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "unexpected DFS stage response request id for intent {} from {}: expected {}, actual {}",
                    response.intent_id,
                    response.replica_node_id,
                    expected_request_id,
                    response.request_id
                ),
            });
        }
        Ok(())
    }

    fn ensure_stage_response_matches_pending(
        &self,
        response: &DfsStageBlobResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_stage_response_request_matches_pending(response)?;
        let pending = self
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "stage",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }

    fn handle_repair_replica(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "repair",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "repair",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
                )
            }) {
            Ok(()) => match self
                .commit_local_metadata(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    dfs_now_unix_nanos(),
                )
                .and_then(|object| {
                    self.put_visible_object_in_metadata_backend(&object)?;
                    let local_node_id = self.node_id.clone();
                    self.put_metadata_receipt(
                        &command.file_system_id,
                        &command.object_id,
                        &command.version,
                        &local_node_id,
                        &command.content_hash,
                    )?;
                    Ok(())
                }) {
                Ok(()) => DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone(),
                },
                Err(error) => DfsBlobTransferResult::Rejected {
                    reason: error.to_string(),
                },
            },
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    fn ensure_repair_response_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_repair_response_request_matches_pending(response)?;
        let pending = self
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "repair",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }

    fn ensure_repair_response_request_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
    ) -> Result<(), DfsError> {
        let Some(pending) = self.pending_writes.get(&response.intent_id) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "missing pending write for repair response: {}",
                    response.intent_id
                ),
            });
        };
        let expected_request_id = pending
            .repair_inflight_request_ids
            .get(&response.replica_node_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "unexpected DFS repair response source for intent {}: {}",
                    response.intent_id, response.replica_node_id
                ),
            })?;
        if expected_request_id != &response.request_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "unexpected DFS repair response request id for intent {} from {}: expected {}, actual {}",
                    response.intent_id,
                    response.replica_node_id,
                    expected_request_id,
                    response.request_id
                ),
            });
        }
        Ok(())
    }

    fn ensure_response_matches_pending_write(
        &self,
        label: &str,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        pending: &PendingNetworkedWrite,
    ) -> Result<(), DfsError> {
        let intent = self
            .coordinator
            .intent(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!("unknown DFS {label} response intent: {intent_id}"),
            })?;
        if &intent.file_system_id != file_system_id
            || &intent.object_id != object_id
            || &intent.target_version != version
        {
            return Err(DfsError::Distributed {
                message: format!("DFS {label} response metadata does not match intent {intent_id}"),
            });
        }
        if !intent.desired_replica_nodes.contains(replica_node_id) {
            return Err(DfsError::Distributed {
                message: format!(
                    "DFS {label} response from undesired replica {replica_node_id} for intent {intent_id}"
                ),
            });
        }
        if &pending.object_id != object_id
            || &pending.version != version
            || pending.content_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: pending.content_hash.clone(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    fn handle_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            let reason = format!(
                "repair replica chunk transfer {} has no chunks",
                command.intent_id
            );
            return self.reject_repair_replica_chunk(requester_node_id, command, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS blob transfer",
            command.total_size_bytes,
            command.chunk_index,
            command.total_chunks,
            &command.bytes,
        ) {
            return self.reject_repair_replica_chunk(requester_node_id, command, error.to_string());
        }
        let transfer_key = blob_transfer_key(
            "repair",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        if !self.incoming_blob_transfers.contains_key(&transfer_key) {
            if let Err(error) = self.ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            ) {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if let Err(error) = self
                .ensure_replica_object_metadata(command.object_metadata.as_ref())
                .and_then(|()| {
                    self.ensure_replica_command_hash_matches_intent(
                        &command.intent_id,
                        &command.object_id,
                        &command.content_hash,
                    )
                })
                .and_then(|()| {
                    self.ensure_no_conflicting_pending_replica_write(
                        "repair",
                        &requester_node_id,
                        &command.intent_id,
                        &command.object_id,
                        &command.version,
                        &command.content_hash,
                    )
                })
                .and_then(|()| {
                    self.ensure_command_base_matches_current(
                        "repair",
                        &command.object_id,
                        command.expected_base_version.as_ref(),
                        &command.version,
                        &command.content_hash,
                    )
                })
            {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if self.local_object_version_manifest_matches(
                &command.object_id,
                &command.version,
                &command.content_hash,
                command.total_size_bytes,
            ) {
                let write_time = dfs_now_unix_nanos();
                let object = self.commit_local_metadata(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    write_time,
                )?;
                self.put_visible_object_in_metadata_backend(&object)?;
                let local_node_id = self.node_id.clone();
                self.put_metadata_receipt(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )?;
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        file_system_id: command.file_system_id,
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        replica_node_id: self.node_id.clone(),
                        result: DfsBlobTransferResult::Committed {
                            content_hash: command.content_hash.clone(),
                        },
                    }),
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash: command.content_hash,
                }));
            }
            let transfer = InProgressBlobTransfer::new(
                command.request_id.clone(),
                command.intent_id.clone(),
                command.file_system_id.clone(),
                command.object_id.clone(),
                command.expected_base_version.clone(),
                command.version.clone(),
                command.content_hash.clone(),
                command.total_size_bytes,
                command.total_chunks,
                command.object_metadata.clone(),
            )?;
            self.incoming_blob_transfers
                .insert(transfer_key.clone(), transfer);
        }
        let transfer = self
            .incoming_blob_transfers
            .get_mut(&transfer_key)
            .expect("transfer inserted");
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.expected_base_version != command.expected_base_version
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
        {
            self.incoming_blob_transfers.remove(&transfer_key);
            let reason = format!(
                "conflicting repair replica chunk metadata for {}",
                command.intent_id
            );
            return self.reject_repair_replica_chunk(requester_node_id, command, reason);
        }
        let bytes = match transfer.insert_chunk(
            command.chunk_index,
            command.total_chunks,
            command.bytes.clone(),
        ) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(error) => {
                self.incoming_blob_transfers.remove(&transfer_key);
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
        };
        let transfer = self
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        Ok(Some(self.handle_repair_replica(
            requester_node_id,
            DfsRepairReplicaCommand {
                request_id: transfer.request_id,
                intent_id: command.intent_id,
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id,
                expected_base_version: transfer.expected_base_version,
                version: transfer.version,
                content_hash: transfer.content_hash,
                bytes,
                object_metadata: transfer.object_metadata,
            },
        )?))
    }

    fn reject_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let transfer_key = blob_transfer_key(
            "repair",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        self.incoming_blob_transfers.remove(&transfer_key);
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version,
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: reason.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::RepairRejected {
            intent_id: command.intent_id,
            object_id: command.object_id,
            replica_node_id: self.node_id.clone(),
            reason,
        }))
    }

    fn handle_repair_replica_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsRepairReplicaResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS repair response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RepairRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            });
        }
        match response.result {
            DfsBlobTransferResult::Committed { ref content_hash }
            | DfsBlobTransferResult::Durable { ref content_hash } => {
                if let Err(error) =
                    self.ensure_repair_response_matches_pending(&response, content_hash)
                {
                    return Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    });
                }
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                self.coordinator.record_durable_receipt(
                    &response.intent_id,
                    response.replica_node_id.clone(),
                )?;
                self.mark_repaired_location(
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                self.clear_repair_retry(&response.intent_id, &response.replica_node_id);
                self.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    replica_node_id: response.replica_node_id,
                    content_hash: content_hash.clone(),
                })
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                match self.ensure_repair_response_request_matches_pending(&response) {
                    Ok(()) => {
                        self.record_repair_retry_scheduled(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        Ok(NetworkedDfsServiceEvent::RepairRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        })
                    }
                    Err(error) => Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }),
                }
            }
        }
    }

    fn handle_fetch_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsFetchBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_blob_fetch_authorized(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
            )
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.content
                    .read_manifest(command.object_id.as_str(), command.version.as_str())
                    .map_err(DfsError::from)
            })
            .and_then(|manifest| {
                if manifest.content_hash != command.content_hash {
                    return Err(DfsError::ContentHashMismatch {
                        expected: command.content_hash.clone(),
                        actual: manifest.content_hash,
                    });
                }
                let bytes = self.content.read_blob(&command.content_hash)?;
                Ok(bytes)
            }) {
            Ok(bytes) => DfsFetchBlobResult::Found {
                content_hash: command.content_hash.clone(),
                bytes,
            },
            Err(DfsError::ContentStore { .. }) => DfsFetchBlobResult::NotFound,
            Err(error) => DfsFetchBlobResult::Rejected {
                reason: error.to_string(),
            },
        };
        match &result {
            DfsFetchBlobResult::Found { bytes, .. }
                if bytes.len() > DFS_BLOB_INLINE_LIMIT_BYTES =>
            {
                self.send_fetch_blob_response_chunks(&requester_node_id, &command, bytes)?;
            }
            _ => {
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::FetchBlobResponse(DfsFetchBlobResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        source_node_id: self.node_id.clone(),
                        result: result.clone(),
                    }),
                )?;
            }
        }
        Ok(match result {
            DfsFetchBlobResult::Found { content_hash, .. } => {
                NetworkedDfsServiceEvent::BlobFetched {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    source_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsFetchBlobResult::NotFound => NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: command.intent_id,
                object_id: command.object_id,
                source_node_id: self.node_id.clone(),
                reason: "blob not found".to_string(),
            },
            DfsFetchBlobResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    source_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    fn send_fetch_blob_response_chunks(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count(bytes)?;
        for (chunk_index, chunk) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    source_node_id: self.node_id.clone(),
                    content_hash: command.content_hash.clone(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    bytes: chunk.to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn handle_fetch_blob_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsFetchBlobResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.source_node_id {
            let reason = format!(
                "DFS fetch response source mismatch: frame source {}, payload source {}",
                frame_source_node_id, response.source_node_id
            );
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason,
            });
        }
        let Some(pending) = self.pending_writes.get(&response.intent_id).cloned() else {
            let reason = format!(
                "distributed DFS error: missing pending write for fetched blob: {}",
                response.intent_id
            );
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: response.source_node_id,
                reason,
            });
        };
        if !pending.fetch_inflight_from.contains(&frame_source_node_id) {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            });
        }
        if pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id)
            != Some(&response.request_id)
        {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response request id".to_string(),
            });
        }
        if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
            pending.fetch_inflight_from.remove(&frame_source_node_id);
            pending
                .fetch_inflight_request_ids
                .remove(&frame_source_node_id);
            pending
                .fetch_inflight_started_ticks
                .remove(&frame_source_node_id);
        }
        match response.result {
            DfsFetchBlobResult::Found {
                content_hash,
                bytes,
            } => {
                if pending.object_id != response.object_id
                    || pending.version != response.version
                    || pending.content_hash != content_hash
                {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                    let reason = DfsError::ContentHashMismatch {
                        expected: pending.content_hash,
                        actual: content_hash,
                    }
                    .to_string();
                    return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        source_node_id: response.source_node_id,
                        reason,
                    });
                }
                if let Some(local_size_bytes) = self.local_object_version_manifest_size_if_matches(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                ) {
                    self.record_fetched_pending_write_size(
                        &response.intent_id,
                        &response.object_id,
                        &response.version,
                        &content_hash,
                        local_size_bytes,
                    )?;
                    self.wake_pending_retries_after_fetch_success(&response.intent_id);
                    return Ok(NetworkedDfsServiceEvent::BlobFetched {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        version: response.version,
                        source_node_id: frame_source_node_id,
                        content_hash,
                    });
                }
                if let Err(error) = self.store_command_bytes(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                    &bytes,
                ) {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                    return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        source_node_id: response.source_node_id,
                        reason: error.to_string(),
                    });
                }
                self.record_fetched_pending_write_size(
                    &response.intent_id,
                    &response.object_id,
                    &response.version,
                    &content_hash,
                    bytes.len() as u64,
                )?;
                self.wake_pending_retries_after_fetch_success(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::BlobFetched {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    source_node_id: frame_source_node_id,
                    content_hash,
                })
            }
            DfsFetchBlobResult::NotFound => {
                self.record_fetch_failed_source_and_wake_retries(
                    &response.intent_id,
                    &response.source_node_id,
                );
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason: "blob not found".to_string(),
                })
            }
            DfsFetchBlobResult::Rejected { reason } => {
                self.record_fetch_failed_source_and_wake_retries(
                    &response.intent_id,
                    &response.source_node_id,
                );
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason,
                })
            }
        }
    }

    fn handle_fetch_blob_response_chunk(
        &mut self,
        frame_source_node_id: NodeId,
        chunk: DfsFetchBlobResponseChunk,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if frame_source_node_id != chunk.source_node_id {
            let reason = format!(
                "DFS fetch response source mismatch: frame source {}, payload source {}",
                frame_source_node_id, chunk.source_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason,
            }));
        }
        let Some(pending) = self.pending_writes.get(&chunk.intent_id).cloned() else {
            let reason = format!(
                "distributed DFS error: missing pending write for fetched blob chunk: {}",
                chunk.intent_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: chunk.source_node_id,
                reason,
            }));
        };
        if !pending.fetch_inflight_from.contains(&frame_source_node_id) {
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            }));
        }
        if pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id)
            != Some(&chunk.request_id)
        {
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response request id".to_string(),
            }));
        }
        if pending.object_id != chunk.object_id
            || pending.version != chunk.version
            || pending.content_hash != chunk.content_hash
        {
            let reason = DfsError::ContentHashMismatch {
                expected: pending.content_hash,
                actual: chunk.content_hash.clone(),
            }
            .to_string();
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if let Some(local_size_bytes) = self.local_object_version_manifest_size_if_matches(
            &chunk.object_id,
            &chunk.version,
            &chunk.content_hash,
        ) {
            self.record_fetched_pending_write_size(
                &chunk.intent_id,
                &chunk.object_id,
                &chunk.version,
                &chunk.content_hash,
                local_size_bytes,
            )?;
            self.clear_fetch_response_transfer_state(
                &chunk.intent_id,
                &chunk.source_node_id,
                &chunk.request_id,
            );
            self.wake_pending_retries_after_fetch_success(&chunk.intent_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                version: chunk.version,
                source_node_id: frame_source_node_id,
                content_hash: chunk.content_hash,
            }));
        }
        if chunk.total_chunks == 0 {
            let reason = format!(
                "chunked DFS fetch response {} has no chunks",
                chunk.request_id
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if pending.size_bytes != 0 && chunk.total_size_bytes != pending.size_bytes {
            let reason = format!(
                "chunked DFS fetch response size does not match pending write: expected {}, actual {}",
                pending.size_bytes, chunk.total_size_bytes
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }

        if !self
            .incoming_fetch_transfers
            .contains_key(&chunk.request_id)
        {
            let transfer = match InProgressFetchBlobTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk.content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
            self.incoming_fetch_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_fetch_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk.content_hash
            || transfer.total_size_bytes != chunk.total_size_bytes
        {
            let reason = format!(
                "conflicting DFS fetch response chunk metadata for {}",
                chunk.request_id
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        let request_id = chunk.request_id.clone();
        let intent_id = chunk.intent_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let source_node_id = chunk.source_node_id.clone();
        let content_hash = chunk.content_hash.clone();
        let bytes =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes.clone())
            {
                Ok(Some(bytes)) => bytes,
                Ok(None) => return Ok(None),
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
        self.incoming_fetch_transfers.remove(&request_id);
        if let Some(pending) = self.pending_writes.get_mut(&intent_id) {
            pending.fetch_inflight_from.remove(&source_node_id);
            pending.fetch_inflight_request_ids.remove(&source_node_id);
            pending.fetch_inflight_started_ticks.remove(&source_node_id);
        }
        if let Some(local_size_bytes) =
            self.local_object_version_manifest_size_if_matches(&object_id, &version, &content_hash)
        {
            self.record_fetched_pending_write_size(
                &intent_id,
                &object_id,
                &version,
                &content_hash,
                local_size_bytes,
            )?;
            self.wake_pending_retries_after_fetch_success(&intent_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
                intent_id,
                object_id,
                version,
                source_node_id,
                content_hash,
            }));
        }
        if let Err(error) = self.store_command_bytes(&object_id, &version, &content_hash, &bytes) {
            self.record_fetch_failed_source_and_wake_retries(&intent_id, &source_node_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id,
                object_id,
                source_node_id,
                reason: error.to_string(),
            }));
        }
        self.record_fetched_pending_write_size(
            &intent_id,
            &object_id,
            &version,
            &content_hash,
            bytes.len() as u64,
        )?;
        self.wake_pending_retries_after_fetch_success(&intent_id);
        Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
            intent_id,
            object_id,
            version,
            source_node_id,
            content_hash,
        }))
    }

    fn reject_fetch_response_chunk_and_fail_source(
        &mut self,
        chunk: DfsFetchBlobResponseChunk,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        self.clear_fetch_response_transfer_state(
            &chunk.intent_id,
            &chunk.source_node_id,
            &chunk.request_id,
        );
        self.record_fetch_failed_source_and_wake_retries(&chunk.intent_id, &chunk.source_node_id);
        Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
            intent_id: chunk.intent_id,
            object_id: chunk.object_id,
            source_node_id: chunk.source_node_id,
            reason,
        }))
    }

    fn record_fetched_pending_write_size(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<(), DfsError> {
        if let Some(pending) = self.pending_writes.get_mut(intent_id)
            && pending.size_bytes == 0
        {
            pending.size_bytes = size_bytes;
        }
        if let Some(backend) = self.metadata_backend.as_mut()
            && let Some(mut version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash.as_deref() == Some(content_hash)
            && version_record.size_bytes != Some(size_bytes)
        {
            version_record.size_bytes = Some(size_bytes);
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    fn wake_pending_retries_after_fetch_success(&mut self, intent_id: &str) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            for state in pending.repair_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            pending.fetch_failed_from.clear();
        }
    }

    fn clear_fetch_response_transfer_state(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
        request_id: &str,
    ) {
        self.incoming_fetch_transfers.remove(request_id);
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_request_ids.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
        }
    }

    fn record_fetch_failed_source_and_wake_retries(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
    ) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_request_ids.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            pending.fetch_failed_from.insert(source_node_id.clone());
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.repair_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
        }
    }

    fn handle_networked_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsReadRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self.current_read_object_metadata(&request.object_id) {
            Ok(object) if !object.current_locations.contains(&self.node_id) => {
                DfsReadResult::Rejected {
                    reason: format!(
                        "node {} is not a current location for DFS object {}",
                        self.node_id, request.object_id
                    ),
                }
            }
            Ok(_) => match self.read_local_object_with_state(&request.object_id, false)? {
                NetworkedDfsReadOutcome::Found { data, .. } => DfsReadResult::Found(data),
                NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => DfsReadResult::Rejected {
                    reason: read_conflicts_reason(conflicts),
                },
                NetworkedDfsReadOutcome::PendingReplication { .. }
                | NetworkedDfsReadOutcome::NotConfirmed { .. }
                | NetworkedDfsReadOutcome::NotFound => DfsReadResult::NotFound,
            },
            Err(DfsError::UnknownObject { .. }) => DfsReadResult::NotFound,
            Err(error) => return Err(error),
        };
        match &result {
            DfsReadResult::Found(data) if data.bytes.len() > DFS_BLOB_INLINE_LIMIT_BYTES => {
                self.send_networked_read_response_chunks(&requester_node_id, &request, data)?;
            }
            _ => {
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::ReadResponse(DfsReadResponse {
                        request_id: request.request_id.clone(),
                        object_id: request.object_id.clone(),
                        result: result.clone(),
                    }),
                )?;
            }
        }
        Ok(NetworkedDfsServiceEvent::RemoteReadServed {
            request_id: request.request_id,
            object_id: request.object_id,
            requester_node_id,
            result,
        })
    }

    fn send_networked_read_response_chunks(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count(&data.bytes)?;
        for (chunk_index, chunk) in data.bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    total_size_bytes: data.bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    bytes: chunk.to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn handle_networked_read_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsReadResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let Some(pending) = self.pending_reads.get(&response.request_id).cloned() else {
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unknown DFS read request".to_string(),
                },
            });
        };
        if source_node_id != pending.target_node_id {
            let reason = format!(
                "unexpected DFS read response source: expected {}, actual {}",
                pending.target_node_id, source_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            });
        }
        self.pending_reads.remove(&response.request_id);
        if pending.object_id != response.object_id {
            self.incoming_read_transfers.remove(&response.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id.clone(),
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "unexpected DFS read response object: expected {}, actual {}",
                        pending.object_id, response.object_id
                    ),
                },
            });
        }
        match &response.result {
            DfsReadResult::Found(data) => {
                if data.object_id != pending.object_id {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "unexpected DFS read response payload object: expected {}, actual {}",
                                pending.object_id, data.object_id
                            ),
                        },
                    });
                }
                if self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                if !self.pending_read_source_still_current(&pending, &source_node_id)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason:
                                "stale DFS read response: source is no longer a current location"
                                    .to_string(),
                        },
                    });
                }
                if data.version != pending.version {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS read response: expected version {}, actual {}",
                                pending.version, data.version
                            ),
                        },
                    });
                }
                if data.content_hash != pending.content_hash {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS read response: expected content hash {:?}, actual {:?}",
                                pending.content_hash, data.content_hash
                            ),
                        },
                    });
                }
                if let Err(error) = self.store_remote_read_data(&source_node_id, data) {
                    if matches!(error, DfsError::ContentHashMismatch { .. }) {
                        self.mark_remote_read_location_missing(
                            &pending.object_id,
                            &pending.version,
                            &pending.content_hash,
                            &source_node_id,
                        )?;
                        return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                            request_id: response.request_id,
                            object_id: response.object_id,
                            source_node_id,
                            result: DfsReadResult::Rejected {
                                reason: error.to_string(),
                            },
                        });
                    }
                    return Err(error);
                }
                self.clear_pending_remote_reads_for_object_version(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                );
            }
            DfsReadResult::NotFound => {
                if self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
            }
            DfsReadResult::Rejected { reason } => {
                if remote_read_rejection_prunes_location(reason) {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                }
            }
        }
        Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: response.request_id,
            object_id: response.object_id,
            source_node_id,
            result: response.result,
        })
    }

    fn mark_remote_read_location_missing(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
        source_node_id: &NodeId,
    ) -> Result<(), DfsError> {
        if let Some(object) = self.metadata.objects.get_mut(object_id) {
            if object.version == *version && object.content_hash == *content_hash {
                object
                    .current_locations
                    .retain(|node_id| node_id != source_node_id);
            }
        }
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(mut object) = backend.get_object(object_id)?
            && object.version == *version
            && object.content_hash == *content_hash
        {
            if !object.current_locations.contains(source_node_id) {
                None
            } else {
                object
                    .current_locations
                    .retain(|node_id| node_id != source_node_id);
                Some(object)
            }
        } else {
            None
        };
        let backend_version = if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(mut version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash == *content_hash
        {
            if !version_record.locations.contains(source_node_id) {
                None
            } else {
                version_record
                    .locations
                    .retain(|node_id| node_id != source_node_id);
                Some(version_record)
            }
        } else {
            None
        };
        if let Some(object) = backend_object {
            self.put_visible_object_in_metadata_backend(&object)?;
        } else if let Some(version_record) = backend_version
            && let Some(backend) = self.metadata_backend.as_mut()
        {
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    fn handle_networked_read_response_chunk(
        &mut self,
        source_node_id: NodeId,
        chunk: DfsReadResponseChunk,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let Some(pending) = self.pending_reads.get(&chunk.request_id).cloned() else {
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unknown DFS read request".to_string(),
                },
            }));
        };
        if source_node_id != pending.target_node_id {
            let reason = format!(
                "unexpected DFS read response source: expected {}, actual {}",
                pending.target_node_id, source_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            }));
        }
        if pending.object_id != chunk.object_id {
            let reason = format!(
                "unexpected DFS read response object: expected {}, actual {}",
                pending.object_id, chunk.object_id
            );
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            }));
        }
        if pending.version != chunk.version || pending.content_hash != chunk.content_hash {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response chunk".to_string(),
                },
            }));
        }
        if self.pending_read_metadata_advanced(&pending)? {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response: object metadata advanced".to_string(),
                },
            }));
        }
        if !self.pending_read_source_still_current(&pending, &source_node_id)? {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response: source is no longer a current location"
                        .to_string(),
                },
            }));
        }
        if let Some(expected_size) = pending.content_size_bytes
            && expected_size != chunk.total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS read response size does not match object metadata: expected {expected_size}, actual {}",
                        chunk.total_size_bytes
                    ),
                },
            }));
        }
        if chunk.total_chunks == 0 {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS read response {} has no chunks",
                        chunk.request_id
                    ),
                },
            }));
        }

        if !self.incoming_read_transfers.contains_key(&chunk.request_id) {
            let transfer = match InProgressReadTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk.content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            self.incoming_read_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_read_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk.content_hash
            || transfer.total_size_bytes != chunk.total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "conflicting DFS read response chunk metadata for {}",
                        chunk.request_id
                    ),
                },
            }));
        }
        let request_id = chunk.request_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let content_hash = chunk.content_hash.clone();
        let bytes = match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes)
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(error) => {
                self.clear_read_response_transfer_state(&chunk.request_id);
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id: chunk.request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
        };
        self.incoming_read_transfers.remove(&request_id);
        self.pending_reads.remove(&request_id);
        let data = DfsReadData {
            object_id: object_id.clone(),
            version,
            content_hash,
            bytes,
        };
        if let Err(error) = self.store_remote_read_data(&source_node_id, &data) {
            if matches!(error, DfsError::ContentHashMismatch { .. }) {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            return Err(error);
        }
        self.clear_pending_remote_reads_for_object_version(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id,
            object_id,
            source_node_id,
            result: DfsReadResult::Found(data),
        }))
    }

    fn clear_read_response_transfer_state(&mut self, request_id: &str) {
        self.pending_reads.remove(request_id);
        self.incoming_read_transfers.remove(request_id);
    }

    fn pending_read_metadata_advanced(
        &self,
        pending: &PendingNetworkedRead,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        Ok(object.version != pending.version
            || object.content_hash != pending.content_hash
            || object.content_size_bytes != pending.content_size_bytes)
    }

    fn pending_read_source_still_current(
        &self,
        pending: &PendingNetworkedRead,
        source_node_id: &NodeId,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        Ok(object.version == pending.version
            && object.content_hash == pending.content_hash
            && object.content_size_bytes == pending.content_size_bytes
            && object.current_locations.contains(source_node_id))
    }

    fn clear_pending_remote_reads_for_object_version(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
    ) {
        let request_ids: Vec<String> = self
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == *object_id
                    && pending.version == *version
                    && pending.content_hash == *content_hash
            })
            .map(|(request_id, _)| request_id.clone())
            .collect();
        for request_id in request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
        }
    }

    fn store_remote_read_data(
        &mut self,
        _source_node_id: &NodeId,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(&data.object_id)?;
        if object.version != data.version {
            return Err(DfsError::ContentHashMismatch {
                expected: object.version.to_string(),
                actual: data.version.to_string(),
            });
        }
        let Some(expected_hash) = object.content_hash.clone() else {
            return Err(DfsError::ContentHashMismatch {
                expected: "committed content hash".to_string(),
                actual: data
                    .content_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            });
        };
        if let Some(actual_hash) = data.content_hash.as_ref()
            && actual_hash != &expected_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash.clone(),
            });
        }
        self.store_command_bytes(&data.object_id, &data.version, &expected_hash, &data.bytes)?;
        let mut object = object;
        object.content_size_bytes = Some(data.bytes.len() as u64);
        self.put_object_version_size_in_metadata_backend(&object, Some(data.bytes.len() as u64))?;
        self.metadata.add_object(object)?;
        Ok(())
    }

    fn retry_pending_stages(&mut self) -> Result<(), DfsError> {
        let pending_intent_ids = self.pending_writes.keys().cloned().collect::<Vec<_>>();
        for intent_id in pending_intent_ids {
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            let Some(pending) = self.pending_writes.get(&intent_id).cloned() else {
                continue;
            };
            let durable_nodes = self
                .coordinator
                .receipts_for_intent(&intent_id)
                .into_iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>();
            let stage_targets = intent
                .desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id && !durable_nodes.contains(*node_id))
                .filter(|node_id| self.stage_retry_is_due(&intent_id, node_id))
                .cloned()
                .collect::<Vec<_>>();
            if stage_targets.is_empty() {
                continue;
            }
            let bytes = match self.content.read_blob(&pending.content_hash) {
                Ok(bytes) => bytes,
                Err(_) => {
                    let fetch_requested =
                        self.request_stage_blob_from_alternate(&intent, &pending)?;
                    if !fetch_requested {
                        for replica_node_id in stage_targets {
                            self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                        }
                    }
                    continue;
                }
            };
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            for replica_node_id in stage_targets {
                let request_id = self.next_request_id();
                let sent = self.send_stage_blob_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &intent.intent_id,
                    &intent.file_system_id,
                    &intent.object_id,
                    intent.base_version.clone(),
                    &intent.target_version,
                    &pending.content_hash,
                    &bytes,
                    object_metadata.clone(),
                )?;
                if sent {
                    self.record_stage_inflight(&intent_id, &replica_node_id, request_id);
                } else {
                    self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                }
            }
        }
        Ok(())
    }

    fn request_stage_blob_from_alternate(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        let mut skipped_failed_source = false;
        for source_node_id in self.stage_blob_sources(&intent.intent_id) {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = self.next_request_id();
            let sent = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: intent.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                }),
            )?;
            if sent {
                self.record_fetch_inflight(&intent.intent_id, source_node_id, request_id);
                return Ok(true);
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.pending_writes.get_mut(&intent.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    fn stage_blob_sources(&self, intent_id: &str) -> Vec<NodeId> {
        self.coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .filter_map(|receipt| {
                (receipt.replica_node_id != self.node_id).then_some(receipt.replica_node_id.clone())
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn record_fetch_inflight(
        &mut self,
        intent_id: &str,
        source_node_id: NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.insert(source_node_id.clone());
            pending
                .fetch_inflight_request_ids
                .insert(source_node_id.clone(), request_id);
            pending
                .fetch_inflight_started_ticks
                .insert(source_node_id, tick);
        }
    }

    fn expire_stale_fetch_requests(&mut self) {
        let tick = self.repair_retry_tick;
        for pending in self.pending_writes.values_mut() {
            let expired_sources = pending
                .fetch_inflight_started_ticks
                .iter()
                .filter_map(|(source_node_id, started_tick)| {
                    let expires_at = started_tick.saturating_add(fetch_inflight_timeout_ticks());
                    (expires_at <= tick).then_some(source_node_id.clone())
                })
                .collect::<Vec<_>>();
            if expired_sources.is_empty() {
                continue;
            }
            for source_node_id in expired_sources {
                pending.fetch_inflight_from.remove(&source_node_id);
                pending.fetch_inflight_request_ids.remove(&source_node_id);
                pending.fetch_inflight_started_ticks.remove(&source_node_id);
                pending.fetch_failed_from.insert(source_node_id);
            }
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.repair_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
        }
    }

    fn stage_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.pending_writes
            .get(intent_id)
            .and_then(|pending| pending.stage_retries.get(replica_node_id))
            .is_none_or(|state| state.next_retry_tick <= self.repair_retry_tick)
    }

    fn record_stage_inflight(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick =
                tick.saturating_add(stage_inflight_retry_delay_ticks(state.attempts));
            pending
                .stage_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
        }
    }

    fn record_stage_retry_scheduled(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            if !state.inflight {
                state.attempts = state.attempts.saturating_add(1);
            }
            state.inflight = false;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
            pending.stage_inflight_request_ids.remove(replica_node_id);
        }
    }

    fn repair_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.pending_writes
            .get(intent_id)
            .and_then(|pending| pending.repair_retries.get(replica_node_id))
            .is_none_or(|state| state.next_retry_tick <= self.repair_retry_tick)
    }

    fn record_repair_inflight(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
            pending
                .repair_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
        }
    }

    fn record_repair_retry_scheduled(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if !state.inflight {
                state.attempts = state.attempts.saturating_add(1);
            }
            state.inflight = false;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
            pending.repair_inflight_request_ids.remove(replica_node_id);
        }
    }

    fn clear_repair_retry(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.repair_retries.remove(replica_node_id);
            pending.repair_inflight_request_ids.remove(replica_node_id);
        }
    }

    fn request_repair_blob_from_alternate(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        let mut skipped_failed_source = false;
        for source_node_id in self.repair_blob_sources(job, pending)? {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = self.next_request_id();
            let sent = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: job.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                }),
            )?;
            if sent {
                self.record_fetch_inflight(&job.intent_id, source_node_id, request_id);
                return Ok(true);
            }
        }
        if skipped_failed_source && let Some(pending) = self.pending_writes.get_mut(&job.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    fn repair_blob_sources(
        &self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<Vec<NodeId>, DfsError> {
        let mut candidates = BTreeSet::new();
        for receipt in self.coordinator.receipts_for_intent(&job.intent_id) {
            if receipt.replica_node_id != self.node_id
                && receipt.replica_node_id != job.missing_replica_node_id
            {
                candidates.insert(receipt.replica_node_id.clone());
            }
        }
        match self.current_read_object_metadata(&pending.object_id) {
            Ok(object) => {
                for node_id in &object.current_locations {
                    if node_id != &self.node_id && node_id != &job.missing_replica_node_id {
                        candidates.insert(node_id.clone());
                    }
                }
            }
            Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(candidates.into_iter().collect())
    }

    fn apply_committed_intent(&mut self, intent_id: &str) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Err(DfsError::Distributed {
                message: format!("unknown committed intent: {intent_id}"),
            });
        };
        self.ensure_intent_epoch_current(&intent)?;
        let pending =
            self.pending_writes
                .get(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing pending write for committed intent: {intent_id}"),
                })?;
        let write_time = dfs_now_unix_nanos();
        self.commit_visible_version_in_metadata_backend(&intent, &pending, write_time)?;
        self.put_metadata_intent(&intent, Some(&pending.content_hash))?;
        self.commit_local_metadata(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
            write_time,
        )?;

        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        if let Some(object) = self.metadata.objects.get_mut(&pending.object_id) {
            object.current_locations = durable_nodes.iter().cloned().collect();
        }

        let commit_targets: Vec<NodeId> = durable_nodes
            .iter()
            .filter(|node_id| *node_id != &self.node_id)
            .cloned()
            .collect();
        let committed_metadata = Some(self.current_read_object_metadata(&pending.object_id)?);
        for replica_node_id in commit_targets {
            let request_id = self.next_request_id();
            self.send_best_effort_message_if_routable(
                &replica_node_id,
                DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: intent.file_system_id.clone(),
                    object_id: pending.object_id.clone(),
                    expected_base_version: intent.base_version.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    object_metadata: committed_metadata.clone(),
                }),
            )?;
        }

        let repair_targets: Vec<NodeId> = self
            .coordinator
            .repair_jobs()
            .iter()
            .filter(|job| job.intent_id == intent_id)
            .map(|job| job.missing_replica_node_id.clone())
            .collect();
        for replica_node_id in repair_targets {
            if replica_node_id == self.node_id
                || !self.repair_retry_is_due(intent_id, &replica_node_id)
            {
                continue;
            }
            let bytes = match self.content.read_blob(&pending.content_hash) {
                Ok(bytes) => bytes,
                Err(_) => {
                    self.record_repair_retry_scheduled(intent_id, &replica_node_id);
                    continue;
                }
            };
            let request_id = self.next_request_id();
            let sent = self.send_repair_replica_if_routable(
                &replica_node_id,
                request_id.clone(),
                intent_id,
                &intent.file_system_id,
                &pending.object_id,
                intent.base_version.clone(),
                &pending.version,
                &pending.content_hash,
                &bytes,
                committed_metadata.clone(),
            )?;
            if !sent {
                self.record_repair_retry_scheduled(intent_id, &replica_node_id);
            } else {
                self.record_repair_inflight(intent_id, &replica_node_id, request_id);
            }
        }
        Ok(())
    }

    fn commit_local_metadata(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        write_time_unix_nanos: u64,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let content_size_bytes = self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
            .ok()
            .filter(|manifest| manifest.content_hash == content_hash)
            .map(|manifest| manifest.size_bytes);
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(backend_object) = backend.get_object(object_id)?
        {
            self.metadata.add_object(backend_object)?;
        }
        let object =
            self.metadata
                .objects
                .get_mut(object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
        let content_unchanged = object
            .content_hash
            .as_ref()
            .is_some_and(|existing| existing == content_hash);
        let already_current = object.version == *version && content_unchanged;
        object.version = version.clone();
        object.content_hash = Some(content_hash.to_string());
        if content_size_bytes.is_some() {
            object.content_size_bytes = content_size_bytes;
        }
        if !content_unchanged {
            object.modified_at_unix_nanos = write_time_unix_nanos;
            object.changed_at_unix_nanos = write_time_unix_nanos;
        }
        if !already_current {
            object.last_modified_by_event_id = Some(EventId::generate());
        }
        if !object.current_locations.contains(&self.node_id) {
            object.current_locations.push(self.node_id.clone());
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        let object = object.clone();
        self.track_metadata_tombstone(&object);
        Ok(object.clone())
    }

    fn store_conflicting_write_content_hash(
        &self,
        request: &DfsWriteRequest,
        bytes: &[u8],
    ) -> Result<Option<String>, DfsError> {
        match self.content.put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            bytes,
        ) {
            Ok(manifest) => Ok(Some(manifest.content_hash)),
            Err(ContentStoreError::VersionHashConflict { new_hash, .. }) => Ok(Some(new_hash)),
            Err(error) => Err(error.into()),
        }
    }

    fn ensure_replicated_confirmed_object(&self, object_id: &DfsObjectId) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedConfirmed {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }
        Ok(())
    }

    fn confirmed_locations_for_object(
        &self,
        object: &DfsObjectMetadata,
        required_replica_node_ids: &[NodeId],
    ) -> Result<Vec<NodeId>, DfsError> {
        let mut confirmed_locations: BTreeSet<NodeId> = object
            .current_locations
            .iter()
            .filter(|node_id| required_replica_node_ids.contains(node_id))
            .cloned()
            .collect();

        if let Some(backend) = self.metadata_backend.as_ref() {
            confirmed_locations.clear();
            let receipts = backend.receipts_for_version(&object.object_id, &object.version)?;
            for receipt in receipts {
                if !required_replica_node_ids.contains(&receipt.replica_node_id) {
                    continue;
                }
                if let Some(expected_hash) = object.content_hash.as_deref() {
                    if receipt.content_hash.as_deref() != Some(expected_hash) {
                        continue;
                    }
                }
                confirmed_locations.insert(receipt.replica_node_id);
            }
        }

        Ok(confirmed_locations.into_iter().collect())
    }

    fn mark_repaired_location(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let mut object = self.current_read_object_metadata(object_id)?;
        if &object.version != version {
            return Ok(());
        }
        if let Some(expected_hash) = object.content_hash.as_deref() {
            if expected_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: content_hash.to_string(),
                });
            }
        }
        if object.current_locations.contains(replica_node_id) {
            return Ok(());
        }
        object.current_locations.push(replica_node_id.clone());
        self.metadata.add_object(object.clone())?;
        self.put_visible_object_in_metadata_backend(&object)?;
        Ok(())
    }

    fn put_metadata_receipt(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        self.ensure_metadata_receipt_accepts(object_id, version, replica_node_id, content_hash)?;
        let inserted = {
            let Some(backend) = self.metadata_backend.as_mut() else {
                return Ok(false);
            };
            let mut existing_matching_receipt = false;
            for receipt in backend.receipts_for_version(object_id, version)? {
                if receipt.replica_node_id != *replica_node_id {
                    continue;
                }
                if receipt.content_hash.as_deref() == Some(content_hash) {
                    existing_matching_receipt = true;
                    break;
                }
            }
            if existing_matching_receipt {
                false
            } else {
                backend.put_receipt(&DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        committed_version: version.clone(),
                        owner_node_id: self.node_id.clone(),
                        event_id: EventId::generate(),
                    },
                    replica_node_id: replica_node_id.clone(),
                    content_hash: Some(content_hash.to_string()),
                    received_at: OffsetDateTime::now_utc(),
                })?;
                true
            }
        };
        match self.mark_repaired_location(object_id, version, replica_node_id, content_hash) {
            Ok(()) | Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(inserted)
    }

    fn ensure_metadata_receipt_accepts(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        for receipt in backend.receipts_for_version(object_id, version)? {
            if receipt.replica_node_id != *replica_node_id {
                continue;
            }
            if receipt.content_hash.as_deref() == Some(content_hash) {
                return Ok(());
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "conflicting DFS replica receipt for {object_id} {version} from {replica_node_id}: existing {:?}, new {content_hash}",
                    receipt.content_hash
                ),
            });
        }
        Ok(())
    }

    fn put_metadata_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: Option<&str>,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let mut record = write_intent_record(intent, content_hash);
        if let Some(existing) = backend.get_intent(&intent.object_id, &intent.intent_id)? {
            record.created_at = existing.created_at;
            let mut comparable_record = record.clone();
            comparable_record.updated_at = existing.updated_at;
            if comparable_record == existing {
                return Ok(());
            }
        }
        backend.put_intent(&record)?;
        Ok(())
    }

    fn put_visible_object_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let mut object = object.clone();
        normalize_current_locations(&mut object.current_locations);
        let size_bytes = self.object_content_size_hint(&object)?;
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        if backend.get_file_system(&object.file_system_id)?.is_none() {
            backend.put_file_system(&DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            })?;
        }
        if backend.get_object(&object.object_id)?.as_ref() != Some(&object) {
            backend.put_object(&object)?;
        }
        put_visible_version_record_if_changed(
            backend.as_mut(),
            object_version_record(&object, size_bytes),
        )?;
        Ok(())
    }

    fn put_object_version_size_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        put_visible_version_record_if_changed(
            backend.as_mut(),
            object_version_record(object, size_bytes),
        )?;
        Ok(())
    }

    pub(crate) fn object_content_size_hint(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<u64>, DfsError> {
        if !matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink) {
            return Ok(None);
        }
        if let Ok(manifest) = self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            return Ok(Some(manifest.size_bytes));
        }
        if object.content_size_bytes.is_some() {
            return Ok(object.content_size_bytes);
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(version) = backend.get_version(&object.object_id, &object.version)?
            && version.size_bytes.is_some()
        {
            return Ok(version.size_bytes);
        }
        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Some(manifest) = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .find(|manifest| &manifest.content_hash == expected_hash)
        {
            return Ok(Some(manifest.size_bytes));
        }
        if let Some(backend) = self.metadata_backend.as_ref() {
            if let Some(expected_hash) = object.content_hash.as_ref()
                && let Some(version) = backend
                    .versions_for_object(&object.object_id)?
                    .into_iter()
                    .rev()
                    .find(|version| version.content_hash.as_ref() == Some(expected_hash))
            {
                return Ok(version.size_bytes);
            }
        }
        Ok(None)
    }

    fn commit_visible_version_in_metadata_backend(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        write_time_unix_nanos: u64,
    ) -> Result<(), DfsError> {
        let expected_object = self.current_object_for_write(&pending.object_id)?;
        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        let durable_locations: Vec<NodeId> = durable_nodes.iter().cloned().collect();
        if expected_object.version == pending.version
            && expected_object.content_hash.as_deref() == Some(pending.content_hash.as_str())
        {
            let mut current_locations = expected_object.current_locations.clone();
            normalize_current_locations(&mut current_locations);
            let size_is_current = pending.size_bytes == 0
                || expected_object.content_size_bytes == Some(pending.size_bytes);
            if current_locations == durable_locations && size_is_current {
                return Ok(());
            }
            let mut repaired_object = expected_object.clone();
            if pending.size_bytes != 0 {
                repaired_object.content_size_bytes = Some(pending.size_bytes);
            }
            repaired_object.current_locations = durable_locations;
            self.put_visible_object_in_metadata_backend(&repaired_object)?;
            self.metadata.add_object(repaired_object)?;
            return Ok(());
        }
        let mut committed_object = expected_object.clone();
        committed_object.version = pending.version.clone();
        committed_object.content_hash = Some(pending.content_hash.clone());
        committed_object.content_size_bytes = Some(pending.size_bytes);
        committed_object.modified_at_unix_nanos = write_time_unix_nanos;
        committed_object.changed_at_unix_nanos = write_time_unix_nanos;
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        committed_object.current_locations = durable_locations;
        let visible_version = object_version_record(&committed_object, Some(pending.size_bytes));
        let required_replica_node_ids: Vec<NodeId> = durable_nodes.into_iter().collect();
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        if backend.get_object(&pending.object_id)?.is_none() {
            backend.apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: namespace_transaction_id_for_object(
                    "create-visible",
                    &pending.object_id,
                    &pending.version,
                ),
                file_system_id: committed_object.file_system_id.clone(),
                objects: vec![committed_object.clone()],
                created_at: OffsetDateTime::now_utc(),
            })?;
            backend.put_version(&visible_version)?;
            return Ok(());
        }
        backend.commit_visible_version(
            &expected_object.version,
            &committed_object,
            &visible_version,
            &required_replica_node_ids,
        )?;
        Ok(())
    }

    fn store_command_bytes(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let actual_hash = content_hash(bytes);
        if actual_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash,
            });
        }
        let manifest =
            self.content
                .put_object_version(object_id.as_str(), version.as_str(), bytes)?;
        if manifest.content_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        Ok(())
    }

    fn local_object_version_manifest_matches(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        expected_size_bytes: u64,
    ) -> bool {
        self.content
            .read_manifest(object_id.as_str(), version.as_str())
            .is_ok_and(|manifest| {
                manifest.content_hash == expected_hash
                    && manifest.size_bytes == expected_size_bytes
                    && self.content.has_blob(expected_hash).unwrap_or(false)
            })
    }

    fn local_object_version_manifest_size_if_matches(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
    ) -> Option<u64> {
        self.content
            .read_manifest(object_id.as_str(), version.as_str())
            .ok()
            .filter(|manifest| {
                manifest.content_hash == expected_hash
                    && self.content.has_blob(expected_hash).unwrap_or(false)
            })
            .map(|manifest| manifest.size_bytes)
    }

    fn ensure_replica_object_metadata(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let Some(object) = object else {
            return Ok(());
        };
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            self.add_file_system(DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            })?;
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(existing) = backend.get_object(&object.object_id)?
        {
            self.metadata.add_object(existing)?;
        } else if self.metadata.object(&object.object_id).is_none() {
            self.add_object_metadata(object.clone())?;
        }
        Ok(())
    }

    fn ensure_replica_command_authorized_with_metadata(
        &mut self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let object = match self.current_read_object_metadata(object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?
            }
            Err(error) => return Err(error),
        };
        if let Some(kv) = self.control_plane.as_mut() {
            if let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard(&object.file_system_id, "root")?
            {
                if &shard.primary_node_id != requester_node_id {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "source is not current metadata shard primary for {}; primary is {} at epoch {}",
                            object.file_system_id, shard.primary_node_id, shard.epoch
                        ),
                    });
                }
            }
        }
        if let Some(backend) = self.metadata_backend.as_ref() {
            if let Some(intent) = backend.get_intent(object_id, intent_id)? {
                if &intent.coordinator_node_id != requester_node_id {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "source is not coordinator for DFS write intent {intent_id}"
                        ),
                    });
                }
                if !intent.desired_replica_node_ids.contains(&self.node_id) {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "local node is not a desired replica for DFS write intent {intent_id}"
                        ),
                    });
                }
                return Ok(());
            }
        }

        if &object.owner_node_id == requester_node_id {
            return Ok(());
        }
        if matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) && object.current_locations.contains(requester_node_id)
        {
            return Ok(());
        }

        Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!(
                "source is neither object owner nor current replica for {object_id}; owner is {}",
                object.owner_node_id
            ),
        })
    }

    fn ensure_replica_command_hash_matches_intent(
        &self,
        intent_id: &str,
        object_id: &DfsObjectId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(intent) = backend.get_intent(object_id, intent_id)? else {
            return Ok(());
        };
        if let Some(expected_hash) = intent.content_hash.as_deref() {
            if expected_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: content_hash.to_string(),
                });
            }
        }
        Ok(())
    }

    fn ensure_blob_fetch_authorized(
        &self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(intent) = backend.get_intent(object_id, intent_id)? else {
            return Ok(());
        };
        if &intent.coordinator_node_id != requester_node_id
            && !intent.desired_replica_node_ids.contains(requester_node_id)
        {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester_node_id.clone(),
                reason: format!(
                    "source is not authorized to fetch DFS blob for intent {intent_id}"
                ),
            });
        }
        if !intent.desired_replica_node_ids.contains(&self.node_id) {
            let has_receipt = backend
                .receipts_for_version(object_id, &intent.target_version)?
                .into_iter()
                .any(|receipt| receipt.replica_node_id == self.node_id);
            if !has_receipt {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!(
                        "local node is not an expected or durable replica for DFS intent {intent_id}"
                    ),
                });
            }
        }
        Ok(())
    }

    fn write_outcome(&self, intent_id: &str) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let intent = self
            .coordinator
            .intent(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!("unknown intent: {intent_id}"),
            })?;
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            let pending = self.pending_writes.get(intent_id);
            let content_hash = pending
                .map(|pending| pending.content_hash.clone())
                .or_else(|| {
                    self.metadata_backend
                        .as_ref()
                        .and_then(|backend| {
                            backend
                                .get_intent(&intent.object_id, intent_id)
                                .ok()
                                .flatten()
                        })
                        .and_then(|intent_record| intent_record.content_hash)
                })
                .or_else(|| {
                    self.current_read_object_metadata(&intent.object_id)
                        .ok()
                        .filter(|object| object.version == intent.target_version)
                        .and_then(|object| object.content_hash)
                })
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing content hash for committed intent: {intent_id}"),
                })?;
            Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: intent_id.to_string(),
                version: pending
                    .map(|pending| pending.version.clone())
                    .unwrap_or_else(|| intent.target_version.clone()),
                content_hash,
                repair_jobs: self
                    .coordinator
                    .repair_jobs()
                    .iter()
                    .filter(|job| job.intent_id == intent_id)
                    .cloned()
                    .collect(),
            })
        } else {
            let _pending =
                self.pending_writes
                    .get(intent_id)
                    .ok_or_else(|| DfsError::Distributed {
                        message: format!("missing pending write: {intent_id}"),
                    })?;
            Ok(NetworkedDfsWriteOutcome::Pending {
                intent_id: intent_id.to_string(),
                durable_receipts: self.coordinator.durable_receipt_count(intent_id),
                required_receipts: intent.policy.min_commit_replicas,
            })
        }
    }

    fn send_dfs_message_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        let protocol_message =
            ProtocolMessage::encode(DFS_PROTOCOL, &message).map_err(DfsError::from_network)?;
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: self.node_id.clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: self.node_id.clone(),
                target_node_id: target_node_id.clone(),
                body: MeshMessageBody::Protocol(protocol_message),
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;
        match self.transport.send(target_node_id, frame) {
            Ok(()) => Ok(true),
            Err(NetworkError::UnknownNode { .. }) => Ok(false),
            Err(error) => Err(DfsError::from_network(error)),
        }
    }

    fn send_stage_blob_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        if bytes.len() <= DFS_BLOB_INLINE_LIMIT_BYTES {
            return self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes: bytes.to_vec(),
                    object_metadata,
                }),
            );
        }

        let total_chunks = blob_chunk_count(bytes)?;
        for (chunk_index, chunk) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    bytes: chunk.to_vec(),
                    object_metadata: object_metadata.clone(),
                }),
            )?;
            if !sent {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn send_best_effort_message_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        match self.send_dfs_message_if_routable(target_node_id, message) {
            Ok(sent) => Ok(sent),
            Err(DfsError::Network { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn send_repair_replica_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        if bytes.len() <= DFS_BLOB_INLINE_LIMIT_BYTES {
            return self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes: bytes.to_vec(),
                    object_metadata,
                }),
            );
        }

        let total_chunks = blob_chunk_count(bytes)?;
        for (chunk_index, chunk) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplicaChunk(DfsRepairReplicaChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    bytes: chunk.to_vec(),
                    object_metadata: object_metadata.clone(),
                }),
            )?;
            if !sent {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn next_request_id(&mut self) -> String {
        let request_id = format!("dfs_net_req_{}", self.next_request_id);
        self.next_request_id += 1;
        request_id
    }
}

fn content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn read_conflicts_reason(conflicts: Vec<DfsConflictReadView>) -> String {
    conflicts
        .into_iter()
        .map(|conflict| format!("{}: {}", conflict.conflict_id, conflict.reason))
        .collect::<Vec<_>>()
        .join("; ")
}

fn dfs_now_unix_nanos() -> u64 {
    OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(1_000_000_000)
}

fn object_version_record(
    object: &DfsObjectMetadata,
    size_bytes: Option<u64>,
) -> DfsObjectVersionRecord {
    let mut locations = object.current_locations.clone();
    normalize_current_locations(&mut locations);
    DfsObjectVersionRecord {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: size_bytes.or(object.content_size_bytes),
        locations,
        created_by_event_id: object.last_modified_by_event_id.clone(),
        created_at: OffsetDateTime::now_utc(),
        visible: true,
    }
}

fn put_visible_version_record_if_changed(
    backend: &mut dyn DfsMetadataBackend,
    mut version_record: DfsObjectVersionRecord,
) -> Result<(), DfsError> {
    if let Some(existing) =
        backend.get_version(&version_record.object_id, &version_record.version)?
    {
        version_record.created_at = existing.created_at;
        if existing == version_record {
            return Ok(());
        }
    }
    backend.put_version(&version_record)?;
    Ok(())
}

fn normalize_current_locations(locations: &mut Vec<NodeId>) {
    let mut seen = BTreeSet::new();
    locations.retain(|node_id| seen.insert(node_id.clone()));
}

fn remote_read_rejection_prunes_location(reason: &str) -> bool {
    reason.contains("is not a current location for DFS object")
}

fn write_intent_record(
    intent: &DistributedWriteIntent,
    content_hash: Option<&str>,
) -> DfsWriteIntentRecord {
    let now = OffsetDateTime::now_utc();
    DfsWriteIntentRecord {
        intent_id: intent.intent_id.clone(),
        file_system_id: intent.file_system_id.clone(),
        object_id: intent.object_id.clone(),
        base_version: intent.base_version.clone(),
        target_version: intent.target_version.clone(),
        coordinator_node_id: intent.requesting_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_nodes.clone(),
        policy: intent.policy.clone(),
        status: write_intent_status(&intent.state),
        content_hash: content_hash.map(ToOwned::to_owned),
        created_at: now,
        updated_at: Some(now),
    }
}

fn write_intent_status(state: &DfsWriteIntentState) -> DfsWriteIntentStatus {
    match state {
        DfsWriteIntentState::AwaitingReceipts => DfsWriteIntentStatus::ReceiptsPending,
        DfsWriteIntentState::Committed => DfsWriteIntentStatus::Committed,
        DfsWriteIntentState::Rejected => DfsWriteIntentStatus::Rejected,
        DfsWriteIntentState::Conflict => DfsWriteIntentStatus::Conflict,
        DfsWriteIntentState::Aborted => DfsWriteIntentStatus::Aborted,
    }
}

fn distributed_intent_from_record(record: &DfsWriteIntentRecord) -> DistributedWriteIntent {
    DistributedWriteIntent {
        intent_id: record.intent_id.clone(),
        file_system_id: record.file_system_id.clone(),
        object_id: record.object_id.clone(),
        base_version: record.base_version.clone(),
        target_version: record.target_version.clone(),
        requesting_node_id: record.coordinator_node_id.clone(),
        owner_epoch: record.owner_epoch,
        desired_replica_nodes: record.desired_replica_node_ids.clone(),
        policy: record.policy.clone(),
        state: distributed_intent_state_from_status(&record.status),
    }
}

fn distributed_intent_state_from_status(status: &DfsWriteIntentStatus) -> DfsWriteIntentState {
    match status {
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending => {
            DfsWriteIntentState::AwaitingReceipts
        }
        DfsWriteIntentStatus::Committed => DfsWriteIntentState::Committed,
        DfsWriteIntentStatus::Rejected => DfsWriteIntentState::Rejected,
        DfsWriteIntentStatus::Conflict => DfsWriteIntentState::Conflict,
        DfsWriteIntentStatus::Aborted => DfsWriteIntentState::Aborted,
    }
}

fn repair_retry_delay_ticks(attempts: u32) -> u64 {
    1_u64 << attempts.saturating_sub(1).min(6)
}

fn stage_inflight_retry_delay_ticks(attempts: u32) -> u64 {
    repair_retry_delay_ticks(attempts).saturating_add(1)
}

fn fetch_inflight_timeout_ticks() -> u64 {
    stage_inflight_retry_delay_ticks(2)
}

fn namespace_transaction_id_for_object(
    kind: &str,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> String {
    format!(
        "namespace_{}_{}",
        kind,
        blake3::hash(format!("{object_id}\0{version}").as_bytes()).to_hex()
    )
}

impl From<DfsConflictRecord> for DfsConflictReadView {
    fn from(conflict: DfsConflictRecord) -> Self {
        Self {
            conflict_id: conflict.conflict_id,
            base_version: conflict.base_version,
            local_version: conflict.local_version,
            remote_version: conflict.remote_version,
            detected_by_node_id: conflict.detected_by_node_id,
            reason: conflict.reason,
            local_content_hash: conflict.local_content_hash,
            remote_content_hash: conflict.remote_content_hash,
            status: conflict.status,
        }
    }
}

fn distributed_error_creates_conflict(error: &DfsDistributedError) -> bool {
    matches!(
        error,
        DfsDistributedError::BaseVersionConflict { .. }
            | DfsDistributedError::DuplicateIntent { .. }
            | DfsDistributedError::PendingIntentConflict { .. }
    )
}

fn conflict_id_for(
    object_id: &DfsObjectId,
    local_version: &DfsVersion,
    local_content_hash: Option<&str>,
    detected_by_node_id: &NodeId,
) -> String {
    let hash_input = format!(
        "{}\0{}\0{}\0{}",
        object_id,
        local_version,
        local_content_hash.unwrap_or(""),
        detected_by_node_id
    );
    format!("conflict_{}", blake3::hash(hash_input.as_bytes()).to_hex())
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DfsError {
    #[error("unknown filesystem: {file_system_id}")]
    UnknownFileSystem { file_system_id: DfsFileSystemId },
    #[error("unknown object: {object_id}")]
    UnknownObject { object_id: DfsObjectId },
    #[error("unsupported DFS policy for local-owned service: {policy:?}")]
    UnsupportedPolicy { policy: DfsObjectPolicy },
    #[error("node {local_node_id} cannot own object {object_id}; owner is {owner_node_id}")]
    NotObjectOwner {
        object_id: DfsObjectId,
        owner_node_id: NodeId,
        local_node_id: NodeId,
    },
    #[error("network error: {message}")]
    Network { message: String },
    #[error("serialization error: {message}")]
    Serialization { message: String },
    #[error("content store error: {message}")]
    ContentStore { message: String },
    #[error("distributed DFS error: {message}")]
    Distributed { message: String },
    #[error("metadata backend error: {message}")]
    Metadata { message: String },
    #[error("invalid DFS replication policy: {message}")]
    InvalidReplicationPolicy { message: String },
    #[error("content hash mismatch: expected {expected}, actual {actual}")]
    ContentHashMismatch { expected: String, actual: String },
    #[error("unauthorized DFS replica command from {source_node_id}: {reason}")]
    UnauthorizedReplicaCommand {
        source_node_id: NodeId,
        reason: String,
    },
    #[error(
        "stale owner epoch for filesystem {file_system_id}: local node {local_node_id}, primary {primary_node_id}, epoch {epoch}"
    )]
    StaleOwnerEpoch {
        file_system_id: DfsFileSystemId,
        local_node_id: NodeId,
        primary_node_id: NodeId,
        epoch: u64,
    },
}

impl DfsError {
    fn from_network(error: sinomo_network::NetworkError) -> Self {
        Self::Network {
            message: error.to_string(),
        }
    }
}

impl From<ContentStoreError> for DfsError {
    fn from(error: ContentStoreError) -> Self {
        Self::ContentStore {
            message: error.to_string(),
        }
    }
}

impl From<DfsDistributedError> for DfsError {
    fn from(error: DfsDistributedError) -> Self {
        Self::Distributed {
            message: error.to_string(),
        }
    }
}

impl From<MetadataBackendError> for DfsError {
    fn from(error: MetadataBackendError) -> Self {
        Self::Metadata {
            message: error.to_string(),
        }
    }
}

impl From<KvError> for DfsError {
    fn from(error: KvError) -> Self {
        Self::Distributed {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_kv::InProcessRaftKvCluster;
    use sinomo_network::InProcessNetwork;
    use std::collections::VecDeque;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Clone, Debug)]
    struct ReceiveOnlyTransport {
        local_node_id: NodeId,
        incoming: VecDeque<sinomo_network::InboundFrame>,
    }

    impl ReceiveOnlyTransport {
        fn new(local_node_id: NodeId) -> Self {
            Self {
                local_node_id,
                incoming: VecDeque::new(),
            }
        }

        fn push_message(
            &mut self,
            source_node_id: NodeId,
            target_node_id: NodeId,
            message: DfsNetworkMessage,
        ) {
            let protocol_message =
                ProtocolMessage::encode(DFS_PROTOCOL, &message).expect("protocol message");
            self.incoming.push_back(sinomo_network::InboundFrame {
                received_by: target_node_id.clone(),
                frame: NetworkFrame {
                    frame_id: NetworkFrameId(1),
                    source_node_id: source_node_id.clone(),
                    target_node_id: target_node_id.clone(),
                    message: MeshMessage {
                        message_id: MeshMessageId(1),
                        origin_node_id: source_node_id,
                        target_node_id,
                        body: MeshMessageBody::Protocol(protocol_message),
                    },
                },
            });
        }
    }

    impl NetworkTransport for ReceiveOnlyTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }

        fn send(
            &mut self,
            target_node_id: &NodeId,
            _frame: NetworkFrame,
        ) -> Result<(), NetworkError> {
            Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })
        }

        fn drain_incoming(&mut self) -> Result<Vec<sinomo_network::InboundFrame>, NetworkError> {
            Ok(self.incoming.drain(..).collect())
        }
    }

    struct FailingSendTransport {
        local_node_id: NodeId,
    }

    impl NetworkTransport for FailingSendTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }

        fn send(
            &mut self,
            _target_node_id: &NodeId,
            _frame: NetworkFrame,
        ) -> Result<(), NetworkError> {
            Err(NetworkError::Io {
                operation: "test send".to_string(),
                detail: "unreachable".to_string(),
            })
        }

        fn drain_incoming(&mut self) -> Result<Vec<sinomo_network::InboundFrame>, NetworkError> {
            Ok(Vec::new())
        }
    }

    fn file_system(owner: NodeId) -> DfsFileSystem {
        DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            display_name: "repo".to_string(),
            owner_node_id: owner,
            subtree_policies: Vec::new(),
        }
    }

    fn file_metadata(owner: NodeId) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("obj_file").expect("object"),
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            path: "src/lib.rs".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: owner.clone(),
            current_locations: vec![owner],
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
            version: DfsVersion::from_legacy_named("version_1").expect("version"),
            policy: DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "sinomo-dfs-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    fn networked_sifs_with_backend(
        root: &Path,
        node_id: NodeId,
    ) -> NetworkedDfsService<sinomo_network::InProcessTransport> {
        let network = InProcessNetwork::new();
        let content = LocalContentStore::new(root.join("content")).expect("content");
        let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
        let mut dfs = NetworkedDfsService::new(network.add_node(node_id), content)
            .with_metadata_backend(metadata)
            .expect("metadata backend");
        dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
        dfs
    }

    #[test]
    fn metadata_replication_cursor_advances_only_after_replica_ack() {
        let root = temp_root("metadata-replication-cursor-after-ack");
        let primary = NodeId::from_legacy_named("node_meta_primary").expect("primary");
        let replica = NodeId::from_legacy_named("node_meta_replica").expect("replica");
        let file_system = file_system(primary.clone());
        let shared_kv = SharedDfsControlPlaneKvClient::new(
            InProcessRaftKvCluster::new(vec![primary.clone()]).expect("kv"),
        );
        shared_kv.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
                .expect("filesystem config");
            control
                .put_metadata_shard(&DfsMetadataShardConfig {
                    file_system_id: file_system.file_system_id.clone(),
                    shard_id: "root".to_string(),
                    primary_node_id: primary.clone(),
                    replica_node_ids: vec![primary.clone(), replica.clone()],
                    epoch: 1,
                    assignment: DfsMetadataShardAssignment::Root,
                    default_policy: DfsObjectPolicy::ReplicatedEventual,
                })
                .expect("metadata shard");
        });

        let network = InProcessNetwork::new();
        let mut drop_replica_mailbox = network.add_node(replica.clone());
        let primary_content =
            LocalContentStore::new(root.join("primary-content")).expect("content");
        let mut primary_backend = LocalJsonMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.join("primary-meta"),
        )
        .expect("primary backend");
        primary_backend
            .put_file_system(&file_system)
            .expect("primary filesystem");
        let mut first = file_metadata(primary.clone());
        first.object_id = DfsObjectId::from_legacy_named("obj_meta_first").expect("object");
        first.path = "/services/metadata/first.txt".to_string();
        first.file_system_id = file_system.file_system_id.clone();
        first.policy = DfsObjectPolicy::ReplicatedEventual;
        primary_backend.put_object(&first).expect("first object");
        let mut primary_dfs =
            NetworkedDfsService::new(network.add_node(primary.clone()), primary_content)
                .with_control_plane(shared_kv.clone())
                .with_metadata_backend(primary_backend)
                .expect("primary dfs");

        let outcomes = primary_dfs
            .replicate_metadata_shard_to_configured_replicas(&file_system.file_system_id, "root")
            .expect("first replication send");
        assert!(
            matches!(
                outcomes.as_slice(),
                [DfsMetadataReplicationOutcome::Sent { .. }]
            ),
            "unexpected first replication outcomes: {outcomes:?}"
        );

        assert_eq!(
            NetworkTransport::drain_incoming(&mut drop_replica_mailbox)
                .expect("drop first batch")
                .len(),
            1,
            "the first metadata batch is intentionally lost before the replica applies it"
        );

        let mut second = file_metadata(primary.clone());
        second.object_id = DfsObjectId::from_legacy_named("obj_meta_second").expect("object");
        second.path = "/services/metadata/second.txt".to_string();
        second.file_system_id = file_system.file_system_id.clone();
        second.policy = DfsObjectPolicy::ReplicatedEventual;
        primary_dfs
            .add_object_metadata(second.clone())
            .expect("second object");

        let outcomes = primary_dfs
            .replicate_metadata_shard_to_configured_replicas(&file_system.file_system_id, "root")
            .expect("retry replication before ack");
        assert!(matches!(
            outcomes.as_slice(),
            [DfsMetadataReplicationOutcome::Sent { entries, .. }] if *entries >= 3
        ));

        let replica_content =
            LocalContentStore::new(root.join("replica-content")).expect("content");
        let replica_backend = LocalJsonMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.join("replica-meta"),
        )
        .expect("replica backend");
        let mut replica_dfs =
            NetworkedDfsService::new(network.add_node(replica.clone()), replica_content)
                .with_metadata_backend(replica_backend)
                .expect("replica dfs");
        let events = replica_dfs
            .process_incoming()
            .expect("replica applies retry");
        assert!(
            matches!(
                events.as_slice(),
                [NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    applied_entries,
                    ..
                }] if *applied_entries >= 3
            ),
            "unexpected replica events: {events:?}"
        );

        let replica_backend = replica_dfs
            .metadata_backend()
            .expect("replica metadata backend");
        assert_eq!(
            replica_backend
                .get_object(&first.object_id)
                .expect("first replicated")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some(first.path.as_str())
        );
        assert_eq!(
            replica_backend
                .get_object(&second.object_id)
                .expect("second replicated")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some(second.path.as_str())
        );

        let owner_events = primary_dfs.process_incoming().expect("owner handles ack");
        assert!(matches!(
            owner_events.as_slice(),
            [NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                source_node_id,
                next_after_sequence,
                ..
            }] if source_node_id == &replica && *next_after_sequence >= 3
        ));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn eventual_write_with_local_receipt_tolerates_unreachable_desired_replica() {
        let root = temp_root("eventual-write-tolerates-unreachable-replica");
        let owner = NodeId::from_legacy_named("node_eventual_tolerates_owner").expect("owner node");
        let replica =
            NodeId::from_legacy_named("node_eventual_tolerates_replica").expect("replica node");
        let object_id = DfsObjectId::from_legacy_named("obj_eventual_tolerates").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let content = LocalContentStore::new(root.join("content")).expect("content");
        let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
        let mut dfs = NetworkedDfsService::new(
            FailingSendTransport {
                local_node_id: owner.clone(),
            },
            content,
        )
        .with_metadata_backend(metadata)
        .expect("metadata backend");
        dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        dfs.add_object_metadata(metadata).expect("object");

        let write = dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2,
                    requesting_node_id: owner.clone(),
                },
                b"eventual bytes".to_vec(),
                vec![owner, replica.clone()],
                DfsReplicationPolicy::new(1, 2).expect("policy"),
            )
            .expect("write should commit locally");

        let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = write else {
            panic!("expected committed write, got {write:?}");
        };
        assert_eq!(repair_jobs.len(), 1);
        assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn receipt_updates_visible_current_locations_for_current_version() {
        let root = temp_root("receipt-updates-current-locations");
        let owner = NodeId::from_legacy_named("node_receipt_location_owner").expect("owner");
        let replica = NodeId::from_legacy_named("node_receipt_location_replica").expect("replica");
        let object_id = DfsObjectId::from_legacy_named("obj_receipt_location").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![owner.clone()];
        metadata.content_hash = Some("blake3:receipt-location".to_string());
        dfs.add_object_metadata(metadata).expect("metadata");

        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            "blake3:receipt-location",
        )
        .expect("receipt");

        let object = dfs
            .current_read_object_metadata(&object_id)
            .expect("current object");
        assert_eq!(object.current_locations, vec![owner, replica]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_clears_partial_blob_transfers() {
        let root = temp_root("hydrate-clears-partial-blob-transfers");
        let node_id = NodeId::from_legacy_named("node_hydrate_clear").expect("node");
        let requester = NodeId::from_legacy_named("node_requester").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_partial_transfer").expect("object");
        let version = DfsVersion::from_legacy_named("version_1").expect("version");
        let mut dfs = networked_sifs_with_backend(&root, node_id);
        dfs.incoming_blob_transfers.insert(
            blob_transfer_key("stage", &requester, "intent_partial", "dfs_net_req_partial"),
            InProgressBlobTransfer::new(
                "dfs_net_req_partial".to_string(),
                "intent_partial".to_string(),
                sifs_file_system_id(),
                object_id,
                None,
                version,
                content_hash(b"partial"),
                8,
                1,
                None,
            )
            .expect("partial transfer"),
        );

        dfs.hydrate_from_metadata_backend().expect("rehydrate");

        assert!(
            dfs.incoming_blob_transfers.is_empty(),
            "rehydration must not keep chunk assemblers from the previous metadata view"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_does_not_resurrect_aborted_pending_write() {
        let root = temp_root("hydrate-skips-aborted-pending-write");
        let owner = NodeId::from_legacy_named("node_hydrate_abort_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_hydrate_abort_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_hydrate_abort").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        dfs.add_object_metadata(metadata).expect("object");

        let write = dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2,
                    requesting_node_id: owner,
                },
                b"aborted pending bytes".to_vec(),
                vec![dfs.node_id().clone(), replica],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("pending write");
        let intent_id = match write {
            NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
            other => panic!("expected pending write, got {other:?}"),
        };
        dfs.abort_pending_write(&intent_id).expect("abort");
        assert!(
            !dfs.pending_writes.contains_key(&intent_id),
            "abort should remove in-memory pending write"
        );

        dfs.hydrate_from_metadata_backend().expect("rehydrate");

        assert_eq!(
            dfs.coordinator()
                .intent(&intent_id)
                .expect("restored aborted intent")
                .state,
            DfsWriteIntentState::Aborted
        );
        assert_eq!(
            dfs.metadata()
                .object(&object_id)
                .expect("visible object")
                .version,
            version_1
        );
        assert!(
            !dfs.pending_writes.contains_key(&intent_id),
            "rehydration must not resurrect pending state for aborted writes"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_converts_competing_pending_intent_to_conflict() {
        let root = temp_root("hydrate-competing-pending-intent-conflict");
        let owner = NodeId::from_legacy_named("node_hydrate_conflict_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_hydrate_conflict_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_hydrate_conflict").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
        let second_bytes = b"recovered competing pending write";
        let network = InProcessNetwork::new();
        let metadata_root = root.join("metadata");
        let content_root = root.join("owner-content");
        let owner_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("owner backend");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("metadata backend");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        owner_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("owner fs");
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner object");

        let first = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner.clone(),
                },
                b"first pending write".to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("first pending write");
        let first_intent_id = match first {
            NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
            other => panic!("expected pending write, got {other:?}"),
        };
        let second_hash = owner_dfs
            .content()
            .put_object_version(object_id.as_str(), version_3.as_str(), second_bytes)
            .expect("second content")
            .content_hash;
        owner_dfs
            .metadata_backend
            .as_mut()
            .expect("backend")
            .put_intent(&DfsWriteIntentRecord {
                intent_id: format!("dfs_intent_{}_99", owner.as_str()),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                base_version: Some(version_1.clone()),
                target_version: version_3.clone(),
                coordinator_node_id: owner.clone(),
                owner_epoch: 0,
                desired_replica_node_ids: vec![owner.clone(), replica],
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(second_hash.clone()),
                created_at: OffsetDateTime::now_utc() + time::Duration::seconds(1),
                updated_at: Some(OffsetDateTime::now_utc() + time::Duration::seconds(1)),
            })
            .expect("competing persisted intent");
        drop(owner_dfs);

        let recovered_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("recovered backend");
        let recovered_owner = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(content_root).expect("recovered content"),
        )
        .with_metadata_backend(recovered_backend)
        .expect("competing pending intent should hydrate as conflict");

        assert!(
            recovered_owner
                .pending_writes
                .contains_key(&first_intent_id),
            "the earliest pending intent should remain repairable"
        );
        assert_eq!(
            recovered_owner
                .metadata()
                .object(&object_id)
                .expect("visible object")
                .version,
            version_1,
            "conflicting recovered intent must not publish a visible version"
        );
        let conflicts = recovered_owner
            .conflicts_for_object(&object_id)
            .expect("conflicts");
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].local_version, version_3);
        assert_eq!(
            conflicts[0].local_content_hash.as_deref(),
            Some(second_hash.as_str())
        );
        let recovered_intent = recovered_owner
            .metadata_backend()
            .expect("backend")
            .get_intent(&object_id, &format!("dfs_intent_{}_99", owner.as_str()))
            .expect("intent")
            .expect("intent");
        assert_eq!(recovered_intent.status, DfsWriteIntentStatus::Conflict);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_skips_fully_repaired_committed_pending_write() {
        let root = temp_root("hydrate-skips-fully-repaired-committed");
        let owner = NodeId::from_legacy_named("node_hydrate_full_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_hydrate_full_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_hydrate_full").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let network = InProcessNetwork::new();
        let metadata_root = root.join("metadata");
        let owner_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("owner backend");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("metadata backend");
        let mut replica_dfs = NetworkedDfsService::new(
            network.add_node(replica.clone()),
            LocalContentStore::new(root.join("replica-content")).expect("replica content"),
        );
        let file_system = file_system(owner.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        owner_dfs
            .add_file_system(file_system.clone())
            .expect("owner fs");
        replica_dfs
            .add_file_system(file_system)
            .expect("replica fs");
        owner_dfs
            .add_object_metadata(metadata.clone())
            .expect("owner object");
        replica_dfs
            .add_object_metadata(metadata)
            .expect("replica object");

        let write = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2,
                    requesting_node_id: owner.clone(),
                },
                b"fully repaired bytes".to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("pending write");
        assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));
        replica_dfs.process_incoming().expect("replica stage");
        owner_dfs.process_incoming().expect("owner ack");
        replica_dfs.process_incoming().expect("replica commit");
        assert!(
            owner_dfs.pending_writes.is_empty(),
            "fully repaired committed writes should leave no local pending repair state"
        );
        assert!(owner_dfs.coordinator().repair_jobs().is_empty());
        drop(owner_dfs);

        let recovered_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("recovered backend");
        let recovered_owner = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
        )
        .with_metadata_backend(recovered_backend)
        .expect("recovered metadata");

        assert!(
            recovered_owner.pending_writes.is_empty(),
            "fully receipted committed intents should not become pending writes after hydration"
        );
        assert!(recovered_owner.coordinator().repair_jobs().is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_restores_committed_pending_write_for_missing_repair() {
        let root = temp_root("hydrate-restores-committed-repair");
        let owner = NodeId::from_legacy_named("node_hydrate_repair_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_hydrate_repair_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_hydrate_repair").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let network = InProcessNetwork::new();
        let metadata_root = root.join("metadata");
        let owner_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("owner backend");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("metadata backend");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        owner_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("owner fs");
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner object");

        let write = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id,
                    target_version: version_2,
                    requesting_node_id: owner.clone(),
                },
                b"repair after restart bytes".to_vec(),
                vec![owner.clone(), replica],
                DfsReplicationPolicy::new(1, 2).expect("policy"),
            )
            .expect("committed write");
        let intent_id = match write {
            NetworkedDfsWriteOutcome::Committed {
                intent_id,
                repair_jobs,
                ..
            } => {
                assert_eq!(repair_jobs.len(), 1);
                intent_id
            }
            other => panic!("expected committed write with repair job, got {other:?}"),
        };
        drop(owner_dfs);

        let recovered_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("recovered backend");
        let recovered_owner = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
        )
        .with_metadata_backend(recovered_backend)
        .expect("recovered metadata");

        assert!(
            recovered_owner.pending_writes.contains_key(&intent_id),
            "committed intents with missing replicas must keep pending state for repair"
        );
        assert_eq!(recovered_owner.coordinator().repair_jobs().len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_ignores_receipts_with_wrong_content_hash() {
        let root = temp_root("hydrate-ignores-wrong-hash-receipt");
        let owner = NodeId::from_legacy_named("node_hydrate_hash_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_hydrate_hash_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_hydrate_hash_receipt").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"receipt hash must match intent bytes";
        let network = InProcessNetwork::new();
        let metadata_root = root.join("metadata");
        let owner_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("owner backend");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("metadata backend");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        owner_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("owner fs");
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner object");

        let write = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner.clone(),
                },
                bytes.to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(1, 2).expect("policy"),
            )
            .expect("committed write with missing repair");
        let intent_id = match write {
            NetworkedDfsWriteOutcome::Committed {
                intent_id,
                repair_jobs,
                ..
            } => {
                assert_eq!(repair_jobs.len(), 1);
                intent_id
            }
            other => panic!("expected committed write with repair job, got {other:?}"),
        };
        let wrong_hash = content_hash(b"receipt points at different bytes");
        owner_dfs
            .metadata_backend
            .as_mut()
            .expect("metadata backend")
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    committed_version: version_2,
                    owner_node_id: owner.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica.clone(),
                content_hash: Some(wrong_hash),
                received_at: OffsetDateTime::now_utc(),
            })
            .expect("stale wrong-hash receipt");
        drop(owner_dfs);

        let recovered_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("recovered backend");
        let recovered_owner = NetworkedDfsService::new(
            network.add_node(owner),
            LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
        )
        .with_metadata_backend(recovered_backend)
        .expect("recovered metadata");

        assert!(
            recovered_owner.pending_writes.contains_key(&intent_id),
            "wrong-hash receipts must not suppress repair state after hydration"
        );
        assert_eq!(recovered_owner.coordinator().repair_jobs().len(), 1);
        assert_eq!(
            recovered_owner.coordinator().repair_jobs()[0].missing_replica_node_id,
            replica
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_ignores_unexpected_replica_receipts() {
        let root = temp_root("hydrate-ignores-unexpected-receipt");
        let owner = NodeId::from_legacy_named("node_hydrate_unexpected_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_hydrate_unexpected_replica").expect("node");
        let object_id =
            DfsObjectId::from_legacy_named("obj_hydrate_unexpected_receipt").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"unexpected receipt should not break recovery";
        let network = InProcessNetwork::new();
        let metadata_root = root.join("metadata");
        let owner_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("owner backend");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("metadata backend");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1;
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        owner_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("owner fs");
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner object");

        let write = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner.clone(),
                },
                bytes.to_vec(),
                vec![owner.clone()],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("self-only committed write");
        let intent_id = match write {
            NetworkedDfsWriteOutcome::Committed { intent_id, .. } => intent_id,
            other => panic!("expected committed write, got {other:?}"),
        };
        let content_hash = content_hash(bytes);
        owner_dfs
            .metadata_backend
            .as_mut()
            .expect("metadata backend")
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    committed_version: version_2,
                    owner_node_id: owner.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica.clone(),
                content_hash: Some(content_hash),
                received_at: OffsetDateTime::now_utc(),
            })
            .expect("unexpected stale receipt");
        drop(owner_dfs);

        let recovered_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("recovered backend");
        let recovered_owner = NetworkedDfsService::new(
            network.add_node(owner),
            LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
        )
        .with_metadata_backend(recovered_backend)
        .expect("unexpected replica receipt should not break hydration");

        assert!(
            recovered_owner.pending_writes.get(&intent_id).is_none(),
            "fully repaired committed write should not be restored as pending"
        );
        assert!(recovered_owner.coordinator().repair_jobs().is_empty());
        assert!(
            recovered_owner
                .coordinator()
                .receipts_for_intent(&intent_id)
                .into_iter()
                .all(|receipt| receipt.replica_node_id != replica),
            "unexpected replica receipt must not be restored into coordinator state"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_applies_commit_ready_receipts_pending_intent() {
        let root = temp_root("hydrate-applies-commit-ready-receipts-pending");
        let owner = NodeId::from_legacy_named("node_hydrate_ready_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_hydrate_ready_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_hydrate_ready").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"commit ready after restart";
        let network = InProcessNetwork::new();
        let metadata_root = root.join("metadata");
        let owner_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("owner backend");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("metadata backend");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        owner_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("owner fs");
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner object");

        let write = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner.clone(),
                },
                bytes.to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("pending write");
        let intent_id = match write {
            NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
            other => panic!("expected pending write, got {other:?}"),
        };
        let content_hash = content_hash(bytes);
        owner_dfs
            .metadata_backend
            .as_mut()
            .expect("metadata backend")
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    committed_version: version_2.clone(),
                    owner_node_id: owner.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica.clone(),
                content_hash: Some(content_hash.clone()),
                received_at: OffsetDateTime::now_utc(),
            })
            .expect("persist replica receipt without visible commit");
        assert_eq!(
            owner_dfs
                .metadata_backend()
                .expect("backend")
                .get_object(&object_id)
                .expect("object")
                .expect("object")
                .version,
            version_1,
            "test setup should leave visible metadata uncommitted before restart"
        );
        drop(owner_dfs);

        let recovered_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("recovered backend");
        let recovered_owner = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
        )
        .with_metadata_backend(recovered_backend)
        .expect("commit-ready receipts-pending intent should hydrate");

        let object = recovered_owner
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("object")
            .expect("object");
        assert_eq!(object.version, version_2);
        assert_eq!(object.content_hash.as_deref(), Some(content_hash.as_str()));
        assert_eq!(
            object
                .current_locations
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([owner, replica])
        );
        assert!(
            recovered_owner.pending_writes.get(&intent_id).is_none(),
            "fully receipted commit-ready intent should be cleaned after visible commit recovery"
        );
        assert!(recovered_owner.coordinator().repair_jobs().is_empty());
        assert_eq!(
            recovered_owner
                .metadata_backend()
                .expect("backend")
                .get_intent(&object_id, &intent_id)
                .expect("intent")
                .expect("intent")
                .status,
            DfsWriteIntentStatus::Committed
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hydrate_from_metadata_backend_restores_staging_pending_write_size_from_local_manifest() {
        let root = temp_root("hydrate-staging-size-from-manifest");
        let owner = NodeId::from_legacy_named("node_hydrate_staging_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_hydrate_staging_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_hydrate_staging_size").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"staging bytes survive restart";
        let network = InProcessNetwork::new();
        let metadata_root = root.join("metadata");
        let content_root = root.join("owner-content");
        let owner_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("owner backend");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("metadata backend");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1;
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        owner_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("owner fs");
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner object");

        let write = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner.clone(),
                },
                bytes.to_vec(),
                vec![owner.clone(), replica],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("staging write");
        let intent_id = match write {
            NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
            other => panic!("expected pending write before replica receipt, got {other:?}"),
        };
        assert!(
            owner_dfs
                .metadata_backend()
                .expect("backend")
                .get_version(&object_id, &version_2)
                .expect("version lookup")
                .is_none(),
            "staging writes should not need a visible version record before commit"
        );
        drop(owner_dfs);

        let recovered_backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
                .expect("recovered backend");
        let recovered_owner = NetworkedDfsService::new(
            network.add_node(owner),
            LocalContentStore::new(content_root).expect("recovered content"),
        )
        .with_metadata_backend(recovered_backend)
        .expect("recovered metadata");

        assert_eq!(
            recovered_owner
                .pending_writes
                .get(&intent_id)
                .expect("restored pending write")
                .size_bytes,
            bytes.len() as u64
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn pending_write_size_hint_ignores_backend_version_with_wrong_hash() {
        let root = temp_root("pending-size-ignores-wrong-hash-version");
        let owner = NodeId::from_legacy_named("node_size_hint_owner").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_size_hint_wrong_hash").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"correct pending content size";
        let correct_hash = content_hash(bytes);
        let wrong_hash = content_hash(b"wrong content");
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut object = file_metadata(owner);
        object.object_id = object_id.clone();
        object.version = version.clone();
        object.content_hash = Some(wrong_hash.clone());
        object.content_size_bytes = Some(999);
        dfs.metadata_backend
            .as_mut()
            .expect("backend")
            .put_version(&object_version_record(&object, Some(999)))
            .expect("wrong hash version");
        dfs.content
            .put_object_version(object_id.as_str(), version.as_str(), bytes)
            .expect("correct local manifest");

        assert_eq!(
            dfs.pending_write_size_hint(&object_id, &version, &correct_hash)
                .expect("size hint"),
            Some(bytes.len() as u64)
        );
        assert_eq!(
            dfs.pending_write_size_hint(&object_id, &version, &wrong_hash)
                .expect("wrong-hash size hint"),
            Some(999)
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn abort_pending_write_clears_inflight_fetch_transfers() {
        let root = temp_root("abort-clears-inflight-fetch-transfers");
        let owner = NodeId::from_legacy_named("node_abort_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_abort_replica").expect("node");
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        let _replica_transport = network.add_node(replica.clone());
        let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
        let object_id = DfsObjectId::from_legacy_named("obj_abort_fetch").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let mut metadata = file_metadata(owner.clone());
        metadata.file_system_id = file_system_id.clone();
        metadata.object_id = object_id.clone();
        metadata.version = version_1;
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        dfs.add_file_system(DfsFileSystem {
            file_system_id: file_system_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        })
        .expect("filesystem");
        dfs.add_object_metadata(metadata).expect("object");

        let write = dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id,
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: owner,
                },
                b"pending bytes".to_vec(),
                vec![dfs.node_id().clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("pending write");
        let intent_id = match write {
            NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
            other => panic!("expected pending write, got {other:?}"),
        };
        let request_id = "dfs_net_req_fetch_abort".to_string();
        let blob_request_id = "dfs_net_req_blob_abort".to_string();
        let fetch_hash = content_hash(b"repair");
        dfs.pending_writes
            .get_mut(&intent_id)
            .expect("pending")
            .fetch_inflight_request_ids
            .insert(replica.clone(), request_id.clone());
        dfs.incoming_fetch_transfers.insert(
            request_id.clone(),
            InProgressFetchBlobTransfer::new(
                object_id.clone(),
                version_2.clone(),
                fetch_hash.clone(),
                b"repair".len() as u64,
                1,
            )
            .expect("fetch transfer"),
        );
        let unrelated_intent_id = "dfs_intent_unrelated_abort".to_string();
        let unrelated_blob_request_id = "dfs_net_req_blob_unrelated".to_string();
        dfs.incoming_blob_transfers.insert(
            blob_transfer_key("stage", &replica, &intent_id, &blob_request_id),
            InProgressBlobTransfer::new(
                blob_request_id.clone(),
                intent_id.clone(),
                sifs_file_system_id(),
                object_id.clone(),
                None,
                version_2.clone(),
                fetch_hash.clone(),
                b"repair".len() as u64,
                1,
                None,
            )
            .expect("blob transfer"),
        );
        dfs.incoming_blob_transfers.insert(
            blob_transfer_key(
                "stage",
                &replica,
                &unrelated_intent_id,
                &unrelated_blob_request_id,
            ),
            InProgressBlobTransfer::new(
                unrelated_blob_request_id,
                unrelated_intent_id.clone(),
                sifs_file_system_id(),
                object_id,
                None,
                version_2,
                fetch_hash,
                b"repair".len() as u64,
                1,
                None,
            )
            .expect("unrelated blob transfer"),
        );

        dfs.abort_pending_write(&intent_id).expect("abort");

        assert!(!dfs.pending_writes.contains_key(&intent_id));
        assert!(
            !dfs.incoming_fetch_transfers.contains_key(&request_id),
            "aborting a write must drop partial fetched-blob assemblers for that write"
        );
        assert!(
            !dfs.incoming_blob_transfers.contains_key(&blob_transfer_key(
                "stage",
                &replica,
                &intent_id,
                &blob_request_id
            )),
            "aborting a write must drop partial inbound blob assemblers for that intent"
        );
        assert!(
            dfs.incoming_blob_transfers.contains_key(&blob_transfer_key(
                "stage",
                &replica,
                &unrelated_intent_id,
                "dfs_net_req_blob_unrelated"
            )),
            "aborting one write must not drop unrelated inbound blob assemblers"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn committed_pending_write_cleanup_drops_stale_stage_inflight_state() {
        let root = temp_root("cleanup-drops-stale-stage-inflight");
        let owner = NodeId::from_legacy_named("node_cleanup_owner").expect("node");
        let stale_replica = NodeId::from_legacy_named("node_cleanup_stale_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_cleanup_stale_stage").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1;
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        let base_version = metadata.version.clone();
        dfs.add_object_metadata(metadata).expect("object");

        let content_hash = content_hash(b"cleanup bytes");
        let intent = dfs
            .coordinator
            .create_intent(
                sifs_file_system_id(),
                object_id.clone(),
                Some(base_version),
                version_2.clone(),
                dfs.node_id().clone(),
                vec![dfs.node_id().clone()],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("intent");
        dfs.coordinator
            .record_durable_receipt(&intent.intent_id, dfs.node_id().clone())
            .expect("commit intent");
        assert!(dfs.coordinator.repair_jobs().is_empty());
        let intent_id = intent.intent_id;
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id,
                version: version_2,
                content_hash,
                size_bytes: b"cleanup bytes".len() as u64,
                stage_retries: BTreeMap::new(),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::from([(
                    stale_replica,
                    "dfs_net_req_stale_stage".to_string(),
                )]),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
                fetch_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_started_ticks: BTreeMap::new(),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        dfs.cleanup_committed_pending_write_if_repaired(&intent_id);

        assert!(
            !dfs.pending_writes.contains_key(&intent_id),
            "fully repaired committed writes must not retain stale stage in-flight state"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn apply_committed_intent_tolerates_missing_local_repair_blob() {
        let root = temp_root("apply-committed-missing-repair-blob");
        let owner = NodeId::from_legacy_named("node_apply_missing_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_apply_missing_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_apply_missing_blob").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"committed bytes missing locally";
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        dfs.add_file_system(file_system(owner.clone()))
            .expect("filesystem");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.version = version_1.clone();
        dfs.add_object_metadata(metadata).expect("object");
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("manifest");
        let hex = manifest
            .content_hash
            .strip_prefix("blake3:")
            .expect("blake3 hash");
        std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
            .expect("remove blob");
        let intent = dfs
            .coordinator
            .create_intent(
                sifs_file_system_id(),
                object_id.clone(),
                Some(version_1),
                version_2.clone(),
                owner.clone(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(1, 2).expect("policy"),
            )
            .expect("intent");
        dfs.coordinator
            .record_durable_receipt(&intent.intent_id, owner.clone())
            .expect("commit with missing repair");
        assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
        dfs.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version_2.clone(),
                content_hash: manifest.content_hash.clone(),
                size_bytes: manifest.size_bytes,
                stage_retries: BTreeMap::new(),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
                fetch_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_started_ticks: BTreeMap::new(),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        dfs.apply_committed_intent(&intent.intent_id)
            .expect("commit applies even when eager repair bytes are missing");

        let object = dfs.metadata().object(&object_id).expect("object");
        assert_eq!(object.version, version_2);
        assert_eq!(object.current_locations, vec![owner]);
        assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
        let pending = dfs
            .pending_writes
            .get(&intent.intent_id)
            .expect("pending write");
        assert!(pending.repair_retries.contains_key(&replica));
        assert!(!pending.repair_inflight_request_ids.contains_key(&replica));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn repair_blob_sources_use_backend_current_locations_over_stale_memory() {
        let root = temp_root("repair-sources-backend-current");
        let owner = NodeId::from_legacy_named("node_repair_source_owner").expect("node");
        let source = NodeId::from_legacy_named("node_repair_source_backend").expect("node");
        let missing = NodeId::from_legacy_named("node_repair_source_missing").expect("node");
        let object_id =
            DfsObjectId::from_legacy_named("obj_repair_source_backend").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"repair source bytes";
        let hash = content_hash(bytes);
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut backend_current = file_metadata(owner.clone());
        backend_current.object_id = object_id.clone();
        backend_current.version = version.clone();
        backend_current.policy = DfsObjectPolicy::ReplicatedEventual;
        backend_current.current_locations = vec![source.clone()];
        backend_current.content_hash = Some(hash.clone());
        backend_current.content_size_bytes = Some(bytes.len() as u64);
        dfs.add_object_metadata(backend_current.clone())
            .expect("backend object");

        let mut stale_memory = backend_current;
        stale_memory.current_locations = vec![owner.clone()];
        dfs.add_transient_object_metadata(stale_memory)
            .expect("stale memory object");
        let pending = PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: hash,
            size_bytes: bytes.len() as u64,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        };
        let job = DistributedRepairJob {
            intent_id: "dfs_intent_repair_source_backend".to_string(),
            file_system_id: sifs_file_system_id(),
            object_id,
            version,
            missing_replica_node_id: missing,
        };

        assert_eq!(
            dfs.repair_blob_sources(&job, &pending)
                .expect("repair sources"),
            vec![source]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn write_outcome_uses_backend_current_metadata_after_pending_state_is_removed() {
        let root = temp_root("write-outcome-backend-current");
        let owner = NodeId::from_legacy_named("node_write_outcome_owner").expect("node");
        let object_id =
            DfsObjectId::from_legacy_named("obj_write_outcome_backend").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"write outcome backend bytes";
        let hash = content_hash(bytes);
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        dfs.add_object_metadata(metadata.clone()).expect("object");
        let intent = dfs
            .coordinator
            .create_intent(
                sifs_file_system_id(),
                object_id.clone(),
                Some(version_1),
                version_2.clone(),
                owner.clone(),
                vec![owner.clone()],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("intent");
        dfs.coordinator
            .record_durable_receipt(&intent.intent_id, owner.clone())
            .expect("commit intent");
        let mut backend_current = metadata;
        backend_current.version = version_2.clone();
        backend_current.content_hash = Some(hash.clone());
        backend_current.content_size_bytes = Some(bytes.len() as u64);
        backend_current.current_locations = vec![owner];
        dfs.add_object_metadata(backend_current)
            .expect("backend current object");
        dfs.remove_transient_object_metadata(&object_id);
        assert!(
            dfs.metadata().object(&object_id).is_none(),
            "test requires backend-only committed metadata"
        );

        let outcome = dfs
            .write_outcome(&intent.intent_id)
            .expect("write outcome from backend metadata");
        assert!(matches!(
            outcome,
            NetworkedDfsWriteOutcome::Committed {
                version,
                content_hash,
                ..
            } if version == version_2 && content_hash == hash
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn malformed_dfs_protocol_frame_does_not_block_following_valid_frame() {
        let root = temp_root("malformed-dfs-frame-does-not-block-valid-frame");
        let requester = NodeId::from_legacy_named("node_bad_frame_requester").expect("node");
        let server = NodeId::from_legacy_named("node_bad_frame_server").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_bad_frame_read").expect("object");
        let bytes = b"valid frame after bad frame";
        let network = InProcessNetwork::new();
        let mut requester_transport = network.add_node(requester.clone());
        let mut dfs = NetworkedDfsService::new(
            network.add_node(server.clone()),
            LocalContentStore::new(root.join("server-content")).expect("server content"),
        );
        dfs.add_file_system(file_system(server.clone()))
            .expect("filesystem");
        let mut metadata = file_metadata(server.clone());
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![server.clone()];
        dfs.put_local_object_version(metadata, bytes)
            .expect("seed object");

        requester_transport
            .send(
                &server,
                NetworkFrame {
                    frame_id: NetworkFrameId(1),
                    source_node_id: requester.clone(),
                    target_node_id: server.clone(),
                    message: MeshMessage {
                        message_id: MeshMessageId(1),
                        origin_node_id: requester.clone(),
                        target_node_id: server.clone(),
                        body: MeshMessageBody::Protocol(ProtocolMessage::new(
                            DFS_PROTOCOL,
                            b"not a sinomo object payload".to_vec(),
                        )),
                    },
                },
            )
            .expect("send malformed frame");
        requester_transport
            .send(
                &server,
                NetworkFrame {
                    frame_id: NetworkFrameId(2),
                    source_node_id: requester.clone(),
                    target_node_id: server.clone(),
                    message: MeshMessage {
                        message_id: MeshMessageId(2),
                        origin_node_id: requester.clone(),
                        target_node_id: server.clone(),
                        body: MeshMessageBody::Protocol(
                            ProtocolMessage::encode(
                                DFS_PROTOCOL,
                                &DfsNetworkMessage::ReadRequest(DfsReadRequest {
                                    request_id: "dfs_net_req_after_bad_frame".to_string(),
                                    object_id: object_id.clone(),
                                }),
                            )
                            .expect("valid read request"),
                        ),
                    },
                },
            )
            .expect("send valid frame");

        let events = dfs
            .process_incoming()
            .expect("malformed frame should be isolated");

        assert!(events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadServed {
                    request_id,
                    object_id: served_object_id,
                    result: DfsReadResult::Found(data),
                    ..
                } if request_id == "dfs_net_req_after_bad_frame"
                    && served_object_id == &object_id
                    && data.bytes == bytes
            )
        }));
        let responses = requester_transport
            .drain_incoming()
            .expect("requester response mailbox");
        assert_eq!(responses.len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn commit_version_applies_when_response_target_is_unroutable() {
        let root = temp_root("commit-response-target-unroutable");
        let owner = NodeId::from_legacy_named("node_commit_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_commit_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_commit_response").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let file_system_id = sifs_file_system_id();
        let bytes = b"commit response target is unavailable";
        let content_hash = content_hash(bytes);
        let content = LocalContentStore::new(root.join("content")).expect("content");
        content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("staged replica bytes");
        let mut transport = ReceiveOnlyTransport::new(replica.clone());
        let object_metadata = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "commit-response.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
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
            version: version_1,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        transport.push_message(
            owner.clone(),
            replica.clone(),
            DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_unroutable_response".to_string(),
                intent_id: "intent_commit_unroutable_response".to_string(),
                file_system_id,
                object_id: object_id.clone(),
                expected_base_version: Some(object_metadata.version.clone()),
                version: version_2.clone(),
                content_hash: content_hash.clone(),
                object_metadata: Some(object_metadata),
            }),
        );
        let mut dfs = NetworkedDfsService::new(transport, content);

        let events = dfs
            .process_incoming()
            .expect("commit should apply even if response cannot be routed");
        assert!(events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::VersionCommitted {
                    object_id: committed,
                    version,
                    content_hash: committed_hash,
                    ..
                } if committed == &object_id && version == &version_2 && committed_hash == &content_hash
            )
        }));
        assert_eq!(
            dfs.read_local_object(&object_id)
                .expect("read committed object")
                .expect("committed data")
                .bytes,
            bytes
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn chunk_assemblers_ignore_identical_duplicate_chunks() {
        let object_id = DfsObjectId::from_legacy_named("obj_duplicate_chunk").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
        let bytes = (0..70_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let first_chunk = bytes[..DFS_BLOB_CHUNK_BYTES].to_vec();
        let second_chunk = bytes[DFS_BLOB_CHUNK_BYTES..].to_vec();
        let hash = content_hash(&bytes);

        let mut blob_transfer = InProgressBlobTransfer::new(
            "dfs_net_req_blob_duplicate".to_string(),
            "dfs_intent_blob_duplicate".to_string(),
            file_system_id,
            object_id.clone(),
            None,
            version.clone(),
            hash.clone(),
            bytes.len() as u64,
            2,
            None,
        )
        .expect("blob transfer");
        assert!(
            blob_transfer
                .insert_chunk(0, 2, first_chunk.clone())
                .expect("first blob chunk")
                .is_none()
        );
        assert!(
            blob_transfer
                .insert_chunk(0, 2, first_chunk.clone())
                .expect("duplicate blob chunk")
                .is_none()
        );
        assert_eq!(
            blob_transfer
                .insert_chunk(1, 2, second_chunk.clone())
                .expect("second blob chunk")
                .expect("assembled blob"),
            bytes
        );

        let mut read_transfer = InProgressReadTransfer::new(
            object_id.clone(),
            version.clone(),
            Some(hash.clone()),
            bytes.len() as u64,
            2,
        )
        .expect("read transfer");
        assert!(
            read_transfer
                .insert_chunk(0, 2, first_chunk.clone())
                .expect("first read chunk")
                .is_none()
        );
        assert!(
            read_transfer
                .insert_chunk(0, 2, first_chunk.clone())
                .expect("duplicate read chunk")
                .is_none()
        );
        assert_eq!(
            read_transfer
                .insert_chunk(1, 2, second_chunk.clone())
                .expect("second read chunk")
                .expect("assembled read"),
            bytes
        );

        let mut fetch_transfer =
            InProgressFetchBlobTransfer::new(object_id, version, hash, bytes.len() as u64, 2)
                .expect("fetch transfer");
        assert!(
            fetch_transfer
                .insert_chunk(0, 2, first_chunk.clone())
                .expect("first fetch chunk")
                .is_none()
        );
        assert!(
            fetch_transfer
                .insert_chunk(0, 2, first_chunk)
                .expect("duplicate fetch chunk")
                .is_none()
        );
        assert_eq!(
            fetch_transfer
                .insert_chunk(1, 2, second_chunk)
                .expect("second fetch chunk")
                .expect("assembled fetch"),
            bytes
        );
    }

    #[test]
    fn chunk_assemblers_reject_conflicting_duplicate_chunks() {
        let object_id =
            DfsObjectId::from_legacy_named("obj_conflicting_duplicate_chunk").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = (0..70_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut first_chunk = bytes[..DFS_BLOB_CHUNK_BYTES].to_vec();
        let mut conflicting_chunk = first_chunk.clone();
        conflicting_chunk[0] ^= 0xff;
        let hash = content_hash(&bytes);

        let mut read_transfer =
            InProgressReadTransfer::new(object_id, version, Some(hash), bytes.len() as u64, 2)
                .expect("read transfer");
        read_transfer
            .insert_chunk(0, 2, std::mem::take(&mut first_chunk))
            .expect("first read chunk");
        let error = read_transfer
            .insert_chunk(0, 2, conflicting_chunk)
            .expect_err("conflicting duplicate chunk is rejected");
        assert!(
            error
                .to_string()
                .contains("conflicting duplicate chunked DFS read response index"),
            "{error}"
        );
    }

    #[test]
    fn stale_pending_remote_read_can_be_retried() {
        let root = temp_root("remote-read-stale-pending-retry");
        let owner = NodeId::from_legacy_named("node_read_retry_owner").expect("node");
        let reader = NodeId::from_legacy_named("node_read_retry_reader").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_transport = network.add_node(owner.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        reader_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("filesystem");
        let mut object = file_metadata(owner.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![owner.clone()];
        object.content_hash = Some(content_hash(b"remote retry bytes"));
        object.content_size_bytes = Some(b"remote retry bytes".len() as u64);
        let object_id = object.object_id.clone();
        reader_dfs.add_object_metadata(object).expect("object");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("initial read request"),
            vec![owner.clone()]
        );
        assert_eq!(
            owner_transport
                .drain_incoming()
                .expect("initial owner mailbox")
                .len(),
            1
        );
        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("active pending read is deduplicated"),
            vec![owner.clone()]
        );
        assert!(
            owner_transport
                .drain_incoming()
                .expect("deduped owner mailbox")
                .is_empty()
        );

        for pending in reader_dfs.pending_reads.values_mut() {
            pending.next_retry_at = Instant::now() - Duration::from_millis(1);
        }
        assert!(
            !reader_dfs
                .has_pending_remote_read_for_current_object(&object_id)
                .expect("expired pending read is not active")
        );
        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("retry stale pending read"),
            vec![owner.clone()]
        );
        assert_eq!(
            owner_transport
                .drain_incoming()
                .expect("retried owner mailbox")
                .len(),
            1
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn process_incoming_prunes_expired_pending_remote_reads() {
        let root = temp_root("remote-read-process-prunes-expired");
        let owner = NodeId::from_legacy_named("node_read_prune_owner").expect("node");
        let reader = NodeId::from_legacy_named("node_read_prune_reader").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_read_prune").expect("object");
        let version = DfsVersion::from_legacy_named("version_1").expect("version");
        let request_id = "dfs_net_req_read_prune".to_string();
        let hash = content_hash(b"expired read bytes");
        let mut dfs = NetworkedDfsService::new(
            ReceiveOnlyTransport::new(reader.clone()),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        dfs.pending_reads.insert(
            request_id.clone(),
            PendingNetworkedRead {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(hash.clone()),
                content_size_bytes: Some(70_000),
                target_node_id: owner,
                next_retry_at: Instant::now()
                    - DFS_CHUNK_TRANSFER_IDLE_TIMEOUT
                    - Duration::from_millis(1),
            },
        );
        dfs.incoming_read_transfers.insert(
            request_id.clone(),
            InProgressReadTransfer {
                object_id,
                version,
                content_hash: Some(hash),
                total_size_bytes: 70_000,
                last_updated_at: Instant::now(),
                chunks: vec![None; 2],
            },
        );

        assert!(
            dfs.process_incoming()
                .expect("process incoming without frames")
                .is_empty()
        );

        assert!(!dfs.pending_reads.contains_key(&request_id));
        assert!(!dfs.incoming_read_transfers.contains_key(&request_id));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn process_incoming_accepts_queued_remote_read_response_before_expiry_prune() {
        let root = temp_root("remote-read-process-accepts-queued-expired");
        let owner = NodeId::from_legacy_named("node_read_late_owner").expect("node");
        let reader = NodeId::from_legacy_named("node_read_late_reader").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_read_late").expect("object");
        let version = DfsVersion::from_legacy_named("version_1").expect("version");
        let request_id = "dfs_net_req_read_late".to_string();
        let bytes = b"late but queued read bytes".to_vec();
        let hash = content_hash(&bytes);
        let mut transport = ReceiveOnlyTransport::new(reader.clone());
        transport.push_message(
            owner.clone(),
            reader.clone(),
            DfsNetworkMessage::ReadResponse(DfsReadResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: Some(hash.clone()),
                    bytes: bytes.clone(),
                }),
            }),
        );
        let mut dfs = NetworkedDfsService::new(
            transport,
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        dfs.add_file_system(file_system(owner.clone()))
            .expect("filesystem");
        let mut object = file_metadata(owner.clone());
        object.object_id = object_id.clone();
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![owner.clone()];
        object.content_hash = Some(hash.clone());
        object.content_size_bytes = Some(bytes.len() as u64);
        object.version = version.clone();
        dfs.add_object_metadata(object).expect("object");
        dfs.pending_reads.insert(
            request_id.clone(),
            PendingNetworkedRead {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(hash.clone()),
                content_size_bytes: Some(bytes.len() as u64),
                target_node_id: owner.clone(),
                next_retry_at: Instant::now()
                    - DFS_CHUNK_TRANSFER_IDLE_TIMEOUT
                    - Duration::from_millis(1),
            },
        );

        let events = dfs.process_incoming().expect("process queued response");

        assert!(events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id: event_request_id,
                    object_id: event_object_id,
                    source_node_id,
                    result: DfsReadResult::Found(_),
                } if event_request_id == &request_id
                    && event_object_id == &object_id
                    && source_node_id == &owner
            )
        }));
        assert!(!dfs.pending_reads.contains_key(&request_id));
        assert_eq!(
            dfs.read_local_object(&object_id)
                .expect("local read")
                .expect("cached data")
                .bytes,
            bytes
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn remote_read_planning_deduplicates_duplicate_locations() {
        let root = temp_root("remote-read-dedupes-duplicate-locations");
        let owner = NodeId::from_legacy_named("node_read_dedupe_owner").expect("node");
        let reader = NodeId::from_legacy_named("node_read_dedupe_reader").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_transport = network.add_node(owner.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        reader_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("filesystem");
        let mut object = file_metadata(owner.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![owner.clone(), owner.clone()];
        object.content_hash = Some(content_hash(b"duplicate location bytes"));
        object.content_size_bytes = Some(b"duplicate location bytes".len() as u64);
        let object_id = object.object_id.clone();
        reader_dfs.add_object_metadata(object).expect("object");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("deduped read request"),
            vec![owner.clone()]
        );
        assert_eq!(
            owner_transport
                .drain_incoming()
                .expect("owner mailbox")
                .len(),
            1
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn remote_read_planning_uses_single_active_source_and_falls_back_after_miss() {
        let root = temp_root("remote-read-single-active-fallback");
        let owner_a = NodeId::from_legacy_named("node_read_source_a").expect("node");
        let owner_b = NodeId::from_legacy_named("node_read_source_b").expect("node");
        let reader = NodeId::from_legacy_named("node_read_single_reader").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_a_transport = network.add_node(owner_a.clone());
        let mut owner_b_transport = network.add_node(owner_b.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        reader_dfs
            .add_file_system(file_system(owner_a.clone()))
            .expect("filesystem");
        let mut object = file_metadata(owner_a.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![owner_a.clone(), owner_b.clone()];
        object.content_hash = Some(content_hash(b"single active read bytes"));
        object.content_size_bytes = Some(b"single active read bytes".len() as u64);
        let object_id = object.object_id.clone();
        reader_dfs.add_object_metadata(object).expect("object");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("first read request"),
            vec![owner_a.clone()]
        );
        assert_eq!(
            owner_a_transport
                .drain_incoming()
                .expect("source a mailbox")
                .len(),
            1
        );
        assert!(
            owner_b_transport
                .drain_incoming()
                .expect("source b mailbox")
                .is_empty(),
            "remote reads should not fan out to every replica when one source is active"
        );
        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("active request is reused"),
            vec![owner_a.clone()]
        );
        let request_id = reader_dfs
            .pending_reads
            .iter()
            .find_map(|(request_id, pending)| {
                (pending.target_node_id == owner_a).then_some(request_id.clone())
            })
            .expect("pending source a read");
        reader_dfs
            .handle_networked_read_response(
                owner_a.clone(),
                DfsReadResponse {
                    request_id,
                    object_id: object_id.clone(),
                    result: DfsReadResult::NotFound,
                },
            )
            .expect("source a miss is handled");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("fallback request after miss"),
            vec![owner_b.clone()]
        );
        assert_eq!(
            owner_b_transport
                .drain_incoming()
                .expect("source b fallback mailbox")
                .len(),
            1
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn pending_remote_read_to_removed_location_does_not_block_fallback() {
        let root = temp_root("remote-read-removed-location-not-active");
        let source_a = NodeId::from_legacy_named("node_read_removed_a").expect("node");
        let source_b = NodeId::from_legacy_named("node_read_removed_b").expect("node");
        let reader = NodeId::from_legacy_named("node_read_removed_reader").expect("node");
        let network = InProcessNetwork::new();
        let mut source_a_transport = network.add_node(source_a.clone());
        let mut source_b_transport = network.add_node(source_b.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        reader_dfs
            .add_file_system(file_system(source_a.clone()))
            .expect("filesystem");
        let mut object = file_metadata(source_a.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![source_a.clone(), source_b.clone()];
        object.content_hash = Some(content_hash(b"removed location bytes"));
        object.content_size_bytes = Some(b"removed location bytes".len() as u64);
        let object_id = object.object_id.clone();
        reader_dfs
            .add_object_metadata(object.clone())
            .expect("object");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("first read request"),
            vec![source_a.clone()]
        );
        assert_eq!(
            source_a_transport
                .drain_incoming()
                .expect("source a mailbox")
                .len(),
            1
        );
        let source_a_request_id = reader_dfs
            .pending_reads
            .iter()
            .find_map(|(request_id, pending)| {
                (pending.target_node_id == source_a).then(|| request_id.clone())
            })
            .expect("source a pending request");
        reader_dfs.incoming_read_transfers.insert(
            source_a_request_id.clone(),
            InProgressReadTransfer {
                object_id: object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                total_size_bytes: object.content_size_bytes.expect("size"),
                last_updated_at: Instant::now(),
                chunks: vec![Some(b"partial".to_vec())],
            },
        );
        object.current_locations = vec![source_b.clone()];
        reader_dfs
            .add_object_metadata(object)
            .expect("advance object");

        assert!(
            !reader_dfs
                .has_pending_remote_read_for_current_object(&object_id)
                .expect("removed source should not count as active pending read")
        );
        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("fallback request after location removal"),
            vec![source_b.clone()]
        );
        assert!(
            !reader_dfs.pending_reads.contains_key(&source_a_request_id),
            "removed source pending read should be pruned when planning fallback"
        );
        assert!(
            !reader_dfs
                .incoming_read_transfers
                .contains_key(&source_a_request_id),
            "removed source partial transfer should be pruned with its pending read"
        );
        assert_eq!(
            source_b_transport
                .drain_incoming()
                .expect("source b mailbox")
                .len(),
            1
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn expired_remote_read_attempt_rotates_to_next_location_before_retrying_same_source() {
        let root = temp_root("remote-read-expired-rotates");
        let source_a = NodeId::from_legacy_named("node_read_expired_a").expect("node");
        let source_b = NodeId::from_legacy_named("node_read_expired_b").expect("node");
        let reader = NodeId::from_legacy_named("node_read_expired_reader").expect("node");
        let network = InProcessNetwork::new();
        let mut source_a_transport = network.add_node(source_a.clone());
        let mut source_b_transport = network.add_node(source_b.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        reader_dfs
            .add_file_system(file_system(source_a.clone()))
            .expect("filesystem");
        let mut object = file_metadata(source_a.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![source_a.clone(), source_b.clone()];
        object.content_hash = Some(content_hash(b"expired rotate bytes"));
        object.content_size_bytes = Some(b"expired rotate bytes".len() as u64);
        let object_id = object.object_id.clone();
        reader_dfs.add_object_metadata(object).expect("object");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("first request"),
            vec![source_a.clone()]
        );
        assert_eq!(
            source_a_transport
                .drain_incoming()
                .expect("source a first mailbox")
                .len(),
            1
        );
        for pending in reader_dfs.pending_reads.values_mut() {
            pending.next_retry_at = Instant::now() - Duration::from_millis(1);
        }

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("rotated request"),
            vec![source_b.clone()]
        );
        assert!(
            source_a_transport
                .drain_incoming()
                .expect("source a retry mailbox")
                .is_empty(),
            "expired read should try a later replica before retrying the same silent source"
        );
        assert_eq!(
            source_b_transport
                .drain_incoming()
                .expect("source b mailbox")
                .len(),
            1
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn late_remote_read_from_removed_location_does_not_clear_fallback_request() {
        let root = temp_root("remote-read-late-removed-location");
        let source_a = NodeId::from_legacy_named("node_read_late_a").expect("node");
        let source_b = NodeId::from_legacy_named("node_read_late_b").expect("node");
        let reader = NodeId::from_legacy_named("node_read_late_reader").expect("node");
        let network = InProcessNetwork::new();
        let mut source_a_transport = network.add_node(source_a.clone());
        let mut source_b_transport = network.add_node(source_b.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        reader_dfs
            .add_file_system(file_system(source_a.clone()))
            .expect("filesystem");
        let bytes = b"late removed source bytes";
        let mut object = file_metadata(source_a.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![source_a.clone(), source_b.clone()];
        object.content_hash = Some(content_hash(bytes));
        object.content_size_bytes = Some(bytes.len() as u64);
        let object_id = object.object_id.clone();
        let version = object.version.clone();
        let content_hash = object.content_hash.clone();
        reader_dfs
            .add_object_metadata(object.clone())
            .expect("object");

        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("request source a");
        assert_eq!(
            source_a_transport
                .drain_incoming()
                .expect("source a mailbox")
                .len(),
            1
        );
        let source_a_request_id = reader_dfs
            .pending_reads
            .iter()
            .find_map(|(request_id, pending)| {
                (pending.target_node_id == source_a).then_some(request_id.clone())
            })
            .expect("source a request");
        object.current_locations = vec![source_b.clone()];
        reader_dfs
            .add_object_metadata(object)
            .expect("advance object");
        reader_dfs
            .request_remote_object_read(&object_id)
            .expect("fallback source b");
        assert_eq!(
            source_b_transport
                .drain_incoming()
                .expect("source b mailbox")
                .len(),
            1
        );
        let source_b_request_id = reader_dfs
            .pending_reads
            .iter()
            .find_map(|(request_id, pending)| {
                (pending.target_node_id == source_b).then_some(request_id.clone())
            })
            .expect("source b request");
        assert!(
            !reader_dfs.pending_reads.contains_key(&source_a_request_id),
            "fallback planning should prune the stale source a request"
        );

        let event = reader_dfs
            .handle_networked_read_response(
                source_a.clone(),
                DfsReadResponse {
                    request_id: source_a_request_id,
                    object_id: object_id.clone(),
                    result: DfsReadResult::Found(DfsReadData {
                        object_id: object_id.clone(),
                        version,
                        content_hash,
                        bytes: bytes.to_vec(),
                    }),
                },
            )
            .expect("late source a response");

        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                source_node_id,
                result: DfsReadResult::Rejected { reason },
                ..
            } if source_node_id == source_a && reason.contains("unknown DFS read request")
        ));
        assert!(
            reader_dfs.pending_reads.contains_key(&source_b_request_id),
            "late response from removed source must not clear the fallback request"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn remote_read_rejection_from_stale_location_falls_back_to_next_source() {
        let root = temp_root("remote-read-rejection-fallback");
        let source_a = NodeId::from_legacy_named("node_read_reject_a").expect("node");
        let source_b = NodeId::from_legacy_named("node_read_reject_b").expect("node");
        let reader = NodeId::from_legacy_named("node_read_reject_reader").expect("node");
        let network = InProcessNetwork::new();
        let mut source_a_transport = network.add_node(source_a.clone());
        let mut source_b_transport = network.add_node(source_b.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        )
        .with_metadata_backend(
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("reader"), root.join("meta"))
                .expect("metadata backend"),
        )
        .expect("metadata backend");
        reader_dfs
            .add_file_system(file_system(source_a.clone()))
            .expect("filesystem");
        let mut object = file_metadata(source_a.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![source_a.clone(), source_b.clone()];
        object.content_hash = Some(content_hash(b"stale rejection bytes"));
        object.content_size_bytes = Some(b"stale rejection bytes".len() as u64);
        let object_id = object.object_id.clone();
        let version = object.version.clone();
        let hash = object.content_hash.clone();
        reader_dfs.add_object_metadata(object).expect("object");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("first read request"),
            vec![source_a.clone()]
        );
        assert_eq!(
            source_a_transport
                .drain_incoming()
                .expect("source a mailbox")
                .len(),
            1
        );
        let request_id = reader_dfs
            .pending_reads
            .iter()
            .find_map(|(request_id, pending)| {
                (pending.target_node_id == source_a).then_some(request_id.clone())
            })
            .expect("pending source a read");
        reader_dfs
            .handle_networked_read_response(
                source_a.clone(),
                DfsReadResponse {
                    request_id,
                    object_id: object_id.clone(),
                    result: DfsReadResult::Rejected {
                        reason: format!(
                            "node {source_a} is not a current location for DFS object {object_id}"
                        ),
                    },
                },
            )
            .expect("stale-location rejection handled");

        assert_eq!(
            reader_dfs
                .current_read_object_metadata(&object_id)
                .expect("current metadata")
                .current_locations,
            vec![source_b.clone()]
        );
        assert_eq!(
            reader_dfs
                .metadata_backend
                .as_ref()
                .expect("backend")
                .get_version(&object_id, &version)
                .expect("version")
                .expect("version")
                .locations,
            vec![source_b.clone()]
        );
        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("fallback after stale-location rejection"),
            vec![source_b.clone()]
        );
        assert_eq!(
            source_b_transport
                .drain_incoming()
                .expect("source b mailbox")
                .len(),
            1
        );
        assert_eq!(
            hash,
            reader_dfs
                .current_read_object_metadata(&object_id)
                .expect("current metadata after fallback")
                .content_hash
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn inline_remote_read_wrong_object_is_isolated_and_falls_back() {
        let root = temp_root("remote-read-wrong-object-isolated");
        let source_a = NodeId::from_legacy_named("node_read_wrong_object_a").expect("node");
        let source_b = NodeId::from_legacy_named("node_read_wrong_object_b").expect("node");
        let reader = NodeId::from_legacy_named("node_read_wrong_object_reader").expect("node");
        let wrong_object_id =
            DfsObjectId::from_legacy_named("obj_wrong_read_response").expect("object");
        let network = InProcessNetwork::new();
        let mut source_a_transport = network.add_node(source_a.clone());
        let mut source_b_transport = network.add_node(source_b.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        )
        .with_metadata_backend(
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("reader"), root.join("meta"))
                .expect("metadata backend"),
        )
        .expect("metadata backend");
        reader_dfs
            .add_file_system(file_system(source_a.clone()))
            .expect("filesystem");
        let mut object = file_metadata(source_a.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![source_a.clone(), source_b.clone()];
        object.content_hash = Some(content_hash(b"right remote bytes"));
        object.content_size_bytes = Some(b"right remote bytes".len() as u64);
        let object_id = object.object_id.clone();
        let version = object.version.clone();
        let hash = object.content_hash.clone();
        reader_dfs.add_object_metadata(object).expect("object");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("first read request"),
            vec![source_a.clone()]
        );
        assert_eq!(
            source_a_transport
                .drain_incoming()
                .expect("source a mailbox")
                .len(),
            1
        );
        let request_id = reader_dfs
            .pending_reads
            .iter()
            .find_map(|(request_id, pending)| {
                (pending.target_node_id == source_a).then_some(request_id.clone())
            })
            .expect("pending source a read");

        let event = reader_dfs
            .handle_networked_read_response(
                source_a.clone(),
                DfsReadResponse {
                    request_id: request_id.clone(),
                    object_id: wrong_object_id.clone(),
                    result: DfsReadResult::Found(DfsReadData {
                        object_id: wrong_object_id,
                        version: version.clone(),
                        content_hash: hash.clone(),
                        bytes: b"right remote bytes".to_vec(),
                    }),
                },
            )
            .expect("wrong-object response is isolated");

        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: completed_request_id,
                object_id: completed_object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            } if completed_request_id == request_id
                && completed_object_id != object_id
                && source_node_id == source_a
                && reason.contains("unexpected DFS read response object")
        ));
        assert!(!reader_dfs.pending_reads.contains_key(&request_id));
        assert_eq!(
            reader_dfs
                .current_read_object_metadata(&object_id)
                .expect("current metadata")
                .current_locations,
            vec![source_b.clone()]
        );
        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("fallback after wrong-object response"),
            vec![source_b.clone()]
        );
        assert_eq!(
            source_b_transport
                .drain_incoming()
                .expect("source b mailbox")
                .len(),
            1
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn inline_remote_read_wrong_payload_object_is_isolated_and_falls_back() {
        let root = temp_root("remote-read-wrong-payload-object-isolated");
        let source_a = NodeId::from_legacy_named("node_read_wrong_payload_a").expect("node");
        let source_b = NodeId::from_legacy_named("node_read_wrong_payload_b").expect("node");
        let reader = NodeId::from_legacy_named("node_read_wrong_payload_reader").expect("node");
        let wrong_object_id =
            DfsObjectId::from_legacy_named("obj_wrong_read_payload").expect("object");
        let network = InProcessNetwork::new();
        let mut source_a_transport = network.add_node(source_a.clone());
        let mut source_b_transport = network.add_node(source_b.clone());
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("reader")).expect("reader content"),
        );
        reader_dfs
            .add_file_system(file_system(source_a.clone()))
            .expect("filesystem");
        let mut object = file_metadata(source_a.clone());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![source_a.clone(), source_b.clone()];
        object.content_hash = Some(content_hash(b"right payload bytes"));
        object.content_size_bytes = Some(b"right payload bytes".len() as u64);
        let object_id = object.object_id.clone();
        let version = object.version.clone();
        let hash = object.content_hash.clone();
        reader_dfs.add_object_metadata(object).expect("object");

        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("first read request"),
            vec![source_a.clone()]
        );
        assert_eq!(
            source_a_transport
                .drain_incoming()
                .expect("source a mailbox")
                .len(),
            1
        );
        let request_id = reader_dfs
            .pending_reads
            .iter()
            .find_map(|(request_id, pending)| {
                (pending.target_node_id == source_a).then_some(request_id.clone())
            })
            .expect("pending source a read");

        let event = reader_dfs
            .handle_networked_read_response(
                source_a.clone(),
                DfsReadResponse {
                    request_id: request_id.clone(),
                    object_id: object_id.clone(),
                    result: DfsReadResult::Found(DfsReadData {
                        object_id: wrong_object_id.clone(),
                        version: version.clone(),
                        content_hash: hash.clone(),
                        bytes: b"right payload bytes".to_vec(),
                    }),
                },
            )
            .expect("wrong-payload response is isolated");

        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: completed_request_id,
                object_id: completed_object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            } if completed_request_id == request_id
                && completed_object_id == object_id
                && source_node_id == source_a
                && reason.contains("unexpected DFS read response payload object")
        ));
        assert!(!reader_dfs.pending_reads.contains_key(&request_id));
        assert_eq!(
            reader_dfs
                .current_read_object_metadata(&object_id)
                .expect("current metadata")
                .current_locations,
            vec![source_b.clone()]
        );
        assert_eq!(
            reader_dfs
                .request_remote_object_read(&object_id)
                .expect("fallback after wrong-payload response"),
            vec![source_b.clone()]
        );
        assert_eq!(
            source_b_transport
                .drain_incoming()
                .expect("source b mailbox")
                .len(),
            1
        );
        assert!(matches!(
            reader_dfs.read_local_object(&wrong_object_id),
            Err(DfsError::UnknownObject { .. })
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn object_metadata_normalizes_duplicate_current_locations() {
        let root = temp_root("metadata-normalizes-duplicate-locations");
        let owner = NodeId::from_legacy_named("node_location_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_location_replica").expect("node");
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut object = file_metadata(owner.clone());
        object.object_id =
            DfsObjectId::from_legacy_named("obj_duplicate_locations").expect("object");
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];

        dfs.add_object_metadata(object.clone())
            .expect("object metadata");

        assert_eq!(
            dfs.metadata()
                .object(&object.object_id)
                .expect("memory object")
                .current_locations,
            vec![owner.clone(), replica.clone()]
        );
        let backend_object = dfs
            .metadata_backend()
            .expect("backend")
            .get_object(&object.object_id)
            .expect("backend read")
            .expect("backend object");
        assert_eq!(
            backend_object.current_locations,
            vec![owner.clone(), replica.clone()]
        );
        let backend_version = dfs
            .metadata_backend()
            .expect("backend")
            .get_version(&object.object_id, &object.version)
            .expect("backend version read")
            .expect("backend version");
        assert_eq!(backend_version.locations, vec![owner, replica]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn object_version_record_normalizes_duplicate_locations() {
        let owner = NodeId::from_legacy_named("node_version_location_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_version_location_replica").expect("node");
        let mut object = file_metadata(owner.clone());
        object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];

        let record = object_version_record(&object, None);

        assert_eq!(record.locations, vec![owner, replica]);
    }

    #[test]
    fn remote_read_content_mismatch_prunes_stale_location() {
        let root = temp_root("remote-read-mismatch-prunes-location");
        let reader = NodeId::from_legacy_named("node_read_mismatch_reader").expect("node");
        let source = NodeId::from_legacy_named("node_read_mismatch_source").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_read_mismatch").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let expected_hash = content_hash(b"expected remote bytes");
        let request_id = "dfs_net_req_read_mismatch".to_string();
        let mut dfs = networked_sifs_with_backend(&root, reader.clone());
        let mut object = file_metadata(source.clone());
        object.object_id = object_id.clone();
        object.version = version.clone();
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![source.clone()];
        object.content_hash = Some(expected_hash.clone());
        object.content_size_bytes = Some(b"expected remote bytes".len() as u64);
        dfs.add_object_metadata(object).expect("object metadata");
        dfs.pending_reads.insert(
            request_id.clone(),
            PendingNetworkedRead {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(expected_hash.clone()),
                content_size_bytes: Some(b"expected remote bytes".len() as u64),
                target_node_id: source.clone(),
                next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
            },
        );

        let event = dfs
            .handle_networked_read_response(
                source.clone(),
                DfsReadResponse {
                    request_id: request_id.clone(),
                    object_id: object_id.clone(),
                    result: DfsReadResult::Found(DfsReadData {
                        object_id: object_id.clone(),
                        version: version.clone(),
                        content_hash: Some(expected_hash),
                        bytes: b"corrupt remote bytes".to_vec(),
                    }),
                },
            )
            .expect("mismatch handled");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: completed_request_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
                ..
            } if completed_request_id == request_id
                && source_node_id == source
                && reason.contains("content hash mismatch")
        ));
        assert!(!dfs.pending_reads.contains_key(&request_id));
        let object = dfs
            .current_read_object_metadata(&object_id)
            .expect("current metadata");
        assert!(
            !object.current_locations.contains(&source),
            "source that returned bytes failing content validation should be pruned"
        );
        assert_eq!(
            dfs.request_remote_object_read(&object_id)
                .expect("no stale source to retry"),
            Vec::<NodeId>::new()
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn remote_read_location_prune_updates_stale_backend_version_record() {
        let root = temp_root("read-prune-updates-version-record");
        let reader = NodeId::from_legacy_named("node_read_prune_reader").expect("node");
        let source = NodeId::from_legacy_named("node_read_prune_source").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_read_prune_version").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let hash = content_hash(b"version-location bytes");
        let mut dfs = networked_sifs_with_backend(&root, reader);
        let mut object = file_metadata(source.clone());
        object.object_id = object_id.clone();
        object.version = version.clone();
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations.clear();
        object.content_hash = Some(hash.clone());
        object.content_size_bytes = Some(b"version-location bytes".len() as u64);
        dfs.add_object_metadata(object.clone())
            .expect("object metadata");
        dfs.metadata_backend
            .as_mut()
            .expect("backend")
            .put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(hash.clone()),
                size_bytes: object.content_size_bytes,
                locations: vec![source.clone()],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            })
            .expect("stale version locations");

        dfs.mark_remote_read_location_missing(&object_id, &version, &Some(hash), &source)
            .expect("prune stale version location");

        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .expect("backend")
                .get_object(&object_id)
                .expect("backend object")
                .expect("backend object")
                .current_locations,
            Vec::<NodeId>::new()
        );
        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .expect("backend")
                .get_version(&object_id, &version)
                .expect("backend version")
                .expect("backend version")
                .locations,
            Vec::<NodeId>::new()
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn idle_chunk_transfer_pruning_releases_request_state() {
        let root = temp_root("idle-chunk-transfer-prune");
        let owner = NodeId::from_legacy_named("node_prune_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_prune_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_prune_transfer").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
        let content_hash = content_hash(b"prune bytes");
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        let stale_at = Instant::now() - DFS_CHUNK_TRANSFER_IDLE_TIMEOUT - Duration::from_secs(1);

        let read_request_id = "dfs_net_req_prune_read".to_string();
        dfs.pending_reads.insert(
            read_request_id.clone(),
            PendingNetworkedRead {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(content_hash.clone()),
                content_size_bytes: Some(70_000),
                target_node_id: replica.clone(),
                next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
            },
        );
        dfs.incoming_read_transfers.insert(
            read_request_id.clone(),
            InProgressReadTransfer {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(content_hash.clone()),
                total_size_bytes: 70_000,
                last_updated_at: stale_at,
                chunks: vec![None; 2],
            },
        );

        let fetch_request_id = "dfs_net_req_prune_fetch".to_string();
        let intent_id = "dfs_intent_prune".to_string();
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                size_bytes: 70_000,
                stage_retries: BTreeMap::from([(
                    replica.clone(),
                    RepairRetryState {
                        attempts: 3,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                repair_retries: BTreeMap::from([(
                    replica.clone(),
                    RepairRetryState {
                        attempts: 3,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::from([replica.clone()]),
                fetch_inflight_request_ids: BTreeMap::from([(
                    replica.clone(),
                    fetch_request_id.clone(),
                )]),
                fetch_inflight_started_ticks: BTreeMap::from([(replica.clone(), 0)]),
                fetch_failed_from: BTreeSet::new(),
            },
        );
        dfs.incoming_fetch_transfers.insert(
            fetch_request_id.clone(),
            InProgressFetchBlobTransfer {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                total_size_bytes: 70_000,
                last_updated_at: stale_at,
                chunks: vec![None; 2],
            },
        );

        let blob_key = blob_transfer_key("stage", &replica, &intent_id, "dfs_net_req_prune_stage");
        dfs.incoming_blob_transfers.insert(
            blob_key.clone(),
            InProgressBlobTransfer {
                request_id: "dfs_net_req_prune_stage".to_string(),
                intent_id: intent_id.clone(),
                file_system_id,
                object_id,
                expected_base_version: None,
                version,
                content_hash,
                total_size_bytes: 70_000,
                object_metadata: None,
                last_updated_at: stale_at,
                chunks: vec![None; 2],
            },
        );

        dfs.prune_idle_chunk_transfers();

        assert!(!dfs.pending_reads.contains_key(&read_request_id));
        assert!(!dfs.incoming_read_transfers.contains_key(&read_request_id));
        assert!(!dfs.incoming_fetch_transfers.contains_key(&fetch_request_id));
        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        assert!(!pending.fetch_inflight_from.contains(&replica));
        assert!(!pending.fetch_inflight_request_ids.contains_key(&replica));
        assert!(!pending.fetch_inflight_started_ticks.contains_key(&replica));
        assert!(pending.fetch_failed_from.contains(&replica));
        assert_eq!(
            pending
                .stage_retries
                .get(&replica)
                .expect("stage retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .stage_retries
                .get(&replica)
                .expect("stage retry")
                .inflight
        );
        assert_eq!(
            pending
                .repair_retries
                .get(&replica)
                .expect("repair retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .repair_retries
                .get(&replica)
                .expect("repair retry")
                .inflight
        );
        assert!(!dfs.incoming_blob_transfers.contains_key(&blob_key));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn retry_pending_repairs_is_idle_noop_without_distributed_work() {
        let root = temp_root("retry-pending-repairs-idle-noop");
        let owner = NodeId::from_legacy_named("node_retry_idle").expect("node");
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        dfs.repair_retry_tick = 41;

        let scheduled = dfs.retry_pending_repairs().expect("idle retry");

        assert!(scheduled.is_empty());
        assert_eq!(dfs.repair_retry_tick, 41);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn retry_pending_repairs_prunes_idle_reads_without_distributed_writes() {
        let root = temp_root("retry-pending-repairs-prunes-idle-reads");
        let owner = NodeId::from_legacy_named("node_retry_prune_owner").expect("node");
        let reader = NodeId::from_legacy_named("node_retry_prune_reader").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_retry_prune").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let content_hash = content_hash(b"retry prune read bytes");
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        dfs.repair_retry_tick = 17;
        let request_id = "dfs_net_req_retry_prune_read".to_string();
        let stale_at = Instant::now() - DFS_CHUNK_TRANSFER_IDLE_TIMEOUT - Duration::from_secs(1);
        dfs.pending_reads.insert(
            request_id.clone(),
            PendingNetworkedRead {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(content_hash.clone()),
                content_size_bytes: Some(70_000),
                target_node_id: reader,
                next_retry_at: Instant::now() + DFS_REMOTE_READ_RETRY_DELAY,
            },
        );
        dfs.incoming_read_transfers.insert(
            request_id.clone(),
            InProgressReadTransfer {
                object_id,
                version,
                content_hash: Some(content_hash),
                total_size_bytes: 70_000,
                last_updated_at: stale_at,
                chunks: vec![None; 2],
            },
        );

        let scheduled = dfs.retry_pending_repairs().expect("retry prune");

        assert!(scheduled.is_empty());
        assert_eq!(dfs.repair_retry_tick, 17);
        assert!(!dfs.pending_reads.contains_key(&request_id));
        assert!(!dfs.incoming_read_transfers.contains_key(&request_id));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn stale_fetch_request_expiry_wakes_stage_and_repair_retries() {
        let root = temp_root("stale-fetch-request-expiry");
        let owner = NodeId::from_legacy_named("node_fetch_timeout_owner").expect("node");
        let source = NodeId::from_legacy_named("node_fetch_timeout_source").expect("node");
        let target = NodeId::from_legacy_named("node_fetch_timeout_target").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_fetch_timeout").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let intent_id = "dfs_intent_fetch_timeout".to_string();
        let request_id = "dfs_net_req_fetch_timeout".to_string();
        let content_hash = content_hash(b"fetch timeout bytes");
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        dfs.repair_retry_tick = fetch_inflight_timeout_ticks();
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id,
                version,
                content_hash,
                size_bytes: 0,
                stage_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 1,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                repair_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 1,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::from([source.clone()]),
                fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id)]),
                fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        dfs.expire_stale_fetch_requests();

        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        assert!(!pending.fetch_inflight_from.contains(&source));
        assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
        assert!(!pending.fetch_inflight_started_ticks.contains_key(&source));
        assert!(pending.fetch_failed_from.contains(&source));
        assert_eq!(
            pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .inflight
        );
        assert_eq!(
            pending
                .repair_retries
                .get(&target)
                .expect("repair retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .repair_retries
                .get(&target)
                .expect("repair retry")
                .inflight
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn fetch_rejection_wakes_stage_and_repair_retries() {
        let root = temp_root("fetch-rejection-wakes-retries");
        let owner = NodeId::from_legacy_named("node_fetch_reject_owner").expect("node");
        let source = NodeId::from_legacy_named("node_fetch_reject_source").expect("node");
        let target = NodeId::from_legacy_named("node_fetch_reject_target").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_fetch_reject").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let intent_id = "dfs_intent_fetch_reject".to_string();
        let request_id = "dfs_net_req_fetch_reject".to_string();
        let content_hash = content_hash(b"fetch reject bytes");
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                size_bytes: b"fetch reject bytes".len() as u64,
                stage_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 2,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                repair_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 2,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::from([source.clone()]),
                fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
                fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        let event = dfs
            .handle_fetch_blob_response(
                source.clone(),
                DfsFetchBlobResponse {
                    request_id,
                    intent_id: intent_id.clone(),
                    object_id,
                    version,
                    source_node_id: source.clone(),
                    result: DfsFetchBlobResult::NotFound,
                },
            )
            .expect("fetch miss handled");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::BlobFetchRejected {
                source_node_id,
                ..
            } if source_node_id == source
        ));
        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        assert!(!pending.fetch_inflight_from.contains(&source));
        assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
        assert!(pending.fetch_failed_from.contains(&source));
        assert_eq!(
            pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .inflight
        );
        assert_eq!(
            pending
                .repair_retries
                .get(&target)
                .expect("repair retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .repair_retries
                .get(&target)
                .expect("repair retry")
                .inflight
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn fetch_store_failure_wakes_stage_and_repair_retries() {
        let root = temp_root("fetch-store-failure-wakes-retries");
        let owner = NodeId::from_legacy_named("node_fetch_store_fail_owner").expect("node");
        let source = NodeId::from_legacy_named("node_fetch_store_fail_source").expect("node");
        let target = NodeId::from_legacy_named("node_fetch_store_fail_target").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_fetch_store_fail").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let intent_id = "dfs_intent_fetch_store_fail".to_string();
        let request_id = "dfs_net_req_fetch_store_fail".to_string();
        let expected_bytes = b"expected fetch bytes";
        let corrupt_bytes = b"corrupt fetch bytes";
        let content_hash = content_hash(expected_bytes);
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                size_bytes: expected_bytes.len() as u64,
                stage_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 2,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                repair_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 2,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::from([source.clone()]),
                fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
                fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        let event = dfs
            .handle_fetch_blob_response(
                source.clone(),
                DfsFetchBlobResponse {
                    request_id,
                    intent_id: intent_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    source_node_id: source.clone(),
                    result: DfsFetchBlobResult::Found {
                        content_hash: content_hash.clone(),
                        bytes: corrupt_bytes.to_vec(),
                    },
                },
            )
            .expect("hash mismatch is isolated to the fetch source");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::BlobFetchRejected {
                source_node_id,
                reason,
                ..
            } if source_node_id == source && reason.contains("content hash mismatch")
        ));
        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        assert!(!pending.fetch_inflight_from.contains(&source));
        assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
        assert!(pending.fetch_failed_from.contains(&source));
        assert_eq!(
            pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .inflight
        );
        assert_eq!(
            pending
                .repair_retries
                .get(&target)
                .expect("repair retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .repair_retries
                .get(&target)
                .expect("repair retry")
                .inflight
        );
        let manifest_error = dfs
            .content
            .read_manifest(object_id.as_str(), version.as_str())
            .expect_err("rejected fetch must not leave a wrong object-version manifest");
        assert!(matches!(
            manifest_error,
            ContentStoreError::Io { source, .. }
                if source.kind() == std::io::ErrorKind::NotFound
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn fetch_response_reuses_existing_local_blob_without_rehashing_payload() {
        let root = temp_root("fetch-response-reuses-existing-local-blob");
        let owner = NodeId::from_legacy_named("node_fetch_reuse_owner").expect("node");
        let source = NodeId::from_legacy_named("node_fetch_reuse_source").expect("node");
        let target = NodeId::from_legacy_named("node_fetch_reuse_target").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_fetch_reuse").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let intent_id = "dfs_intent_fetch_reuse".to_string();
        let request_id = "dfs_net_req_fetch_reuse".to_string();
        let bytes = b"already fetched bytes";
        let mut stale_payload = bytes.to_vec();
        stale_payload[0] ^= 0xff;
        stale_payload.extend_from_slice(b" with a different duplicate payload length");
        let mut dfs = NetworkedDfsService::new(
            InProcessNetwork::new().add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version.as_str(), bytes)
            .expect("seed existing local blob");
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: manifest.content_hash.clone(),
                size_bytes: bytes.len() as u64,
                stage_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 2,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::from([source.clone()]),
                fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
                fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        let event = dfs
            .handle_fetch_blob_response(
                source.clone(),
                DfsFetchBlobResponse {
                    request_id,
                    intent_id: intent_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    source_node_id: source.clone(),
                    result: DfsFetchBlobResult::Found {
                        content_hash: manifest.content_hash.clone(),
                        bytes: stale_payload,
                    },
                },
            )
            .expect("existing local blob should satisfy fetch");

        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::BlobFetched {
                source_node_id,
                ..
            } if source_node_id == source
        ));
        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        assert!(!pending.fetch_inflight_from.contains(&source));
        assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
        assert!(pending.fetch_failed_from.is_empty());
        assert_eq!(
            pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .next_retry_tick,
            0
        );
        assert_eq!(
            dfs.content
                .read_blob(&manifest.content_hash)
                .expect("existing blob"),
            bytes
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn chunked_fetch_store_failure_is_rejected_without_hard_error() {
        let root = temp_root("chunked-fetch-store-failure-isolated");
        let owner = NodeId::from_legacy_named("node_chunk_fetch_fail_owner").expect("node");
        let source = NodeId::from_legacy_named("node_chunk_fetch_fail_source").expect("node");
        let target = NodeId::from_legacy_named("node_chunk_fetch_fail_target").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_chunk_fetch_fail").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let intent_id = "dfs_intent_chunk_fetch_fail".to_string();
        let request_id = "dfs_net_req_chunk_fetch_fail".to_string();
        let expected_bytes = (0..(DFS_BLOB_CHUNK_BYTES + 7))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut corrupt_bytes = expected_bytes.clone();
        corrupt_bytes[DFS_BLOB_CHUNK_BYTES] ^= 0xff;
        let content_hash = content_hash(&expected_bytes);
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                size_bytes: expected_bytes.len() as u64,
                stage_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 2,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::from([source.clone()]),
                fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
                fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        let total_chunks = blob_chunk_count(&corrupt_bytes).expect("chunk count");
        for (chunk_index, chunk_bytes) in corrupt_bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let event = dfs
                .handle_fetch_blob_response_chunk(
                    source.clone(),
                    DfsFetchBlobResponseChunk {
                        request_id: request_id.clone(),
                        intent_id: intent_id.clone(),
                        object_id: object_id.clone(),
                        version: version.clone(),
                        source_node_id: source.clone(),
                        content_hash: content_hash.clone(),
                        total_size_bytes: corrupt_bytes.len() as u64,
                        chunk_index: chunk_index as u32,
                        total_chunks,
                        bytes: chunk_bytes.to_vec(),
                    },
                )
                .expect("corrupt fetch chunk is isolated");
            if chunk_index == 0 {
                assert!(event.is_none());
            } else {
                assert!(matches!(
                    event,
                    Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                        source_node_id,
                        reason,
                        ..
                    }) if source_node_id == source && reason.contains("content hash mismatch")
                ));
            }
        }
        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        assert!(!pending.fetch_inflight_from.contains(&source));
        assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
        assert!(pending.fetch_failed_from.contains(&source));
        assert_eq!(
            pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .next_retry_tick,
            0
        );
        assert!(
            !pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .inflight
        );
        assert!(
            dfs.content
                .read_manifest(object_id.as_str(), version.as_str())
                .is_err(),
            "rejected chunked fetch must not leave a wrong object-version manifest"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn chunked_fetch_response_reuses_existing_local_blob_without_rehashing_payload() {
        let root = temp_root("chunked-fetch-reuses-existing-local-blob");
        let owner = NodeId::from_legacy_named("node_chunk_fetch_reuse_owner").expect("node");
        let source = NodeId::from_legacy_named("node_chunk_fetch_reuse_source").expect("node");
        let target = NodeId::from_legacy_named("node_chunk_fetch_reuse_target").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_chunk_fetch_reuse").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let intent_id = "dfs_intent_chunk_fetch_reuse".to_string();
        let request_id = "dfs_net_req_chunk_fetch_reuse".to_string();
        let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 11))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut stale_payload = bytes.clone();
        stale_payload[DFS_BLOB_CHUNK_BYTES] ^= 0xff;
        stale_payload.extend_from_slice(b" with a different duplicate payload length");
        let mut dfs = NetworkedDfsService::new(
            InProcessNetwork::new().add_node(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version.as_str(), &bytes)
            .expect("seed existing local blob");
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: manifest.content_hash.clone(),
                size_bytes: bytes.len() as u64,
                stage_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 2,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::from([source.clone()]),
                fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
                fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        let total_chunks = blob_chunk_count(&stale_payload).expect("chunk count");
        let first_chunk = stale_payload
            .chunks(DFS_BLOB_CHUNK_BYTES)
            .next()
            .expect("first chunk");
        let event = dfs
            .handle_fetch_blob_response_chunk(
                source.clone(),
                DfsFetchBlobResponseChunk {
                    request_id: request_id.clone(),
                    intent_id: intent_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    source_node_id: source.clone(),
                    content_hash: manifest.content_hash.clone(),
                    total_size_bytes: stale_payload.len() as u64,
                    chunk_index: 0,
                    total_chunks,
                    bytes: first_chunk.to_vec(),
                },
            )
            .expect("existing local blob should satisfy chunked fetch")
            .expect("chunked fetch should complete from the existing local blob immediately");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::BlobFetched {
                source_node_id,
                ..
            } if source_node_id == source
        ));
        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        assert!(!pending.fetch_inflight_from.contains(&source));
        assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
        assert!(pending.fetch_failed_from.is_empty());
        assert_eq!(
            pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .next_retry_tick,
            0
        );
        assert_eq!(
            dfs.content
                .read_blob(&manifest.content_hash)
                .expect("existing blob"),
            bytes
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn chunked_fetch_response_teaches_unknown_pending_write_size() {
        let root = temp_root("chunked-fetch-teaches-size");
        let owner = NodeId::from_legacy_named("node_fetch_size_owner").expect("node");
        let source = NodeId::from_legacy_named("node_fetch_size_source").expect("node");
        let target = NodeId::from_legacy_named("node_fetch_size_target").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_fetch_size").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let intent_id = "dfs_intent_fetch_size".to_string();
        let request_id = "dfs_net_req_fetch_size".to_string();
        let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let content_hash = content_hash(&bytes);
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        dfs.metadata_backend
            .as_mut()
            .expect("metadata backend")
            .put_version(&DfsObjectVersionRecord {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(content_hash.clone()),
                size_bytes: None,
                locations: vec![source.clone()],
                created_by_event_id: None,
                created_at: OffsetDateTime::now_utc(),
                visible: false,
            })
            .expect("seed size-less version record");
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                size_bytes: 0,
                stage_retries: BTreeMap::from([(
                    target.clone(),
                    RepairRetryState {
                        attempts: 2,
                        next_retry_tick: 100,
                        inflight: true,
                    },
                )]),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::from([source.clone()]),
                fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
                fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        for (chunk_index, chunk_bytes) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let event = dfs
                .handle_fetch_blob_response_chunk(
                    source.clone(),
                    DfsFetchBlobResponseChunk {
                        request_id: request_id.clone(),
                        intent_id: intent_id.clone(),
                        object_id: object_id.clone(),
                        version: version.clone(),
                        source_node_id: source.clone(),
                        content_hash: content_hash.clone(),
                        total_size_bytes: bytes.len() as u64,
                        chunk_index: chunk_index as u32,
                        total_chunks: blob_chunk_count(&bytes).expect("chunk count"),
                        bytes: chunk_bytes.to_vec(),
                    },
                )
                .expect("chunk handled");
            if chunk_index == 0 {
                assert!(event.is_none());
            } else {
                assert!(matches!(
                    event,
                    Some(NetworkedDfsServiceEvent::BlobFetched {
                        intent_id: event_intent,
                        object_id: event_object_id,
                        source_node_id,
                        ..
                    }) if event_intent == intent_id
                        && event_object_id == object_id
                        && source_node_id == source
                ));
            }
        }

        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        assert_eq!(pending.size_bytes, bytes.len() as u64);
        assert!(!pending.fetch_inflight_from.contains(&source));
        assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
        assert!(pending.fetch_failed_from.is_empty());
        assert_eq!(
            pending
                .stage_retries
                .get(&target)
                .expect("stage retry")
                .next_retry_tick,
            0
        );
        assert_eq!(
            dfs.content.read_blob(&content_hash).expect("fetched blob"),
            bytes
        );
        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .expect("metadata backend")
                .get_version(&object_id, &version)
                .expect("version lookup")
                .expect("version record")
                .size_bytes,
            Some(bytes.len() as u64)
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn repair_retry_schedule_does_not_double_count_completed_inflight_attempt() {
        let root = temp_root("repair-retry-no-double-count");
        let owner = NodeId::from_legacy_named("node_retry_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_retry_replica").expect("node");
        let network = InProcessNetwork::new();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        let intent_id = "dfs_intent_retry".to_string();
        dfs.pending_writes.insert(
            intent_id.clone(),
            PendingNetworkedWrite {
                object_id: DfsObjectId::from_legacy_named("obj_retry").expect("object"),
                version: DfsVersion::from_legacy_named("version_2").expect("version"),
                content_hash: "blake3:retry".to_string(),
                size_bytes: 5,
                stage_retries: BTreeMap::new(),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
                fetch_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_started_ticks: BTreeMap::new(),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        dfs.repair_retry_tick = 10;
        dfs.record_repair_inflight(&intent_id, &replica, "dfs_net_req_repair".to_string());
        dfs.record_repair_retry_scheduled(&intent_id, &replica);

        let pending = dfs.pending_writes.get(&intent_id).expect("pending write");
        let state = pending
            .repair_retries
            .get(&replica)
            .expect("repair retry state");
        assert_eq!(state.attempts, 1);
        assert_eq!(state.next_retry_tick, 11);
        assert!(!state.inflight);
        assert!(!pending.repair_inflight_request_ids.contains_key(&replica));
        assert!(!dfs.repair_retry_is_due(&intent_id, &replica));
        dfs.repair_retry_tick = 11;
        assert!(dfs.repair_retry_is_due(&intent_id, &replica));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn repair_response_conflicting_receipt_preserves_retry_state() {
        let root = temp_root("repair-conflicting-receipt-keeps-retry");
        let owner = NodeId::from_legacy_named("node_repair_conflict_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_repair_conflict_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_repair_conflict").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"repair response with a durable conflicting receipt";
        let content_hash = content_hash(bytes);
        let request_id = "dfs_net_req_repair_conflicting_receipt".to_string();
        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        dfs.add_object_metadata(metadata).expect("metadata");
        let intent = dfs
            .coordinator
            .create_intent(
                sifs_file_system_id(),
                object_id.clone(),
                Some(version_1),
                version_2.clone(),
                owner.clone(),
                vec![replica.clone()],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("intent");
        dfs.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version_2.clone(),
                content_hash: content_hash.clone(),
                size_bytes: bytes.len() as u64,
                stage_retries: BTreeMap::new(),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
                fetch_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_started_ticks: BTreeMap::new(),
                fetch_failed_from: BTreeSet::new(),
            },
        );
        dfs.record_repair_inflight(&intent.intent_id, &replica, request_id.clone());
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version_2,
            &replica,
            "blake3:stale-conflicting-repair-receipt",
        )
        .expect("seed conflicting receipt");

        let error = dfs
            .handle_repair_replica_response(
                replica.clone(),
                DfsRepairReplicaResponse {
                    request_id: request_id.clone(),
                    intent_id: intent.intent_id.clone(),
                    file_system_id: sifs_file_system_id(),
                    object_id,
                    version: version_2,
                    replica_node_id: replica.clone(),
                    result: DfsBlobTransferResult::Durable { content_hash },
                },
            )
            .expect_err("conflicting durable receipt must reject repair response");
        assert!(
            error
                .to_string()
                .contains("conflicting DFS replica receipt"),
            "unexpected error: {error}"
        );

        let pending = dfs
            .pending_writes
            .get(&intent.intent_id)
            .expect("pending write");
        assert!(
            pending.repair_retries.contains_key(&replica),
            "rejected repair response must keep retry state"
        );
        assert_eq!(
            pending.repair_inflight_request_ids.get(&replica),
            Some(&request_id),
            "rejected repair response must not clear the in-flight request before durable acceptance"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn repair_command_conflicting_receipt_does_not_commit_visible_metadata() {
        let root = temp_root("repair-command-conflicting-receipt-no-visible-commit");
        let owner = NodeId::from_legacy_named("node_repair_cmd_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_repair_cmd_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_repair_cmd_conflict").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"repair command should not commit after receipt conflict";
        let content_hash = content_hash(bytes);
        let mut dfs = networked_sifs_with_backend(&root, replica.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![owner.clone()];
        dfs.add_object_metadata(metadata).expect("metadata");
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version_2,
            &replica,
            "blake3:stale-conflicting-repair-command-receipt",
        )
        .expect("seed conflicting receipt");

        let event = dfs
            .handle_repair_replica(
                owner,
                DfsRepairReplicaCommand {
                    request_id: "dfs_net_req_repair_cmd_conflict".to_string(),
                    intent_id: "dfs_intent_repair_cmd_conflict".to_string(),
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    expected_base_version: Some(version_1.clone()),
                    version: version_2.clone(),
                    content_hash,
                    bytes: bytes.to_vec(),
                    object_metadata: None,
                },
            )
            .expect("repair command response event");
        assert!(
            matches!(
                event,
                NetworkedDfsServiceEvent::RepairRejected {
                    ref reason,
                    ..
                } if reason.contains("conflicting DFS replica receipt")
            ),
            "repair command with conflicting receipt must be rejected, got {event:?}"
        );

        let object = dfs
            .current_read_object_metadata(&object_id)
            .expect("visible metadata");
        assert_eq!(
            object.version, version_1,
            "rejected repair command must not advance visible metadata"
        );
        assert!(
            !object.current_locations.contains(&replica),
            "rejected repair command must not add the local replica to visible locations"
        );
        let backend_object = dfs
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("backend object")
            .expect("backend object exists");
        assert_eq!(
            backend_object.version, version_1,
            "rejected repair command must not publish visible metadata to the backend"
        );
        assert!(
            dfs.content
                .read_manifest(object_id.as_str(), version_2.as_str())
                .is_err(),
            "receipt preflight should reject the repair before storing orphaned content"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn stage_command_conflicting_receipt_does_not_store_orphan_content() {
        let root = temp_root("stage-command-conflicting-receipt-no-orphan-content");
        let owner = NodeId::from_legacy_named("node_stage_cmd_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_stage_cmd_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_stage_cmd_conflict").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"stage command should reject before storing orphan content";
        let content_hash = content_hash(bytes);
        let mut dfs = networked_sifs_with_backend(&root, replica.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![owner.clone()];
        dfs.add_object_metadata(metadata).expect("metadata");
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version_2,
            &replica,
            "blake3:stale-conflicting-stage-command-receipt",
        )
        .expect("seed conflicting receipt");

        let event = dfs
            .handle_stage_blob(
                owner,
                DfsStageBlobCommand {
                    request_id: "dfs_net_req_stage_cmd_conflict".to_string(),
                    intent_id: "dfs_intent_stage_cmd_conflict".to_string(),
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    expected_base_version: Some(version_1),
                    version: version_2.clone(),
                    content_hash,
                    bytes: bytes.to_vec(),
                    object_metadata: None,
                },
            )
            .expect("stage command response event");
        assert!(
            matches!(
                event,
                NetworkedDfsServiceEvent::BlobStageRejected {
                    ref reason,
                    ..
                } if reason.contains("conflicting DFS replica receipt")
            ),
            "stage command with conflicting receipt must be rejected, got {event:?}"
        );
        assert!(
            dfs.content
                .read_manifest(object_id.as_str(), version_2.as_str())
                .is_err(),
            "stage receipt preflight should reject before storing orphaned content"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn stage_chunk_existing_manifest_conflicting_receipt_returns_rejection() {
        let root = temp_root("stage-chunk-existing-manifest-conflicting-receipt");
        let owner = NodeId::from_legacy_named("node_stage_chunk_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_stage_chunk_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_stage_chunk_conflict").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"stage chunk duplicate fast path must reject receipt conflict";
        let mut dfs = networked_sifs_with_backend(&root, replica.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![owner.clone()];
        dfs.add_object_metadata(metadata).expect("metadata");
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("existing manifest");
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version_2,
            &replica,
            "blake3:stale-conflicting-stage-chunk-receipt",
        )
        .expect("seed conflicting receipt");

        let event = dfs
            .handle_stage_blob_chunk(
                owner,
                DfsStageBlobChunkCommand {
                    request_id: "dfs_net_req_stage_chunk_conflict".to_string(),
                    intent_id: "dfs_intent_stage_chunk_conflict".to_string(),
                    file_system_id: sifs_file_system_id(),
                    object_id,
                    expected_base_version: Some(version_1),
                    version: version_2,
                    content_hash: manifest.content_hash,
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: 0,
                    total_chunks: 1,
                    bytes: bytes.to_vec(),
                    object_metadata: None,
                },
            )
            .expect("stage chunk conflict should be isolated")
            .expect("stage chunk conflict event");
        assert!(
            matches!(
                event,
                NetworkedDfsServiceEvent::BlobStageRejected {
                    ref reason,
                    ..
                } if reason.contains("conflicting DFS replica receipt")
            ),
            "stage chunk duplicate fast path must reject receipt conflict, got {event:?}"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn committed_intent_applies_when_commit_notification_target_is_unroutable() {
        let root = temp_root("commit-notify-target-unroutable");
        let owner = NodeId::from_legacy_named("node_commit_notify_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_commit_notify_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_commit_notify").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let mut dfs = NetworkedDfsService::new(
            ReceiveOnlyTransport::new(owner.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        dfs.add_object_metadata(metadata.clone()).expect("metadata");
        let bytes = b"committed bytes despite unroutable notify";
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("content");
        let intent = dfs
            .coordinator
            .create_intent(
                metadata.file_system_id.clone(),
                object_id.clone(),
                Some(version_1),
                version_2.clone(),
                owner,
                vec![replica.clone()],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("intent");
        dfs.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version_2.clone(),
                content_hash: manifest.content_hash.clone(),
                size_bytes: manifest.size_bytes,
                stage_retries: BTreeMap::new(),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
                fetch_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_started_ticks: BTreeMap::new(),
                fetch_failed_from: BTreeSet::new(),
            },
        );
        let committed = dfs
            .coordinator
            .record_durable_receipt(&intent.intent_id, replica.clone())
            .expect("durable receipt commits intent");
        assert!(matches!(committed.state, DfsWriteIntentState::Committed));

        dfs.apply_committed_intent(&intent.intent_id)
            .expect("post-commit replica notification is best effort");

        let object = dfs.metadata().object(&object_id).expect("object");
        assert_eq!(object.version, version_2);
        assert_eq!(
            object.content_hash.as_deref(),
            Some(manifest.content_hash.as_str())
        );
        assert_eq!(object.current_locations, vec![replica]);
        let read = dfs
            .read_local_object_with_state(&object_id, false)
            .expect("read state");
        assert!(matches!(
            read,
            NetworkedDfsReadOutcome::Found {
                data,
                freshness: NetworkedDfsReadFreshness::Current
            } if data.bytes == bytes
        ));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn stage_blob_persists_when_response_target_is_unroutable() {
        let root = temp_root("stage-response-target-unroutable");
        let owner = NodeId::from_legacy_named("node_stage_notify_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_stage_notify_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_stage_notify").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"stage durable despite lost requester";
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![owner.clone()];
        let mut dfs = NetworkedDfsService::new(
            ReceiveOnlyTransport::new(replica.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );

        let event = dfs
            .handle_stage_blob(
                owner,
                DfsStageBlobCommand {
                    request_id: "dfs_net_req_stage_unroutable".to_string(),
                    intent_id: "dfs_intent_stage_unroutable".to_string(),
                    file_system_id: metadata.file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: Some(metadata.version.clone()),
                    version: version.clone(),
                    content_hash: content_hash(bytes),
                    bytes: bytes.to_vec(),
                    object_metadata: Some(metadata),
                },
            )
            .expect("unroutable response target should not fail durable stage");

        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged {
                object_id: staged_object_id,
                version: staged_version,
                replica_node_id,
                ..
            } if staged_object_id == object_id
                && staged_version == version
                && replica_node_id == replica
        ));
        let manifest = dfs
            .content
            .read_manifest(object_id.as_str(), version.as_str())
            .expect("stage manifest");
        assert_eq!(
            dfs.content.read_blob(&manifest.content_hash).expect("blob"),
            bytes
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn repair_replica_commits_when_response_target_is_unroutable() {
        let root = temp_root("repair-response-target-unroutable");
        let owner = NodeId::from_legacy_named("node_repair_notify_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_repair_notify_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_repair_notify").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"repair committed despite lost requester";
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![owner.clone()];
        let mut dfs = NetworkedDfsService::new(
            ReceiveOnlyTransport::new(replica.clone()),
            LocalContentStore::new(root.join("content")).expect("content"),
        );

        let event = dfs
            .handle_repair_replica(
                owner,
                DfsRepairReplicaCommand {
                    request_id: "dfs_net_req_repair_unroutable".to_string(),
                    intent_id: "dfs_intent_repair_unroutable".to_string(),
                    file_system_id: metadata.file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: Some(metadata.version.clone()),
                    version: version.clone(),
                    content_hash: content_hash(bytes),
                    bytes: bytes.to_vec(),
                    object_metadata: Some(metadata),
                },
            )
            .expect("unroutable response target should not fail repair commit");

        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::ReplicaRepaired {
                object_id: repaired_object_id,
                version: repaired_version,
                replica_node_id,
                ..
            } if repaired_object_id == object_id
                && repaired_version == version
                && replica_node_id == replica
        ));
        let read = dfs
            .read_local_object_with_state(&object_id, false)
            .expect("read repaired object");
        assert!(matches!(
            read,
            NetworkedDfsReadOutcome::Found {
                data,
                freshness: NetworkedDfsReadFreshness::Current
            } if data.version == version && data.bytes == bytes
        ));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn chunked_stage_to_unroutable_target_stops_after_first_unsent_chunk() {
        let root = temp_root("chunked-stage-unroutable-stops");
        let owner = NodeId::from_legacy_named("node_chunk_stage_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_chunk_stage_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_chunk_stage").expect("object");
        let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut dfs = NetworkedDfsService::new(
            ReceiveOnlyTransport::new(owner),
            LocalContentStore::new(root.join("content")).expect("content"),
        );

        let sent = dfs
            .send_stage_blob_if_routable(
                &replica,
                "dfs_net_req_chunk_stage_unroutable".to_string(),
                "dfs_intent_chunk_stage_unroutable",
                &file_system_id,
                &object_id,
                None,
                &version,
                &content_hash(&bytes),
                &bytes,
                None,
            )
            .expect("send stage");

        assert!(!sent);
        assert_eq!(
            dfs.next_frame_id, 2,
            "large stage should stop after the first unroutable chunk send"
        );
        assert_eq!(dfs.next_message_id, 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn chunked_repair_to_unroutable_target_stops_after_first_unsent_chunk() {
        let root = temp_root("chunked-repair-unroutable-stops");
        let owner = NodeId::from_legacy_named("node_chunk_repair_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_chunk_repair_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_chunk_repair").expect("object");
        let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut dfs = NetworkedDfsService::new(
            ReceiveOnlyTransport::new(owner),
            LocalContentStore::new(root.join("content")).expect("content"),
        );

        let sent = dfs
            .send_repair_replica_if_routable(
                &replica,
                "dfs_net_req_chunk_repair_unroutable".to_string(),
                "dfs_intent_chunk_repair_unroutable",
                &file_system_id,
                &object_id,
                None,
                &version,
                &content_hash(&bytes),
                &bytes,
                None,
            )
            .expect("send repair");

        assert!(!sent);
        assert_eq!(
            dfs.next_frame_id, 2,
            "large repair should stop after the first unroutable chunk send"
        );
        assert_eq!(dfs.next_message_id, 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn chunked_fetch_response_to_unroutable_requester_stops_after_first_unsent_chunk() {
        let root = temp_root("chunked-fetch-response-unroutable-stops");
        let source = NodeId::from_legacy_named("node_chunk_fetch_source").expect("node");
        let requester = NodeId::from_legacy_named("node_chunk_fetch_requester").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_chunk_fetch").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut dfs = NetworkedDfsService::new(
            ReceiveOnlyTransport::new(source),
            LocalContentStore::new(root.join("content")).expect("content"),
        );

        dfs.send_fetch_blob_response_chunks(
            &requester,
            &DfsFetchBlobCommand {
                request_id: "dfs_net_req_fetch_response_unroutable".to_string(),
                intent_id: "dfs_intent_fetch_response_unroutable".to_string(),
                object_id,
                version,
                content_hash: content_hash(&bytes),
            },
            &bytes,
        )
        .expect("send fetch chunks");

        assert_eq!(
            dfs.next_frame_id, 2,
            "large fetch response should stop after the first unroutable chunk send"
        );
        assert_eq!(dfs.next_message_id, 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn chunked_read_response_to_unroutable_requester_stops_after_first_unsent_chunk() {
        let root = temp_root("chunked-read-response-unroutable-stops");
        let source = NodeId::from_legacy_named("node_chunk_read_source").expect("node");
        let requester = NodeId::from_legacy_named("node_chunk_read_requester").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_chunk_read").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = (0..(DFS_BLOB_CHUNK_BYTES + 8192))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut dfs = NetworkedDfsService::new(
            ReceiveOnlyTransport::new(source),
            LocalContentStore::new(root.join("content")).expect("content"),
        );

        dfs.send_networked_read_response_chunks(
            &requester,
            &DfsReadRequest {
                request_id: "dfs_net_req_read_response_unroutable".to_string(),
                object_id: object_id.clone(),
            },
            &DfsReadData {
                object_id,
                version,
                content_hash: Some(content_hash(&bytes)),
                bytes,
            },
        )
        .expect("send read chunks");

        assert_eq!(
            dfs.next_frame_id, 2,
            "large read response should stop after the first unroutable chunk send"
        );
        assert_eq!(dfs.next_message_id, 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn read_current_object_uses_hash_addressed_blob_without_manifest_scan() {
        let root = temp_root("read-current-object-hash-addressed");
        let node_id = NodeId::from_legacy_named("node_hash_addressed").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_hash_addressed").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"shared content-addressed bytes";
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        let seed_manifest = dfs
            .content
            .put_object_version("obj_other_hash_owner", "version_1", bytes)
            .expect("seed blob");
        let mut metadata = file_metadata(node_id);
        metadata.object_id = object_id.clone();
        metadata.path = "services/hash-addressed.txt".to_string();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.version = version.clone();
        metadata.content_hash = Some(seed_manifest.content_hash.clone());
        metadata.content_size_bytes = Some(bytes.len() as u64);
        dfs.add_object_metadata(metadata).expect("metadata");

        let outcome = dfs
            .read_local_object_with_state(&object_id, false)
            .expect("read");

        match outcome {
            NetworkedDfsReadOutcome::Found { data, freshness } => {
                assert_eq!(freshness, NetworkedDfsReadFreshness::Current);
                assert_eq!(data.object_id, object_id);
                assert_eq!(data.version, version);
                assert_eq!(
                    data.content_hash.as_deref(),
                    Some(seed_manifest.content_hash.as_str())
                );
                assert_eq!(data.bytes, bytes);
            }
            other => {
                panic!("expected current object bytes from hash-addressed blob, got {other:?}")
            }
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn missing_current_manifest_blob_returns_pending_replication() {
        let root = temp_root("read-missing-current-manifest-blob");
        let node_id = NodeId::from_legacy_named("node_missing_blob").expect("node");
        let replica = NodeId::from_legacy_named("node_missing_blob_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_missing_blob").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"missing local blob bytes";
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version.as_str(), bytes)
            .expect("seed manifest");
        let hex = manifest
            .content_hash
            .strip_prefix("blake3:")
            .expect("blake3 hash");
        std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
            .expect("remove blob");
        let mut metadata = file_metadata(node_id);
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.version = version.clone();
        metadata.current_locations = vec![replica.clone()];
        metadata.content_hash = Some(manifest.content_hash.clone());
        metadata.content_size_bytes = Some(bytes.len() as u64);
        dfs.add_object_metadata(metadata).expect("metadata");

        let outcome = dfs
            .read_local_object_with_state(&object_id, false)
            .expect("missing blob is treated as cache miss");

        assert!(matches!(
            outcome,
            NetworkedDfsReadOutcome::PendingReplication {
                object_id: pending_object_id,
                version: pending_version,
                content_hash,
                current_locations,
            } if pending_object_id == object_id
                && pending_version == version
                && content_hash.as_deref() == Some(manifest.content_hash.as_str())
                && current_locations == vec![replica]
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn reconcile_local_manifests_requires_current_blob_bytes() {
        let root = temp_root("reconcile-requires-current-blob");
        let node_id = NodeId::from_legacy_named("node_reconcile_missing_blob").expect("node");
        let remote = NodeId::from_legacy_named("node_reconcile_remote").expect("node");
        let object_id =
            DfsObjectId::from_legacy_named("obj_reconcile_missing_blob").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"reconcile missing local bytes";
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version.as_str(), bytes)
            .expect("seed manifest");
        let hex = manifest
            .content_hash
            .strip_prefix("blake3:")
            .expect("blake3 hash");
        std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
            .expect("remove blob");
        let mut metadata = file_metadata(node_id);
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.version = version.clone();
        metadata.current_locations = vec![remote.clone()];
        metadata.content_hash = Some(manifest.content_hash.clone());
        metadata.content_size_bytes = Some(bytes.len() as u64);
        dfs.add_object_metadata(metadata).expect("metadata");

        let reconciliation = dfs
            .reconcile_local_manifests(&object_id)
            .expect("reconcile");

        assert_eq!(reconciliation.refreshed_receipts, 0);
        assert!(reconciliation.conflicts.is_empty());
        assert!(reconciliation.missing_current);
        assert!(
            dfs.metadata_backend()
                .expect("backend")
                .receipts_for_version(&object_id, &version)
                .expect("receipts")
                .is_empty()
        );
        assert_eq!(
            dfs.metadata()
                .object(&object_id)
                .expect("object")
                .current_locations,
            vec![remote]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn commit_version_rejects_manifest_without_blob_bytes() {
        let root = temp_root("commit-rejects-manifest-without-blob");
        let owner = NodeId::from_legacy_named("node_commit_missing_blob_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_commit_missing_blob_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_commit_missing_blob").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"missing commit blob bytes";
        let network = InProcessNetwork::new();
        let _owner_transport = network.add_node(owner.clone());
        let mut replica_dfs = NetworkedDfsService::new(
            network.add_node(replica.clone()),
            LocalContentStore::new(root.join("replica")).expect("replica content"),
        );
        replica_dfs
            .add_file_system(file_system(owner.clone()))
            .expect("filesystem");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.version = version_1.clone();
        metadata.current_locations = vec![owner.clone()];
        replica_dfs
            .add_object_metadata(metadata.clone())
            .expect("metadata");
        let manifest = replica_dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("stage manifest");
        let hex = manifest
            .content_hash
            .strip_prefix("blake3:")
            .expect("blake3 hash");
        std::fs::remove_file(root.join("replica").join("blobs").join(&hex[..2]).join(hex))
            .expect("remove blob");

        let event = replica_dfs
            .handle_commit_version(
                owner,
                DfsCommitVersionCommand {
                    request_id: "dfs_net_req_commit_missing_blob".to_string(),
                    intent_id: "dfs_intent_commit_missing_blob".to_string(),
                    file_system_id: metadata.file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: Some(metadata.version.clone()),
                    version: version_2.clone(),
                    content_hash: manifest.content_hash.clone(),
                    object_metadata: Some(metadata),
                },
            )
            .expect("commit response");

        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected {
                object_id: rejected_object_id,
                reason,
                ..
            } if rejected_object_id == object_id && reason.contains("committed blob bytes are missing")
        ));
        assert!(matches!(
            replica_dfs.read_local_object_with_state(&object_id, false),
            Ok(NetworkedDfsReadOutcome::NotFound)
        ));
        let object = replica_dfs.metadata().object(&object_id).expect("object");
        assert_eq!(object.version, version_1);
        assert_eq!(object.content_hash, None);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn commit_version_conflicting_receipt_does_not_advance_visible_metadata() {
        let root = temp_root("commit-conflicting-receipt-no-visible-advance");
        let owner = NodeId::from_legacy_named("node_commit_conflict_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_commit_conflict_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_commit_conflict").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"commit must not advance over conflicting durable receipt";
        let mut dfs = networked_sifs_with_backend(&root, replica.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![owner.clone()];
        dfs.add_object_metadata(metadata.clone()).expect("metadata");
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("local staged content");
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version_2,
            &replica,
            "blake3:stale-conflicting-commit-receipt",
        )
        .expect("seed conflicting receipt");

        let event = dfs
            .handle_commit_version(
                owner,
                DfsCommitVersionCommand {
                    request_id: "dfs_net_req_commit_conflict".to_string(),
                    intent_id: "dfs_intent_commit_conflict".to_string(),
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    expected_base_version: Some(version_1.clone()),
                    version: version_2.clone(),
                    content_hash: manifest.content_hash,
                    object_metadata: Some(metadata),
                },
            )
            .expect("commit conflict response");

        assert!(
            matches!(
                event,
                NetworkedDfsServiceEvent::RepairRejected {
                    ref reason,
                    ..
                } if reason.contains("conflicting DFS replica receipt")
            ),
            "commit with conflicting receipt must be rejected, got {event:?}"
        );
        let object = dfs
            .current_read_object_metadata(&object_id)
            .expect("visible metadata");
        assert_eq!(
            object.version, version_1,
            "rejected commit must not advance visible metadata"
        );
        let receipts = dfs
            .metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version_2)
            .expect("receipts");
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].content_hash.as_deref(),
            Some("blake3:stale-conflicting-commit-receipt")
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn commit_version_repairs_missing_local_receipt_before_visible_commit() {
        let root = temp_root("commit-repairs-missing-local-receipt");
        let owner = NodeId::from_legacy_named("node_commit_receipt_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_commit_receipt_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_commit_receipt").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"commit should publish missing local receipt";
        let mut dfs = networked_sifs_with_backend(&root, replica.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.version = version_1.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.current_locations = vec![owner.clone()];
        dfs.add_object_metadata(metadata.clone()).expect("metadata");
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("local staged content");

        let event = dfs
            .handle_commit_version(
                owner,
                DfsCommitVersionCommand {
                    request_id: "dfs_net_req_commit_receipt".to_string(),
                    intent_id: "dfs_intent_commit_receipt".to_string(),
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    expected_base_version: Some(version_1),
                    version: version_2.clone(),
                    content_hash: manifest.content_hash.clone(),
                    object_metadata: Some(metadata),
                },
            )
            .expect("commit response");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::VersionCommitted {
                object_id: committed,
                ..
            } if committed == object_id
        ));
        let object = dfs
            .current_read_object_metadata(&object_id)
            .expect("visible metadata");
        assert_eq!(object.version, version_2);
        assert_eq!(
            object.content_hash.as_deref(),
            Some(manifest.content_hash.as_str())
        );
        let receipts = dfs
            .metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version_2)
            .expect("receipts");
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].replica_node_id, replica);
        assert_eq!(
            receipts[0].content_hash.as_deref(),
            Some(manifest.content_hash.as_str())
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn stale_read_skips_missing_stale_blobs() {
        let root = temp_root("read-stale-skips-missing-blob");
        let node_id = NodeId::from_legacy_named("node_stale_skip").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_stale_skip").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        let readable_stale = dfs
            .content
            .put_object_version(object_id.as_str(), version_1.as_str(), b"readable stale")
            .expect("readable stale");
        let missing_stale = dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), b"missing stale")
            .expect("missing stale");
        let hex = missing_stale
            .content_hash
            .strip_prefix("blake3:")
            .expect("blake3 hash");
        std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
            .expect("remove missing stale blob");
        let mut metadata = file_metadata(node_id);
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.version = version_3;
        metadata.content_hash = Some(content_hash(b"unavailable current"));
        metadata.content_size_bytes = Some(b"unavailable current".len() as u64);
        dfs.add_object_metadata(metadata).expect("metadata");

        let outcome = dfs
            .read_local_object_with_state(&object_id, true)
            .expect("stale read skips missing blob");

        assert!(matches!(
            outcome,
            NetworkedDfsReadOutcome::Found {
                data,
                freshness: NetworkedDfsReadFreshness::Stale { .. },
            } if data.version == version_1
                && data.content_hash.as_deref() == Some(readable_stale.content_hash.as_str())
                && data.bytes == b"readable stale"
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_owned_write_routes_to_owner() {
        let owner = NodeId::from_legacy_named("node_desktop").expect("node");
        let remote = NodeId::from_legacy_named("node_mobile").expect("node");
        let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
        let object_id = DfsObjectId::from_legacy_named("obj_file").expect("object");
        let mut store = DfsMetadataStore::new();
        store.add_file_system(file_system(owner.clone()));
        store
            .add_object(DfsObjectMetadata {
                object_id: object_id.clone(),
                file_system_id,
                path: "src/lib.rs".to_string(),
                kind: DfsObjectKind::File,
                owner_node_id: owner.clone(),
                current_locations: vec![owner.clone()],
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
                version: DfsVersion::from_legacy_named("version_1").expect("version"),
                policy: DfsObjectPolicy::LocalOwned,
                created_by_event_id: None,
                last_modified_by_event_id: None,
            })
            .expect("add object");

        let route = store
            .route_write(
                DfsWriteRequest {
                    file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
                    object_id,
                    target_version: DfsVersion::from_legacy_named("version_2").expect("version"),
                    requesting_node_id: remote.clone(),
                },
                &remote,
            )
            .expect("route write");

        assert_eq!(
            route.action,
            WriteRouteAction::RouteToOwner {
                owner_node_id: owner
            }
        );
    }

    #[test]
    fn sifs_recursive_clear_survives_metadata_backend_reopen() {
        let root = temp_root("sifs-recursive-clear-reopen");
        let node_id = NodeId::from_legacy_named("node_sifs_clear").expect("node");
        let file_path = "/services/sandbox/instances/sandbox_clear/tmp/0/session-cache.txt";
        let clear_root = "/services/sandbox/instances/sandbox_clear/tmp/0";

        {
            let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
            dfs.posix_mkdir_all(clear_root).expect("mkdir");
            dfs.posix_write_file(
                file_path,
                b"tmp",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write");
            assert_eq!(dfs.posix_read_file(file_path).expect("read"), b"tmp");
            dfs.posix_clear_dir_recursive(clear_root)
                .expect("clear recursive");
            assert!(matches!(
                dfs.posix_read_file(file_path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }

        {
            let dfs = networked_sifs_with_backend(&root, node_id.clone());
            assert!(matches!(
                dfs.posix_read_file(file_path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }

        {
            let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
            dfs.posix_mkdir_all(clear_root)
                .expect("recreate cleared root");
            dfs.posix_write_file(
                file_path,
                b"fresh",
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("rewrite after clear");
            assert_eq!(
                dfs.posix_read_file(file_path).expect("fresh read"),
                b"fresh"
            );
        }

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_recursive_clear_preserves_hardlinks_outside_cleared_tree() {
        let root = temp_root("sifs-recursive-clear-hardlink-outside");
        let node_id = NodeId::from_legacy_named("node_sifs_clear_hardlink").expect("node");
        let clear_root = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0";
        let file_path = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0/cache.txt";
        let outside_alias =
            "/services/sandbox/instances/sandbox_clear_hardlink/work/cache-alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(clear_root).expect("mkdir clear root");
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_hardlink/work")
            .expect("mkdir work");
        dfs.posix_write_file(
            file_path,
            b"cache",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write cache");
        dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
            .expect("old file times");
        let before = dfs.posix_stat(file_path).expect("file stat");
        let object_id = before.object_id.clone();
        dfs.posix_hardlink(file_path, outside_alias)
            .expect("hardlink outside");

        dfs.posix_clear_dir_recursive(clear_root)
            .expect("clear recursive");

        assert!(matches!(
            dfs.posix_stat(file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
        let alias_stat = dfs.posix_stat(outside_alias).expect("alias survives");
        assert_eq!(alias_stat.object_id, object_id);
        assert_eq!(alias_stat.link_count, 1);
        assert_eq!(
            alias_stat.modified_at_unix_nanos, before.modified_at_unix_nanos,
            "clearing a hardlinked path must not change file content mtime"
        );
        assert!(
            alias_stat.changed_at_unix_nanos > before.changed_at_unix_nanos,
            "promoting a surviving hardlink alias must advance ctime because the canonical path changed"
        );
        assert_ne!(
            alias_stat.version, before.version,
            "promoting a surviving hardlink alias must advance the metadata version"
        );
        assert_eq!(
            dfs.posix_read_file(outside_alias).expect("read alias"),
            b"cache"
        );

        let reloaded = networked_sifs_with_backend(&root, node_id);
        let alias_stat = reloaded
            .posix_stat(outside_alias)
            .expect("reloaded alias survives");
        assert_eq!(alias_stat.object_id, object_id);
        assert_eq!(alias_stat.link_count, 1);
        assert_eq!(
            reloaded
                .posix_read_file(outside_alias)
                .expect("reloaded read alias"),
            b"cache"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_recursive_clear_follows_directory_symlink_argument() {
        let root = temp_root("sifs-recursive-clear-directory-symlink");
        let node_id = NodeId::from_legacy_named("node_sifs_clear_symlink").expect("node");
        let base = "/services/sandbox/instances/sandbox_clear_symlink";
        let real_dir = format!("{base}/tmp/0");
        let link_dir = format!("{base}/work/current-tmp");
        let file_path = format!("{real_dir}/session-cache.txt");

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(&real_dir).expect("mkdir real tmp");
        dfs.posix_mkdir_all(&format!("{base}/work"))
            .expect("mkdir work");
        dfs.posix_write_file(
            &file_path,
            b"cache",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write cache");
        dfs.posix_symlink(
            "../tmp/0",
            &link_dir,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("symlink directory");

        dfs.posix_clear_dir_recursive(&link_dir)
            .expect("clear via directory symlink");

        assert!(matches!(
            dfs.posix_read_file(&file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            dfs.posix_lstat(&link_dir)
                .expect("link survives clear")
                .kind,
            DfsObjectKind::Symlink
        );
        assert!(
            dfs.posix_list_dir(&real_dir)
                .expect("real dir survives")
                .is_empty()
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_recursive_clear_is_scoped_to_target_subtree() {
        let root = temp_root("sifs-recursive-clear-scoped-subtree");
        let node_id = NodeId::from_legacy_named("node_sifs_clear_scoped").expect("node");
        let target_root = "/services/sandbox/instances/sandbox_clear_scoped/tmp/0";
        let target_file = "/services/sandbox/instances/sandbox_clear_scoped/tmp/0/nested/cache.txt";
        let sibling_file =
            "/services/sandbox/instances/sandbox_clear_scoped/tmp/1/nested/cache.txt";
        let unrelated_file =
            "/services/sandbox/instances/sandbox_clear_scoped/work/unrelated/cache.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(&format!("{target_root}/nested"))
            .expect("mkdir target");
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_scoped/tmp/1/nested")
            .expect("mkdir sibling");
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_scoped/work/unrelated")
            .expect("mkdir unrelated");
        for path in [target_file, sibling_file, unrelated_file] {
            dfs.posix_write_file(
                path,
                path.as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write file");
        }

        dfs.posix_clear_dir_recursive(target_root)
            .expect("clear target subtree");

        assert!(matches!(
            dfs.posix_read_file(target_file),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            dfs.posix_read_file(sibling_file).expect("sibling survives"),
            sibling_file.as_bytes()
        );
        assert_eq!(
            dfs.posix_read_file(unrelated_file)
                .expect("unrelated survives"),
            unrelated_file.as_bytes()
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_self_only_eventual_write_uses_local_fast_path() {
        let root = temp_root("sifs-self-only-eventual-fast-path");
        let node_id = NodeId::from_legacy_named("node_sifs_fast_path").expect("node");
        let file_path = "/services/sandbox/instances/sandbox_fast/work/site.py";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_fast/work")
            .expect("mkdir");
        let stat = dfs
            .posix_write_file(
                file_path,
                b"print('fast path')\n",
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("write");

        assert_eq!(stat.size_bytes, Some(19));
        assert_eq!(
            dfs.posix_read_file(file_path).expect("read"),
            b"print('fast path')\n"
        );
        assert!(
            dfs.pending_writes.is_empty(),
            "self-only eventual writes should not leave distributed intents"
        );
        assert!(dfs.coordinator.repair_jobs().is_empty());

        let journal_count = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();
        assert!(
            journal_count < 64,
            "self-only write should not emit per-file distributed intent/receipt/commit journals"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn visible_metadata_publication_is_idempotent() {
        let root = temp_root("visible-metadata-publication-idempotent");
        let node_id = NodeId::from_legacy_named("node_visible_metadata_idempotent").expect("node");
        let file_path = "/services/sandbox/instances/sandbox_meta/work/site.py";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_meta/work")
            .expect("mkdir");
        let stat = dfs
            .posix_write_file(
                file_path,
                b"print('metadata')\n",
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("write");
        let object = dfs
            .current_read_object_metadata(&stat.object_id)
            .expect("current object");
        let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();

        dfs.put_visible_object_in_metadata_backend(&object)
            .expect("repeat visible object");
        dfs.put_visible_object_in_metadata_backend(&object)
            .expect("repeat visible object again");
        dfs.put_object_version_size_in_metadata_backend(&object, stat.size_bytes)
            .expect("repeat visible version size");

        let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();
        assert_eq!(
            journal_count_after, journal_count_before,
            "re-publishing unchanged object/version metadata should not emit journal entries"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn duplicate_commit_metadata_application_is_idempotent() {
        let root = temp_root("duplicate-commit-metadata-idempotent");
        let node_id = NodeId::from_legacy_named("node_duplicate_commit_meta").expect("node");
        let object_id =
            DfsObjectId::from_legacy_named("obj_duplicate_commit_meta").expect("object");
        let bytes = b"duplicate commit bytes";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        let mut metadata = file_metadata(node_id.clone());
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.last_modified_by_event_id =
            Some(EventId::from_legacy_named("evt_duplicate_commit_base").expect("event"));
        let version = metadata.version.clone();
        let hash = dfs
            .put_local_object_version(metadata, bytes)
            .expect("local version");
        let before = dfs
            .current_read_object_metadata(&object_id)
            .expect("current object");
        let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();

        let committed = dfs
            .commit_local_metadata(&object_id, &version, &hash, dfs_now_unix_nanos())
            .expect("duplicate commit metadata");
        dfs.put_visible_object_in_metadata_backend(&committed)
            .expect("duplicate visible publish");

        let after = dfs
            .current_read_object_metadata(&object_id)
            .expect("current object after duplicate commit");
        assert_eq!(
            after.last_modified_by_event_id,
            before.last_modified_by_event_id
        );
        assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
        assert_eq!(after.changed_at_unix_nanos, before.changed_at_unix_nanos);
        let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();
        assert_eq!(
            journal_count_after, journal_count_before,
            "duplicate committed metadata application must not emit visible metadata churn"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn duplicate_committed_intent_application_does_not_recommit_visible_version() {
        let root = temp_root("duplicate-committed-intent-idempotent");
        let owner = NodeId::from_legacy_named("node_duplicate_intent_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_duplicate_intent_replica").expect("node");
        let object_id =
            DfsObjectId::from_legacy_named("obj_duplicate_intent_commit").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"duplicate committed intent bytes";

        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.version = version_1.clone();
        dfs.add_object_metadata(metadata).expect("object");
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("manifest");
        let intent = dfs
            .coordinator
            .create_intent(
                sifs_file_system_id(),
                object_id.clone(),
                Some(version_1),
                version_2.clone(),
                owner.clone(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(1, 2).expect("policy"),
            )
            .expect("intent");
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version_2,
            &owner,
            &manifest.content_hash,
        )
        .expect("backend receipt");
        dfs.coordinator
            .record_durable_receipt(&intent.intent_id, owner.clone())
            .expect("commit with repair");
        dfs.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version_2,
                content_hash: manifest.content_hash.clone(),
                size_bytes: manifest.size_bytes,
                stage_retries: BTreeMap::new(),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
                fetch_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_started_ticks: BTreeMap::new(),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        dfs.apply_committed_intent(&intent.intent_id)
            .expect("first commit apply");
        let before = dfs
            .current_read_object_metadata(&object_id)
            .expect("current object after first apply");
        let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();

        dfs.apply_committed_intent(&intent.intent_id)
            .expect("duplicate commit apply");

        let after = dfs
            .current_read_object_metadata(&object_id)
            .expect("current object after duplicate apply");
        assert_eq!(after, before);
        let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();
        assert_eq!(
            journal_count_after, journal_count_before,
            "reapplying an already-visible committed intent must not append a commit journal entry"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn committed_intent_repairs_already_current_stale_locations() {
        let root = temp_root("committed-intent-repairs-stale-locations");
        let owner = NodeId::from_legacy_named("node_stale_location_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_stale_location_replica").expect("node");
        let object_id =
            DfsObjectId::from_legacy_named("obj_stale_location_commit").expect("object");
        let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
        let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
        let bytes = b"already current with stale locations";

        let mut dfs = networked_sifs_with_backend(&root, owner.clone());
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        metadata.version = version_1.clone();
        dfs.add_object_metadata(metadata.clone())
            .expect("base visible object");
        let manifest = dfs
            .content
            .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
            .expect("manifest");
        let intent = dfs
            .coordinator
            .create_intent(
                sifs_file_system_id(),
                object_id.clone(),
                Some(version_1),
                version_2.clone(),
                owner.clone(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect("intent");
        let mut stale_visible = metadata;
        stale_visible.version = version_2.clone();
        stale_visible.current_locations = vec![owner.clone()];
        stale_visible.content_hash = Some(manifest.content_hash.clone());
        stale_visible.content_size_bytes = Some(manifest.size_bytes);
        dfs.add_object_metadata(stale_visible)
            .expect("stale visible object");
        for node_id in [&owner, &replica] {
            dfs.put_metadata_receipt(
                &sifs_file_system_id(),
                &object_id,
                &version_2,
                node_id,
                &manifest.content_hash,
            )
            .expect("backend receipt");
            dfs.coordinator
                .record_durable_receipt(&intent.intent_id, node_id.clone())
                .expect("durable receipt");
        }
        dfs.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite {
                object_id: object_id.clone(),
                version: version_2,
                content_hash: manifest.content_hash.clone(),
                size_bytes: manifest.size_bytes,
                stage_retries: BTreeMap::new(),
                repair_retries: BTreeMap::new(),
                stage_inflight_request_ids: BTreeMap::new(),
                repair_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
                fetch_inflight_request_ids: BTreeMap::new(),
                fetch_inflight_started_ticks: BTreeMap::new(),
                fetch_failed_from: BTreeSet::new(),
            },
        );

        dfs.apply_committed_intent(&intent.intent_id)
            .expect("repair stale visible locations");

        let object = dfs
            .current_read_object_metadata(&object_id)
            .expect("current object");
        let current_locations = object
            .current_locations
            .into_iter()
            .collect::<BTreeSet<_>>();
        let expected_locations = [owner.clone(), replica.clone()].into_iter().collect();
        assert_eq!(
            current_locations, expected_locations,
            "already-current commits must still repair stale durable replica locations"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn duplicate_metadata_receipt_publication_is_idempotent() {
        let root = temp_root("metadata-receipt-publication-idempotent");
        let owner = NodeId::from_legacy_named("node_receipt_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_receipt_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_receipt_idempotent").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let content_hash = "blake3:receipt-idempotent";

        let mut dfs = networked_sifs_with_backend(&root, owner);
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            content_hash,
        )
        .expect("first receipt");
        let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();

        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            content_hash,
        )
        .expect("duplicate receipt");

        let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();
        assert_eq!(
            journal_count_after, journal_count_before,
            "duplicate replica receipts for the same content hash should not emit journal entries"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn conflicting_metadata_receipt_from_same_replica_is_rejected() {
        let root = temp_root("metadata-receipt-conflicting-hash-rejected");
        let owner = NodeId::from_legacy_named("node_receipt_conflict_owner").expect("node");
        let replica = NodeId::from_legacy_named("node_receipt_conflict_replica").expect("node");
        let object_id = DfsObjectId::from_legacy_named("obj_receipt_conflict").expect("object");
        let version = DfsVersion::from_legacy_named("version_2").expect("version");
        let content_hash = "blake3:receipt-original";
        let conflicting_hash = "blake3:receipt-conflicting";

        let mut dfs = networked_sifs_with_backend(&root, owner);
        assert!(
            dfs.put_metadata_receipt(
                &sifs_file_system_id(),
                &object_id,
                &version,
                &replica,
                content_hash,
            )
            .expect("first receipt")
        );
        let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();

        let error = dfs
            .put_metadata_receipt(
                &sifs_file_system_id(),
                &object_id,
                &version,
                &replica,
                conflicting_hash,
            )
            .expect_err("conflicting receipt must be rejected");

        assert!(
            error
                .to_string()
                .contains("conflicting DFS replica receipt"),
            "unexpected error: {error}"
        );
        let receipts = dfs
            .metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version)
            .expect("receipts");
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].content_hash.as_deref(), Some(content_hash));
        let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();
        assert_eq!(
            journal_count_after, journal_count_before,
            "rejected conflicting receipt must not overwrite receipt metadata or emit journals"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_symlink_preserves_target_as_object_content() {
        let root = temp_root("sifs-posix-symlink");
        let node_id = NodeId::from_legacy_named("node_sifs_symlink").expect("node");
        let link_path = "/services/sandbox/instances/sandbox_link/work/lib";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_link/work")
            .expect("mkdir");
        let stat = dfs
            .posix_symlink(
                "../shared/lib",
                link_path,
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("symlink");

        assert_eq!(stat.kind, DfsObjectKind::Symlink);
        assert_eq!(stat.size_bytes, Some("../shared/lib".len() as u64));
        assert_eq!(
            dfs.posix_readlink(link_path).expect("readlink"),
            "../shared/lib"
        );
        dfs.posix_set_symlink_times(link_path, Some(12_345), Some(67_890))
            .expect("old symlink times");
        let before_readlink = dfs.posix_lstat(link_path).expect("before readlink lstat");
        assert_eq!(
            dfs.posix_readlink_update_access(link_path)
                .expect("readlink update access"),
            "../shared/lib"
        );
        let stat = dfs.posix_lstat(link_path).expect("symlink lstat");
        assert!(stat.accessed_at_unix_nanos > 12_345);
        assert_eq!(stat.modified_at_unix_nanos, 67_890);
        assert_eq!(
            stat.changed_at_unix_nanos, before_readlink.changed_at_unix_nanos,
            "readlink access update must not advance symlink ctime"
        );
        assert_ne!(
            stat.version, before_readlink.version,
            "readlink access update must advance the DFS metadata version so it persists across distributed reconciliation"
        );
        assert!(matches!(
            dfs.posix_symlink(
                "other",
                link_path,
                DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
            ),
            Err(DfsPosixError::AlreadyExists { .. })
        ));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_trailing_slash_requires_directory() {
        let root = temp_root("sifs-posix-trailing-slash-directory");
        let node_id = NodeId::from_legacy_named("node_sifs_trailing_slash").expect("node");
        let work = "/services/sandbox/instances/sandbox_trailing/work";
        let real_dir = "/services/sandbox/instances/sandbox_trailing/work/real-dir";
        let file_path = "/services/sandbox/instances/sandbox_trailing/work/file.txt";
        let link_dir = "/services/sandbox/instances/sandbox_trailing/work/link-dir";
        let link_file = "/services/sandbox/instances/sandbox_trailing/work/link-file";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(real_dir).expect("mkdir real dir");
        dfs.posix_write_file(
            file_path,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write file");
        dfs.posix_symlink(
            "real-dir",
            link_dir,
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("symlink dir");
        dfs.posix_symlink(
            "file.txt",
            link_file,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("symlink file");

        assert_eq!(
            dfs.posix_lstat(link_dir).expect("plain lstat").kind,
            DfsObjectKind::Symlink
        );
        assert_eq!(
            dfs.posix_lstat(&format!("{link_dir}/"))
                .expect("trailing slash lstat follows directory symlink")
                .kind,
            DfsObjectKind::Directory
        );
        assert!(matches!(
            dfs.posix_read_file(&format!("{file_path}/")),
            Err(DfsPosixError::NotDirectory { .. })
        ));
        assert!(matches!(
            dfs.posix_read_file(&format!("{link_file}/")),
            Err(DfsPosixError::NotDirectory { .. })
        ));
        assert!(matches!(
            dfs.posix_readlink(&format!("{link_dir}/")),
            Err(DfsPosixError::NotSymlink { .. })
        ));
        assert!(matches!(
            dfs.posix_rename(&format!("{file_path}/"), &format!("{work}/renamed.txt")),
            Err(DfsPosixError::NotDirectory { .. })
        ));
        assert!(matches!(
            dfs.posix_rename(file_path, &format!("{work}/missing-destination/")),
            Err(DfsPosixError::NotDirectory { .. })
        ));
        assert!(matches!(
            dfs.posix_hardlink(&format!("{file_path}/"), &format!("{work}/file-alias.txt")),
            Err(DfsPosixError::NotDirectory { .. })
        ));
        assert!(matches!(
            dfs.posix_hardlink(file_path, &format!("{work}/missing-alias/")),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert!(matches!(
            dfs.posix_symlink(
                "file.txt",
                &format!("{work}/missing-link/"),
                DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
            ),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert!(matches!(
            dfs.posix_symlink(
                "file.txt",
                &format!("{real_dir}/"),
                DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()])
            ),
            Err(DfsPosixError::AlreadyExists { .. })
        ));
        assert!(matches!(
            dfs.posix_unlink(&format!("{link_dir}/")),
            Err(DfsPosixError::IsDirectory { .. })
        ));
        assert_eq!(
            dfs.posix_lstat(link_dir)
                .expect("symlink survives failed unlink")
                .kind,
            DfsObjectKind::Symlink
        );
        assert_eq!(
            dfs.posix_stat(real_dir).expect("target survives").kind,
            DfsObjectKind::Directory
        );
        assert_eq!(
            dfs.posix_list_dir(&format!("{work}/"))
                .expect("list with trailing slash")
                .len(),
            4
        );
        dfs.posix_mkdir_all(&format!("{work}/rename-source-dir"))
            .expect("mkdir rename source dir");
        dfs.posix_rename(
            &format!("{work}/rename-source-dir"),
            &format!("{work}/renamed-dir/"),
        )
        .expect("rename directory to trailing slash missing path");
        assert_eq!(
            dfs.posix_stat(&format!("{work}/renamed-dir"))
                .expect("renamed directory")
                .kind,
            DfsObjectKind::Directory
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rmdir_reports_directory_not_empty() {
        let root = temp_root("sifs-posix-rmdir-not-empty");
        let node_id = NodeId::from_legacy_named("node_sifs_rmdir").expect("node");
        let dir_path = "/services/sandbox/instances/sandbox_rmdir/work";
        let file_path = "/services/sandbox/instances/sandbox_rmdir/work/file.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(dir_path).expect("mkdir");
        dfs.posix_write_file(
            file_path,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");

        assert!(matches!(
            dfs.posix_rmdir(dir_path),
            Err(DfsPosixError::DirectoryNotEmpty { .. })
        ));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_stat_reports_directory_link_counts() {
        let root = temp_root("sifs-posix-link-count");
        let node_id = NodeId::from_legacy_named("node_sifs_link_count").expect("node");
        let parent = "/services/sandbox/instances/sandbox_links/work";
        let child_dir = "/services/sandbox/instances/sandbox_links/work/child";
        let nested_file = "/services/sandbox/instances/sandbox_links/work/file.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(child_dir).expect("mkdir child");
        dfs.posix_write_file(
            nested_file,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");

        assert_eq!(dfs.posix_stat(parent).expect("parent stat").link_count, 3);
        assert_eq!(dfs.posix_stat(child_dir).expect("child stat").link_count, 2);
        assert_eq!(
            dfs.posix_stat(nested_file).expect("file stat").link_count,
            1
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_hardlink_shares_content_and_link_count() {
        let root = temp_root("sifs-posix-hardlink-shared-content");
        let node_id = NodeId::from_legacy_named("node_sifs_hardlink").expect("node");
        let original = "/services/sandbox/instances/sandbox_hardlink/work/file.txt";
        let link = "/services/sandbox/instances/sandbox_hardlink/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_hardlink/work")
            .expect("mkdir");
        dfs.posix_write_file(
            original,
            b"original",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write original");
        dfs.posix_set_times(original, Some(12_345), Some(67_890))
            .expect("set old times");
        dfs.posix_hardlink(original, link).expect("hardlink");

        let linked_ctime = dfs
            .posix_stat(original)
            .expect("original linked stat")
            .changed_at_unix_nanos;
        assert!(linked_ctime > 67_890);
        assert_eq!(
            dfs.posix_stat(original).expect("original stat").link_count,
            2
        );
        assert_eq!(dfs.posix_stat(link).expect("link stat").link_count, 2);
        dfs.posix_write_file(
            link,
            b"updated",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write link");
        assert_eq!(
            dfs.posix_read_file(original).expect("read original"),
            b"updated"
        );
        assert_eq!(dfs.posix_read_file(link).expect("read link"), b"updated");

        dfs.posix_unlink(link).expect("unlink alias");
        let remaining_stat = dfs.posix_stat(original).expect("remaining stat");
        assert_eq!(remaining_stat.link_count, 1);
        assert!(remaining_stat.changed_at_unix_nanos >= linked_ctime);
        assert!(matches!(
            dfs.posix_read_file(link),
            Err(DfsPosixError::NotFound { .. })
        ));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_hardlink_preserves_symlink_object() {
        let root = temp_root("sifs-posix-hardlink-symlink");
        let node_id = NodeId::from_legacy_named("node_sifs_hardlink_symlink").expect("node");
        let work = "/services/sandbox/instances/sandbox_hardlink_symlink/work";
        let target = "/services/sandbox/instances/sandbox_hardlink_symlink/work/target.txt";
        let link = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link.txt";
        let alias = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link-alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            target,
            b"target",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write target");
        dfs.posix_symlink(
            "target.txt",
            link,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("symlink");
        dfs.posix_hardlink(link, alias).expect("hardlink symlink");

        let link_stat = dfs.posix_lstat(link).expect("link lstat");
        let alias_stat = dfs.posix_lstat(alias).expect("alias lstat");
        assert_eq!(link_stat.kind, DfsObjectKind::Symlink);
        assert_eq!(alias_stat.kind, DfsObjectKind::Symlink);
        assert_eq!(link_stat.object_id, alias_stat.object_id);
        assert_eq!(link_stat.link_count, 2);
        assert_eq!(alias_stat.link_count, 2);
        assert_eq!(
            dfs.posix_readlink(alias).expect("alias readlink"),
            "target.txt"
        );

        dfs.posix_unlink(link).expect("unlink original symlink");
        assert!(matches!(
            dfs.posix_readlink(link),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            dfs.posix_readlink(alias).expect("remaining alias readlink"),
            "target.txt"
        );
        assert_eq!(
            dfs.posix_lstat(alias).expect("remaining alias").link_count,
            1
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_unlink_promotes_hardlink_when_canonical_removed() {
        let root = temp_root("sifs-posix-hardlink-promote");
        let node_id = NodeId::from_legacy_named("node_sifs_hardlink_promote").expect("node");
        let original = "/services/sandbox/instances/sandbox_hardlink_promote/work/file.txt";
        let link = "/services/sandbox/instances/sandbox_hardlink_promote/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_hardlink_promote/work")
            .expect("mkdir");
        dfs.posix_write_file(
            original,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write original");
        let original_object_id = dfs.posix_stat(original).expect("original stat").object_id;
        dfs.posix_hardlink(original, link).expect("hardlink");

        dfs.posix_unlink(original).expect("unlink canonical");
        assert!(matches!(
            dfs.posix_read_file(original),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            dfs.posix_read_file(link).expect("read promoted"),
            b"content"
        );
        let promoted = dfs.posix_stat(link).expect("promoted stat");
        assert_eq!(promoted.object_id, original_object_id);
        assert_eq!(promoted.link_count, 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_records_old_path_tombstone() {
        let root = temp_root("sifs-posix-rename-tombstone");
        let node_id = NodeId::from_legacy_named("node_sifs_rename_tombstone").expect("node");
        let old_path = "/services/sandbox/instances/sandbox_rename/work/old.txt";
        let new_path = "/services/sandbox/instances/sandbox_rename/work/new.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_rename/work")
            .expect("mkdir");
        dfs.posix_write_file(
            old_path,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write old");
        let old_version = dfs.posix_stat(old_path).expect("old stat").version;
        dfs.posix_rename(old_path, new_path).expect("rename");

        assert!(matches!(
            dfs.posix_stat(old_path),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(dfs.posix_read_file(new_path).expect("read new"), b"content");
        let tombstone = dfs
            .metadata
            .objects
            .values()
            .find(|object| object.path == old_path && object.kind == DfsObjectKind::Tombstone)
            .expect("old path tombstone");
        assert_ne!(tombstone.version, old_version);

        let reloaded = networked_sifs_with_backend(&root, node_id);
        assert!(matches!(
            reloaded.posix_stat(old_path),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            reloaded
                .posix_read_file(new_path)
                .expect("reloaded read new"),
            b"content"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_directory_does_not_resurrect_deleted_children() {
        let root = temp_root("sifs-posix-rename-dir-deleted-child");
        let node_id =
            NodeId::from_legacy_named("node_sifs_rename_dir_deleted_child").expect("node");
        let old_dir = "/services/sandbox/instances/sandbox_rename_deleted/work/old";
        let new_dir = "/services/sandbox/instances/sandbox_rename_deleted/work/new";
        let old_child = format!("{old_dir}/deleted.txt");
        let new_child = format!("{new_dir}/deleted.txt");

        {
            let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
            dfs.posix_mkdir_all(old_dir).expect("mkdir old dir");
            dfs.posix_write_file(
                &old_child,
                b"deleted",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write child");
            dfs.posix_unlink(&old_child).expect("unlink child");
            dfs.posix_rename(old_dir, new_dir).expect("rename dir");

            assert!(matches!(
                dfs.posix_read_file(&new_child),
                Err(DfsPosixError::NotFound { .. })
            ));
            assert!(
                dfs.posix_list_dir(new_dir)
                    .expect("new dir listing")
                    .is_empty()
            );
        }

        let reloaded = networked_sifs_with_backend(&root, node_id);
        assert!(matches!(
            reloaded.posix_read_file(&new_child),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert!(
            reloaded
                .posix_list_dir(new_dir)
                .expect("reloaded new dir listing")
                .is_empty()
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_over_existing_file_survives_reload() {
        let root = temp_root("sifs-posix-rename-over-existing-file-reload");
        let node_id =
            NodeId::from_legacy_named("node_sifs_rename_over_existing_reload").expect("node");
        let work = "/services/sandbox/instances/sandbox_rename_over_existing/work";
        let source = "/services/sandbox/instances/sandbox_rename_over_existing/work/source.txt";
        let dest = "/services/sandbox/instances/sandbox_rename_over_existing/work/dest.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            source,
            b"source",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write source");
        let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;
        dfs.posix_write_file(
            dest,
            b"dest",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write dest");

        dfs.posix_rename(source, dest).expect("rename over dest");

        assert!(matches!(
            dfs.posix_stat(source),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(dfs.posix_read_file(dest).expect("read dest"), b"source");
        assert_eq!(
            dfs.posix_stat(dest).expect("dest stat").object_id,
            source_object_id
        );

        let reloaded = networked_sifs_with_backend(&root, node_id);
        assert!(matches!(
            reloaded.posix_stat(source),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            reloaded.posix_read_file(dest).expect("reloaded read dest"),
            b"source"
        );
        assert_eq!(
            reloaded
                .posix_stat(dest)
                .expect("reloaded dest stat")
                .object_id,
            source_object_id
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_hardlink_over_canonical_preserves_target() {
        let root = temp_root("sifs-posix-rename-hardlink-over-canonical");
        let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink").expect("node");
        let canonical = "/services/sandbox/instances/sandbox_rename_hardlink/work/file.txt";
        let alias = "/services/sandbox/instances/sandbox_rename_hardlink/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_rename_hardlink/work")
            .expect("mkdir");
        dfs.posix_write_file(
            canonical,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write canonical");
        let canonical_object_id = dfs.posix_stat(canonical).expect("canonical stat").object_id;
        dfs.posix_hardlink(canonical, alias).expect("hardlink");
        assert_eq!(dfs.posix_stat(alias).expect("alias stat").link_count, 2);

        dfs.posix_rename(alias, canonical)
            .expect("rename alias over canonical");

        let alias_stat = dfs.posix_stat(alias).expect("alias remains");
        let canonical_stat = dfs.posix_stat(canonical).expect("canonical remains");
        assert_eq!(canonical_stat.object_id, canonical_object_id);
        assert_eq!(alias_stat.object_id, canonical_object_id);
        assert_eq!(canonical_stat.link_count, 2);
        assert_eq!(alias_stat.link_count, 2);
        assert_eq!(
            dfs.posix_read_file(canonical).expect("read canonical"),
            b"content"
        );
        assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"content");

        let reloaded = networked_sifs_with_backend(&root, node_id);
        assert_eq!(
            reloaded
                .posix_stat(alias)
                .expect("reloaded alias")
                .link_count,
            2
        );
        assert_eq!(
            reloaded.posix_read_file(canonical).expect("reloaded read"),
            b"content"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_over_canonical_preserves_other_hardlinks() {
        let root = temp_root("sifs-posix-rename-over-canonical-hardlinks");
        let node_id =
            NodeId::from_legacy_named("node_sifs_rename_over_canonical_hardlinks").expect("node");
        let work = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work";
        let source = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/source.txt";
        let canonical =
            "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/canonical.txt";
        let alias = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            canonical,
            b"old",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write canonical");
        let old_object_id = dfs.posix_stat(canonical).expect("canonical stat").object_id;
        dfs.posix_hardlink(canonical, alias).expect("hardlink");
        dfs.posix_write_file(
            source,
            b"new",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write source");
        let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;

        dfs.posix_rename(source, canonical)
            .expect("rename source over canonical");

        assert!(matches!(
            dfs.posix_stat(source),
            Err(DfsPosixError::NotFound { .. })
        ));
        let canonical_stat = dfs
            .posix_stat(canonical)
            .expect("canonical stat after rename");
        let alias_stat = dfs.posix_stat(alias).expect("alias stat after rename");
        assert_eq!(canonical_stat.object_id, source_object_id);
        assert_eq!(canonical_stat.link_count, 1);
        assert_eq!(alias_stat.object_id, old_object_id);
        assert_eq!(alias_stat.link_count, 1);
        assert_eq!(
            dfs.posix_read_file(canonical).expect("read canonical"),
            b"new"
        );
        assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"old");

        let reloaded = networked_sifs_with_backend(&root, node_id);
        assert_eq!(
            reloaded
                .posix_stat(alias)
                .expect("reloaded alias")
                .object_id,
            old_object_id
        );
        assert_eq!(
            reloaded
                .posix_read_file(canonical)
                .expect("reloaded canonical"),
            b"new"
        );
        assert_eq!(
            reloaded.posix_read_file(alias).expect("reloaded alias"),
            b"old"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_hardlink_over_canonical_preserves_both_targets() {
        let root = temp_root("sifs-posix-rename-hardlink-over-other-canonical");
        let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink_over_other_canonical")
            .expect("node");
        let work = "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work";
        let source_canonical =
            "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/source.txt";
        let source_alias =
            "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/source-alias.txt";
        let dest_canonical =
            "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/dest.txt";
        let dest_alias =
            "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/dest-alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            source_canonical,
            b"source",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write source");
        dfs.posix_hardlink(source_canonical, source_alias)
            .expect("source hardlink");
        let source_object_id = dfs
            .posix_stat(source_canonical)
            .expect("source stat")
            .object_id;
        dfs.posix_write_file(
            dest_canonical,
            b"dest",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write dest");
        dfs.posix_hardlink(dest_canonical, dest_alias)
            .expect("dest hardlink");
        let dest_object_id = dfs.posix_stat(dest_canonical).expect("dest stat").object_id;

        dfs.posix_rename(source_alias, dest_canonical)
            .expect("rename source alias over dest canonical");

        let source_stat = dfs
            .posix_stat(source_canonical)
            .expect("source canonical remains");
        let renamed_stat = dfs.posix_stat(dest_canonical).expect("renamed source");
        let dest_alias_stat = dfs.posix_stat(dest_alias).expect("dest alias remains");
        assert_eq!(source_stat.object_id, source_object_id);
        assert_eq!(renamed_stat.object_id, source_object_id);
        assert_eq!(source_stat.link_count, 2);
        assert_eq!(renamed_stat.link_count, 2);
        assert_eq!(dest_alias_stat.object_id, dest_object_id);
        assert_eq!(dest_alias_stat.link_count, 1);
        assert_eq!(
            dfs.posix_read_file(source_canonical)
                .expect("read source canonical"),
            b"source"
        );
        assert_eq!(
            dfs.posix_read_file(dest_canonical)
                .expect("read renamed source"),
            b"source"
        );
        assert_eq!(
            dfs.posix_read_file(dest_alias).expect("read dest alias"),
            b"dest"
        );
        assert!(matches!(
            dfs.posix_stat(source_alias),
            Err(DfsPosixError::NotFound { .. })
        ));

        let reloaded = networked_sifs_with_backend(&root, node_id);
        assert_eq!(
            reloaded
                .posix_stat(dest_canonical)
                .expect("reloaded renamed source")
                .object_id,
            source_object_id
        );
        assert_eq!(
            reloaded
                .posix_stat(dest_alias)
                .expect("reloaded dest alias")
                .object_id,
            dest_object_id
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_hardlink_alias_updates_target_ctime() {
        let root = temp_root("sifs-posix-rename-hardlink-alias-ctime");
        let node_id =
            NodeId::from_legacy_named("node_sifs_rename_hardlink_alias_ctime").expect("node");
        let work = "/services/sandbox/instances/sandbox_rename_alias_ctime/work";
        let canonical = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/canonical.txt";
        let alias = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/alias.txt";
        let renamed =
            "/services/sandbox/instances/sandbox_rename_alias_ctime/work/renamed-alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            canonical,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write canonical");
        dfs.posix_hardlink(canonical, alias).expect("hardlink");
        let before = dfs.posix_stat(canonical).expect("before stat");
        std::thread::sleep(std::time::Duration::from_millis(1));

        dfs.posix_rename(alias, renamed)
            .expect("rename hardlink alias");

        let canonical_after = dfs.posix_stat(canonical).expect("canonical remains");
        let renamed_after = dfs.posix_stat(renamed).expect("renamed alias");
        assert_eq!(canonical_after.object_id, before.object_id);
        assert_eq!(renamed_after.object_id, before.object_id);
        assert_eq!(canonical_after.link_count, 2);
        assert_eq!(renamed_after.link_count, 2);
        assert!(
            canonical_after.changed_at_unix_nanos > before.changed_at_unix_nanos,
            "renaming a hardlink alias must advance target inode ctime"
        );
        assert_eq!(
            canonical_after.changed_at_unix_nanos,
            renamed_after.changed_at_unix_nanos
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_over_hardlink_alias_updates_replaced_target_ctime() {
        let root = temp_root("sifs-posix-rename-over-hardlink-alias-ctime");
        let node_id =
            NodeId::from_legacy_named("node_sifs_rename_over_hardlink_alias_ctime").expect("node");
        let work = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work";
        let source = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/source.txt";
        let canonical =
            "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/canonical.txt";
        let alias = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            canonical,
            b"old",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write canonical");
        dfs.posix_hardlink(canonical, alias).expect("hardlink");
        let old_before = dfs.posix_stat(canonical).expect("old before");
        dfs.posix_write_file(
            source,
            b"new",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write source");
        let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;
        std::thread::sleep(std::time::Duration::from_millis(1));

        dfs.posix_rename(source, alias)
            .expect("rename source over hardlink alias");

        let canonical_after = dfs.posix_stat(canonical).expect("canonical remains");
        let alias_after = dfs.posix_stat(alias).expect("alias replaced");
        assert_eq!(canonical_after.object_id, old_before.object_id);
        assert_eq!(canonical_after.link_count, 1);
        assert!(
            canonical_after.changed_at_unix_nanos > old_before.changed_at_unix_nanos,
            "replacing a hardlink alias must advance the replaced target inode ctime"
        );
        assert_eq!(alias_after.object_id, source_object_id);
        assert_eq!(alias_after.link_count, 1);
        assert_eq!(dfs.posix_read_file(canonical).expect("read old"), b"old");
        assert_eq!(dfs.posix_read_file(alias).expect("read new"), b"new");

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_set_times_updates_hardlink_target_metadata() {
        let root = temp_root("sifs-posix-set-times-hardlink");
        let node_id = NodeId::from_legacy_named("node_sifs_set_times").expect("node");
        let original = "/services/sandbox/instances/sandbox_set_times/work/file.txt";
        let link = "/services/sandbox/instances/sandbox_set_times/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_times/work")
            .expect("mkdir");
        dfs.posix_write_file(
            original,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write original");
        dfs.posix_hardlink(original, link).expect("hardlink");

        dfs.posix_set_times(link, Some(12_345), Some(67_890))
            .expect("set times through link");
        let original_stat = dfs.posix_stat(original).expect("original stat");
        let link_stat = dfs.posix_stat(link).expect("link stat");
        assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
        assert_eq!(original_stat.modified_at_unix_nanos, 67_890);
        assert_eq!(link_stat.accessed_at_unix_nanos, 12_345);
        assert_eq!(link_stat.modified_at_unix_nanos, 67_890);
        assert!(original_stat.changed_at_unix_nanos > 67_890);
        assert_eq!(
            original_stat.changed_at_unix_nanos,
            link_stat.changed_at_unix_nanos
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_set_times_without_fields_is_noop() {
        let root = temp_root("sifs-posix-set-times-noop");
        let node_id = NodeId::from_legacy_named("node_sifs_set_times_noop").expect("node");
        let file = "/services/sandbox/instances/sandbox_set_times_noop/work/file.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_times_noop/work")
            .expect("mkdir");
        dfs.posix_write_file(
            file,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write file");
        dfs.posix_set_times(file, Some(12_345), Some(67_890))
            .expect("set baseline times");
        let before = dfs.posix_stat(file).expect("before stat");

        let after = dfs
            .posix_set_times(file, None, None)
            .expect("set no timestamp fields");

        assert_eq!(after.accessed_at_unix_nanos, before.accessed_at_unix_nanos);
        assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
        assert_eq!(after.changed_at_unix_nanos, before.changed_at_unix_nanos);
        assert_eq!(
            after.version, before.version,
            "omitting both timestamps must not churn the distributed metadata version"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_set_owner_updates_hardlink_target_metadata() {
        let root = temp_root("sifs-posix-set-owner-hardlink");
        let node_id = NodeId::from_legacy_named("node_sifs_set_owner").expect("node");
        let original = "/services/sandbox/instances/sandbox_set_owner/work/file.txt";
        let link = "/services/sandbox/instances/sandbox_set_owner/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_owner/work")
            .expect("mkdir");
        dfs.posix_write_file(
            original,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write original");
        dfs.posix_hardlink(original, link).expect("hardlink");

        dfs.posix_set_owner(link, 1001, 1002)
            .expect("set owner through link");
        let original_stat = dfs.posix_stat(original).expect("original stat");
        let link_stat = dfs.posix_stat(link).expect("link stat");
        assert_eq!(original_stat.uid, 1001);
        assert_eq!(original_stat.gid, 1002);
        assert_eq!(link_stat.uid, 1001);
        assert_eq!(link_stat.gid, 1002);
        assert_eq!(
            original_stat.changed_at_unix_nanos,
            link_stat.changed_at_unix_nanos
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_set_mode_zero_is_reported_as_zero() {
        let root = temp_root("sifs-posix-set-mode-zero");
        let node_id = NodeId::from_legacy_named("node_sifs_set_mode_zero").expect("node");
        let file = "/services/sandbox/instances/sandbox_set_mode_zero/work/file.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_mode_zero/work")
            .expect("mkdir");
        dfs.posix_write_file(
            file,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write file");

        let changed = dfs.posix_set_mode(file, 0).expect("chmod 0000");
        assert_eq!(changed.mode, 0);
        assert_eq!(dfs.posix_stat(file).expect("stat").mode, 0);
        assert!(
            dfs.posix_stat(file).expect("stat").changed_at_unix_nanos
                >= changed.changed_at_unix_nanos
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_metadata_only_versions_preserve_content_size() {
        let root = temp_root("sifs-posix-metadata-only-size");
        let node_id = NodeId::from_legacy_named("node_sifs_metadata_only_size").expect("node");
        let work = "/services/sandbox/instances/sandbox_metadata_size/work";
        let file = "/services/sandbox/instances/sandbox_metadata_size/work/file.txt";
        let renamed = "/services/sandbox/instances/sandbox_metadata_size/work/renamed.txt";
        let link = "/services/sandbox/instances/sandbox_metadata_size/work/link.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            file,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write file");
        assert_eq!(
            dfs.posix_set_mode(file, 0o600).expect("chmod").size_bytes,
            Some(7)
        );
        assert_eq!(
            dfs.posix_rename(file, renamed).expect("rename").size_bytes,
            Some(7)
        );

        dfs.posix_symlink(
            "renamed.txt",
            link,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("symlink");
        assert_eq!(
            dfs.posix_set_symlink_times(link, Some(1), Some(2))
                .expect("set symlink times")
                .size_bytes,
            Some("renamed.txt".len() as u64)
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_recreate_file_refreshes_creation_metadata() {
        let root = temp_root("sifs-posix-recreate-file-times");
        let node_id = NodeId::from_legacy_named("node_sifs_recreate_file").expect("node");
        let file_path = "/services/sandbox/instances/sandbox_recreate/work/file.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_recreate/work")
            .expect("mkdir");
        dfs.posix_write_file(
            file_path,
            b"old",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write old");
        dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
            .expect("old times");
        dfs.posix_unlink(file_path).expect("unlink");

        let recreated = dfs
            .posix_write_file(
                file_path,
                b"new",
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("recreate");

        assert_eq!(recreated.size_bytes, Some(3));
        assert!(recreated.accessed_at_unix_nanos > 12_345);
        assert!(recreated.modified_at_unix_nanos > 67_890);
        assert!(recreated.created_at_unix_nanos > 67_890);
        assert_eq!(dfs.posix_read_file(file_path).expect("read"), b"new");

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_recreate_directory_refreshes_creation_metadata() {
        let root = temp_root("sifs-posix-recreate-dir-times");
        let node_id = NodeId::from_legacy_named("node_sifs_recreate_dir").expect("node");
        let dir_path = "/services/sandbox/instances/sandbox_recreate_dir/work/dir";

        let mut dfs = networked_sifs_with_backend(&root, node_id);
        dfs.posix_mkdir_all(dir_path).expect("mkdir");
        dfs.posix_set_times(dir_path, Some(12_345), Some(67_890))
            .expect("old times");
        dfs.posix_rmdir(dir_path).expect("rmdir");
        let recreated = dfs.posix_mkdir(dir_path).expect("recreate dir");

        assert!(recreated.accessed_at_unix_nanos > 12_345);
        assert!(recreated.modified_at_unix_nanos > 67_890);
        assert!(recreated.created_at_unix_nanos > 67_890);

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_write_updates_mtime_without_changing_atime() {
        let root = temp_root("sifs-posix-write-mtime");
        let node_id = NodeId::from_legacy_named("node_sifs_write_mtime").expect("node");
        let original = "/services/sandbox/instances/sandbox_write_mtime/work/file.txt";
        let link = "/services/sandbox/instances/sandbox_write_mtime/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_write_mtime/work")
            .expect("mkdir");
        dfs.posix_write_file(
            original,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write original");
        dfs.posix_hardlink(original, link).expect("hardlink");
        dfs.posix_set_times(original, Some(12_345), Some(67_890))
            .expect("set old times");

        let stat = dfs
            .posix_write_file(
                link,
                b"updated",
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("write through hardlink");

        assert_eq!(stat.accessed_at_unix_nanos, 12_345);
        assert!(stat.modified_at_unix_nanos > 67_890);
        let original_stat = dfs.posix_stat(original).expect("original stat");
        let link_stat = dfs.posix_stat(link).expect("link stat");
        assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
        assert_eq!(link_stat.accessed_at_unix_nanos, 12_345);
        assert_eq!(
            original_stat.modified_at_unix_nanos,
            link_stat.modified_at_unix_nanos
        );
        assert!(original_stat.modified_at_unix_nanos > 67_890);
        assert_eq!(
            original_stat.changed_at_unix_nanos,
            original_stat.modified_at_unix_nanos
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_write_at_merges_bytes_and_preserves_hardlink_identity() {
        let root = temp_root("sifs-posix-write-at-hardlink");
        let node_id = NodeId::from_legacy_named("node_sifs_write_at").expect("node");
        let work = "/services/sandbox/instances/sandbox_write_at/work";
        let original = "/services/sandbox/instances/sandbox_write_at/work/file.txt";
        let alias = "/services/sandbox/instances/sandbox_write_at/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            original,
            b"abcdef",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write original");
        dfs.posix_hardlink(original, alias).expect("hardlink");

        let stat = dfs
            .posix_write_file_at(
                alias,
                2,
                b"XYZ",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write at hardlink");

        assert_eq!(stat.size_bytes, Some(6));
        assert_eq!(
            dfs.posix_read_file(original).expect("read original"),
            b"abXYZf"
        );
        assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"abXYZf");
        let original_stat = dfs.posix_stat(original).expect("original stat");
        let alias_stat = dfs.posix_stat(alias).expect("alias stat");
        assert_eq!(original_stat.object_id, alias_stat.object_id);
        assert_eq!(original_stat.link_count, 2);
        assert_eq!(alias_stat.link_count, 2);

        let stat = dfs
            .posix_write_file_at(
                original,
                8,
                b"!",
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("sparse write");

        assert_eq!(stat.size_bytes, Some(9));
        assert_eq!(
            dfs.posix_read_file(alias).expect("read sparse alias"),
            b"abXYZf\0\0!"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_truncate_preserves_hardlink_identity() {
        let root = temp_root("sifs-posix-truncate-hardlink");
        let node_id = NodeId::from_legacy_named("node_sifs_truncate").expect("node");
        let work = "/services/sandbox/instances/sandbox_truncate/work";
        let original = "/services/sandbox/instances/sandbox_truncate/work/file.txt";
        let alias = "/services/sandbox/instances/sandbox_truncate/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            original,
            b"abcdef",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write original");
        dfs.posix_hardlink(original, alias).expect("hardlink");

        let stat = dfs
            .posix_truncate_file(
                alias,
                3,
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("truncate through hardlink");
        assert_eq!(stat.size_bytes, Some(3));
        assert_eq!(dfs.posix_read_file(original).expect("read"), b"abc");
        assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"abc");
        assert_eq!(
            dfs.posix_stat(original).expect("original stat").object_id,
            dfs.posix_stat(alias).expect("alias stat").object_id
        );

        let stat = dfs
            .posix_truncate_file(
                original,
                5,
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("extend sparse through original");
        assert_eq!(stat.size_bytes, Some(5));
        assert_eq!(
            dfs.posix_read_file(alias).expect("read sparse alias"),
            b"abc\0\0"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_distributed_write_rejects_stale_backend_base_version() {
        let root = temp_root("sifs-stale-write-base");
        let node_id = NodeId::from_legacy_named("node_sifs_stale_write_base").expect("node");
        let work = "/services/sandbox/instances/sandbox_stale_write/work";
        let file_path = "/services/sandbox/instances/sandbox_stale_write/work/file.txt";

        let mut writer = networked_sifs_with_backend(&root, node_id.clone());
        writer.posix_mkdir_all(work).expect("mkdir");
        writer
            .posix_write_file(
                file_path,
                b"abcdef",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("initial write");

        let stale = networked_sifs_with_backend(&root, node_id.clone());
        let stale_stat = stale.posix_stat(file_path).expect("stale stat");

        writer
            .posix_write_file_at(
                file_path,
                0,
                b"XY",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("fresh offset write");
        let fresh_stat = writer.posix_stat(file_path).expect("fresh stat");
        let journal_count_before_conflict = writer
            .metadata_backend()
            .expect("writer metadata backend")
            .journal_since(0)
            .expect("journal before conflict")
            .len();

        let mut stale_writer = stale;
        let stale_bytes = b"stale overwrite".to_vec();
        let stale_hash = content_hash(&stale_bytes);
        let request = DfsWriteRequest {
            file_system_id: stale_stat.file_system_id.clone(),
            object_id: stale_stat.object_id.clone(),
            target_version: crate::namespace::next_version(&stale_stat.version),
            requesting_node_id: node_id.clone(),
        };
        let outcome = stale_writer
            .write_distributed_eventual_with_base(
                request,
                Some(stale_stat.version.clone()),
                stale_bytes,
                vec![node_id.clone()],
                DfsReplicationPolicy::new(1, 1).expect("policy"),
            )
            .expect("stale write is reported as conflict");
        let conflict = match outcome {
            NetworkedDfsWriteOutcome::Conflict { conflict } => conflict,
            other => panic!("stale write must conflict instead of overwriting: {other:?}"),
        };
        assert_eq!(
            conflict.local_content_hash.as_deref(),
            Some(stale_hash.as_str()),
            "stale writer conflict should preserve the losing writer content hash"
        );
        assert_eq!(
            stale_writer
                .content()
                .read_blob(&stale_hash)
                .expect("stale conflict content"),
            b"stale overwrite"
        );

        let journal_after_conflict = stale_writer
            .metadata_backend()
            .expect("stale writer metadata backend")
            .journal_since(0)
            .expect("journal after conflict");
        assert_eq!(
            journal_after_conflict.len(),
            journal_count_before_conflict + 1,
            "stale writer conflict must append to the shared metadata journal without overwriting fresh writer entries"
        );
        assert!(matches!(
            journal_after_conflict.last().map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutConflict { .. })
        ));

        let mut reloaded = networked_sifs_with_backend(&root, node_id);
        assert_eq!(
            reloaded
                .posix_stat(file_path)
                .expect("stat after conflict")
                .content_hash,
            fresh_stat.content_hash,
            "stale conflict publication must not replace the committed content hash"
        );
        assert!(
            matches!(
                reloaded.posix_read_file(file_path),
                Err(DfsPosixError::Conflict { .. })
            ),
            "open DFS conflicts must not be reported as missing files"
        );
        reloaded
            .update_conflict_status(
                &conflict.object_id,
                &conflict.conflict_id,
                DfsConflictStatus::Resolved,
            )
            .expect("resolve conflict");
        assert_eq!(
            reloaded.posix_read_file(file_path).expect("read winner"),
            b"XYcdef"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_distributed_write_validates_replica_plan_before_storing_content() {
        let root = temp_root("sifs-invalid-replica-plan-no-content");
        let node_id = NodeId::from_legacy_named("node_sifs_invalid_plan").expect("node");
        let work = "/services/sandbox/instances/sandbox_invalid_plan/work";
        let file_path = "/services/sandbox/instances/sandbox_invalid_plan/work/file.txt";
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            file_path,
            b"initial",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("initial write");
        let stat = dfs.posix_stat(file_path).expect("stat");
        let rejected_bytes = b"replica plan is invalid";
        let rejected_hash = content_hash(rejected_bytes);
        let target_version = crate::namespace::next_version(&stat.version);

        let error = dfs
            .write_distributed_eventual_with_base(
                DfsWriteRequest {
                    file_system_id: stat.file_system_id.clone(),
                    object_id: stat.object_id.clone(),
                    target_version: target_version.clone(),
                    requesting_node_id: node_id.clone(),
                },
                Some(stat.version),
                rejected_bytes.to_vec(),
                vec![node_id],
                DfsReplicationPolicy::new(2, 2).expect("policy"),
            )
            .expect_err("invalid replica plan should fail before content write");

        assert!(
            matches!(error, DfsError::Distributed { .. }),
            "invalid replica plan should be reported as distributed validation failure: {error:?}"
        );
        assert!(
            !dfs.content
                .has_blob(&rejected_hash)
                .expect("rejected blob check"),
            "rejected write bytes must not be persisted before replica validation"
        );
        assert!(
            dfs.content
                .read_manifest(stat.object_id.as_str(), target_version.as_str())
                .is_err(),
            "rejected write must not publish a content manifest"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_relink_after_canonical_unlink_does_not_create_hardlink_cycle() {
        let root = temp_root("sifs-posix-relink-after-canonical-unlink");
        let node_id = NodeId::from_legacy_named("node_sifs_relink_after_unlink").expect("node");
        let work = "/services/sandbox/instances/sandbox_relink_after_unlink/work";
        let original = "/services/sandbox/instances/sandbox_relink_after_unlink/work/file.txt";
        let alias = "/services/sandbox/instances/sandbox_relink_after_unlink/work/alias.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir");
        dfs.posix_write_file(
            original,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write original");
        dfs.posix_hardlink(original, alias).expect("hardlink alias");
        dfs.posix_write_file(
            alias,
            b"updated",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write alias");
        dfs.posix_unlink(original).expect("unlink canonical path");

        dfs.posix_hardlink(alias, original)
            .expect("relink original path");

        let original_stat = dfs.posix_stat(original).expect("original stat");
        let alias_stat = dfs.posix_stat(alias).expect("alias stat");
        assert_eq!(original_stat.object_id, alias_stat.object_id);
        assert_eq!(original_stat.link_count, 2);
        assert_eq!(alias_stat.link_count, 2);
        assert_eq!(
            dfs.posix_read_file(original).expect("read original"),
            b"updated"
        );
        assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"updated");

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_read_updates_atime_without_changing_mtime() {
        let root = temp_root("sifs-posix-read-atime");
        let node_id = NodeId::from_legacy_named("node_sifs_read_atime").expect("node");
        let file_path = "/services/sandbox/instances/sandbox_read_atime/work/file.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_read_atime/work")
            .expect("mkdir");
        dfs.posix_write_file(
            file_path,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");
        dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
            .expect("old times");
        let before = dfs.posix_stat(file_path).expect("before stat");

        assert_eq!(
            dfs.posix_read_file_update_access(file_path)
                .expect("read update access"),
            b"content"
        );

        let stat = dfs.posix_stat(file_path).expect("stat");
        assert!(stat.accessed_at_unix_nanos > 12_345);
        assert_eq!(stat.modified_at_unix_nanos, 67_890);
        assert_ne!(
            stat.version, before.version,
            "atime update must advance the DFS metadata version so it persists across distributed reconciliation"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_list_updates_directory_atime_without_changing_mtime() {
        let root = temp_root("sifs-posix-list-atime");
        let node_id = NodeId::from_legacy_named("node_sifs_list_atime").expect("node");
        let dir_path = "/services/sandbox/instances/sandbox_list_atime/work";
        let file_path = "/services/sandbox/instances/sandbox_list_atime/work/file.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(dir_path).expect("mkdir");
        dfs.posix_write_file(
            file_path,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");
        dfs.posix_set_times(dir_path, Some(12_345), Some(67_890))
            .expect("old times");

        let entries = dfs
            .posix_list_dir_update_access(dir_path)
            .expect("list update access");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["file.txt"]
        );

        let stat = dfs.posix_stat(dir_path).expect("stat");
        assert!(stat.accessed_at_unix_nanos > 12_345);
        assert_eq!(stat.modified_at_unix_nanos, 67_890);

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_namespace_mutations_update_parent_directory_mtime() {
        let root = temp_root("sifs-posix-parent-mtime");
        let node_id = NodeId::from_legacy_named("node_sifs_parent_mtime").expect("node");
        let work = "/services/sandbox/instances/sandbox_parent_mtime/work";
        let other = "/services/sandbox/instances/sandbox_parent_mtime/other";
        let file = "/services/sandbox/instances/sandbox_parent_mtime/work/file.txt";
        let alias = "/services/sandbox/instances/sandbox_parent_mtime/work/alias.txt";
        let link = "/services/sandbox/instances/sandbox_parent_mtime/work/link.txt";
        let renamed = "/services/sandbox/instances/sandbox_parent_mtime/other/renamed.txt";
        let dir = "/services/sandbox/instances/sandbox_parent_mtime/work/dir";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir work");
        dfs.posix_mkdir_all(other).expect("mkdir other");

        dfs.posix_set_times(work, Some(12_345), Some(67_890))
            .expect("old work times");
        let before_create = dfs.posix_stat(work).expect("work before create");
        std::thread::sleep(std::time::Duration::from_millis(1));
        dfs.posix_write_file(
            file,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("create file");
        let after_create = dfs.posix_stat(work).expect("work after create");
        assert!(after_create.modified_at_unix_nanos > 67_890);
        assert!(
            after_create.changed_at_unix_nanos > before_create.changed_at_unix_nanos,
            "creating a directory entry must advance parent directory ctime"
        );

        dfs.posix_set_times(work, Some(12_345), Some(67_890))
            .expect("reset work times");
        dfs.posix_write_file(
            file,
            b"updated",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("overwrite file");
        assert_eq!(
            dfs.posix_stat(work)
                .expect("work after overwrite")
                .modified_at_unix_nanos,
            67_890,
            "overwriting existing file content must not change parent directory mtime"
        );

        dfs.posix_hardlink(file, alias).expect("hardlink");
        assert!(
            dfs.posix_stat(work)
                .expect("work after hardlink")
                .modified_at_unix_nanos
                > 67_890
        );

        dfs.posix_set_times(work, Some(12_345), Some(67_890))
            .expect("reset work times");
        dfs.posix_symlink(
            "file.txt",
            link,
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("symlink");
        assert!(
            dfs.posix_stat(work)
                .expect("work after symlink")
                .modified_at_unix_nanos
                > 67_890
        );

        dfs.posix_set_times(work, Some(12_345), Some(67_890))
            .expect("reset work times");
        dfs.posix_mkdir(dir).expect("mkdir child");
        assert!(
            dfs.posix_stat(work)
                .expect("work after mkdir")
                .modified_at_unix_nanos
                > 67_890
        );

        dfs.posix_set_times(work, Some(12_345), Some(67_890))
            .expect("reset work times");
        dfs.posix_rmdir(dir).expect("rmdir child");
        assert!(
            dfs.posix_stat(work)
                .expect("work after rmdir")
                .modified_at_unix_nanos
                > 67_890
        );

        dfs.posix_set_times(work, Some(12_345), Some(67_890))
            .expect("reset work times");
        dfs.posix_unlink(alias).expect("unlink alias");
        assert!(
            dfs.posix_stat(work)
                .expect("work after unlink")
                .modified_at_unix_nanos
                > 67_890
        );

        dfs.posix_set_times(work, Some(12_345), Some(67_890))
            .expect("reset work times");
        dfs.posix_set_times(other, Some(22_222), Some(88_888))
            .expect("old other times");
        dfs.posix_rename(file, renamed).expect("rename across dirs");
        assert!(
            dfs.posix_stat(work)
                .expect("work after rename")
                .modified_at_unix_nanos
                > 67_890
        );
        assert!(
            dfs.posix_stat(other)
                .expect("other after rename")
                .modified_at_unix_nanos
                > 88_888
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_posix_rename_updates_moved_object_ctime_without_changing_mtime() {
        let root = temp_root("sifs-posix-rename-ctime");
        let node_id = NodeId::from_legacy_named("node_sifs_rename_ctime").expect("node");
        let work = "/services/sandbox/instances/sandbox_rename_ctime/work";
        let source = "/services/sandbox/instances/sandbox_rename_ctime/work/source.txt";
        let renamed = "/services/sandbox/instances/sandbox_rename_ctime/work/renamed.txt";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(work).expect("mkdir work");
        dfs.posix_write_file(
            source,
            b"content",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write source");
        dfs.posix_set_times(source, Some(12_345), Some(67_890))
            .expect("old source times");

        let before = dfs.posix_stat(source).expect("before stat");
        dfs.posix_rename(source, renamed).expect("rename");
        let after = dfs.posix_stat(renamed).expect("after stat");

        assert_eq!(after.object_id, before.object_id);
        assert_eq!(after.created_at_unix_nanos, before.created_at_unix_nanos);
        assert_eq!(
            after.modified_at_unix_nanos, before.modified_at_unix_nanos,
            "rename must not change file content mtime"
        );
        assert!(
            after.changed_at_unix_nanos > before.changed_at_unix_nanos,
            "rename must advance ctime because the path metadata changed"
        );
        assert_ne!(
            after.version, before.version,
            "rename metadata update must advance the object metadata version"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_owned_read_is_routed_to_owner_node() {
        let owner = NodeId::from_legacy_named("node_desktop").expect("node");
        let remote = NodeId::from_legacy_named("node_mobile").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_dfs = LocalOwnedDfsService::new(network.add_node(owner.clone()));
        let mut remote_dfs = LocalOwnedDfsService::new(network.add_node(remote.clone()));
        let metadata = file_metadata(owner.clone());
        let object_id = metadata.object_id.clone();

        owner_dfs.add_file_system(file_system(owner.clone()));
        remote_dfs.add_file_system(file_system(owner.clone()));
        owner_dfs
            .put_local_object(metadata.clone(), b"owned bytes".to_vec())
            .expect("seed owner object");
        remote_dfs
            .add_object_metadata(metadata)
            .expect("replicate metadata");

        let read = remote_dfs.read_object(&object_id).expect("read routes");
        let request_id = match read {
            DfsReadOutcome::Routed {
                request_id,
                owner_node_id,
            } => {
                assert_eq!(owner_node_id, owner);
                request_id
            }
            DfsReadOutcome::Local { .. } => panic!("remote read should route to owner"),
        };

        let owner_events = owner_dfs.process_incoming().expect("owner handles read");
        assert!(matches!(
            &owner_events[..],
            [DfsServiceEvent::ReadRequestHandled {
                result: DfsReadResult::Found(_),
                ..
            }]
        ));

        let remote_events = remote_dfs
            .process_incoming()
            .expect("remote receives read response");
        assert!(matches!(
            &remote_events[..],
            [DfsServiceEvent::ReadResponseReceived {
                request_id: received_request_id,
                result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                ..
            }] if received_request_id == &request_id && bytes == b"owned bytes"
        ));
    }

    #[test]
    fn local_owned_write_is_routed_to_owner_node_and_committed() {
        let owner = NodeId::from_legacy_named("node_desktop").expect("node");
        let remote = NodeId::from_legacy_named("node_mobile").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_dfs = LocalOwnedDfsService::new(network.add_node(owner.clone()));
        let mut remote_dfs = LocalOwnedDfsService::new(network.add_node(remote.clone()));
        let metadata = file_metadata(owner.clone());
        let object_id = metadata.object_id.clone();
        let file_system_id = metadata.file_system_id.clone();

        owner_dfs.add_file_system(file_system(owner.clone()));
        remote_dfs.add_file_system(file_system(owner.clone()));
        owner_dfs
            .put_local_object(metadata.clone(), b"old bytes".to_vec())
            .expect("seed owner object");
        remote_dfs
            .add_object_metadata(metadata)
            .expect("replicate metadata");

        let target_version = DfsVersion::from_legacy_named("version_2").expect("version");
        let write = remote_dfs
            .write_object(
                DfsWriteRequest {
                    file_system_id,
                    object_id: object_id.clone(),
                    target_version: target_version.clone(),
                    requesting_node_id: remote.clone(),
                },
                b"updated bytes".to_vec(),
            )
            .expect("write routes");
        let request_id = match write {
            DfsWriteOutcome::Routed {
                request_id,
                owner_node_id,
            } => {
                assert_eq!(owner_node_id, owner);
                request_id
            }
            DfsWriteOutcome::Local { .. } => panic!("remote write should route to owner"),
        };

        let owner_events = owner_dfs.process_incoming().expect("owner handles write");
        assert!(matches!(
            &owner_events[..],
            [DfsServiceEvent::WriteRequestHandled {
                result: DfsWriteResult::Committed { .. },
                ..
            }]
        ));

        let owner_read = owner_dfs
            .read_object(&object_id)
            .expect("owner reads local");
        assert!(matches!(
            owner_read,
            DfsReadOutcome::Local {
                data: Some(DfsReadData { bytes, version, .. }),
                ..
            } if bytes == b"updated bytes" && version == target_version
        ));

        let remote_events = remote_dfs
            .process_incoming()
            .expect("remote receives write response");
        assert!(matches!(
            &remote_events[..],
            [DfsServiceEvent::WriteResponseReceived {
                request_id: received_request_id,
                result: DfsWriteResult::Committed { receipt, .. },
                ..
            }] if received_request_id == &request_id
                && receipt.committed_version == target_version
                && receipt.owner_node_id == owner
        ));
        assert_eq!(
            remote_dfs
                .metadata()
                .object(&object_id)
                .expect("metadata updated")
                .version,
            target_version
        );
    }

    #[test]
    fn dfs_policy_violation_event_is_structured_for_supervision() {
        let owner = NodeId::from_legacy_named("node_desktop").expect("node");
        let remote = NodeId::from_legacy_named("node_mobile").expect("node");
        let event = DfsPolicyViolation {
            requesting_node_id: remote,
            executing_node_id: owner.clone(),
            file_system_id: Some(DfsFileSystemId::from_legacy_named("sifs").expect("filesystem")),
            object_id: Some(DfsObjectId::from_legacy_named("obj_file").expect("object")),
            reason: "write attempted against non-owner".to_string(),
        }
        .event();

        assert_eq!(event.kind.as_str(), "dfs.policy.violation");
        assert_eq!(event.target_node_id, Some(owner));
        assert_eq!(
            event.payload["file_system_id"],
            DfsFileSystemId::from_legacy_named("sifs")
                .expect("filesystem")
                .as_str()
        );
    }

    #[test]
    fn explicit_dfs_roots_are_preserved() {
        let root = PathBuf::from("/tmp/sinomo-dfs-explicit-root");

        assert_eq!(
            resolve_dfs_metadata_root(Some(root.join("metadata"))).expect("metadata root"),
            root.join("metadata")
        );
        assert_eq!(
            resolve_dfs_pin_root(Some(root.join("pins"))).expect("pin root"),
            root.join("pins")
        );
    }
}
