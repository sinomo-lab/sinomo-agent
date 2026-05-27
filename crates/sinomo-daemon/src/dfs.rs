use crate::{
    DaemonError, DaemonPaths, DaemonRunOptions, put_cluster_kv_control_record,
    send_dfs_metadata_journal_batch_to_endpoint, send_dfs_metadata_snapshot_install_to_endpoint,
    send_dfs_protocol_frame_to_endpoint,
};
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord,
    ClusterMemberRole, ClusterTransportKind, DfsObjectPolicy,
};
use sinomo_dfs::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
    DfsExposeStore, DfsFileSystemConfig, DfsMetadataBackend, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsMetadataSnapshotInstallCommand,
    DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult, DfsNodeAdvertisement,
    DfsObjectVersionRecord, DfsOperatorFileSystemView, DfsOperatorInspector, DfsOperatorObjectView,
    DfsOperatorRepairView, DfsOperatorStatus, DfsProtocolFrameCommand, DfsProtocolFrameResponse,
    DfsReplicationPolicy, DfsSubtreePolicy, DfsWriteOptions, LocalContentStore,
    LocalJsonMetadataBackend, LocalShardedMetadataBackend, NetworkedDfsService,
    content_service_key, content_services_prefix, default_dfs_metadata_shards_root,
    distributed_sifs, distributed_sifs_bootstrap_objects, fallback_dfs_node_id,
    file_system_config_key, metadata_service_key, metadata_shard_key, sifs_node_files_path,
};
use sinomo_kv::{KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};
use sinomo_local::SinomoDataDir;
use sinomo_network::{ClusterPaths, InboundFrame, NetworkError, NetworkFrame, NetworkTransport};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

const DFS_METADATA_REPLICATION_TIMEOUT: Duration = Duration::from_secs(3);
const DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES: usize =
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT;
const DFS_METADATA_REPLICATION_MAX_BATCH_BYTES: usize = DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT;
const DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize = 64;
const DFS_METADATA_REPLICATION_MAX_ENDPOINTS_PER_TARGET: usize = 3;
const DFS_METADATA_REPLICATION_BATCH_TARGET_TIMEOUT: Duration = Duration::from_secs(5);
const DFS_PROTOCOL_FRAME_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug)]
struct DaemonDfsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
}

impl DaemonDfsControlPlaneKvClient {
    fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
        }
    }

    fn network_error(&self, source: NetworkError) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }

    fn read_only_error(&self) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_snapshot.display().to_string(),
            detail: "daemon DFS control-plane client is read-only; update cluster DFS control records through cluster KV".to_string(),
        }
    }
}

impl sinomo_dfs::DfsControlPlaneKvClient for DaemonDfsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_network::read_cluster_kv_entry(&self.cluster_paths, key)
            .map_err(|source| self.network_error(source))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_network::read_cluster_kv_prefix_entries(&self.cluster_paths, prefix)
            .map_err(|source| self.network_error(source))
    }

    fn put(&mut self, _key: String, _value: Vec<u8>) -> Result<KvRevision, KvError> {
        Err(self.read_only_error())
    }

    fn txn(&mut self, _txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        Err(self.read_only_error())
    }
}

pub(crate) fn with_daemon_dfs_control_plane<T: NetworkTransport>(
    service: NetworkedDfsService<T>,
    cluster_paths: &ClusterPaths,
) -> NetworkedDfsService<T> {
    if cluster_paths.kv_snapshot.exists() {
        service.with_control_plane(DaemonDfsControlPlaneKvClient::new(cluster_paths))
    } else {
        service
    }
}

type DfsProtocolFrameSender = Arc<
    dyn Fn(
            &ClusterEndpointRecord,
            &DfsProtocolFrameCommand,
            Duration,
        ) -> Result<DfsProtocolFrameResponse, DaemonError>
        + Send
        + Sync,
>;

type DfsMetadataJournalBatchSender = Arc<
    dyn Fn(
            &ClusterEndpointRecord,
            &DfsMetadataJournalBatchCommand,
            Duration,
        ) -> Result<DfsMetadataJournalBatchResponse, DaemonError>
        + Send
        + Sync,
>;

type DfsMetadataSnapshotInstallSender = Arc<
    dyn Fn(
            &ClusterEndpointRecord,
            &DfsMetadataSnapshotInstallCommand,
            Duration,
        ) -> Result<DfsMetadataSnapshotInstallResponse, DaemonError>
        + Send
        + Sync,
>;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DaemonDfsMetadataReplicationReport {
    pub outcomes: Vec<DaemonDfsMetadataReplicationOutcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DaemonDfsMetadataReplicationOutcome {
    MissingShard {
        file_system_id: sinomo_dfs::DfsFileSystemId,
        shard_id: DfsMetadataShardId,
    },
    NotPrimary {
        file_system_id: sinomo_dfs::DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        local_node_id: sinomo_node::NodeId,
        primary_node_id: sinomo_node::NodeId,
        epoch: u64,
    },
    Applied {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        entries: usize,
        next_after_sequence: u64,
    },
    NoChanges {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    MissingEndpoint {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    Rejected {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    SnapshotInstalled {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
        archived_existing: bool,
    },
    Failed {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    BacklogRemaining {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
        max_batches: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DaemonDfsSmokeReport {
    pub path: String,
    pub file_system_id: sinomo_dfs::DfsFileSystemId,
    pub object_id: sinomo_dfs::DfsObjectId,
    pub version: sinomo_dfs::DfsVersion,
    pub content_hash: Option<String>,
    pub size_bytes: Option<u64>,
    pub requested_replica_node_ids: Vec<sinomo_node::NodeId>,
    pub committed_replica_node_ids: Vec<sinomo_node::NodeId>,
    pub pending_repair_count: usize,
    pub metadata_replication: DaemonDfsMetadataReplicationReport,
    pub attempts: Vec<DaemonDfsSmokeAttempt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DaemonDfsSmokeAttempt {
    pub replica_node_ids: Vec<sinomo_node::NodeId>,
    pub path: String,
    pub committed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DaemonDfsInspectionRequest {
    Status,
    FileSystem {
        file_system_id: sinomo_dfs::DfsFileSystemId,
    },
    Path {
        file_system_id: sinomo_dfs::DfsFileSystemId,
        path: String,
    },
    Object {
        object_id: sinomo_dfs::DfsObjectId,
    },
    Conflicts {
        file_system_id: Option<sinomo_dfs::DfsFileSystemId>,
        object_id: Option<sinomo_dfs::DfsObjectId>,
    },
    Repair {
        file_system_id: Option<sinomo_dfs::DfsFileSystemId>,
        object_id: Option<sinomo_dfs::DfsObjectId>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DaemonDfsInspectionResponse {
    Status {
        status: DfsOperatorStatus,
    },
    FileSystem {
        file_system: Option<DfsOperatorFileSystemView>,
    },
    Path {
        object: Option<DfsOperatorObjectView>,
    },
    Object {
        object: Option<DfsOperatorObjectView>,
    },
    Conflicts {
        conflicts: Vec<sinomo_dfs::DfsConflictRecord>,
    },
    Repair {
        repairs: Vec<DfsOperatorRepairView>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct LegacyDaemonDfsMetadataReplicationCursor {
    target_node_id: sinomo_node::NodeId,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
}

pub fn ensure_default_local_dfs_expose(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let node_id = if cluster_paths.state.exists() {
        sinomo_network::read_cluster_state_without_kv(&cluster_paths)
            .map(|state| state.local_node_id)
            .unwrap_or_else(|_| fallback_dfs_node_id())
    } else {
        fallback_dfs_node_id()
    };
    DfsExposeStore::new(data_dir.dfs_expose_config_file())
        .ensure_default(&data_dir.root(), node_id)?;
    Ok(())
}

pub fn handle_daemon_dfs_expose_config(paths: &DaemonPaths) -> Result<String, DaemonError> {
    ensure_default_local_dfs_expose(paths)?;
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let config_path = data_dir.dfs_expose_config_file();
    let config = DfsExposeStore::new(&config_path).load()?;
    serde_json::to_string_pretty(&config).map_err(|source| DaemonError::Json {
        path: config_path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn handle_daemon_dfs_status(
    paths: &DaemonPaths,
) -> Result<sinomo_dfs::DfsOperatorStatus, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    inspect_daemon_dfs_status(&data_dir, &cluster_paths)
}

pub fn handle_daemon_dfs_inspection(
    paths: &DaemonPaths,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let metadata = open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)?;
    let inspector = DfsOperatorInspector::new(metadata.as_ref());
    let response = match request {
        DaemonDfsInspectionRequest::Status => DaemonDfsInspectionResponse::Status {
            status: inspector.status()?,
        },
        DaemonDfsInspectionRequest::FileSystem { file_system_id } => {
            DaemonDfsInspectionResponse::FileSystem {
                file_system: inspector.file_system(&file_system_id)?,
            }
        }
        DaemonDfsInspectionRequest::Path {
            file_system_id,
            path,
        } => DaemonDfsInspectionResponse::Path {
            object: inspector.object_by_path(&file_system_id, &path)?,
        },
        DaemonDfsInspectionRequest::Object { object_id } => DaemonDfsInspectionResponse::Object {
            object: inspector.object(&object_id)?,
        },
        DaemonDfsInspectionRequest::Conflicts {
            file_system_id,
            object_id,
        } => DaemonDfsInspectionResponse::Conflicts {
            conflicts: inspector.conflicts(file_system_id.as_ref(), object_id.as_ref())?,
        },
        DaemonDfsInspectionRequest::Repair {
            file_system_id,
            object_id,
        } => DaemonDfsInspectionResponse::Repair {
            repairs: inspector.repairs(file_system_id.as_ref(), object_id.as_ref())?,
        },
    };
    Ok(response)
}

fn inspect_daemon_dfs_status(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<sinomo_dfs::DfsOperatorStatus, DaemonError> {
    let metadata = open_daemon_dfs_metadata_backend(data_dir, cluster_paths)?;
    let mut status = DfsOperatorInspector::new(metadata.as_ref())
        .status()
        .map_err(DaemonError::from)?;
    if let Ok(state) = sinomo_network::read_cluster_state_without_kv(cluster_paths) {
        for shard in &mut status.metadata_shards {
            if shard.primary_node_id.as_ref() == Some(&state.local_node_id) {
                shard.replay_cursors.clear();
            } else {
                shard.replica_lag.clear();
            }
        }
    }
    Ok(status)
}

pub(crate) fn open_daemon_dfs_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Box<dyn DfsMetadataBackend>, DaemonError> {
    if cluster_paths.state.exists()
        && let Some(metadata) = open_daemon_sharded_dfs_metadata_backend(data_dir, cluster_paths)?
    {
        return Ok(Box::new(metadata));
    }
    sinomo_dfs::open_root_local_metadata_backend(sinomo_dfs::default_dfs_metadata_root(data_dir))
        .map(|backend| Box::new(backend) as Box<dyn DfsMetadataBackend>)
        .map_err(Into::into)
}

pub(crate) fn open_daemon_sharded_dfs_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Option<LocalShardedMetadataBackend>, DaemonError> {
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let mut shards = sinomo_network::read_cluster_kv_prefix_entries(
        cluster_paths,
        &format!("{}/", sinomo_dfs::metadata_shards_prefix(&file_system_id)),
    )?
    .into_iter()
    .map(|entry| entry.decode::<DfsMetadataShardConfig>())
    .collect::<Result<Vec<_>, _>>()?;
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok(None);
    }
    sinomo_dfs::open_local_sharded_metadata_backend(
        default_dfs_metadata_shards_root(data_dir),
        shards,
    )
    .map(Some)
    .map_err(Into::into)
}

pub fn handle_daemon_dfs_metadata_journal_batch(
    paths: &DaemonPaths,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let mut backend = LocalJsonMetadataBackend::new(
        command.batch.shard_id.clone(),
        data_dir
            .dfs_dir()
            .join("metadata")
            .join(path_safe_segment(&command.batch.shard_id.0)),
    )?;
    let shard_id = command.batch.shard_id.clone();
    let next_after_sequence = command.batch.next_after_sequence;
    match backend.apply_journal_batch(&command.batch) {
        Ok(applied_entries) => Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries,
            result: DfsMetadataJournalBatchResult::Applied,
        }),
        Err(error) => Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries: 0,
            result: DfsMetadataJournalBatchResult::Rejected {
                reason: error.to_string(),
            },
        }),
    }
}

pub fn handle_daemon_dfs_metadata_snapshot_install(
    paths: &DaemonPaths,
    command: DfsMetadataSnapshotInstallCommand,
) -> Result<DfsMetadataSnapshotInstallResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let shard = sinomo_network::read_cluster_kv_prefix_values::<DfsMetadataShardConfig>(
        &cluster_paths,
        &format!(
            "{}/",
            sinomo_dfs::metadata_shards_prefix(&sinomo_dfs::sifs_file_system_id())
        ),
    )?
    .into_iter()
    .find(|shard| shard.shard_id == command.shard_id.0.as_str());
    let Some(shard) = shard else {
        return Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: "metadata snapshot install has no local shard config".to_string(),
            },
        });
    };
    if shard.primary_node_id != command.source_node_id {
        return Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: format!(
                    "metadata snapshot source {} is not shard {} primary {}",
                    command.source_node_id, shard.shard_id, shard.primary_node_id
                ),
            },
        });
    }
    if shard.primary_node_id == state.local_node_id {
        return Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: "metadata primary must not install a replica snapshot".to_string(),
            },
        });
    }
    if !shard.replica_node_ids.contains(&state.local_node_id) {
        return Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: format!(
                    "local node {} is not configured as a replica for metadata shard {}",
                    state.local_node_id, shard.shard_id
                ),
            },
        });
    }
    let mut backend = LocalJsonMetadataBackend::new(
        command.shard_id.clone(),
        data_dir
            .dfs_dir()
            .join("metadata")
            .join(path_safe_segment(&command.shard_id.0)),
    )?;
    match backend.replace_with_replicated_snapshot(
        command.shard_id.clone(),
        command.next_after_sequence,
        command.snapshot,
    ) {
        Ok(archived_existing) => Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Installed { archived_existing },
        }),
        Err(error) => Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: error.to_string(),
            },
        }),
    }
}

pub fn handle_daemon_dfs_protocol_frame(
    paths: &DaemonPaths,
    command: DfsProtocolFrameCommand,
) -> Result<DfsProtocolFrameResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let local_node_id = local_dfs_protocol_node_id(&data_dir)?;
    if command.frame.target_node_id != local_node_id {
        return Err(DaemonError::Parse {
            detail: format!(
                "dfs protocol frame target {} does not match local node {}",
                command.frame.target_node_id, local_node_id
            ),
        });
    }
    let cluster_paths = ClusterPaths::new(&data_dir);
    let metadata_backend = open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)?;
    let content_store = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let transport = DaemonDfsProtocolTransport::new(local_node_id, command.frame);
    let mut service = with_daemon_dfs_control_plane(
        NetworkedDfsService::new(transport, content_store),
        &cluster_paths,
    )
    .with_boxed_metadata_backend(metadata_backend)?;
    service.process_incoming()?;
    let outbound_frames = service.into_transport().outbound_frames;
    Ok(DfsProtocolFrameResponse {
        request_id: command.request_id,
        outbound_frames,
    })
}

pub fn ensure_default_distributed_dfs(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let state = if publish_cluster_control {
        sinomo_network::read_cluster_state(&cluster_paths)?
    } else {
        sinomo_network::read_cluster_state_without_kv(&cluster_paths)?
    };
    let file_system = default_cluster_sifs(&state);
    let local_has_primary_metadata_shards = if publish_cluster_control {
        put_cluster_kv_control_record(
            &cluster_paths,
            &state.local_node_id,
            file_system_config_key(&file_system.file_system_id),
            &DfsFileSystemConfig::from(&file_system),
        )?;
        let metadata_shards =
            default_cluster_metadata_shards(&data_dir, &cluster_paths, &state, &file_system)?;
        let local_has_primary_metadata_shards = metadata_shards
            .iter()
            .any(|shard| shard.primary_node_id == state.local_node_id);
        let publishable_shards = metadata_shards.iter().filter(|shard| {
            local_has_primary_metadata_shards
                || matches!(shard.assignment, DfsMetadataShardAssignment::Root)
        });
        for shard in publishable_shards {
            put_cluster_kv_control_record(
                &cluster_paths,
                &state.local_node_id,
                metadata_shard_key(&file_system.file_system_id, &shard.shard_id),
                shard,
            )?;
        }
        if local_has_primary_metadata_shards {
            materialize_default_cluster_sifs_metadata_with_shards(
                &data_dir,
                &state,
                &file_system,
                &metadata_shards,
            )?;
        }
        publish_dfs_service_advertisements_for_cluster(&cluster_paths, &state, options)?;
        local_has_primary_metadata_shards
    } else {
        default_cluster_sifs_owner(&state) == state.local_node_id
    };
    if local_has_primary_metadata_shards && !publish_cluster_control {
        materialize_default_cluster_sifs_metadata(&data_dir, &cluster_paths, &state, &file_system)?;
    }
    Ok(())
}

fn materialize_default_cluster_sifs_metadata(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    file_system: &sinomo_dfs::DfsFileSystem,
) -> Result<(), DaemonError> {
    let mut backend = open_daemon_dfs_metadata_backend(data_dir, cluster_paths)?;
    materialize_default_cluster_sifs_metadata_in_backend(backend.as_mut(), state, file_system)
}

fn materialize_default_cluster_sifs_metadata_with_shards(
    data_dir: &SinomoDataDir,
    state: &ClusterBootstrapState,
    file_system: &sinomo_dfs::DfsFileSystem,
    shards: &[DfsMetadataShardConfig],
) -> Result<(), DaemonError> {
    let mut backend = sinomo_dfs::open_local_sharded_metadata_backend(
        default_dfs_metadata_shards_root(data_dir),
        shards.to_vec(),
    )?;
    backend.ensure_file_system_on_all_shards(file_system)?;
    materialize_default_cluster_sifs_metadata_in_backend(&mut backend, state, file_system)?;
    backend.migrate_objects_to_configured_shards(&file_system.file_system_id, usize::MAX)?;
    Ok(())
}

fn materialize_default_cluster_sifs_metadata_in_backend(
    backend: &mut dyn DfsMetadataBackend,
    state: &ClusterBootstrapState,
    file_system: &sinomo_dfs::DfsFileSystem,
) -> Result<(), DaemonError> {
    if backend.get_file_system(&file_system.file_system_id)? != Some(file_system.clone()) {
        backend.put_file_system(file_system)?;
    }
    for object in default_cluster_sifs_bootstrap_objects(state) {
        if backend
            .get_object_by_path(&object.file_system_id, object.path.trim_matches('/'))?
            .is_none()
        {
            backend.put_object(&object)?;
            backend.put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at: OffsetDateTime::now_utc(),
                visible: true,
            })?;
        }
    }
    Ok(())
}

pub fn run_daemon_dfs_maintenance_tick(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let metadata = open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)?;
    let endpoints = sinomo_network::cluster_dfs_protocol_targets(&cluster_paths, &state);
    let mut service = with_daemon_dfs_control_plane(
        NetworkedDfsService::new(
            DaemonDfsEndpointTransport::new(state.local_node_id, endpoints),
            content,
        ),
        &cluster_paths,
    )
    .with_boxed_metadata_backend(metadata)?;
    let scheduled_repairs = service.retry_pending_repairs()?;
    if !scheduled_repairs.is_empty() {
        service.process_incoming()?;
    }
    log_dfs_metadata_replication_report(publish_default_dfs_metadata_journal(paths)?);
    Ok(())
}

pub fn log_dfs_metadata_replication_report(report: DaemonDfsMetadataReplicationReport) {
    for outcome in report.outcomes {
        match outcome {
            DaemonDfsMetadataReplicationOutcome::Applied {
                target_node_id,
                entries,
                ..
            } => {
                eprintln!(
                    "sinomo daemon replicated {entries} DFS metadata journal entries to {target_node_id}"
                );
            }
            DaemonDfsMetadataReplicationOutcome::SnapshotInstalled {
                target_node_id,
                next_after_sequence,
                ..
            } => {
                eprintln!(
                    "sinomo daemon repaired DFS metadata replica {target_node_id} with snapshot through {next_after_sequence}"
                );
            }
            DaemonDfsMetadataReplicationOutcome::Rejected {
                target_node_id,
                reason,
                ..
            }
            | DaemonDfsMetadataReplicationOutcome::Failed {
                target_node_id,
                reason,
                ..
            } => {
                eprintln!(
                    "sinomo daemon DFS metadata replication to {target_node_id} failed: {reason}"
                );
            }
            DaemonDfsMetadataReplicationOutcome::BacklogRemaining {
                target_node_id,
                max_batches,
                ..
            } => {
                eprintln!(
                    "sinomo daemon DFS metadata replication to {target_node_id} still has backlog after {max_batches} batches"
                );
            }
            DaemonDfsMetadataReplicationOutcome::NoChanges { .. }
            | DaemonDfsMetadataReplicationOutcome::MissingEndpoint { .. }
            | DaemonDfsMetadataReplicationOutcome::MissingShard { .. }
            | DaemonDfsMetadataReplicationOutcome::NotPrimary { .. } => {}
        }
    }
}

fn publish_dfs_service_advertisements_for_cluster(
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: &DaemonRunOptions,
) -> Result<(), DaemonError> {
    for member in dfs_service_members(state) {
        let endpoint = dfs_service_endpoint_for_member(state, member, options);
        let metadata = stable_dfs_advertisement(
            cluster_paths,
            metadata_service_key(&member.node_id),
            DfsNodeAdvertisement::metadata(member.node_id.clone(), endpoint.clone()),
        )?;
        let content = stable_dfs_advertisement(
            cluster_paths,
            content_service_key(&member.node_id),
            DfsNodeAdvertisement::content(member.node_id.clone(), endpoint),
        )?;
        put_cluster_kv_control_record(
            cluster_paths,
            &state.local_node_id,
            metadata_service_key(&member.node_id),
            &metadata,
        )?;
        put_cluster_kv_control_record(
            cluster_paths,
            &state.local_node_id,
            content_service_key(&member.node_id),
            &content,
        )?;
    }
    Ok(())
}

fn stable_dfs_advertisement(
    cluster_paths: &ClusterPaths,
    key: String,
    desired: DfsNodeAdvertisement,
) -> Result<DfsNodeAdvertisement, DaemonError> {
    let Some(current) =
        sinomo_network::read_cluster_kv_value::<DfsNodeAdvertisement>(cluster_paths, &key)?
    else {
        return Ok(desired);
    };
    if current.node_id == desired.node_id
        && current.endpoint == desired.endpoint
        && current.accepts_metadata == desired.accepts_metadata
        && current.accepts_content == desired.accepts_content
        && current.capacity_weight == desired.capacity_weight
        && current.zone == desired.zone
    {
        return Ok(current);
    }
    Ok(desired)
}

fn default_cluster_sifs(state: &ClusterBootstrapState) -> sinomo_dfs::DfsFileSystem {
    let owner_node_id = default_cluster_sifs_owner(state);
    let mut file_system = distributed_sifs(owner_node_id.clone());
    file_system.owner_node_id = owner_node_id;
    file_system.subtree_policies = default_cluster_sifs_subtree_policies(state);
    file_system
}

fn default_cluster_sifs_owner(state: &ClusterBootstrapState) -> sinomo_node::NodeId {
    stable_dfs_service_node_ids(state)
        .into_iter()
        .next()
        .unwrap_or_else(|| state.local_node_id.clone())
}

fn default_cluster_sifs_subtree_policies(state: &ClusterBootstrapState) -> Vec<DfsSubtreePolicy> {
    let mut policies = stable_dfs_service_node_ids(state)
        .into_iter()
        .map(|node_id| DfsSubtreePolicy {
            path_prefix: sifs_node_files_path(&node_id),
            policy: DfsObjectPolicy::LocalOwned,
        })
        .collect::<Vec<_>>();
    if policies.is_empty() {
        policies.push(DfsSubtreePolicy {
            path_prefix: sifs_node_files_path(&state.local_node_id),
            policy: DfsObjectPolicy::LocalOwned,
        });
    }
    policies.push(DfsSubtreePolicy {
        path_prefix: sinomo_dfs::SIFS_USERS_PATH.to_string(),
        policy: DfsObjectPolicy::ReplicatedEventual,
    });
    policies.push(DfsSubtreePolicy {
        path_prefix: sinomo_dfs::SIFS_SERVICES_PATH.to_string(),
        policy: DfsObjectPolicy::ReplicatedEventual,
    });
    policies
}

fn default_cluster_sifs_bootstrap_objects(
    state: &ClusterBootstrapState,
) -> Vec<sinomo_dfs::DfsObjectMetadata> {
    let mut seen = std::collections::BTreeSet::new();
    let mut objects = Vec::new();
    let mut node_ids = stable_dfs_service_node_ids(state);
    if node_ids.is_empty() {
        node_ids.push(state.local_node_id.clone());
    }
    for node_id in node_ids {
        for object in distributed_sifs_bootstrap_objects(node_id) {
            if seen.insert(object.path.clone()) {
                objects.push(object);
            }
        }
    }
    objects
}

fn default_cluster_metadata_shards(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    file_system: &sinomo_dfs::DfsFileSystem,
) -> Result<Vec<DfsMetadataShardConfig>, DaemonError> {
    Ok(vec![
        default_cluster_metadata_shard(
            data_dir,
            cluster_paths,
            state,
            file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )?,
        default_cluster_metadata_shard(
            data_dir,
            cluster_paths,
            state,
            file_system,
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: sinomo_dfs::SIFS_SERVICES_PATH.to_string(),
            },
            1,
        )?,
        default_cluster_metadata_shard(
            data_dir,
            cluster_paths,
            state,
            file_system,
            "users",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: sinomo_dfs::SIFS_USERS_PATH.to_string(),
            },
            2,
        )?,
    ])
}

#[cfg(test)]
fn default_cluster_root_metadata_shard(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    file_system: &sinomo_dfs::DfsFileSystem,
) -> Result<DfsMetadataShardConfig, DaemonError> {
    default_cluster_metadata_shard(
        data_dir,
        cluster_paths,
        state,
        file_system,
        "root",
        DfsMetadataShardAssignment::Root,
        0,
    )
}

fn default_cluster_metadata_shard(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    file_system: &sinomo_dfs::DfsFileSystem,
    shard_id: &str,
    assignment: DfsMetadataShardAssignment,
    primary_offset: usize,
) -> Result<DfsMetadataShardConfig, DaemonError> {
    let service_node_ids = stable_dfs_service_node_ids(state);
    let bootstrap_primary_node_id = default_cluster_sifs_owner(state);
    let target_primary_node_id = default_metadata_shard_primary_node_id(
        &service_node_ids,
        &bootstrap_primary_node_id,
        primary_offset,
    );
    let target_replica_node_ids =
        metadata_shard_replica_node_ids(&service_node_ids, &target_primary_node_id, 2);
    let key = metadata_shard_key(&file_system.file_system_id, shard_id);
    let mut desired = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.to_string(),
        primary_node_id: target_primary_node_id,
        replica_node_ids: target_replica_node_ids,
        epoch: 1,
        assignment,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let Some(current) =
        sinomo_network::read_cluster_kv_value::<DfsMetadataShardConfig>(cluster_paths, &key)?
    else {
        desired.primary_node_id = bootstrap_primary_node_id.clone();
        desired.replica_node_ids =
            metadata_shard_replica_node_ids(&service_node_ids, &bootstrap_primary_node_id, 2);
        return Ok(desired);
    };
    if current.primary_node_id != desired.primary_node_id
        && !default_metadata_shard_primary_transfer_ready(
            data_dir,
            state,
            &current,
            &desired.primary_node_id,
        )?
    {
        return Ok(current);
    }
    if metadata_shard_layout_matches(&current, &desired) {
        return Ok(current);
    }
    desired.epoch = current.epoch.saturating_add(1);
    Ok(desired)
}

fn default_metadata_shard_primary_node_id(
    service_node_ids: &[sinomo_node::NodeId],
    fallback_node_id: &sinomo_node::NodeId,
    primary_offset: usize,
) -> sinomo_node::NodeId {
    if service_node_ids.is_empty() {
        return fallback_node_id.clone();
    }
    service_node_ids[primary_offset % service_node_ids.len()].clone()
}

fn metadata_shard_replica_node_ids(
    service_node_ids: &[sinomo_node::NodeId],
    primary_node_id: &sinomo_node::NodeId,
    max_replicas: usize,
) -> Vec<sinomo_node::NodeId> {
    if service_node_ids.len() <= 1 || max_replicas == 0 {
        return Vec::new();
    }
    let primary_index = service_node_ids
        .iter()
        .position(|node_id| node_id == primary_node_id)
        .unwrap_or(0);
    (1..service_node_ids.len())
        .map(|offset| service_node_ids[(primary_index + offset) % service_node_ids.len()].clone())
        .take(max_replicas)
        .collect()
}

fn default_metadata_shard_primary_transfer_ready(
    data_dir: &SinomoDataDir,
    state: &ClusterBootstrapState,
    current: &DfsMetadataShardConfig,
    target_primary_node_id: &sinomo_node::NodeId,
) -> Result<bool, DaemonError> {
    if current.primary_node_id == *target_primary_node_id {
        return Ok(true);
    }
    if current.primary_node_id != state.local_node_id {
        return Ok(false);
    }
    if !current
        .replica_node_ids
        .iter()
        .any(|node_id| node_id == target_primary_node_id)
    {
        return Ok(false);
    }
    let shard_id = DfsMetadataShardId::new(current.shard_id.clone());
    let backend = LocalJsonMetadataBackend::new(
        shard_id.clone(),
        data_dir
            .dfs_dir()
            .join("metadata")
            .join(path_safe_segment(&current.shard_id)),
    )?;
    let journal_last_sequence = backend
        .metadata_shard_statuses()?
        .into_iter()
        .find(|status| status.shard_id == shard_id)
        .map(|status| status.journal_last_sequence)
        .unwrap_or(0);
    let target_replayed_through =
        backend.metadata_replication_cursor(target_primary_node_id, &shard_id)?;
    Ok(target_replayed_through >= journal_last_sequence)
}

fn metadata_shard_layout_matches(
    left: &DfsMetadataShardConfig,
    right: &DfsMetadataShardConfig,
) -> bool {
    left.file_system_id == right.file_system_id
        && left.shard_id == right.shard_id
        && left.primary_node_id == right.primary_node_id
        && left.replica_node_ids == right.replica_node_ids
        && left.assignment == right.assignment
        && left.default_policy == right.default_policy
}

fn stable_dfs_service_node_ids(state: &ClusterBootstrapState) -> Vec<sinomo_node::NodeId> {
    let mut node_ids = dfs_service_members(state)
        .into_iter()
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    node_ids.sort();
    node_ids.dedup();
    node_ids
}

fn dfs_service_members(state: &ClusterBootstrapState) -> Vec<&ClusterMemberRecord> {
    state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized() && !matches!(member.role, ClusterMemberRole::NonVoting)
        })
        .collect()
}

fn dfs_service_endpoint_for_member(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    options: &DaemonRunOptions,
) -> String {
    if member.node_id == state.local_node_id {
        return format!("loopback://{}:{}", options.host, options.port);
    }
    member
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
        .min_by_key(|endpoint| dfs_service_endpoint_rank(endpoint))
        .or_else(|| member.endpoints.first())
        .map(|endpoint| endpoint.address.clone())
        .unwrap_or_else(|| format!("cluster-member://{}", member.node_id))
}

fn dfs_service_endpoint_rank(endpoint: &ClusterEndpointRecord) -> (u8, u16, String) {
    let privacy_rank = match endpoint.privacy {
        ClusterEndpointPrivacy::InternetDirect => 0,
        ClusterEndpointPrivacy::Relayed => 1,
        ClusterEndpointPrivacy::Lan => 2,
        ClusterEndpointPrivacy::LocalOnly => 3,
    };
    (privacy_rank, endpoint.priority, endpoint.address.clone())
}

#[derive(Debug)]
struct DaemonDfsProtocolTransport {
    local_node_id: sinomo_node::NodeId,
    inbound_frames: Vec<InboundFrame>,
    outbound_frames: Vec<NetworkFrame>,
}

impl DaemonDfsProtocolTransport {
    fn new(local_node_id: sinomo_node::NodeId, inbound_frame: NetworkFrame) -> Self {
        Self {
            local_node_id: local_node_id.clone(),
            inbound_frames: vec![InboundFrame {
                received_by: local_node_id,
                frame: inbound_frame,
            }],
            outbound_frames: Vec::new(),
        }
    }
}

impl NetworkTransport for DaemonDfsProtocolTransport {
    fn local_node_id(&self) -> &sinomo_node::NodeId {
        &self.local_node_id
    }

    fn send(
        &mut self,
        _target_node_id: &sinomo_node::NodeId,
        frame: NetworkFrame,
    ) -> Result<(), NetworkError> {
        self.outbound_frames.push(frame);
        Ok(())
    }

    fn drain_incoming(&mut self) -> Result<Vec<InboundFrame>, NetworkError> {
        Ok(std::mem::take(&mut self.inbound_frames))
    }
}

#[derive(Clone)]
pub struct DaemonDfsEndpointTransport {
    local_node_id: sinomo_node::NodeId,
    endpoints_by_node_id: BTreeMap<sinomo_node::NodeId, Vec<ClusterEndpointRecord>>,
    incoming_frames: VecDeque<InboundFrame>,
    timeout: Duration,
    sender: DfsProtocolFrameSender,
}

impl std::fmt::Debug for DaemonDfsEndpointTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonDfsEndpointTransport")
            .field("local_node_id", &self.local_node_id)
            .field("endpoints_by_node_id", &self.endpoints_by_node_id)
            .field("incoming_frames", &self.incoming_frames)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl DaemonDfsEndpointTransport {
    pub fn new(
        local_node_id: sinomo_node::NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
    ) -> Self {
        Self::with_sender(
            local_node_id,
            endpoints,
            DFS_PROTOCOL_FRAME_TIMEOUT,
            Arc::new(send_dfs_protocol_frame_to_endpoint),
        )
    }

    fn with_sender(
        local_node_id: sinomo_node::NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        timeout: Duration,
        sender: DfsProtocolFrameSender,
    ) -> Self {
        let mut endpoints_by_node_id: BTreeMap<sinomo_node::NodeId, Vec<ClusterEndpointRecord>> =
            BTreeMap::new();
        for endpoint in endpoints {
            if endpoint.node_id != local_node_id {
                endpoints_by_node_id
                    .entry(endpoint.node_id.clone())
                    .or_default()
                    .push(endpoint);
            }
        }
        Self {
            local_node_id,
            endpoints_by_node_id,
            incoming_frames: VecDeque::new(),
            timeout,
            sender,
        }
    }

    fn deliver_returned_frame(&mut self, frame: NetworkFrame) -> Result<(), NetworkError> {
        let target_node_id = frame.target_node_id.clone();
        if target_node_id == self.local_node_id {
            self.incoming_frames.push_back(InboundFrame {
                received_by: self.local_node_id.clone(),
                frame,
            });
            return Ok(());
        }
        self.send_frame_to_endpoint(&target_node_id, frame, false)
    }

    fn send_frame_to_endpoint(
        &mut self,
        target_node_id: &sinomo_node::NodeId,
        frame: NetworkFrame,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        if require_local_source && &frame.source_node_id != self.local_node_id() {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: frame.source_node_id,
            });
        }
        if !require_local_source
            && frame.source_node_id != self.local_node_id
            && !self
                .endpoints_by_node_id
                .contains_key(&frame.source_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: frame.source_node_id,
            });
        }
        if &frame.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: frame.target_node_id,
            });
        }
        let endpoints = self
            .endpoints_by_node_id
            .get(target_node_id)
            .cloned()
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })?;
        let command = DfsProtocolFrameCommand {
            request_id: format!("dfs_protocol_frame_{}", frame.frame_id.0),
            frame,
        };
        let mut failures = Vec::new();
        for endpoint in &endpoints {
            match (self.sender)(endpoint, &command, self.timeout) {
                Ok(response) => {
                    for returned_frame in response.outbound_frames {
                        self.deliver_returned_frame(returned_frame)?;
                    }
                    return Ok(());
                }
                Err(error) => failures.push(format!("{} failed: {error}", endpoint.address)),
            }
        }
        Err(NetworkError::Io {
            operation: format!("send DFS protocol frame to {target_node_id}"),
            detail: failures.join("; "),
        })
    }
}

impl NetworkTransport for DaemonDfsEndpointTransport {
    fn local_node_id(&self) -> &sinomo_node::NodeId {
        &self.local_node_id
    }

    fn send(
        &mut self,
        target_node_id: &sinomo_node::NodeId,
        frame: NetworkFrame,
    ) -> Result<(), NetworkError> {
        self.send_frame_to_endpoint(target_node_id, frame, true)
    }

    fn drain_incoming(&mut self) -> Result<Vec<InboundFrame>, NetworkError> {
        Ok(self.incoming_frames.drain(..).collect())
    }
}

pub fn publish_default_dfs_metadata_journal(
    paths: &DaemonPaths,
) -> Result<DaemonDfsMetadataReplicationReport, DaemonError> {
    publish_default_dfs_metadata_journal_with_senders(
        paths,
        Arc::new(send_dfs_metadata_journal_batch_to_endpoint),
        Arc::new(send_dfs_metadata_snapshot_install_to_endpoint),
    )
}

#[cfg(test)]
fn publish_default_dfs_metadata_journal_with_sender(
    paths: &DaemonPaths,
    sender: DfsMetadataJournalBatchSender,
) -> Result<DaemonDfsMetadataReplicationReport, DaemonError> {
    publish_default_dfs_metadata_journal_with_senders(
        paths,
        sender,
        Arc::new(send_dfs_metadata_snapshot_install_to_endpoint),
    )
}

fn publish_default_dfs_metadata_journal_with_senders(
    paths: &DaemonPaths,
    sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
) -> Result<DaemonDfsMetadataReplicationReport, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsMetadataReplicationReport {
            outcomes: Vec::new(),
        });
    }
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let mut shards = sinomo_network::read_cluster_kv_prefix_entries(
        &cluster_paths,
        &format!(
            "{}/",
            sinomo_dfs::metadata_shards_prefix(&file_system.file_system_id)
        ),
    )?
    .into_iter()
    .map(|entry| entry.decode::<DfsMetadataShardConfig>())
    .collect::<Result<Vec<_>, _>>()?;
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok(DaemonDfsMetadataReplicationReport {
            outcomes: vec![DaemonDfsMetadataReplicationOutcome::MissingShard {
                file_system_id: file_system.file_system_id,
                shard_id: DfsMetadataShardId::new("root"),
            }],
        });
    };
    let endpoints = sinomo_network::cluster_dfs_protocol_targets(&cluster_paths, &state);
    let mut outcomes = Vec::new();
    for shard in shards {
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        if shard.primary_node_id != state.local_node_id {
            outcomes.push(DaemonDfsMetadataReplicationOutcome::NotPrimary {
                file_system_id: shard.file_system_id,
                shard_id,
                local_node_id: state.local_node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
            continue;
        }
        let mut backend = LocalJsonMetadataBackend::new(
            shard_id.clone(),
            data_dir
                .dfs_dir()
                .join("metadata")
                .join(path_safe_segment(&shard.shard_id)),
        )?;
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != &state.local_node_id)
        {
            let mut after_sequence = migrate_legacy_dfs_metadata_replication_cursor(
                &mut backend,
                &data_dir,
                &target_node_id,
                &shard_id,
            )?;
            let target_endpoints =
                dfs_metadata_replication_target_endpoints(&endpoints, &target_node_id);

            let mut sent_batches = 0usize;
            loop {
                let Some(batch) = backend.journal_batch_since_limited(
                    after_sequence,
                    DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES,
                    DFS_METADATA_REPLICATION_MAX_BATCH_BYTES,
                )?
                else {
                    if sent_batches == 0 {
                        outcomes.push(DaemonDfsMetadataReplicationOutcome::NoChanges {
                            target_node_id,
                            shard_id: shard_id.clone(),
                            after_sequence,
                        });
                    }
                    break;
                };
                if target_endpoints.is_empty() {
                    outcomes.push(DaemonDfsMetadataReplicationOutcome::MissingEndpoint {
                        target_node_id,
                        shard_id: shard_id.clone(),
                        after_sequence,
                    });
                    break;
                }
                let next_after_sequence = batch.next_after_sequence;
                let entry_count = batch.entries.len();
                let command = DfsMetadataJournalBatchCommand {
                    request_id: format!(
                        "dfs_meta_{}_{}_{}",
                        state.local_node_id, target_node_id, next_after_sequence
                    ),
                    batch,
                };
                let mut failures = Vec::new();
                let mut delivered = false;
                let mut terminal = false;
                let target_started = Instant::now();
                for endpoint in &target_endpoints {
                    let Some(attempt_timeout) = DFS_METADATA_REPLICATION_BATCH_TARGET_TIMEOUT
                        .checked_sub(target_started.elapsed())
                    else {
                        failures.push(format!(
                            "metadata journal replication target deadline exceeded after {}ms",
                            DFS_METADATA_REPLICATION_BATCH_TARGET_TIMEOUT.as_millis()
                        ));
                        break;
                    };
                    let attempt_timeout = attempt_timeout.min(DFS_METADATA_REPLICATION_TIMEOUT);
                    match sender(endpoint, &command, attempt_timeout) {
                        Ok(response) => {
                            if response.request_id != command.request_id
                                || response.shard_id != shard_id
                                || response.next_after_sequence != next_after_sequence
                            {
                                failures.push(format!(
                                    "{} returned mismatched metadata journal response",
                                    endpoint.address
                                ));
                                continue;
                            }
                            match response.result {
                                DfsMetadataJournalBatchResult::Applied => {
                                    backend.put_metadata_replication_cursor(
                                        &target_node_id,
                                        &shard_id,
                                        next_after_sequence,
                                    )?;
                                    after_sequence = next_after_sequence;
                                    sent_batches += 1;
                                    outcomes.push(DaemonDfsMetadataReplicationOutcome::Applied {
                                        target_node_id: target_node_id.clone(),
                                        shard_id: shard_id.clone(),
                                        entries: entry_count,
                                        next_after_sequence,
                                    });
                                    delivered = true;
                                    break;
                                }
                                DfsMetadataJournalBatchResult::Rejected { reason } => {
                                    if metadata_journal_rejection_is_sequence_conflict(&reason) {
                                        let snapshot_next_after_sequence =
                                            backend.journal_last_sequence();
                                        let snapshot_command = DfsMetadataSnapshotInstallCommand {
                                            request_id: format!(
                                                "dfs_meta_snapshot_{}_{}_{}",
                                                state.local_node_id,
                                                target_node_id,
                                                snapshot_next_after_sequence
                                            ),
                                            source_node_id: state.local_node_id.clone(),
                                            shard_id: shard_id.clone(),
                                            next_after_sequence: snapshot_next_after_sequence,
                                            snapshot: backend.export_table_snapshot(),
                                            reason: reason.clone(),
                                        };
                                        match snapshot_sender(
                                            endpoint,
                                            &snapshot_command,
                                            attempt_timeout,
                                        ) {
                                            Ok(snapshot_response) => {
                                                if snapshot_response.request_id
                                                    != snapshot_command.request_id
                                                    || snapshot_response.shard_id != shard_id
                                                    || snapshot_response.next_after_sequence
                                                        != snapshot_next_after_sequence
                                                {
                                                    failures.push(format!(
                                                        "{} returned mismatched metadata snapshot install response",
                                                        endpoint.address
                                                    ));
                                                    continue;
                                                }
                                                match snapshot_response.result {
                                                    DfsMetadataSnapshotInstallResult::Installed {
                                                        archived_existing,
                                                    } => {
                                                        backend.put_metadata_replication_cursor(
                                                            &target_node_id,
                                                            &shard_id,
                                                            snapshot_next_after_sequence,
                                                        )?;
                                                        after_sequence =
                                                            snapshot_next_after_sequence;
                                                        sent_batches += 1;
                                                        outcomes.push(
                                                            DaemonDfsMetadataReplicationOutcome::SnapshotInstalled {
                                                                target_node_id: target_node_id.clone(),
                                                                shard_id: shard_id.clone(),
                                                                next_after_sequence:
                                                                    snapshot_next_after_sequence,
                                                                archived_existing,
                                                            },
                                                        );
                                                        delivered = true;
                                                        break;
                                                    }
                                                    DfsMetadataSnapshotInstallResult::Rejected {
                                                        reason: snapshot_reason,
                                                    } => {
                                                        outcomes.push(
                                                            DaemonDfsMetadataReplicationOutcome::Rejected {
                                                                target_node_id: target_node_id.clone(),
                                                                shard_id: shard_id.clone(),
                                                                reason: format!(
                                                                    "{reason}; snapshot repair rejected: {snapshot_reason}"
                                                                ),
                                                            },
                                                        );
                                                        delivered = true;
                                                        terminal = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            Err(error) => {
                                                failures.push(format!(
                                                    "{} snapshot repair failed: {error}",
                                                    endpoint.address
                                                ));
                                                continue;
                                            }
                                        }
                                    }
                                    outcomes.push(DaemonDfsMetadataReplicationOutcome::Rejected {
                                        target_node_id: target_node_id.clone(),
                                        shard_id: shard_id.clone(),
                                        reason,
                                    });
                                    delivered = true;
                                    terminal = true;
                                    break;
                                }
                            }
                        }
                        Err(error) => {
                            failures.push(format!("{} failed: {error}", endpoint.address));
                        }
                    }
                }
                if !delivered {
                    outcomes.push(DaemonDfsMetadataReplicationOutcome::Failed {
                        target_node_id,
                        shard_id: shard_id.clone(),
                        reason: failures.join("; "),
                    });
                    break;
                }
                if terminal {
                    break;
                }
                if sent_batches >= DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET {
                    if backend
                        .journal_batch_since_limited(
                            after_sequence,
                            DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES,
                            DFS_METADATA_REPLICATION_MAX_BATCH_BYTES,
                        )?
                        .is_some()
                    {
                        outcomes.push(DaemonDfsMetadataReplicationOutcome::BacklogRemaining {
                            target_node_id,
                            shard_id: shard_id.clone(),
                            after_sequence,
                            max_batches: DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
                        });
                    }
                    break;
                }
            }
        }
    }
    Ok(DaemonDfsMetadataReplicationReport { outcomes })
}

fn dfs_metadata_replication_target_endpoints<'a>(
    endpoints: &'a [ClusterEndpointRecord],
    target_node_id: &sinomo_node::NodeId,
) -> Vec<&'a ClusterEndpointRecord> {
    let mut candidates = endpoints
        .iter()
        .filter(|endpoint| endpoint.node_id == *target_node_id)
        .collect::<Vec<_>>();
    candidates.sort_by_key(|endpoint| dfs_metadata_replication_endpoint_order_key(endpoint));
    let mut selected = Vec::new();
    let mut deferred = Vec::new();
    let mut seen_routes = std::collections::BTreeSet::new();
    let mut seen_classes = std::collections::BTreeSet::new();
    for endpoint in candidates {
        if !seen_routes.insert((
            format!("{:?}", endpoint.transport),
            endpoint.address.clone(),
        )) {
            continue;
        }
        if seen_classes.insert(dfs_metadata_replication_endpoint_class(endpoint)) {
            selected.push(endpoint);
        } else {
            deferred.push(endpoint);
        }
        if selected.len() >= DFS_METADATA_REPLICATION_MAX_ENDPOINTS_PER_TARGET {
            return selected;
        }
    }
    for endpoint in deferred {
        if selected.len() >= DFS_METADATA_REPLICATION_MAX_ENDPOINTS_PER_TARGET {
            break;
        }
        selected.push(endpoint);
    }
    selected
}

fn dfs_metadata_replication_endpoint_order_key(
    endpoint: &ClusterEndpointRecord,
) -> (u8, u16, String) {
    (
        dfs_metadata_replication_endpoint_class(endpoint),
        endpoint.priority,
        endpoint.address.clone(),
    )
}

fn dfs_metadata_replication_endpoint_class(endpoint: &ClusterEndpointRecord) -> u8 {
    match (&endpoint.transport, &endpoint.privacy) {
        (
            ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback,
            ClusterEndpointPrivacy::Lan,
        ) => 0,
        (
            ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback,
            ClusterEndpointPrivacy::InternetDirect,
        ) => 1,
        (ClusterTransportKind::Iroh, _) => 2,
        (ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback, _) => 3,
        _ => 4,
    }
}

fn metadata_journal_rejection_is_sequence_conflict(reason: &str) -> bool {
    reason.contains("metadata journal sequence")
        && reason.contains("already exists with a different entry")
}

pub fn run_daemon_dfs_smoke(paths: &DaemonPaths) -> Result<DaemonDfsSmokeReport, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Err(DaemonError::ClusterOrphan);
    }
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let mut content_nodes =
        sinomo_network::read_cluster_kv_prefix_entries(&cluster_paths, content_services_prefix())?
            .into_iter()
            .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok())
            .filter(|advertisement| advertisement.accepts_content)
            .map(|advertisement| advertisement.node_id)
            .collect::<Vec<_>>();
    content_nodes.sort();
    content_nodes.dedup();

    let endpoints = sinomo_network::cluster_dfs_protocol_targets(&cluster_paths, &state);
    let endpoint_node_ids = endpoints
        .iter()
        .map(|endpoint| endpoint.node_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let remote_candidates = content_nodes
        .into_iter()
        .filter(|node_id| node_id != &state.local_node_id)
        .filter(|node_id| endpoint_node_ids.contains(node_id))
        .collect::<Vec<_>>();
    if remote_candidates.is_empty() {
        return Err(DaemonError::Parse {
            detail: "DFS smoke requires an advertised remote content node with a daemon endpoint"
                .to_string(),
        });
    }

    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let metadata = open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)?;
    let mut service = with_daemon_dfs_control_plane(
        NetworkedDfsService::new(
            DaemonDfsEndpointTransport::new(state.local_node_id.clone(), endpoints),
            content,
        ),
        &cluster_paths,
    )
    .with_boxed_metadata_backend(metadata)?;
    service.bootstrap_distributed_sifs()?;
    service.posix_mkdir_all("/services/dfs-smoke")?;

    let nonce = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let mut attempts = Vec::new();
    let mut successful = None;
    for remote_node_id in remote_candidates {
        let replica_candidates = vec![state.local_node_id.clone(), remote_node_id.clone()];
        let path = format!("/services/dfs-smoke/{nonce}-{remote_node_id}.txt");
        let body = format!("sinomo dfs smoke {nonce} {remote_node_id}\n");
        match service.posix_write_file(
            &path,
            body.as_bytes(),
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2)?,
                replica_candidates: replica_candidates.clone(),
                wait_for_commit: true,
            },
        ) {
            Ok(stat) => {
                attempts.push(DaemonDfsSmokeAttempt {
                    replica_node_ids: replica_candidates.clone(),
                    path: path.clone(),
                    committed: true,
                    detail: "committed".to_string(),
                });
                successful = Some((stat, replica_candidates));
                break;
            }
            Err(error) => {
                let event_detail = dfs_event_summary(service.last_events());
                let detail = if event_detail.is_empty() {
                    error.to_string()
                } else {
                    format!("{error}; events: {}", event_detail.join("; "))
                };
                attempts.push(DaemonDfsSmokeAttempt {
                    replica_node_ids: replica_candidates,
                    path,
                    committed: false,
                    detail,
                });
            }
        }
    }
    let Some((stat, replica_candidates)) = successful else {
        return Err(DaemonError::Parse {
            detail: format!(
                "DFS smoke did not commit on any remote content node: {}",
                attempts
                    .iter()
                    .map(|attempt| format!("{} => {}", attempt.replica_node_ids[1], attempt.detail))
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        });
    };
    let cluster_paths = ClusterPaths::new(&data_dir);
    let status = inspect_daemon_dfs_status(&data_dir, &cluster_paths)?;
    let metadata_replication = publish_default_dfs_metadata_journal(paths)?;
    Ok(DaemonDfsSmokeReport {
        path: stat.path,
        file_system_id: stat.file_system_id,
        object_id: stat.object_id,
        version: stat.version,
        content_hash: stat.content_hash,
        size_bytes: stat.size_bytes,
        requested_replica_node_ids: replica_candidates,
        committed_replica_node_ids: stat.current_locations,
        pending_repair_count: status.pending_repair_count,
        metadata_replication,
        attempts,
    })
}

fn daemon_data_dir(paths: &DaemonPaths) -> Result<&std::path::Path, DaemonError> {
    paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon root has no data directory parent".to_string(),
    })
}

fn dfs_event_summary(events: &[sinomo_dfs::NetworkedDfsServiceEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            sinomo_dfs::NetworkedDfsServiceEvent::BlobStageRejected {
                replica_node_id,
                reason,
                ..
            } => Some(format!("stage rejected by {replica_node_id}: {reason}")),
            sinomo_dfs::NetworkedDfsServiceEvent::BlobFetchRejected {
                source_node_id,
                reason,
                ..
            } => Some(format!("fetch rejected by {source_node_id}: {reason}")),
            sinomo_dfs::NetworkedDfsServiceEvent::RepairRejected {
                replica_node_id,
                reason,
                ..
            } => Some(format!("repair rejected by {replica_node_id}: {reason}")),
            _ => None,
        })
        .collect()
}

fn local_dfs_protocol_node_id(
    data_dir: &SinomoDataDir,
) -> Result<sinomo_node::NodeId, DaemonError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    if !cluster_paths.state.exists() {
        return Err(DaemonError::ClusterOrphan);
    }
    Ok(sinomo_network::read_cluster_state_without_kv(&cluster_paths)?.local_node_id)
}

fn migrate_legacy_dfs_metadata_replication_cursor(
    backend: &mut LocalJsonMetadataBackend,
    data_dir: &SinomoDataDir,
    target_node_id: &sinomo_node::NodeId,
    shard_id: &DfsMetadataShardId,
) -> Result<u64, DaemonError> {
    let mut after_sequence = backend.metadata_replication_cursor(target_node_id, shard_id)?;
    let legacy_path =
        legacy_dfs_metadata_replication_cursor_path(data_dir, target_node_id, shard_id);
    let Some(legacy_cursor) = read_legacy_dfs_metadata_replication_cursor(&legacy_path)? else {
        return Ok(after_sequence);
    };
    if legacy_cursor.target_node_id != *target_node_id || legacy_cursor.shard_id != *shard_id {
        return Err(DaemonError::Parse {
            detail: format!(
                "legacy DFS metadata replication cursor {} does not match target {target_node_id} shard {}",
                legacy_path.display(),
                shard_id.0,
            ),
        });
    }
    let legacy_after_sequence = legacy_cursor.next_after_sequence;
    if legacy_after_sequence > after_sequence {
        backend.put_metadata_replication_cursor(target_node_id, shard_id, legacy_after_sequence)?;
        after_sequence = legacy_after_sequence;
    }
    match fs::remove_file(&legacy_path) {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(DaemonError::Io {
                path: legacy_path.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    Ok(after_sequence)
}

fn legacy_dfs_metadata_replication_cursor_path(
    data_dir: &SinomoDataDir,
    target_node_id: &sinomo_node::NodeId,
    shard_id: &DfsMetadataShardId,
) -> std::path::PathBuf {
    data_dir
        .dfs_dir()
        .join("metadata-replication-cursors")
        .join(path_safe_segment(&target_node_id.to_string()))
        .join(format!("{}.json", path_safe_segment(&shard_id.0)))
}

fn read_legacy_dfs_metadata_replication_cursor(
    path: &std::path::Path,
) -> Result<Option<LegacyDaemonDfsMetadataReplicationCursor>, DaemonError> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| DaemonError::Json {
                path: path.display().to_string(),
                detail: source.to_string(),
            }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

#[cfg(test)]
fn write_legacy_dfs_metadata_replication_cursor(
    path: &std::path::Path,
    cursor: &LegacyDaemonDfsMetadataReplicationCursor,
) -> Result<(), DaemonError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| DaemonError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes = serde_json::to_vec_pretty(cursor).map_err(|source| DaemonError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(path, bytes).map_err(|source| DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn path_safe_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterTransportKind,
        DfsObjectPolicy,
    };
    use sinomo_dfs::{
        DfsBlobTransferResult, DfsNetworkMessage, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
        DfsProtocolFrameCommand, DfsReplicationPolicy, DfsStageBlobCommand, DfsVersion,
        DfsWriteOptions,
    };
    use sinomo_network::{
        MeshMessage, MeshMessageBody, MeshMessageId, NetworkFrame, NetworkFrameId, ProtocolMessage,
    };
    use sinomo_node::NodeId;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn daemon_dfs_metadata_journal_batch_applies_to_local_backend() {
        let root = temp_root("daemon_dfs_metadata_journal_batch");
        let paths = DaemonPaths::with_root(root.join("data").join("daemon"));
        let source_root = root.join("source-meta");
        let node_id = NodeId::from_legacy_named("node_daemon_dfs_metadata").expect("node");
        let file_system = distributed_sifs(node_id.clone());
        let object = distributed_sifs_bootstrap_objects(node_id)
            .into_iter()
            .next()
            .expect("bootstrap object");
        let mut source =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("root"), source_root)
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

        let response = handle_daemon_dfs_metadata_journal_batch(
            &paths,
            DfsMetadataJournalBatchCommand {
                request_id: "dfs_meta_req_1".to_string(),
                batch,
            },
        )
        .expect("apply batch");
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
        assert_eq!(response.applied_entries, 3);

        let target = LocalJsonMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.join("data").join("dfs").join("metadata").join("root"),
        )
        .expect("target backend");
        assert_eq!(
            target.get_object(&object.object_id).expect("target object"),
            Some(object)
        );

        cleanup(&root);
    }

    #[test]
    fn daemon_dfs_metadata_journal_batch_uses_path_safe_shard_directory() {
        let root = temp_root("daemon_dfs_metadata_journal_batch_safe_shard");
        let paths = DaemonPaths::with_root(root.join("data").join("daemon"));
        let source_root = root.join("source-meta");
        let node_id = NodeId::from_legacy_named("node_daemon_dfs_metadata_safe").expect("node");
        let file_system = distributed_sifs(node_id.clone());
        let object = distributed_sifs_bootstrap_objects(node_id)
            .into_iter()
            .next()
            .expect("bootstrap object");
        let shard_id = DfsMetadataShardId::new("root/frag");
        let mut source =
            LocalJsonMetadataBackend::new(shard_id.clone(), source_root).expect("source backend");
        source.put_file_system(&file_system).expect("filesystem");
        source.put_object(&object).expect("object");
        let mut subscription = source.subscribe(0);
        let batch = source
            .poll_subscription(&mut subscription)
            .expect("source batch");

        let response = handle_daemon_dfs_metadata_journal_batch(
            &paths,
            DfsMetadataJournalBatchCommand {
                request_id: "dfs_meta_req_safe".to_string(),
                batch,
            },
        )
        .expect("apply batch");
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);

        let target = LocalJsonMetadataBackend::new(
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

        cleanup(&root);
    }

    #[test]
    fn daemon_dfs_status_reads_default_metadata_backend() {
        let root = temp_root("daemon_dfs_status");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let node_id = NodeId::from_legacy_named("node_daemon_dfs_status").expect("node");
        let file_system = distributed_sifs(node_id.clone());
        let object = distributed_sifs_bootstrap_objects(node_id)
            .into_iter()
            .next()
            .expect("bootstrap object");
        let mut backend = LocalJsonMetadataBackend::new(
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

        let status = handle_daemon_dfs_status(&paths).expect("dfs status");

        assert_eq!(status.file_system_count, 1);
        assert_eq!(status.object_count, 1);
        assert_eq!(status.version_count, 1);

        cleanup(&root);
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
        let cluster_id = sinomo_network::ClusterId::from_legacy_named("cluster_dfs_status_sharded")
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &node_id,
            sinomo_kv::KvCommand::Txn(sinomo_kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &root_shard,
                        None,
                    )
                    .expect("root shard op"),
                    sinomo_kv::KvOp::put_serde(
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
        let mut backend = open_daemon_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
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

        let status = handle_daemon_dfs_status(&paths).expect("dfs status");

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
                target_node_id: remote_node_id,
                replicated_through_sequence: 1,
                lag_entries: 2,
            }
        );

        cleanup(&root);
    }

    #[test]
    fn daemon_dfs_status_reports_replica_lag_only_from_primary() {
        let root = temp_root("daemon_dfs_status_replica_lag_primary_only");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_status_replica_lag")
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("root shard op"),
            ),
        )
        .expect("write shard config");
        let mut backend = open_daemon_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
            .expect("open sharded backend")
            .expect("configured backend");
        backend.put_file_system(&file_system).expect("filesystem");

        let status = handle_daemon_dfs_status(&paths).expect("dfs status");

        assert_eq!(status.metadata_shards.len(), 1);
        assert!(status.metadata_shards[0].replica_lag.is_empty());

        cleanup(&root);
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
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_cursor")
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
            sinomo_node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.21:21353")];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
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
        let mut backend = LocalJsonMetadataBackend::new(
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
            legacy_dfs_metadata_replication_cursor_path(&data_dir, &remote, &shard_id);
        write_legacy_dfs_metadata_replication_cursor(
            &legacy_path,
            &LegacyDaemonDfsMetadataReplicationCursor {
                target_node_id: remote.clone(),
                shard_id: shard_id.clone(),
                next_after_sequence: 1,
            },
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
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert_eq!(
            report.outcomes,
            vec![DaemonDfsMetadataReplicationOutcome::Applied {
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
        let backend = LocalJsonMetadataBackend::new(
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

        cleanup(&root);
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
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_all")
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
            sinomo_node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.22:21353")];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

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
            sinomo_network::append_cluster_raft_command(
                &cluster_paths,
                &local,
                sinomo_kv::KvCommand::Op(
                    sinomo_kv::KvOp::put_serde(
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
            let mut backend = LocalJsonMetadataBackend::new(
                DfsMetadataShardId::new(shard_name),
                data_dir
                    .dfs_dir()
                    .join("metadata")
                    .join(path_safe_segment(shard_name)),
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
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert_eq!(
            report.outcomes,
            vec![
                DaemonDfsMetadataReplicationOutcome::Applied {
                    target_node_id: remote.clone(),
                    shard_id: DfsMetadataShardId::new("root"),
                    entries: 2,
                    next_after_sequence: 2,
                },
                DaemonDfsMetadataReplicationOutcome::Applied {
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

        cleanup(&root);
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
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_drain")
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
            sinomo_node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.23:21353")];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut backend = LocalJsonMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        let base_object = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        let object_count = DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES + 7;
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
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        let expected_entries = 1 + object_count;
        assert_eq!(
            report.outcomes,
            vec![
                DaemonDfsMetadataReplicationOutcome::Applied {
                    target_node_id: remote.clone(),
                    shard_id: shard_id.clone(),
                    entries: DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES,
                    next_after_sequence: DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES as u64,
                },
                DaemonDfsMetadataReplicationOutcome::Applied {
                    target_node_id: remote.clone(),
                    shard_id: shard_id.clone(),
                    entries: expected_entries - DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES,
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
                DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES,
                expected_entries - DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES,
            ]
        );
        drop(sent);
        let backend = LocalJsonMetadataBackend::new(
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

        cleanup(&root);
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
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_bad_ack")
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
            sinomo_node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.24:21353")];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut backend = LocalJsonMetadataBackend::new(
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
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert!(matches!(
            report.outcomes.as_slice(),
            [DaemonDfsMetadataReplicationOutcome::Failed { reason, .. }]
                if reason.contains("mismatched metadata journal response")
        ));
        let backend = LocalJsonMetadataBackend::new(
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

        cleanup(&root);
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
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_rejected")
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
            sinomo_node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.25:21353")];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut backend = LocalJsonMetadataBackend::new(
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
            [DaemonDfsMetadataReplicationOutcome::Rejected { reason, .. }]
                if reason.contains("conflicting journal entry")
        ));
        let backend = LocalJsonMetadataBackend::new(
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

        cleanup(&root);
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
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_metadata_snapshot_repair")
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
            sinomo_node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.26:21353")];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut backend = LocalJsonMetadataBackend::new(
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
            vec![DaemonDfsMetadataReplicationOutcome::SnapshotInstalled {
                target_node_id: remote.clone(),
                shard_id: shard_id.clone(),
                next_after_sequence: 1,
                archived_existing: true,
            }]
        );
        assert_eq!(snapshot_installs.lock().expect("lock").len(), 1);
        let backend = LocalJsonMetadataBackend::new(
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

        cleanup(&root);
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
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_bounds")
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
            sinomo_node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        let lan_primary = test_endpoint(&remote, "10.0.0.10:21353");
        let lan_secondary = test_endpoint(&remote, "10.0.0.11:21353");
        let mut public = test_endpoint(&remote, "203.0.113.10:21353");
        public.privacy = ClusterEndpointPrivacy::InternetDirect;
        let iroh = ClusterEndpointRecord {
            endpoint_id: sinomo_network::ClusterEndpointId::derived(
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");
        let mut backend = LocalJsonMetadataBackend::new(
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
                    .push(endpoint.address.clone());
                Err(DaemonError::Network {
                    address: endpoint.address.clone(),
                    detail: "simulated failure".to_string(),
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert!(matches!(
            report.outcomes.as_slice(),
            [DaemonDfsMetadataReplicationOutcome::Failed { .. }]
        ));
        assert_eq!(
            attempted.lock().expect("attempted lock").as_slice(),
            &[lan_primary.address, public.address, iroh.address],
            "metadata replication should try one LAN, one public TCP, and one iroh fallback instead of walking every stale endpoint"
        );

        cleanup(&root);
    }

    #[test]
    fn daemon_dfs_protocol_frame_processes_stage_blob_and_returns_response_frame() {
        let root = temp_root("daemon_dfs_protocol_frame_stage_blob");
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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &replica,
            sinomo_kv::KvCommand::Txn(sinomo_kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_kv::KvOp::put_serde(
                        file_system_config_key(&file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config op"),
                    sinomo_kv::KvOp::put_serde(
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
        let object_id = DfsObjectId::from_legacy_named("obj_dfs_protocol_stage").expect("object");
        let version = DfsVersion::from_legacy_named("version_dfs_protocol_stage").expect("version");
        let bytes = b"daemon dfs protocol frame bytes".to_vec();
        let content_hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());
        let object = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "services/daemon-dfs-protocol.bin".to_string(),
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
        });
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(1),
            source_node_id: owner.clone(),
            target_node_id: replica.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(1),
                origin_node_id: owner.clone(),
                target_node_id: replica.clone(),
                body: MeshMessageBody::Protocol(
                    ProtocolMessage::encode(sinomo_dfs::DFS_PROTOCOL, &message)
                        .expect("protocol message"),
                ),
            },
        };

        let response = handle_daemon_dfs_protocol_frame(
            &paths,
            DfsProtocolFrameCommand {
                request_id: "dfs_protocol_frame_rpc_1".to_string(),
                frame,
            },
        )
        .expect("protocol frame");

        assert_eq!(response.request_id, "dfs_protocol_frame_rpc_1");
        assert_eq!(response.outbound_frames.len(), 1);
        let MeshMessageBody::Protocol(protocol) = &response.outbound_frames[0].message.body else {
            panic!("expected protocol response frame");
        };
        let response_message: DfsNetworkMessage = protocol.decode().expect("decode response");
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
        let backend = open_daemon_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
            .expect("open sharded backend")
            .expect("configured backend");
        assert!(
            backend
                .get_object(&object_id)
                .expect("backend object")
                .is_none(),
            "non-primary content replicas must not append authoritative metadata while handling DFS protocol frames"
        );

        cleanup(&root);
    }

    #[test]
    fn daemon_dfs_protocol_frame_rejects_wrong_target_node() {
        let root = temp_root("daemon_dfs_protocol_frame_wrong_target");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let local = NodeId::from_legacy_named("node_dfs_protocol_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_protocol_remote").expect("remote");
        write_test_cluster_state(&data_root, &local);
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(1),
            source_node_id: remote.clone(),
            target_node_id: remote.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(1),
                origin_node_id: remote.clone(),
                target_node_id: remote.clone(),
                body: MeshMessageBody::Heartbeat(sinomo_network::Heartbeat {
                    node_id: remote.clone(),
                    tick: 1,
                }),
            },
        };

        let error = handle_daemon_dfs_protocol_frame(
            &paths,
            DfsProtocolFrameCommand {
                request_id: "dfs_protocol_frame_rpc_wrong_target".to_string(),
                frame,
            },
        )
        .expect_err("wrong target rejected");
        assert!(error.to_string().contains("does not match local node"));

        cleanup(&root);
    }

    #[test]
    fn daemon_dfs_endpoint_transport_sends_rpc_and_queues_returned_frames() {
        let local = NodeId::from_legacy_named("node_dfs_endpoint_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_endpoint_remote").expect("remote");
        let endpoint = test_endpoint(&remote, "192.0.2.10:21353");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_for_sender = Arc::clone(&calls);
        let local_for_sender = local.clone();
        let remote_for_sender = remote.clone();
        let sender: DfsProtocolFrameSender = Arc::new(move |endpoint, command, _timeout| {
            calls_for_sender
                .lock()
                .expect("calls lock")
                .push((endpoint.node_id.clone(), command.frame.clone()));
            Ok(DfsProtocolFrameResponse {
                request_id: command.request_id.clone(),
                outbound_frames: vec![NetworkFrame {
                    frame_id: NetworkFrameId(100),
                    source_node_id: remote_for_sender.clone(),
                    target_node_id: local_for_sender.clone(),
                    message: MeshMessage {
                        message_id: MeshMessageId(100),
                        origin_node_id: remote_for_sender.clone(),
                        target_node_id: local_for_sender.clone(),
                        body: MeshMessageBody::Heartbeat(sinomo_network::Heartbeat {
                            node_id: remote_for_sender.clone(),
                            tick: 1,
                        }),
                    },
                }],
            })
        });
        let mut transport = DaemonDfsEndpointTransport::with_sender(
            local.clone(),
            vec![endpoint],
            Duration::from_millis(1),
            sender,
        );
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(7),
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(7),
                origin_node_id: local.clone(),
                target_node_id: remote.clone(),
                body: MeshMessageBody::Heartbeat(sinomo_network::Heartbeat {
                    node_id: local.clone(),
                    tick: 7,
                }),
            },
        };

        transport.send(&remote, frame.clone()).expect("send frame");
        let recorded = calls.lock().expect("calls lock");
        assert_eq!(recorded.as_slice(), &[(remote.clone(), frame)]);
        drop(recorded);
        let incoming = transport.drain_incoming().expect("incoming frames");
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].received_by, local);
        assert_eq!(incoming[0].frame.source_node_id, remote);
    }

    #[test]
    fn daemon_dfs_endpoint_transport_relays_remote_returned_frames_preserving_source() {
        let local = NodeId::from_legacy_named("node_dfs_endpoint_relay_local").expect("local");
        let primary =
            NodeId::from_legacy_named("node_dfs_endpoint_relay_primary").expect("primary");
        let replica =
            NodeId::from_legacy_named("node_dfs_endpoint_relay_replica").expect("replica");
        let primary_endpoint = test_endpoint(&primary, "192.0.2.12:21353");
        let replica_endpoint = test_endpoint(&replica, "192.0.2.13:21353");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_for_sender = Arc::clone(&calls);
        let local_for_sender = local.clone();
        let primary_for_sender = primary.clone();
        let replica_for_sender = replica.clone();
        let sender: DfsProtocolFrameSender = Arc::new(move |endpoint, command, _timeout| {
            calls_for_sender.lock().expect("calls lock").push((
                endpoint.node_id.clone(),
                command.frame.source_node_id.clone(),
                command.frame.target_node_id.clone(),
            ));
            if command.frame.target_node_id == primary_for_sender {
                return Ok(DfsProtocolFrameResponse {
                    request_id: command.request_id.clone(),
                    outbound_frames: vec![NetworkFrame {
                        frame_id: NetworkFrameId(101),
                        source_node_id: primary_for_sender.clone(),
                        target_node_id: replica_for_sender.clone(),
                        message: MeshMessage {
                            message_id: MeshMessageId(101),
                            origin_node_id: primary_for_sender.clone(),
                            target_node_id: replica_for_sender.clone(),
                            body: MeshMessageBody::Heartbeat(sinomo_network::Heartbeat {
                                node_id: primary_for_sender.clone(),
                                tick: 1,
                            }),
                        },
                    }],
                });
            }
            if command.frame.target_node_id == replica_for_sender {
                return Ok(DfsProtocolFrameResponse {
                    request_id: command.request_id.clone(),
                    outbound_frames: vec![NetworkFrame {
                        frame_id: NetworkFrameId(102),
                        source_node_id: replica_for_sender.clone(),
                        target_node_id: local_for_sender.clone(),
                        message: MeshMessage {
                            message_id: MeshMessageId(102),
                            origin_node_id: replica_for_sender.clone(),
                            target_node_id: local_for_sender.clone(),
                            body: MeshMessageBody::Heartbeat(sinomo_network::Heartbeat {
                                node_id: replica_for_sender.clone(),
                                tick: 2,
                            }),
                        },
                    }],
                });
            }
            Ok(DfsProtocolFrameResponse {
                request_id: command.request_id.clone(),
                outbound_frames: Vec::new(),
            })
        });
        let mut transport = DaemonDfsEndpointTransport::with_sender(
            local.clone(),
            vec![primary_endpoint, replica_endpoint],
            Duration::from_millis(1),
            sender,
        );
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(9),
            source_node_id: local.clone(),
            target_node_id: primary.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(9),
                origin_node_id: local.clone(),
                target_node_id: primary.clone(),
                body: MeshMessageBody::Heartbeat(sinomo_network::Heartbeat {
                    node_id: local.clone(),
                    tick: 9,
                }),
            },
        };

        transport.send(&primary, frame).expect("send through relay");

        assert_eq!(
            calls.lock().expect("calls lock").as_slice(),
            &[
                (primary.clone(), local.clone(), primary.clone()),
                (replica.clone(), primary.clone(), replica.clone()),
            ]
        );
        let incoming = transport.drain_incoming().expect("incoming frames");
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].received_by, local);
        assert_eq!(incoming[0].frame.source_node_id, replica);
    }

    #[test]
    fn daemon_dfs_endpoint_transport_completes_synchronous_replica_write() {
        let root = temp_root("daemon_dfs_endpoint_transport_replica_write");
        let local = NodeId::from_legacy_named("node_dfs_endpoint_write_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_endpoint_write_remote").expect("remote");
        let endpoint = test_endpoint(&remote, "192.0.2.11:21353");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_for_sender = Arc::clone(&calls);
        let sender: DfsProtocolFrameSender = Arc::new(move |_endpoint, command, _timeout| {
            let MeshMessageBody::Protocol(protocol) = &command.frame.message.body else {
                panic!("expected protocol frame");
            };
            let message: DfsNetworkMessage = protocol.decode().expect("decode dfs message");
            let DfsNetworkMessage::StageBlob(stage) = message else {
                return Ok(DfsProtocolFrameResponse {
                    request_id: command.request_id.clone(),
                    outbound_frames: Vec::new(),
                });
            };
            calls_for_sender
                .lock()
                .expect("calls lock")
                .push(stage.request_id.clone());
            let response = DfsNetworkMessage::StageBlobResponse(sinomo_dfs::DfsStageBlobResponse {
                request_id: stage.request_id,
                intent_id: stage.intent_id,
                file_system_id: stage.file_system_id,
                object_id: stage.object_id,
                version: stage.version,
                replica_node_id: command.frame.target_node_id.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: stage.content_hash,
                },
            });
            Ok(DfsProtocolFrameResponse {
                request_id: command.request_id.clone(),
                outbound_frames: vec![NetworkFrame {
                    frame_id: NetworkFrameId(100),
                    source_node_id: command.frame.target_node_id.clone(),
                    target_node_id: command.frame.source_node_id.clone(),
                    message: MeshMessage {
                        message_id: MeshMessageId(100),
                        origin_node_id: command.frame.target_node_id.clone(),
                        target_node_id: command.frame.source_node_id.clone(),
                        body: MeshMessageBody::Protocol(
                            ProtocolMessage::encode(sinomo_dfs::DFS_PROTOCOL, &response)
                                .expect("encode response"),
                        ),
                    },
                }],
            })
        });
        let transport = DaemonDfsEndpointTransport::with_sender(
            local.clone(),
            vec![endpoint],
            Duration::from_millis(1),
            sender,
        );
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut dfs = NetworkedDfsService::new(transport, content);
        dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
        dfs.posix_mkdir_all("/services").expect("services dir");

        let stat = dfs
            .posix_write_file(
                "/services/daemon-endpoint-transport.txt",
                b"replicated bytes",
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                    replica_candidates: vec![local.clone(), remote.clone()],
                    wait_for_commit: true,
                },
            )
            .expect("replicated write");

        assert_eq!(calls.lock().expect("calls lock").len(), 1);
        assert_eq!(stat.current_locations, vec![local, remote]);
        assert_eq!(stat.size_bytes, Some(b"replicated bytes".len() as u64));

        cleanup(&root);
    }

    #[test]
    fn dfs_service_members_include_authorized_voters_and_learners_only() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_service_members")
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

        let service_nodes = dfs_service_members(&state)
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
        let file_system = default_cluster_sifs(&state);

        let expected_node_ids = stable_dfs_service_node_ids(&state);
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let file_system = default_cluster_sifs(&state);
        let expected_primary = stable_dfs_service_node_ids(&state)[0].clone();
        let old_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: expected_primary.clone(),
            replica_node_ids: Vec::new(),
            epoch: 9,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &state.local_node_id,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &old_shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write old shard");

        let migrated =
            default_cluster_root_metadata_shard(&data_dir, &cluster_paths, &state, &file_system)
                .expect("default shard");
        assert_eq!(migrated.primary_node_id, expected_primary);
        assert_eq!(migrated.epoch, 10);

        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &state.local_node_id,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &migrated,
                    None,
                )
                .expect("put migrated op"),
            ),
        )
        .expect("write migrated shard");
        let stable =
            default_cluster_root_metadata_shard(&data_dir, &cluster_paths, &state, &file_system)
                .expect("stable shard");
        assert_eq!(stable, migrated);

        cleanup(&root);
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let file_system = default_cluster_sifs(&state);
        let shards =
            default_cluster_metadata_shards(&data_dir, &cluster_paths, &state, &file_system)
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

        cleanup(&root);
    }

    #[test]
    fn default_cluster_metadata_shard_transfers_after_candidate_replays_journal() {
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
        let service_nodes = stable_dfs_service_node_ids(&state);
        let bootstrap_primary = service_nodes[0].clone();
        let target_primary = service_nodes[1].clone();
        state.local_node_id = bootstrap_primary.clone();
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let file_system = default_cluster_sifs(&state);
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
        sinomo_network::append_cluster_raft_command(
            &cluster_paths,
            &state.local_node_id,
            sinomo_kv::KvCommand::Op(
                sinomo_kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "services"),
                    &services_shard,
                    None,
                )
                .expect("put services shard op"),
            ),
        )
        .expect("write services shard");
        let shard_id = DfsMetadataShardId::new("services");
        let mut backend = LocalJsonMetadataBackend::new(
            shard_id.clone(),
            data_dir
                .dfs_dir()
                .join("metadata")
                .join(path_safe_segment("services")),
        )
        .expect("services backend");
        backend.put_file_system(&file_system).expect("filesystem");

        let waiting =
            default_cluster_metadata_shards(&data_dir, &cluster_paths, &state, &file_system)
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
            default_cluster_metadata_shards(&data_dir, &cluster_paths, &state, &file_system)
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

        cleanup(&root);
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
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_bootstrap_migrate_services")
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
        let mut local_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id,
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        local_member.capabilities =
            sinomo_node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        state.members.push(local_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
            .expect("sync raft");
        let file_system = default_cluster_sifs(&state);
        let mut legacy_object = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        legacy_object.object_id =
            DfsObjectId::from_legacy_named("obj_legacy_services_metadata").expect("object");
        legacy_object.path = "/services/legacy.txt".to_string();
        {
            let mut legacy_root = LocalJsonMetadataBackend::new(
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

        ensure_default_distributed_dfs(&paths, &DaemonRunOptions::default(), true)
            .expect("bootstrap dfs");

        let shards = sinomo_network::read_cluster_kv_prefix_entries(
            &cluster_paths,
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

        let backend = open_daemon_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
            .expect("open sharded backend")
            .expect("configured backend");
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &legacy_object.path)
                .expect("migrated services path"),
            Some(legacy_object.clone())
        );
        assert_eq!(
            LocalJsonMetadataBackend::new(
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

        cleanup(&root);
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
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_bootstrap_non_primary")
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
        assert_ne!(default_cluster_sifs_owner(&state), local);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

        ensure_default_distributed_dfs(&paths, &DaemonRunOptions::default(), false)
            .expect("bootstrap dfs");

        let backend = LocalJsonMetadataBackend::new(
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

        cleanup(&root);
    }

    #[test]
    fn dfs_service_endpoint_prefers_internet_direct_for_remote_members() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_dfs_service_endpoint")
                .expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_service_endpoint_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_service_endpoint_remote").expect("remote");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs service endpoint",
            local,
            "local",
            "pk-local",
            now,
        );
        let mut member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id,
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        member.endpoints = vec![
            sinomo_network::daemon_lan_endpoint(&remote, "10.0.0.41:21353".to_string(), 10),
            ClusterEndpointRecord {
                endpoint_id: sinomo_core::Id::from_legacy_named(
                    "endpoint_dfs_service_internet_direct",
                )
                .expect("endpoint id"),
                node_id: remote,
                transport: sinomo_core::ClusterTransportKind::LanTcp,
                address: "129.146.106.76:21353".to_string(),
                source: sinomo_core::ClusterEndpointSource::Observed,
                privacy: ClusterEndpointPrivacy::InternetDirect,
                priority: 20,
                last_observed_latency_ms: None,
            },
        ];

        assert_eq!(
            dfs_service_endpoint_for_member(&state, &member, &DaemonRunOptions::default()),
            "129.146.106.76:21353"
        );
    }

    fn write_test_cluster_state(data_root: &std::path::Path, node_id: &NodeId) {
        let data_dir = SinomoDataDir::new(data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named("cluster_daemon_dfs_protocol")
                .expect("cluster");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon dfs protocol",
            node_id.clone(),
            "daemon dfs protocol node",
            format!("pk-{node_id}"),
            now,
        );
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    }

    fn test_cluster_state_with_members(
        cluster_name: &str,
        local_name: &str,
        remote_names: &[&str],
    ) -> sinomo_core::ClusterBootstrapState {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_network::ClusterId::from_legacy_named(cluster_name).expect("cluster id");
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
            endpoint_id: sinomo_network::ClusterEndpointId::derived(
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

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-daemon-dfs-{name}-{nanos}"))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = fs::remove_dir_all(path);
    }
}
