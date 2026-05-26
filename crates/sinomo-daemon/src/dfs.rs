use crate::{
    DaemonError, DaemonPaths, DaemonRunOptions, put_cluster_kv_control_record,
    send_dfs_metadata_journal_batch_to_endpoint, send_dfs_protocol_frame_to_endpoint,
};
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord,
    ClusterMemberRole, DfsObjectPolicy,
};
use sinomo_dfs::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
    DfsExposeStore, DfsFileSystemConfig, DfsMetadataBackend, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNodeAdvertisement, DfsObjectVersionRecord,
    DfsOperatorInspector, DfsProtocolFrameCommand, DfsProtocolFrameResponse, DfsReplicationPolicy,
    DfsSubtreePolicy, DfsWriteOptions, LocalContentStore, LocalJsonMetadataBackend,
    LocalShardedMetadataBackend, NetworkedDfsService, content_service_key, content_services_prefix,
    default_dfs_metadata_shards_root, distributed_sifs, distributed_sifs_bootstrap_objects,
    fallback_dfs_node_id, file_system_config_key, metadata_service_key, metadata_shard_key,
    sifs_node_files_path,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{ClusterPaths, InboundFrame, NetworkError, NetworkFrame, NetworkTransport};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;

const DFS_METADATA_REPLICATION_TIMEOUT: Duration = Duration::from_secs(3);
const DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES: usize =
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT;
const DFS_METADATA_REPLICATION_MAX_BATCH_BYTES: usize = DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT;
const DFS_PROTOCOL_FRAME_TIMEOUT: Duration = Duration::from_secs(3);

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
    Failed {
        target_node_id: sinomo_node::NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
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

fn inspect_daemon_dfs_status(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<sinomo_dfs::DfsOperatorStatus, DaemonError> {
    if cluster_paths.state.exists()
        && let Some(metadata) = open_daemon_sharded_dfs_metadata_backend(data_dir, cluster_paths)?
    {
        return DfsOperatorInspector::new(&metadata)
            .status()
            .map_err(DaemonError::from);
    }
    sinomo_dfs::inspect_dfs_status(data_dir.dfs_dir().join("metadata").join("root"))
        .map_err(DaemonError::from)
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
    let metadata_backend = LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )?;
    let content_store = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let transport = DaemonDfsProtocolTransport::new(local_node_id, command.frame);
    let mut service = NetworkedDfsService::new(transport, content_store)
        .with_metadata_backend(metadata_backend)?;
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
    let mut backend = LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )?;
    if backend.get_file_system(&file_system.file_system_id)? != Some(file_system.clone()) {
        backend.put_file_system(&file_system)?;
    }
    for object in default_cluster_sifs_bootstrap_objects(&state) {
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
    if !publish_cluster_control {
        return Ok(());
    }

    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        file_system_config_key(&file_system.file_system_id),
        &DfsFileSystemConfig::from(&file_system),
    )?;
    let root_shard = default_cluster_root_metadata_shard(&cluster_paths, &state, &file_system)?;
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        metadata_shard_key(&file_system.file_system_id, "root"),
        &root_shard,
    )?;
    publish_dfs_service_advertisements_for_cluster(&cluster_paths, &state, options)?;
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
    let metadata = LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )?;
    let endpoints = sinomo_network::cluster_dfs_protocol_targets(&cluster_paths, &state);
    let mut service = NetworkedDfsService::new(
        DaemonDfsEndpointTransport::new(state.local_node_id, endpoints),
        content,
    )
    .with_metadata_backend(metadata)?;
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

fn default_cluster_root_metadata_shard(
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    file_system: &sinomo_dfs::DfsFileSystem,
) -> Result<DfsMetadataShardConfig, DaemonError> {
    let primary_node_id = default_cluster_sifs_owner(state);
    let replica_node_ids = stable_dfs_service_node_ids(state)
        .into_iter()
        .filter(|node_id| node_id != &primary_node_id)
        .take(2)
        .collect::<Vec<_>>();
    let key = metadata_shard_key(&file_system.file_system_id, "root");
    let mut desired = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id,
        replica_node_ids,
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let Some(current) =
        sinomo_network::read_cluster_kv_value::<DfsMetadataShardConfig>(cluster_paths, &key)?
    else {
        return Ok(desired);
    };
    if metadata_shard_layout_matches(&current, &desired) {
        return Ok(current);
    }
    desired.epoch = current.epoch.saturating_add(1);
    Ok(desired)
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
        self.send(&target_node_id, frame)
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
        if &frame.source_node_id != self.local_node_id() {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: frame.source_node_id,
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

    fn drain_incoming(&mut self) -> Result<Vec<InboundFrame>, NetworkError> {
        Ok(self.incoming_frames.drain(..).collect())
    }
}

pub fn publish_default_dfs_metadata_journal(
    paths: &DaemonPaths,
) -> Result<DaemonDfsMetadataReplicationReport, DaemonError> {
    publish_default_dfs_metadata_journal_with_sender(
        paths,
        Arc::new(send_dfs_metadata_journal_batch_to_endpoint),
    )
}

fn publish_default_dfs_metadata_journal_with_sender(
    paths: &DaemonPaths,
    sender: DfsMetadataJournalBatchSender,
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
            let after_sequence = migrate_legacy_dfs_metadata_replication_cursor(
                &mut backend,
                &data_dir,
                &target_node_id,
                &shard_id,
            )?;
            let Some(batch) = backend.journal_batch_since_limited(
                after_sequence,
                DFS_METADATA_REPLICATION_MAX_BATCH_ENTRIES,
                DFS_METADATA_REPLICATION_MAX_BATCH_BYTES,
            )?
            else {
                outcomes.push(DaemonDfsMetadataReplicationOutcome::NoChanges {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
                continue;
            };
            let target_endpoints = endpoints
                .iter()
                .filter(|endpoint| endpoint.node_id == target_node_id)
                .collect::<Vec<_>>();
            if target_endpoints.is_empty() {
                outcomes.push(DaemonDfsMetadataReplicationOutcome::MissingEndpoint {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
                continue;
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
            for endpoint in target_endpoints {
                match sender(endpoint, &command, DFS_METADATA_REPLICATION_TIMEOUT) {
                    Ok(response) => match response.result {
                        DfsMetadataJournalBatchResult::Applied => {
                            backend.put_metadata_replication_cursor(
                                &target_node_id,
                                &shard_id,
                                next_after_sequence,
                            )?;
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
                            outcomes.push(DaemonDfsMetadataReplicationOutcome::Rejected {
                                target_node_id: target_node_id.clone(),
                                shard_id: shard_id.clone(),
                                reason,
                            });
                            delivered = true;
                            break;
                        }
                    },
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
            }
        }
    }
    Ok(DaemonDfsMetadataReplicationReport { outcomes })
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
    let metadata = LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )?;
    let mut service = NetworkedDfsService::new(
        DaemonDfsEndpointTransport::new(state.local_node_id.clone(), endpoints),
        content,
    )
    .with_metadata_backend(metadata)?;
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
        let file_system = distributed_sifs(node_id.clone());
        let root_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_id.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let services_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: node_id.clone(),
            replica_node_ids: Vec::new(),
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

        let status = handle_daemon_dfs_status(&paths).expect("dfs status");

        assert_eq!(status.file_system_count, 1);
        assert_eq!(status.object_count, 1);
        assert_eq!(status.version_count, 1);

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
    fn daemon_dfs_protocol_frame_processes_stage_blob_and_returns_response_frame() {
        let root = temp_root("daemon_dfs_protocol_frame_stage_blob");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let owner = NodeId::from_legacy_named("node_dfs_protocol_owner").expect("owner");
        let replica = NodeId::from_legacy_named("node_dfs_protocol_replica").expect("replica");
        write_test_cluster_state(&data_root, &replica);
        let file_system_id = sinomo_dfs::sifs_file_system_id();
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
        let old_primary = stable_dfs_service_node_ids(&state)
            .into_iter()
            .rev()
            .find(|node_id| node_id != &expected_primary)
            .expect("remote primary");
        let old_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: old_primary,
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

        let migrated = default_cluster_root_metadata_shard(&cluster_paths, &state, &file_system)
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
        let stable = default_cluster_root_metadata_shard(&cluster_paths, &state, &file_system)
            .expect("stable shard");
        assert_eq!(stable, migrated);

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
