use crate::{
    DaemonError, DaemonPaths, DaemonRunOptions, cluster_has_local_quorum,
    put_cluster_kv_control_record, send_dfs_metadata_journal_batch_to_endpoint,
    send_dfs_protocol_frame_to_endpoint,
};
use serde::{Deserialize, Serialize};
use sinomo_core::{ClusterEndpointRecord, DfsObjectPolicy};
use sinomo_dfs::{
    DfsExposeStore, DfsFileSystemConfig, DfsMetadataBackend, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNodeAdvertisement, DfsObjectVersionRecord,
    DfsProtocolFrameCommand, DfsProtocolFrameResponse, LocalContentStore, LocalJsonMetadataBackend,
    NetworkedDfsService, content_service_key, distributed_sifs, distributed_sifs_bootstrap_objects,
    fallback_dfs_node_id, file_system_config_key, metadata_service_key, metadata_shard_key,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{ClusterPaths, InboundFrame, NetworkError, NetworkFrame, NetworkTransport};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;

const DFS_METADATA_REPLICATION_TIMEOUT: Duration = Duration::from_secs(3);
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DaemonDfsMetadataReplicationReport {
    pub outcomes: Vec<DaemonDfsMetadataReplicationOutcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DaemonDfsMetadataReplicationOutcome {
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
struct DaemonDfsMetadataReplicationCursor {
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
            .join(command.batch.shard_id.0.clone()),
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
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let mut backend = LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )?;
    if backend.get_file_system(&file_system.file_system_id)? != Some(file_system.clone()) {
        backend.put_file_system(&file_system)?;
    }
    for object in distributed_sifs_bootstrap_objects(state.local_node_id.clone()) {
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
    if !publish_cluster_control || !cluster_has_local_quorum(&cluster_paths, &state) {
        return Ok(());
    }

    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        file_system_config_key(&file_system.file_system_id),
        &DfsFileSystemConfig::from(&file_system),
    )?;
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        metadata_shard_key(&file_system.file_system_id, "root"),
        &DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: state.local_node_id.clone(),
            replica_node_ids: state
                .members
                .iter()
                .filter(|member| member.node_id != state.local_node_id)
                .take(2)
                .map(|member| member.node_id.clone())
                .collect(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    )?;
    let endpoint = format!("loopback://{}:{}", options.host, options.port);
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        metadata_service_key(&state.local_node_id),
        &DfsNodeAdvertisement::metadata(state.local_node_id.clone(), endpoint.clone()),
    )?;
    put_cluster_kv_control_record(
        &cluster_paths,
        &state.local_node_id,
        content_service_key(&state.local_node_id),
        &DfsNodeAdvertisement::content(state.local_node_id.clone(), endpoint),
    )?;
    Ok(())
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
        for endpoints in endpoints_by_node_id.values_mut() {
            *endpoints =
                sinomo_network::prefer_bounded_daemon_command_targets(std::mem::take(endpoints));
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
    let store = sinomo_network::read_cluster_kv_store(&cluster_paths)?;
    let Some(entry) = store.get(&metadata_shard_key(&file_system.file_system_id, "root")) else {
        return Ok(DaemonDfsMetadataReplicationReport {
            outcomes: Vec::new(),
        });
    };
    let shard: DfsMetadataShardConfig = entry.decode()?;
    if shard.primary_node_id != state.local_node_id {
        return Ok(DaemonDfsMetadataReplicationReport {
            outcomes: Vec::new(),
        });
    }

    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let backend = LocalJsonMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join(&shard.shard_id),
    )?;
    let endpoints = sinomo_network::cluster_sync_targets_with_connectivity(&cluster_paths, &state);
    let mut outcomes = Vec::new();
    for target_node_id in shard
        .replica_node_ids
        .into_iter()
        .filter(|target_node_id| target_node_id != &state.local_node_id)
    {
        let cursor_path =
            dfs_metadata_replication_cursor_path(&data_dir, &target_node_id, &shard_id);
        let after_sequence = read_dfs_metadata_replication_cursor(&cursor_path)?
            .map(|cursor| cursor.next_after_sequence)
            .unwrap_or(0);
        let entries =
            backend.hydrate_journal_entry_payloads(backend.journal_since(after_sequence)?)?;
        let Some(last_entry) = entries.last() else {
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
        let next_after_sequence = last_entry.sequence;
        let entry_count = entries.len();
        let command = DfsMetadataJournalBatchCommand {
            request_id: format!(
                "dfs_meta_{}_{}_{}",
                state.local_node_id, target_node_id, next_after_sequence
            ),
            batch: sinomo_dfs::DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                entries,
                next_after_sequence,
            },
        };
        let mut failures = Vec::new();
        let mut delivered = false;
        for endpoint in target_endpoints {
            match send_dfs_metadata_journal_batch_to_endpoint(
                endpoint,
                &command,
                DFS_METADATA_REPLICATION_TIMEOUT,
            ) {
                Ok(response) => match response.result {
                    DfsMetadataJournalBatchResult::Applied => {
                        write_dfs_metadata_replication_cursor(
                            &cursor_path,
                            &DaemonDfsMetadataReplicationCursor {
                                target_node_id: target_node_id.clone(),
                                shard_id: shard_id.clone(),
                                next_after_sequence,
                            },
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
    Ok(DaemonDfsMetadataReplicationReport { outcomes })
}

fn daemon_data_dir(paths: &DaemonPaths) -> Result<&std::path::Path, DaemonError> {
    paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon root has no data directory parent".to_string(),
    })
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

fn dfs_metadata_replication_cursor_path(
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

fn read_dfs_metadata_replication_cursor(
    path: &std::path::Path,
) -> Result<Option<DaemonDfsMetadataReplicationCursor>, DaemonError> {
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

fn write_dfs_metadata_replication_cursor(
    path: &std::path::Path,
    cursor: &DaemonDfsMetadataReplicationCursor,
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
        DfsProtocolFrameCommand, DfsStageBlobCommand, DfsVersion,
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
