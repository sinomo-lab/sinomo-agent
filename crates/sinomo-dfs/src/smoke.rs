use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ClusterPaths;
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use time::OffsetDateTime;

use crate::DfsMessageTransport;
use crate::{
    DfsControlPlaneKvClient, DfsError, DfsFileSystemId, DfsMessageSender,
    DfsMetadataEndpointTransport, DfsMetadataJournalBatchSender, DfsMetadataJournalPublisherHost,
    DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationReport,
    DfsMetadataJournalReplicationTransport, DfsMetadataShardConfig,
    DfsMetadataSnapshotInstallSender, DfsNodeAdvertisement, DfsObjectId, DfsPosixError, DfsVersion,
    DfsWriteOptions, LocalDfsMetadataJournalPublisherHost, NetworkedDfsService,
    NetworkedDfsServiceEvent, cluster_dfs_data_endpoints, cluster_dfs_metadata_shards,
    content_services_prefix, distributed_sifs, inspect_cluster_aware_dfs_status,
    open_cluster_dfs_service_with_message_sender, publish_dfs_metadata_journal_with_host,
};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsSmokeReport {
    pub path: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub size_bytes: Option<u64>,
    pub requested_replica_node_ids: Vec<NodeId>,
    pub committed_replica_node_ids: Vec<NodeId>,
    pub pending_repair_count: usize,
    pub metadata_replication: DfsMetadataJournalReplicationReport,
    pub attempts: Vec<DfsSmokeAttempt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsSmokeAttempt {
    pub replica_node_ids: Vec<NodeId>,
    pub path: String,
    pub committed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsSmokeOptions {
    pub nonce: Option<i128>,
    pub metadata_replication: DfsMetadataJournalReplicationOptions,
}

impl Default for DfsSmokeOptions {
    fn default() -> Self {
        Self {
            nonce: None,
            metadata_replication: DfsMetadataJournalReplicationOptions::default(),
        }
    }
}

pub trait DfsSmokeHost {
    fn pending_repair_count(&mut self) -> Result<usize, DfsError>;
}

#[derive(Clone, Debug)]
pub struct LocalDfsSmokeHost {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
}

impl LocalDfsSmokeHost {
    pub fn new(data_dir: SinomoDataDir, cluster_paths: ClusterPaths) -> Self {
        Self {
            data_dir,
            cluster_paths,
        }
    }
}

impl DfsSmokeHost for LocalDfsSmokeHost {
    fn pending_repair_count(&mut self) -> Result<usize, DfsError> {
        inspect_cluster_aware_dfs_status(&self.data_dir, &self.cluster_paths)
            .map(|status| status.pending_repair_count)
    }
}

#[cfg(test)]
pub(crate) fn run_cluster_dfs_smoke(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    message_sender: DfsMessageSender,
    metadata_sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsSmokeOptions,
) -> Result<Option<DfsSmokeReport>, DfsError> {
    run_cluster_dfs_smoke_with_message_sender(
        data_dir,
        cluster_paths,
        message_sender,
        metadata_sender,
        snapshot_sender,
        control_plane,
        options,
    )
}

pub(crate) fn run_cluster_dfs_smoke_with_message_sender(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    message_sender: DfsMessageSender,
    metadata_sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsSmokeOptions,
) -> Result<Option<DfsSmokeReport>, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if !cluster_paths.state.exists() {
        return Ok(None);
    }

    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    let local_node_id = state.local_node_id.clone();
    let content_advertisements = sinomo_core::kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        content_services_prefix(),
    )
    .map_err(DfsError::from)?
    .into_iter()
    .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok())
    .collect::<Vec<_>>();
    let content_nodes = dfs_smoke_content_node_ids(
        &local_node_id,
        content_advertisements,
        OffsetDateTime::now_utc(),
    );

    let file_system = distributed_sifs(local_node_id.clone());
    let metadata_shards = cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?;
    let endpoints = cluster_dfs_data_endpoints(cluster_paths, &state);
    let routable_node_ids = endpoints
        .iter()
        .map(|endpoint| endpoint.node_id.clone())
        .collect::<Vec<_>>();
    let mut service = open_cluster_dfs_service_with_message_sender(
        data_dir,
        cluster_paths,
        local_node_id.clone(),
        endpoints.clone(),
        message_sender,
        control_plane,
    )?;
    let mut smoke_host = LocalDfsSmokeHost::new(data_dir.clone(), cluster_paths.clone());
    let mut metadata_publisher_host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let mut metadata_transport =
        DfsMetadataEndpointTransport::new(endpoints, metadata_sender, snapshot_sender);
    run_dfs_smoke_with_host(
        &mut service,
        &local_node_id,
        content_nodes,
        routable_node_ids,
        &mut smoke_host,
        file_system.file_system_id,
        metadata_shards,
        &mut metadata_publisher_host,
        &mut metadata_transport,
        options,
    )
    .map(Some)
}

const DFS_SMOKE_CONTENT_ADVERTISEMENT_MAX_AGE_SECONDS: i64 = 60 * 60;

pub(crate) fn dfs_smoke_content_node_ids(
    local_node_id: &NodeId,
    advertisements: Vec<DfsNodeAdvertisement>,
    now: OffsetDateTime,
) -> Vec<NodeId> {
    let mut content_nodes = Vec::new();
    let mut fresh_remote_content_nodes = Vec::new();
    for advertisement in advertisements {
        if !advertisement.accepts_content {
            continue;
        }
        if advertisement.node_id != *local_node_id
            && dfs_smoke_content_advertisement_is_fresh(&advertisement, now)
        {
            fresh_remote_content_nodes.push(advertisement.node_id.clone());
        }
        content_nodes.push(advertisement.node_id);
    }

    let mut selected = if fresh_remote_content_nodes.is_empty() {
        content_nodes
    } else {
        fresh_remote_content_nodes
    };
    selected.sort();
    selected.dedup();
    selected
}

fn dfs_smoke_content_advertisement_is_fresh(
    advertisement: &DfsNodeAdvertisement,
    now: OffsetDateTime,
) -> bool {
    advertisement.accepts_content
        && (advertisement.updated_at >= now
            || now - advertisement.updated_at
                <= time::Duration::seconds(DFS_SMOKE_CONTENT_ADVERTISEMENT_MAX_AGE_SECONDS))
}

pub fn run_dfs_smoke_with_host<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    local_node_id: &NodeId,
    content_node_ids: Vec<NodeId>,
    routable_node_ids: Vec<NodeId>,
    smoke_host: &mut impl DfsSmokeHost,
    file_system_id: DfsFileSystemId,
    metadata_shards: Vec<DfsMetadataShardConfig>,
    metadata_publisher_host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsSmokeOptions,
) -> Result<DfsSmokeReport, DfsError> {
    let replica_candidate_sets =
        dfs_smoke_replica_candidate_sets(local_node_id, content_node_ids, routable_node_ids);
    service
        .bootstrap_distributed_sifs()
        .map_err(dfs_posix_error)?;
    service
        .posix_mkdir_all("/services/dfs-smoke")
        .map_err(dfs_posix_error)?;

    let nonce = options
        .nonce
        .unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp_nanos());
    let mut attempts = Vec::new();
    let mut successful = None;
    for replica_candidates in replica_candidate_sets {
        let target_label = replica_candidates
            .iter()
            .map(|node_id| node_id.to_string())
            .collect::<Vec<_>>()
            .join("-");
        let path = format!("/services/dfs-smoke/{nonce}-{target_label}.txt");
        let body = format!("sinomo dfs smoke {nonce} {target_label}\n");
        match service
            .posix_write_file(
                &path,
                body.as_bytes(),
                DfsWriteOptions::replicated_eventual(replica_candidates.clone()),
            )
            .map_err(dfs_posix_error)
        {
            Ok(stat) => {
                attempts.push(DfsSmokeAttempt {
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
                attempts.push(DfsSmokeAttempt {
                    replica_node_ids: replica_candidates,
                    path,
                    committed: false,
                    detail,
                });
            }
        }
    }
    let Some((stat, replica_candidates)) = successful else {
        return Err(DfsError::Distributed {
            message: format!(
                "DFS smoke did not commit on any candidate replica set: {}",
                attempts
                    .iter()
                    .map(|attempt| format!(
                        "{} => {}",
                        attempt
                            .replica_node_ids
                            .iter()
                            .map(|node_id| node_id.to_string())
                            .collect::<Vec<_>>()
                            .join(","),
                        attempt.detail
                    ))
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        });
    };
    let pending_repair_count = smoke_host.pending_repair_count()?;
    let metadata_replication = publish_dfs_metadata_journal_with_host(
        file_system_id,
        local_node_id,
        metadata_shards,
        metadata_publisher_host,
        metadata_transport,
        options.metadata_replication,
    )?;

    Ok(DfsSmokeReport {
        path: stat.path,
        file_system_id: stat.file_system_id,
        object_id: stat.object_id,
        version: stat.version,
        content_hash: stat.content_hash,
        size_bytes: stat.size_bytes,
        requested_replica_node_ids: replica_candidates,
        committed_replica_node_ids: stat.current_locations,
        pending_repair_count,
        metadata_replication,
        attempts,
    })
}

pub(crate) fn dfs_smoke_replica_candidate_sets(
    local_node_id: &NodeId,
    mut content_node_ids: Vec<NodeId>,
    routable_node_ids: Vec<NodeId>,
) -> Vec<Vec<NodeId>> {
    content_node_ids.sort();
    content_node_ids.dedup();
    let content_node_ids = content_node_ids.into_iter().collect::<BTreeSet<_>>();
    let mut seen_remote_candidates = BTreeSet::new();
    let mut remote_candidates = Vec::new();
    for node_id in routable_node_ids {
        if node_id == *local_node_id
            || !content_node_ids.contains(&node_id)
            || !seen_remote_candidates.insert(node_id.clone())
        {
            continue;
        }
        remote_candidates.push(node_id);
    }
    if remote_candidates.is_empty() {
        vec![vec![local_node_id.clone()]]
    } else {
        remote_candidates
            .into_iter()
            .map(|remote_node_id| vec![local_node_id.clone(), remote_node_id])
            .collect()
    }
}

fn dfs_event_summary(events: &[NetworkedDfsServiceEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            NetworkedDfsServiceEvent::BlobStageRejected {
                replica_node_id,
                reason,
                ..
            } => Some(format!("stage rejected by {replica_node_id}: {reason}")),
            NetworkedDfsServiceEvent::BlobFetchRejected {
                source_node_id,
                reason,
                ..
            } => Some(format!("fetch rejected by {source_node_id}: {reason}")),
            NetworkedDfsServiceEvent::RepairRejected {
                replica_node_id,
                reason,
                ..
            } => Some(format!("repair rejected by {replica_node_id}: {reason}")),
            NetworkedDfsServiceEvent::RepairRetryFailed { reason } => {
                Some(format!("repair retry failed: {reason}"))
            }
            _ => None,
        })
        .collect()
}

fn dfs_posix_error(error: DfsPosixError) -> DfsError {
    DfsError::Metadata {
        message: error.to_string(),
    }
}
