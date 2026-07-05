//! Default SIFS and metadata bootstrap for cluster-backed DFS.
//!
//! This module owns the default distributed SIFS topology, shard layout, and
//! service advertisements that make a cluster's DFS usable after bootstrap.

use serde::Serialize;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterPaths, cluster_healthy_route_next_hop,
};
use sinomo_core::node::NodeId;
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord,
    ClusterMemberRole, DfsObjectPolicy,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use time::OffsetDateTime;

use crate::{
    AsyncDfsControlPlaneKvClient, DfsControlPlaneKvClient, DfsError, DfsFileSystem,
    DfsFileSystemConfig, DfsFileSystemId, DfsMessageSender, DfsMetadataBackend,
    DfsMetadataJournalBatchSender, DfsMetadataShardAssignment, DfsMetadataShardConfig,
    DfsMetadataShardId, DfsMetadataSnapshotInstallSender, DfsNodeAdvertisement, DfsPeerTransport,
    DfsSmokeOptions, DfsSmokeReport, DfsSubtreePolicy, SIFS_SERVICES_PATH, SIFS_USERS_PATH,
    content_service_key, default_dfs_metadata_shards_root, dfs_message_sender_from_peer_transport,
    dfs_metadata_journal_batch_sender_from_peer_transport,
    dfs_metadata_snapshot_install_sender_from_peer_transport, distributed_sifs,
    file_system_config_key, materialize_default_sifs_metadata,
    materialize_default_sifs_metadata_on_shards, metadata_service_key, metadata_shard_key,
    open_cluster_aware_dfs_metadata_backend, open_local_metadata_shard_backend,
    open_local_sharded_metadata_backend, run_cluster_dfs_smoke_with_message_sender,
    sifs_node_files_path,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDefaultClusterBootstrapOptions {
    pub publish_cluster_control: bool,
    pub materialize_local_metadata: bool,
    pub local_endpoint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDefaultClusterSmokeOptions {
    pub bootstrap: DfsDefaultClusterBootstrapOptions,
    pub smoke: DfsSmokeOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDefaultClusterBootstrapReport {
    pub file_system: DfsFileSystem,
    pub metadata_shards: Vec<DfsMetadataShardConfig>,
    pub local_has_primary_metadata_shards: bool,
}

pub trait DfsDefaultClusterBootstrapHost {
    fn connectivity_graph(
        &self,
        state: &ClusterBootstrapState,
    ) -> Option<ClusterConnectivityGraph> {
        let _ = state;
        None
    }

    fn metadata_service_node_ids(
        &self,
        state: &ClusterBootstrapState,
        service_node_ids: Vec<NodeId>,
    ) -> Vec<NodeId> {
        let _ = state;
        service_node_ids
    }

    fn current_metadata_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError>;

    fn metadata_shard_primary_transfer_ready(
        &mut self,
        current: &DfsMetadataShardConfig,
        target_primary_node_id: &NodeId,
    ) -> Result<bool, DfsError>;

    fn put_file_system_config(&mut self, config: &DfsFileSystemConfig) -> Result<(), DfsError>;

    fn put_metadata_shard(&mut self, shard: &DfsMetadataShardConfig) -> Result<(), DfsError>;

    fn current_node_advertisement(
        &self,
        key: &str,
    ) -> Result<Option<DfsNodeAdvertisement>, DfsError>;

    fn put_node_advertisement(
        &mut self,
        key: String,
        advertisement: &DfsNodeAdvertisement,
    ) -> Result<(), DfsError>;

    fn materialize_default_sifs_metadata(
        &mut self,
        file_system: &DfsFileSystem,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> Result<(), DfsError>;

    fn materialize_default_sifs_metadata_on_shards(
        &mut self,
        file_system: &DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> Result<(), DfsError>;
}

pub type DfsDefaultClusterBootstrapFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DfsError>> + Send + 'a>>;

pub trait DfsDefaultClusterAsyncBootstrapHost: Send {
    fn connectivity_graph(
        &self,
        state: &ClusterBootstrapState,
    ) -> Option<ClusterConnectivityGraph> {
        let _ = state;
        None
    }

    fn metadata_service_node_ids(
        &self,
        state: &ClusterBootstrapState,
        service_node_ids: Vec<NodeId>,
    ) -> Vec<NodeId> {
        let _ = state;
        service_node_ids
    }

    fn current_metadata_shard_async<'a>(
        &'a mut self,
        file_system_id: DfsFileSystemId,
        shard_id: String,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Option<DfsMetadataShardConfig>>;

    fn metadata_shard_primary_transfer_ready_async<'a>(
        &'a mut self,
        current: DfsMetadataShardConfig,
        target_primary_node_id: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, bool>;

    fn put_file_system_config_async<'a>(
        &'a mut self,
        config: DfsFileSystemConfig,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()>;

    fn put_metadata_shard_async<'a>(
        &'a mut self,
        shard: DfsMetadataShardConfig,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()>;

    fn current_node_advertisement_async<'a>(
        &'a mut self,
        key: String,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Option<DfsNodeAdvertisement>>;

    fn put_node_advertisement_async<'a>(
        &'a mut self,
        key: String,
        advertisement: DfsNodeAdvertisement,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()>;

    fn materialize_default_sifs_metadata_async<'a>(
        &'a mut self,
        file_system: DfsFileSystem,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()>;

    fn materialize_default_sifs_metadata_on_shards_async<'a>(
        &'a mut self,
        file_system: DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()>;
}

#[derive(Debug)]
pub struct DfsControlPlaneDefaultClusterBootstrapHost {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    local_node_id: NodeId,
    control_plane: Box<dyn DfsControlPlaneKvClient>,
}

impl DfsControlPlaneDefaultClusterBootstrapHost {
    pub fn new(
        data_dir: SinomoDataDir,
        cluster_paths: ClusterPaths,
        local_node_id: NodeId,
        control_plane: Box<dyn DfsControlPlaneKvClient>,
    ) -> Self {
        Self {
            data_dir,
            cluster_paths,
            local_node_id,
            control_plane,
        }
    }

    fn put_serde_if_changed<T>(&mut self, key: String, value: &T) -> Result<(), DfsError>
    where
        T: Serialize,
    {
        let encoded = sinomo_core::kv::encode_kv_value(value)?;
        if self
            .control_plane
            .get(&key)?
            .is_some_and(|entry| entry.value == encoded)
        {
            return Ok(());
        }
        self.control_plane.put(key, encoded)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    local_node_id: NodeId,
    control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
}

impl DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    pub(crate) fn new(
        data_dir: SinomoDataDir,
        cluster_paths: ClusterPaths,
        local_node_id: NodeId,
        control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
    ) -> Self {
        Self {
            data_dir,
            cluster_paths,
            local_node_id,
            control_plane,
        }
    }

    async fn put_serde_if_changed<T>(&mut self, key: String, value: &T) -> Result<(), DfsError>
    where
        T: Serialize,
    {
        let encoded = sinomo_core::kv::encode_kv_value(value)?;
        if self
            .control_plane
            .get_async(&key)
            .await?
            .is_some_and(|entry| entry.value == encoded)
        {
            return Ok(());
        }
        self.control_plane.put_async(key, encoded).await?;
        Ok(())
    }
}

impl DfsDefaultClusterBootstrapHost for DfsControlPlaneDefaultClusterBootstrapHost {
    fn connectivity_graph(
        &self,
        state: &ClusterBootstrapState,
    ) -> Option<ClusterConnectivityGraph> {
        ClusterConnectivityGraph::try_read_for_local(&self.cluster_paths, state).ok()
    }

    fn metadata_service_node_ids(
        &self,
        state: &ClusterBootstrapState,
        service_node_ids: Vec<NodeId>,
    ) -> Vec<NodeId> {
        active_metadata_service_node_ids(&self.cluster_paths, state, service_node_ids)
    }

    fn current_metadata_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let key = metadata_shard_key(file_system_id, shard_id);
        self.control_plane
            .get(&key)?
            .map(|entry| {
                entry
                    .decode::<DfsMetadataShardConfig>()
                    .map_err(DfsError::from)
            })
            .transpose()
    }

    fn metadata_shard_primary_transfer_ready(
        &mut self,
        current: &DfsMetadataShardConfig,
        target_primary_node_id: &NodeId,
    ) -> Result<bool, DfsError> {
        if metadata_shard_primary_transfer_ready_from_local_backend(
            &self.data_dir,
            &self.local_node_id,
            current,
            target_primary_node_id,
        )? {
            return Ok(true);
        }
        if !metadata_shard_primary_route_unhealthy(
            &self.cluster_paths,
            &self.local_node_id,
            &current.primary_node_id,
        ) {
            return Ok(false);
        }
        metadata_shard_local_replica_failover_ready_from_local_backend(
            &self.data_dir,
            &self.local_node_id,
            current,
            target_primary_node_id,
        )
    }

    fn put_file_system_config(&mut self, config: &DfsFileSystemConfig) -> Result<(), DfsError> {
        self.put_serde_if_changed(file_system_config_key(&config.file_system_id), config)
    }

    fn put_metadata_shard(&mut self, shard: &DfsMetadataShardConfig) -> Result<(), DfsError> {
        self.put_serde_if_changed(
            metadata_shard_key(&shard.file_system_id, &shard.shard_id),
            shard,
        )
    }

    fn current_node_advertisement(
        &self,
        key: &str,
    ) -> Result<Option<DfsNodeAdvertisement>, DfsError> {
        self.control_plane
            .get(key)?
            .map(|entry| {
                entry
                    .decode::<DfsNodeAdvertisement>()
                    .map_err(DfsError::from)
            })
            .transpose()
    }

    fn put_node_advertisement(
        &mut self,
        key: String,
        advertisement: &DfsNodeAdvertisement,
    ) -> Result<(), DfsError> {
        self.put_serde_if_changed(key, advertisement)
    }

    fn materialize_default_sifs_metadata(
        &mut self,
        file_system: &DfsFileSystem,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> Result<(), DfsError> {
        let mut backend =
            open_cluster_aware_dfs_metadata_backend(&self.data_dir, &self.cluster_paths)?;
        materialize_default_sifs_metadata(
            backend.as_mut(),
            file_system,
            bootstrap_owner_node_ids,
            fallback_owner_node_id,
            OffsetDateTime::now_utc(),
        )
    }

    fn materialize_default_sifs_metadata_on_shards(
        &mut self,
        file_system: &DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> Result<(), DfsError> {
        let mut backend = open_local_sharded_metadata_backend(
            default_dfs_metadata_shards_root(&self.data_dir),
            shards,
        )?;
        materialize_default_sifs_metadata_on_shards(
            &mut backend,
            file_system,
            bootstrap_owner_node_ids,
            fallback_owner_node_id,
            OffsetDateTime::now_utc(),
        )
    }
}

impl DfsDefaultClusterAsyncBootstrapHost for DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    fn connectivity_graph(
        &self,
        state: &ClusterBootstrapState,
    ) -> Option<ClusterConnectivityGraph> {
        ClusterConnectivityGraph::try_read_for_local(&self.cluster_paths, state).ok()
    }

    fn metadata_service_node_ids(
        &self,
        state: &ClusterBootstrapState,
        service_node_ids: Vec<NodeId>,
    ) -> Vec<NodeId> {
        active_metadata_service_node_ids(&self.cluster_paths, state, service_node_ids)
    }

    fn current_metadata_shard_async<'a>(
        &'a mut self,
        file_system_id: DfsFileSystemId,
        shard_id: String,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Option<DfsMetadataShardConfig>> {
        Box::pin(async move {
            let key = metadata_shard_key(&file_system_id, &shard_id);
            self.control_plane
                .get_async(&key)
                .await?
                .map(|entry| {
                    entry
                        .decode::<DfsMetadataShardConfig>()
                        .map_err(DfsError::from)
                })
                .transpose()
        })
    }

    fn metadata_shard_primary_transfer_ready_async<'a>(
        &'a mut self,
        current: DfsMetadataShardConfig,
        target_primary_node_id: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, bool> {
        Box::pin(async move {
            if metadata_shard_primary_transfer_ready_from_local_backend(
                &self.data_dir,
                &self.local_node_id,
                &current,
                &target_primary_node_id,
            )? {
                return Ok(true);
            }
            if !metadata_shard_primary_route_unhealthy(
                &self.cluster_paths,
                &self.local_node_id,
                &current.primary_node_id,
            ) {
                return Ok(false);
            }
            metadata_shard_local_replica_failover_ready_from_local_backend(
                &self.data_dir,
                &self.local_node_id,
                &current,
                &target_primary_node_id,
            )
        })
    }

    fn put_file_system_config_async<'a>(
        &'a mut self,
        config: DfsFileSystemConfig,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move {
            self.put_serde_if_changed(file_system_config_key(&config.file_system_id), &config)
                .await
        })
    }

    fn put_metadata_shard_async<'a>(
        &'a mut self,
        shard: DfsMetadataShardConfig,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move {
            self.put_serde_if_changed(
                metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                &shard,
            )
            .await
        })
    }

    fn current_node_advertisement_async<'a>(
        &'a mut self,
        key: String,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Option<DfsNodeAdvertisement>> {
        Box::pin(async move {
            self.control_plane
                .get_async(&key)
                .await?
                .map(|entry| {
                    entry
                        .decode::<DfsNodeAdvertisement>()
                        .map_err(DfsError::from)
                })
                .transpose()
        })
    }

    fn put_node_advertisement_async<'a>(
        &'a mut self,
        key: String,
        advertisement: DfsNodeAdvertisement,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move { self.put_serde_if_changed(key, &advertisement).await })
    }

    fn materialize_default_sifs_metadata_async<'a>(
        &'a mut self,
        file_system: DfsFileSystem,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move {
            let mut backend =
                open_cluster_aware_dfs_metadata_backend(&self.data_dir, &self.cluster_paths)?;
            materialize_default_sifs_metadata(
                backend.as_mut(),
                &file_system,
                bootstrap_owner_node_ids,
                fallback_owner_node_id,
                OffsetDateTime::now_utc(),
            )
        })
    }

    fn materialize_default_sifs_metadata_on_shards_async<'a>(
        &'a mut self,
        file_system: DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move {
            let mut backend = open_local_sharded_metadata_backend(
                default_dfs_metadata_shards_root(&self.data_dir),
                shards.clone(),
            )?;
            materialize_default_sifs_metadata_on_shards(
                &mut backend,
                &file_system,
                bootstrap_owner_node_ids,
                fallback_owner_node_id,
                OffsetDateTime::now_utc(),
            )
        })
    }
}

pub fn metadata_shard_primary_transfer_ready_from_local_backend(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    current: &DfsMetadataShardConfig,
    target_primary_node_id: &NodeId,
) -> Result<bool, DfsError> {
    if current.primary_node_id == *target_primary_node_id {
        return Ok(true);
    }
    if current.primary_node_id != *local_node_id {
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
    let backend = open_local_metadata_shard_backend(data_dir, shard_id.clone())?;
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

fn metadata_shard_local_replica_failover_ready_from_local_backend(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    current: &DfsMetadataShardConfig,
    target_primary_node_id: &NodeId,
) -> Result<bool, DfsError> {
    if target_primary_node_id != local_node_id {
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
    let backend = open_local_metadata_shard_backend(data_dir, shard_id.clone())?;
    Ok(backend
        .metadata_shard_statuses()?
        .into_iter()
        .any(|status| status.shard_id == shard_id))
}

fn metadata_shard_primary_route_unhealthy(
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    primary_node_id: &NodeId,
) -> bool {
    if primary_node_id == local_node_id {
        return false;
    }
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths) else {
        return false;
    };
    let Ok(graph) = ClusterConnectivityGraph::try_read_for_local(cluster_paths, &state) else {
        return false;
    };
    cluster_healthy_route_next_hop(&graph, local_node_id, primary_node_id).is_none()
}

fn active_metadata_service_node_ids(
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    mut service_node_ids: Vec<NodeId>,
) -> Vec<NodeId> {
    if service_node_ids.len() <= 1 {
        return service_node_ids;
    }
    let Ok(graph) = ClusterConnectivityGraph::try_read_for_local(cluster_paths, state) else {
        return service_node_ids;
    };
    let mut active = service_node_ids
        .iter()
        .filter(|node_id| {
            **node_id == state.local_node_id
                || cluster_healthy_route_next_hop(&graph, &state.local_node_id, node_id).is_some()
        })
        .cloned()
        .collect::<Vec<_>>();
    active.sort();
    active.dedup();
    if active.is_empty() {
        service_node_ids.sort();
        service_node_ids.dedup();
        service_node_ids
    } else {
        active
    }
}

pub fn ensure_default_distributed_dfs_with_host(
    state: &ClusterBootstrapState,
    host: &mut impl DfsDefaultClusterBootstrapHost,
    options: DfsDefaultClusterBootstrapOptions,
) -> Result<DfsDefaultClusterBootstrapReport, DfsError> {
    let file_system = default_cluster_sifs(state);
    let mut metadata_shards = Vec::new();
    let local_has_primary_metadata_shards = if options.publish_cluster_control {
        host.put_file_system_config(&DfsFileSystemConfig::from(&file_system))?;
        metadata_shards = default_cluster_metadata_shards_with_host(host, state, &file_system)?;
        let local_has_primary_metadata_shards = metadata_shards
            .iter()
            .any(|shard| shard.primary_node_id == state.local_node_id);
        let publishable_shards = metadata_shards.iter().filter(|shard| {
            local_has_primary_metadata_shards
                || matches!(shard.assignment, DfsMetadataShardAssignment::Root)
        });
        for shard in publishable_shards {
            host.put_metadata_shard(shard)?;
        }
        if local_has_primary_metadata_shards && options.materialize_local_metadata {
            host.materialize_default_sifs_metadata_on_shards(
                &file_system,
                metadata_shards.clone(),
                stable_dfs_service_node_ids(state),
                state.local_node_id.clone(),
            )?;
        }
        publish_dfs_service_advertisements_with_host(host, state, &options.local_endpoint)?;
        local_has_primary_metadata_shards
    } else {
        default_cluster_sifs_owner(state) == state.local_node_id
    };
    if local_has_primary_metadata_shards
        && !options.publish_cluster_control
        && options.materialize_local_metadata
    {
        host.materialize_default_sifs_metadata(
            &file_system,
            stable_dfs_service_node_ids(state),
            state.local_node_id.clone(),
        )?;
    }

    Ok(DfsDefaultClusterBootstrapReport {
        file_system,
        metadata_shards,
        local_has_primary_metadata_shards,
    })
}

pub async fn ensure_default_distributed_dfs_with_async_host(
    state: &ClusterBootstrapState,
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    options: DfsDefaultClusterBootstrapOptions,
) -> Result<DfsDefaultClusterBootstrapReport, DfsError> {
    let file_system = default_cluster_sifs(state);
    let mut metadata_shards = Vec::new();
    let local_has_primary_metadata_shards = if options.publish_cluster_control {
        host.put_file_system_config_async(DfsFileSystemConfig::from(&file_system))
            .await?;
        metadata_shards =
            default_cluster_metadata_shards_with_async_host(host, state, &file_system).await?;
        let local_has_primary_metadata_shards = metadata_shards
            .iter()
            .any(|shard| shard.primary_node_id == state.local_node_id);
        let publishable_shards = metadata_shards.iter().filter(|shard| {
            local_has_primary_metadata_shards
                || matches!(shard.assignment, DfsMetadataShardAssignment::Root)
        });
        for shard in publishable_shards {
            host.put_metadata_shard_async(shard.clone()).await?;
        }
        if local_has_primary_metadata_shards && options.materialize_local_metadata {
            host.materialize_default_sifs_metadata_on_shards_async(
                file_system.clone(),
                metadata_shards.clone(),
                stable_dfs_service_node_ids(state),
                state.local_node_id.clone(),
            )
            .await?;
        }
        publish_dfs_service_advertisements_with_async_host(host, state, &options.local_endpoint)
            .await?;
        local_has_primary_metadata_shards
    } else {
        default_cluster_sifs_owner(state) == state.local_node_id
    };
    if local_has_primary_metadata_shards
        && !options.publish_cluster_control
        && options.materialize_local_metadata
    {
        host.materialize_default_sifs_metadata_async(
            file_system.clone(),
            stable_dfs_service_node_ids(state),
            state.local_node_id.clone(),
        )
        .await?;
    }

    Ok(DfsDefaultClusterBootstrapReport {
        file_system,
        metadata_shards,
        local_has_primary_metadata_shards,
    })
}

pub fn ensure_default_cluster_dfs(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    control_plane: Box<dyn DfsControlPlaneKvClient>,
    options: DfsDefaultClusterBootstrapOptions,
) -> Result<Option<DfsDefaultClusterBootstrapReport>, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = if options.publish_cluster_control {
        sinomo_core::cluster::read_cluster_state(cluster_paths)
    } else {
        sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
    }
    .map_err(DfsError::from_network)?;
    let mut host = DfsControlPlaneDefaultClusterBootstrapHost::new(
        data_dir.clone(),
        cluster_paths.clone(),
        state.local_node_id.clone(),
        control_plane,
    );
    ensure_default_distributed_dfs_with_host(&state, &mut host, options).map(Some)
}

pub async fn ensure_default_cluster_dfs_async(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
    options: DfsDefaultClusterBootstrapOptions,
) -> Result<Option<DfsDefaultClusterBootstrapReport>, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = if options.publish_cluster_control {
        sinomo_core::cluster::read_cluster_state(&cluster_paths)
    } else {
        sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
    }
    .map_err(DfsError::from_network)?;
    let mut host = DfsControlPlaneDefaultClusterAsyncBootstrapHost::new(
        data_dir.clone(),
        cluster_paths.clone(),
        state.local_node_id.clone(),
        control_plane,
    );
    ensure_default_distributed_dfs_with_async_host(&state, &mut host, options)
        .await
        .map(Some)
}

#[cfg(test)]
pub(crate) fn run_default_cluster_dfs_smoke(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    message_sender: DfsMessageSender,
    metadata_sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    bootstrap_control_plane: Box<dyn DfsControlPlaneKvClient>,
    smoke_control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsDefaultClusterSmokeOptions,
) -> Result<Option<DfsSmokeReport>, DfsError> {
    run_default_cluster_dfs_smoke_with_message_sender(
        data_dir,
        cluster_paths,
        message_sender,
        metadata_sender,
        snapshot_sender,
        bootstrap_control_plane,
        smoke_control_plane,
        options,
    )
}

pub(crate) fn run_default_cluster_dfs_smoke_with_message_sender(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    message_sender: DfsMessageSender,
    metadata_sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    bootstrap_control_plane: Box<dyn DfsControlPlaneKvClient>,
    smoke_control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsDefaultClusterSmokeOptions,
) -> Result<Option<DfsSmokeReport>, DfsError> {
    if ensure_default_cluster_dfs(
        data_dir,
        cluster_paths,
        bootstrap_control_plane,
        options.bootstrap,
    )?
    .is_none()
    {
        return Ok(None);
    }
    run_cluster_dfs_smoke_with_message_sender(
        data_dir,
        cluster_paths,
        message_sender,
        metadata_sender,
        snapshot_sender,
        smoke_control_plane,
        options.smoke,
    )
}

pub fn run_default_cluster_dfs_smoke_with_peer_transport(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    bootstrap_control_plane: Box<dyn DfsControlPlaneKvClient>,
    smoke_control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsDefaultClusterSmokeOptions,
) -> Result<Option<DfsSmokeReport>, DfsError> {
    run_default_cluster_dfs_smoke_with_message_sender(
        data_dir,
        cluster_paths,
        dfs_message_sender_from_peer_transport(Arc::clone(&peer_transport)),
        dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        bootstrap_control_plane,
        smoke_control_plane,
        options,
    )
}

pub fn default_cluster_sifs(state: &ClusterBootstrapState) -> DfsFileSystem {
    let owner_node_id = default_cluster_sifs_owner(state);
    let mut file_system = distributed_sifs(owner_node_id.clone());
    file_system.owner_node_id = owner_node_id;
    file_system.subtree_policies = default_cluster_sifs_subtree_policies(state);
    file_system
}

pub fn default_cluster_sifs_owner(state: &ClusterBootstrapState) -> NodeId {
    stable_dfs_service_node_ids(state)
        .into_iter()
        .next()
        .unwrap_or_else(|| state.local_node_id.clone())
}

pub fn default_cluster_sifs_subtree_policies(
    state: &ClusterBootstrapState,
) -> Vec<DfsSubtreePolicy> {
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
        path_prefix: SIFS_USERS_PATH.to_string(),
        policy: DfsObjectPolicy::ReplicatedEventual,
    });
    policies.push(DfsSubtreePolicy {
        path_prefix: SIFS_SERVICES_PATH.to_string(),
        policy: DfsObjectPolicy::ReplicatedEventual,
    });
    policies
}

pub fn default_cluster_metadata_shards_with_host(
    host: &mut impl DfsDefaultClusterBootstrapHost,
    state: &ClusterBootstrapState,
    file_system: &DfsFileSystem,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    Ok(vec![
        default_cluster_metadata_shard_with_host(
            host,
            state,
            file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )?,
        default_cluster_metadata_shard_with_host(
            host,
            state,
            file_system,
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            1,
        )?,
        default_cluster_metadata_shard_with_host(
            host,
            state,
            file_system,
            "users",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_USERS_PATH.to_string(),
            },
            2,
        )?,
    ])
}

pub async fn default_cluster_metadata_shards_with_async_host(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    file_system: &DfsFileSystem,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    Ok(vec![
        default_cluster_metadata_shard_with_async_host(
            host,
            state,
            file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .await?,
        default_cluster_metadata_shard_with_async_host(
            host,
            state,
            file_system,
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            1,
        )
        .await?,
        default_cluster_metadata_shard_with_async_host(
            host,
            state,
            file_system,
            "users",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_USERS_PATH.to_string(),
            },
            2,
        )
        .await?,
    ])
}

pub fn default_cluster_metadata_shard_with_host(
    host: &mut impl DfsDefaultClusterBootstrapHost,
    state: &ClusterBootstrapState,
    file_system: &DfsFileSystem,
    shard_id: &str,
    assignment: DfsMetadataShardAssignment,
    primary_offset: usize,
) -> Result<DfsMetadataShardConfig, DfsError> {
    let service_node_ids =
        host.metadata_service_node_ids(state, stable_dfs_service_node_ids(state));
    let bootstrap_primary_node_id = default_cluster_sifs_owner(state);
    let target_primary_node_id = default_metadata_shard_primary_node_id(
        &service_node_ids,
        &bootstrap_primary_node_id,
        primary_offset,
    );
    let target_replica_node_ids =
        default_metadata_shard_replica_node_ids(&service_node_ids, &target_primary_node_id);
    let mut desired = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.to_string(),
        primary_node_id: target_primary_node_id,
        replica_node_ids: target_replica_node_ids,
        epoch: 1,
        assignment,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let Some(current) = host.current_metadata_shard(&file_system.file_system_id, shard_id)? else {
        desired.primary_node_id = bootstrap_primary_node_id.clone();
        desired.replica_node_ids =
            default_metadata_shard_replica_node_ids(&service_node_ids, &bootstrap_primary_node_id);
        return Ok(desired);
    };
    if current.primary_node_id != desired.primary_node_id
        && !host.metadata_shard_primary_transfer_ready(&current, &desired.primary_node_id)?
    {
        let mut expanded = desired.clone();
        expanded.primary_node_id = current.primary_node_id.clone();
        expanded.replica_node_ids =
            default_metadata_shard_replica_node_ids(&service_node_ids, &current.primary_node_id);
        if metadata_shard_layout_matches(&current, &expanded) {
            return Ok(current);
        }
        expanded.epoch = current.epoch.saturating_add(1);
        return Ok(expanded);
    }
    if metadata_shard_layout_matches(&current, &desired) {
        return Ok(current);
    }
    desired.epoch = current.epoch.saturating_add(1);
    Ok(desired)
}

pub async fn default_cluster_metadata_shard_with_async_host(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    file_system: &DfsFileSystem,
    shard_id: &str,
    assignment: DfsMetadataShardAssignment,
    primary_offset: usize,
) -> Result<DfsMetadataShardConfig, DfsError> {
    let service_node_ids =
        host.metadata_service_node_ids(state, stable_dfs_service_node_ids(state));
    let bootstrap_primary_node_id = default_cluster_sifs_owner(state);
    let target_primary_node_id = default_metadata_shard_primary_node_id(
        &service_node_ids,
        &bootstrap_primary_node_id,
        primary_offset,
    );
    let target_replica_node_ids =
        default_metadata_shard_replica_node_ids(&service_node_ids, &target_primary_node_id);
    let mut desired = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.to_string(),
        primary_node_id: target_primary_node_id,
        replica_node_ids: target_replica_node_ids,
        epoch: 1,
        assignment,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let Some(current) = host
        .current_metadata_shard_async(file_system.file_system_id.clone(), shard_id.to_string())
        .await?
    else {
        desired.primary_node_id = bootstrap_primary_node_id.clone();
        desired.replica_node_ids =
            default_metadata_shard_replica_node_ids(&service_node_ids, &bootstrap_primary_node_id);
        return Ok(desired);
    };
    if current.primary_node_id != desired.primary_node_id
        && !host
            .metadata_shard_primary_transfer_ready_async(
                current.clone(),
                desired.primary_node_id.clone(),
            )
            .await?
    {
        let mut expanded = desired.clone();
        expanded.primary_node_id = current.primary_node_id.clone();
        expanded.replica_node_ids =
            default_metadata_shard_replica_node_ids(&service_node_ids, &current.primary_node_id);
        if metadata_shard_layout_matches(&current, &expanded) {
            return Ok(current);
        }
        expanded.epoch = current.epoch.saturating_add(1);
        return Ok(expanded);
    }
    if metadata_shard_layout_matches(&current, &desired) {
        return Ok(current);
    }
    desired.epoch = current.epoch.saturating_add(1);
    Ok(desired)
}

pub fn default_metadata_shard_primary_node_id(
    service_node_ids: &[NodeId],
    fallback_node_id: &NodeId,
    primary_offset: usize,
) -> NodeId {
    if service_node_ids.is_empty() {
        return fallback_node_id.clone();
    }
    service_node_ids[primary_offset % service_node_ids.len()].clone()
}

pub fn default_metadata_shard_replica_node_ids(
    service_node_ids: &[NodeId],
    primary_node_id: &NodeId,
) -> Vec<NodeId> {
    metadata_shard_replica_node_ids(
        service_node_ids,
        primary_node_id,
        service_node_ids.len().saturating_sub(1),
    )
}

pub fn metadata_shard_replica_node_ids(
    service_node_ids: &[NodeId],
    primary_node_id: &NodeId,
    max_replicas: usize,
) -> Vec<NodeId> {
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

pub fn metadata_shard_layout_matches(
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

pub fn stable_dfs_service_node_ids(state: &ClusterBootstrapState) -> Vec<NodeId> {
    let mut node_ids = dfs_service_members(state)
        .into_iter()
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    node_ids.sort();
    node_ids.dedup();
    node_ids
}

pub fn dfs_service_members(state: &ClusterBootstrapState) -> Vec<&ClusterMemberRecord> {
    state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized() && !matches!(member.role, ClusterMemberRole::NonVoting)
        })
        .collect()
}

pub fn dfs_service_endpoint_for_member(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    local_endpoint: &str,
) -> String {
    dfs_service_endpoint_for_member_with_graph(state, member, local_endpoint, None)
}

pub(crate) fn dfs_service_endpoint_for_member_with_graph(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    local_endpoint: &str,
    graph: Option<&ClusterConnectivityGraph>,
) -> String {
    if member.node_id == state.local_node_id {
        return local_endpoint.to_string();
    }
    member
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
        .filter(|endpoint| {
            sinomo_core::network::cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
        })
        .min_by_key(|endpoint| dfs_service_endpoint_route_rank(graph, endpoint))
        .or_else(|| {
            member
                .endpoints
                .iter()
                .filter(|endpoint| {
                    sinomo_core::network::cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
                })
                .min_by_key(|endpoint| dfs_service_endpoint_route_rank(graph, endpoint))
        })
        .map(|endpoint| endpoint.address.clone())
        .unwrap_or_else(|| format!("cluster-member://{}", member.node_id))
}

fn dfs_service_endpoint_route_rank(
    graph: Option<&ClusterConnectivityGraph>,
    endpoint: &ClusterEndpointRecord,
) -> (u8, u8, u16, String) {
    let connectivity_rank = match graph.and_then(|graph| graph.state_for_endpoint(endpoint)) {
        Some(sinomo_core::network::ClusterConnectivityState::Healthy) => 0,
        Some(sinomo_core::network::ClusterConnectivityState::Degraded) => 1,
        None => 2,
        Some(sinomo_core::network::ClusterConnectivityState::Unreachable) => 3,
    };
    let (privacy_rank, priority, address) = dfs_service_endpoint_rank(endpoint);
    (connectivity_rank, privacy_rank, priority, address)
}

pub fn dfs_service_endpoint_rank(endpoint: &ClusterEndpointRecord) -> (u8, u16, String) {
    let privacy_rank = match endpoint.privacy {
        ClusterEndpointPrivacy::InternetDirect => 0,
        ClusterEndpointPrivacy::Relayed => 1,
        ClusterEndpointPrivacy::Lan => 2,
        ClusterEndpointPrivacy::LocalOnly => 3,
    };
    (privacy_rank, endpoint.priority, endpoint.address.clone())
}

fn publish_dfs_service_advertisements_with_host(
    host: &mut impl DfsDefaultClusterBootstrapHost,
    state: &ClusterBootstrapState,
    local_endpoint: &str,
) -> Result<(), DfsError> {
    let graph = host.connectivity_graph(state);
    for member in dfs_service_members(state) {
        let endpoint = dfs_service_endpoint_for_member_with_graph(
            state,
            member,
            local_endpoint,
            graph.as_ref(),
        );
        let metadata_key = metadata_service_key(&member.node_id);
        let content_key = content_service_key(&member.node_id);
        let metadata = stable_dfs_advertisement(
            host,
            &metadata_key,
            DfsNodeAdvertisement::metadata(member.node_id.clone(), endpoint.clone()),
        )?;
        let content = stable_dfs_advertisement(
            host,
            &content_key,
            DfsNodeAdvertisement::content(member.node_id.clone(), endpoint),
        )?;
        host.put_node_advertisement(metadata_key, &metadata)?;
        host.put_node_advertisement(content_key, &content)?;
    }
    Ok(())
}

async fn publish_dfs_service_advertisements_with_async_host(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    local_endpoint: &str,
) -> Result<(), DfsError> {
    let graph = host.connectivity_graph(state);
    for member in dfs_service_members(state) {
        let endpoint = dfs_service_endpoint_for_member_with_graph(
            state,
            member,
            local_endpoint,
            graph.as_ref(),
        );
        let metadata_key = metadata_service_key(&member.node_id);
        let content_key = content_service_key(&member.node_id);
        let metadata = stable_dfs_advertisement_async(
            host,
            metadata_key.clone(),
            DfsNodeAdvertisement::metadata(member.node_id.clone(), endpoint.clone()),
        )
        .await?;
        let content = stable_dfs_advertisement_async(
            host,
            content_key.clone(),
            DfsNodeAdvertisement::content(member.node_id.clone(), endpoint),
        )
        .await?;
        host.put_node_advertisement_async(metadata_key, metadata)
            .await?;
        host.put_node_advertisement_async(content_key, content)
            .await?;
    }
    Ok(())
}

fn stable_dfs_advertisement(
    host: &impl DfsDefaultClusterBootstrapHost,
    key: &str,
    desired: DfsNodeAdvertisement,
) -> Result<DfsNodeAdvertisement, DfsError> {
    let Some(current) = host.current_node_advertisement(key)? else {
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

async fn stable_dfs_advertisement_async(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    key: String,
    desired: DfsNodeAdvertisement,
) -> Result<DfsNodeAdvertisement, DfsError> {
    let Some(current) = host.current_node_advertisement_async(key).await? else {
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
