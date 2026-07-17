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
use sinomo_core::{ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, DfsObjectPolicy};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use time::OffsetDateTime;

use crate::control_plane::topology_preserving_metadata_shard_replicas;
use crate::service_sync::rebind_completed_metadata_shard_membership_handoff;
use crate::{
    AsyncDfsControlPlaneKvClient, DfsControlPlaneAsyncStore, DfsControlPlaneKvClient,
    DfsControlPlaneStore, DfsError, DfsFileSystem, DfsFileSystemConfig, DfsFileSystemId,
    DfsMessageSender, DfsMetadataBackend, DfsMetadataJournalBatchSender,
    DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardHeadWatermark,
    DfsMetadataShardId, DfsMetadataShardMembershipHandoff, DfsMetadataShardMembershipHandoffProof,
    DfsMetadataSnapshotInstallSender, DfsNodeAdvertisement, DfsPeerTransport, DfsSmokeOptions,
    DfsSmokeReport, DfsSubtreePolicy, SIFS_SERVICES_PATH, SIFS_USERS_PATH, content_service_key,
    default_dfs_metadata_shards_root, dfs_message_sender_from_peer_transport,
    dfs_metadata_journal_batch_sender_from_peer_transport,
    dfs_metadata_snapshot_install_sender_from_peer_transport, distributed_sifs,
    file_system_config_key, local_dfs_metadata_shard_path, materialize_default_sifs_metadata,
    materialize_default_sifs_metadata_on_shards, metadata_service_key,
    metadata_shard_head_watermark_key, metadata_shard_key, metadata_shard_membership_handoff_key,
    metadata_shard_membership_handoff_proof_key, open_cluster_aware_dfs_metadata_backend,
    open_local_metadata_shard_backend, open_local_sharded_metadata_backend,
    run_cluster_dfs_smoke_with_message_sender, sifs_node_files_path,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDefaultClusterBootstrapOptions {
    pub publish_cluster_control: bool,
    pub materialize_local_metadata: bool,
    /// Failure domain advertised for this process's local DFS service.
    ///
    /// Remote advertisements retain their previously published zones so a
    /// bootstrap pass on one node cannot erase topology owned by another.
    pub local_zone: Option<String>,
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

    fn current_file_system_config(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystemConfig>, DfsError> {
        let _ = file_system_id;
        Ok(None)
    }

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

    fn current_file_system_config_async<'a>(
        &'a mut self,
        file_system_id: DfsFileSystemId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Option<DfsFileSystemConfig>> {
        let _ = file_system_id;
        Box::pin(async { Ok(None) })
    }

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

    fn compare_and_put_metadata_shard(
        &mut self,
        desired: &DfsMetadataShardConfig,
    ) -> Result<(), DfsError> {
        let mut control = DfsControlPlaneStore::new(self.control_plane.as_mut());
        let current = control.get_metadata_shard(&desired.file_system_id, &desired.shard_id)?;
        validate_bootstrap_metadata_shard_transition(current.as_ref(), desired)?;
        if current.as_ref() == Some(desired) {
            return Ok(());
        }
        if let Some(current) = current.as_ref() {
            let pending = control.begin_metadata_shard_membership_handoff(current, desired)?;
            return Err(DfsError::Distributed {
                message: if pending.is_some() {
                    format!(
                        "metadata shard {} membership or primary-role change from epoch {} to {} is fenced and parked pending exact DFS catch-up of every retiring and desired member",
                        desired.shard_id, current.epoch, desired.epoch
                    )
                } else {
                    format!(
                        "metadata shard {} changed concurrently while beginning its membership handoff; retry",
                        desired.shard_id
                    )
                },
            });
        }
        if !control.compare_and_put_metadata_shard(current.as_ref(), desired)? {
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} changed concurrently during bootstrap; retry",
                    desired.shard_id
                ),
            });
        }
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

    async fn compare_and_put_metadata_shard(
        &mut self,
        desired: &DfsMetadataShardConfig,
    ) -> Result<(), DfsError> {
        let key = metadata_shard_key(&desired.file_system_id, &desired.shard_id);
        let current = self
            .control_plane
            .get_async(&key)
            .await?
            .map(|entry| entry.decode::<DfsMetadataShardConfig>())
            .transpose()?;
        validate_bootstrap_metadata_shard_transition(current.as_ref(), desired)?;
        if current.as_ref() == Some(desired) {
            return Ok(());
        }
        if let Some(current) = current.as_ref() {
            let pending = DfsControlPlaneAsyncStore::new(self.control_plane.as_mut())
                .begin_metadata_shard_membership_handoff(current, desired)
                .await?;
            return Err(DfsError::Distributed {
                message: if pending.is_some() {
                    format!(
                        "metadata shard {} membership or primary-role change from epoch {} to {} is fenced and parked pending exact DFS catch-up of every retiring and desired member",
                        desired.shard_id, current.epoch, desired.epoch
                    )
                } else {
                    format!(
                        "metadata shard {} changed concurrently while beginning its membership handoff; retry",
                        desired.shard_id
                    )
                },
            });
        }
        if !DfsControlPlaneAsyncStore::new(self.control_plane.as_mut())
            .compare_and_put_metadata_shard(current.as_ref(), desired)
            .await?
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} changed concurrently during bootstrap; retry",
                    desired.shard_id
                ),
            });
        }
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

    fn current_file_system_config(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystemConfig>, DfsError> {
        let key = file_system_config_key(file_system_id);
        self.control_plane
            .get(&key)?
            .map(|entry| {
                entry
                    .decode::<DfsFileSystemConfig>()
                    .map_err(DfsError::from)
            })
            .transpose()
    }

    fn metadata_shard_primary_transfer_ready(
        &mut self,
        current: &DfsMetadataShardConfig,
        target_primary_node_id: &NodeId,
    ) -> Result<bool, DfsError> {
        let watermark = DfsControlPlaneStore::new(self.control_plane.as_mut())
            .get_metadata_shard_head_watermark(&current.file_system_id, &current.shard_id)?;
        if metadata_shard_primary_transfer_ready_from_local_backend(
            &self.data_dir,
            &self.local_node_id,
            current,
            target_primary_node_id,
            watermark.as_ref(),
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
        let watermark = DfsControlPlaneStore::new(self.control_plane.as_mut())
            .get_metadata_shard_head_watermark(&current.file_system_id, &current.shard_id)?;
        metadata_shard_local_replica_failover_ready_from_local_backend(
            &self.data_dir,
            &self.local_node_id,
            current,
            target_primary_node_id,
            watermark.as_ref(),
        )
    }

    fn put_file_system_config(&mut self, config: &DfsFileSystemConfig) -> Result<(), DfsError> {
        self.put_serde_if_changed(file_system_config_key(&config.file_system_id), config)
    }

    fn put_metadata_shard(&mut self, shard: &DfsMetadataShardConfig) -> Result<(), DfsError> {
        self.compare_and_put_metadata_shard(shard)
    }

    fn current_node_advertisement(
        &self,
        key: &str,
    ) -> Result<Option<DfsNodeAdvertisement>, DfsError> {
        let Some(entry) = self.control_plane.get(key)? else {
            return Ok(None);
        };
        Ok(entry.decode::<DfsNodeAdvertisement>().ok())
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
            shards.clone(),
        )?;
        for active in &shards {
            let store = DfsControlPlaneStore::new(self.control_plane.as_mut());
            let pending = store
                .get_metadata_shard_membership_handoff(&active.file_system_id, &active.shard_id)?;
            let proof = store.get_metadata_shard_membership_handoff_proof(
                &active.file_system_id,
                &active.shard_id,
            )?;
            rebind_completed_metadata_shard_membership_handoff(
                &mut backend,
                &self.local_node_id,
                active,
                pending.as_ref(),
                proof.as_ref(),
            )?;
        }
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

    fn current_file_system_config_async<'a>(
        &'a mut self,
        file_system_id: DfsFileSystemId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Option<DfsFileSystemConfig>> {
        Box::pin(async move {
            let key = file_system_config_key(&file_system_id);
            self.control_plane
                .get_async(&key)
                .await?
                .map(|entry| {
                    entry
                        .decode::<DfsFileSystemConfig>()
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
            let watermark = DfsControlPlaneAsyncStore::new(self.control_plane.as_mut())
                .get_metadata_shard_head_watermark(&current.file_system_id, &current.shard_id)
                .await?;
            if metadata_shard_primary_transfer_ready_from_local_backend(
                &self.data_dir,
                &self.local_node_id,
                &current,
                &target_primary_node_id,
                watermark.as_ref(),
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
            let watermark_key =
                metadata_shard_head_watermark_key(&current.file_system_id, &current.shard_id);
            let watermark = self
                .control_plane
                .get_async(&watermark_key)
                .await?
                .map(|entry| entry.decode::<DfsMetadataShardHeadWatermark>())
                .transpose()?;
            metadata_shard_local_replica_failover_ready_from_local_backend(
                &self.data_dir,
                &self.local_node_id,
                &current,
                &target_primary_node_id,
                watermark.as_ref(),
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
        Box::pin(async move { self.compare_and_put_metadata_shard(&shard).await })
    }

    fn current_node_advertisement_async<'a>(
        &'a mut self,
        key: String,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Option<DfsNodeAdvertisement>> {
        Box::pin(async move {
            let Some(entry) = self.control_plane.get_async(&key).await? else {
                return Ok(None);
            };
            Ok(entry.decode::<DfsNodeAdvertisement>().ok())
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
            let mut completion_evidence = Vec::with_capacity(shards.len());
            for active in &shards {
                let pending = self
                    .control_plane
                    .get_async(&metadata_shard_membership_handoff_key(
                        &active.file_system_id,
                        &active.shard_id,
                    ))
                    .await?
                    .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
                    .transpose()?;
                let proof = self
                    .control_plane
                    .get_async(&metadata_shard_membership_handoff_proof_key(
                        &active.file_system_id,
                        &active.shard_id,
                    ))
                    .await?
                    .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoffProof>())
                    .transpose()?;
                completion_evidence.push((pending, proof));
            }
            let mut backend = open_local_sharded_metadata_backend(
                default_dfs_metadata_shards_root(&self.data_dir),
                shards.clone(),
            )?;
            for (active, (pending, proof)) in shards.iter().zip(&completion_evidence) {
                rebind_completed_metadata_shard_membership_handoff(
                    &mut backend,
                    &self.local_node_id,
                    active,
                    pending.as_ref(),
                    proof.as_ref(),
                )?;
            }
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
    watermark: Option<&DfsMetadataShardHeadWatermark>,
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
    let Some(watermark) = watermark else {
        return Ok(false);
    };
    if !metadata_shard_head_watermark_certifies_failover_candidate(
        current,
        target_primary_node_id,
        watermark,
    ) {
        return Ok(false);
    }
    let shard_id = DfsMetadataShardId::new(current.shard_id.clone());
    if !local_dfs_metadata_shard_path(data_dir, &shard_id).exists() {
        return Ok(false);
    }
    let backend = open_local_metadata_shard_backend(data_dir, shard_id.clone())?;
    let journal_last_sequence = backend
        .metadata_shard_statuses()?
        .into_iter()
        .find(|status| status.shard_id == shard_id)
        .map(|status| status.journal_last_sequence)
        .unwrap_or(0);
    if watermark.journal_sequence == 0 || journal_last_sequence < watermark.journal_sequence {
        return Ok(false);
    }
    if journal_last_sequence == watermark.journal_sequence {
        let fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
            &backend,
            &shard_id,
            journal_last_sequence,
        )?;
        return Ok(fingerprint == watermark.state_fingerprint);
    }

    // A membership-expansion handoff publishes an exact watermark for the
    // expanded ring. Ordinary writes can advance the primary immediately
    // afterward, while healthy journal replication advances only the durable
    // sender cursor and intentionally avoids another KV watermark write. Use
    // that cursor solely as admission to begin the next fenced handoff. It is
    // not activation evidence: the handoff still freezes the current primary
    // head and requires a fresh exact-prefix acknowledgement from every member
    // before its control-plane CAS can promote the target.
    let target_cursor = backend.metadata_replication_cursor(target_primary_node_id, &shard_id)?;
    let cursor_covers_current_head = target_cursor >= journal_last_sequence;
    if cursor_covers_current_head {
        tracing::trace!(
            target: "sinomo.dfs.perf",
            shard = %current.shard_id,
            target_primary = %target_primary_node_id,
            certified_sequence = watermark.journal_sequence,
            journal_last_sequence,
            target_cursor,
            "metadata-primary-handoff cursor preflight admitted stale certified head"
        );
    }
    Ok(cursor_covers_current_head)
}

fn validate_bootstrap_metadata_shard_transition(
    current: Option<&DfsMetadataShardConfig>,
    desired: &DfsMetadataShardConfig,
) -> Result<(), DfsError> {
    let Some(current) = current else {
        if desired.epoch == 1 {
            return Ok(());
        }
        return Err(DfsError::Distributed {
            message: format!(
                "cannot create metadata shard {} at non-initial epoch {}",
                desired.shard_id, desired.epoch
            ),
        });
    };
    if current == desired {
        return Ok(());
    }
    if current.file_system_id != desired.file_system_id || current.shard_id != desired.shard_id {
        return Err(DfsError::Distributed {
            message: "metadata-shard bootstrap transition cannot change shard identity".to_string(),
        });
    }
    let Some(next_epoch) = current.epoch.checked_add(1) else {
        return Err(DfsError::Distributed {
            message: format!(
                "metadata shard {} authority epoch is exhausted",
                current.shard_id
            ),
        });
    };
    if desired.epoch != next_epoch {
        return Err(DfsError::Distributed {
            message: format!(
                "stale metadata-shard bootstrap transition for {}: current epoch {}, desired epoch {}",
                current.shard_id, current.epoch, desired.epoch
            ),
        });
    }
    Ok(())
}

fn metadata_shard_local_replica_failover_ready_from_local_backend(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    current: &DfsMetadataShardConfig,
    target_primary_node_id: &NodeId,
    watermark: Option<&DfsMetadataShardHeadWatermark>,
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
    let Some(watermark) = watermark else {
        return Ok(false);
    };
    if !metadata_shard_head_watermark_certifies_failover_candidate(
        current,
        local_node_id,
        watermark,
    ) {
        return Ok(false);
    }
    let shard_id = DfsMetadataShardId::new(current.shard_id.clone());
    if !local_dfs_metadata_shard_path(data_dir, &shard_id).exists() {
        return Ok(false);
    }
    let backend = open_local_metadata_shard_backend(data_dir, shard_id.clone())?;
    let Some(status) = backend
        .metadata_shard_statuses()?
        .into_iter()
        .find(|status| status.shard_id == shard_id)
    else {
        return Ok(false);
    };
    let replayed_through_sequence = status
        .replay_cursors
        .iter()
        .filter(|cursor| cursor.source_shard_id == shard_id)
        .map(|cursor| cursor.replayed_through_sequence)
        .max()
        .unwrap_or(0);
    if watermark.journal_sequence == 0
        || status.journal_last_sequence < watermark.journal_sequence
        || replayed_through_sequence < watermark.journal_sequence
    {
        return Ok(false);
    }
    let Some(ack_floor) = backend.metadata_prefix_ack_floor(&shard_id)? else {
        return Ok(false);
    };
    if ack_floor.journal_sequence < watermark.journal_sequence {
        return Ok(false);
    }
    if ack_floor.journal_sequence == watermark.journal_sequence
        && ack_floor.state_fingerprint != watermark.state_fingerprint
    {
        return Ok(false);
    }
    if status.journal_last_sequence == watermark.journal_sequence {
        let fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
            &backend,
            &shard_id,
            watermark.journal_sequence,
        )?;
        return Ok(fingerprint == watermark.state_fingerprint);
    }
    Ok(true)
}

fn metadata_shard_head_watermark_certifies_failover_candidate(
    current: &DfsMetadataShardConfig,
    local_node_id: &NodeId,
    watermark: &DfsMetadataShardHeadWatermark,
) -> bool {
    if watermark.file_system_id != current.file_system_id
        || watermark.shard_id != current.shard_id
        || watermark.primary_node_id != current.primary_node_id
        || watermark.epoch != current.epoch
        || watermark.state_fingerprint.is_empty()
    {
        return false;
    }
    let configured_node_ids = std::iter::once(current.primary_node_id.clone())
        .chain(current.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let acknowledged_node_ids = watermark
        .acknowledged_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    acknowledged_node_ids.contains(&current.primary_node_id)
        && acknowledged_node_ids.contains(local_node_id)
        && acknowledged_node_ids
            .iter()
            .all(|node_id| configured_node_ids.contains(node_id))
        && acknowledged_node_ids.len() >= configured_node_ids.len() / 2 + 1
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

fn stable_default_file_system_config(
    file_system: &DfsFileSystem,
    current: Option<DfsFileSystemConfig>,
) -> DfsFileSystemConfig {
    let mut desired = DfsFileSystemConfig::from(file_system);
    if let Some(current) = current {
        for rule in current.subtree_policies {
            crate::placement::upsert_placement_rule(&mut desired.subtree_policies, rule);
        }
    }
    desired
}

pub fn ensure_default_distributed_dfs_with_host(
    state: &ClusterBootstrapState,
    host: &mut impl DfsDefaultClusterBootstrapHost,
    options: DfsDefaultClusterBootstrapOptions,
) -> Result<DfsDefaultClusterBootstrapReport, DfsError> {
    let file_system = default_cluster_sifs(state);
    let mut metadata_shards = Vec::new();
    let local_has_primary_metadata_shards = if options.publish_cluster_control {
        let config = stable_default_file_system_config(
            &file_system,
            host.current_file_system_config(&file_system.file_system_id)?,
        );
        host.put_file_system_config(&config)?;
        // Service discovery is independent of metadata-shard handoff. Publish
        // it first so a fenced or retrying shard transition cannot hide
        // otherwise eligible content nodes from placement planning.
        publish_dfs_service_advertisements_with_host(host, state, options.local_zone.as_deref())?;
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
        local_has_primary_metadata_shards
    } else {
        if options.local_zone.is_some() {
            publish_local_dfs_service_advertisement_with_host(
                host,
                state,
                options.local_zone.as_deref(),
            )?;
        }
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
        let current = host
            .current_file_system_config_async(file_system.file_system_id.clone())
            .await?;
        let config = stable_default_file_system_config(&file_system, current);
        host.put_file_system_config_async(config).await?;
        // Keep placement discovery available even while metadata membership
        // is fenced for catch-up or retries a concurrent handoff.
        publish_dfs_service_advertisements_with_async_host(
            host,
            state,
            options.local_zone.as_deref(),
        )
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
        local_has_primary_metadata_shards
    } else {
        if options.local_zone.is_some() {
            publish_local_dfs_service_advertisement_with_async_host(
                host,
                state,
                options.local_zone.as_deref(),
            )
            .await?;
        }
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
        .map(|node_id| {
            DfsSubtreePolicy::legacy(sifs_node_files_path(&node_id), DfsObjectPolicy::LocalOwned)
        })
        .collect::<Vec<_>>();
    if policies.is_empty() {
        policies.push(DfsSubtreePolicy::legacy(
            sifs_node_files_path(&state.local_node_id),
            DfsObjectPolicy::LocalOwned,
        ));
    }
    policies.push(DfsSubtreePolicy::legacy(
        SIFS_USERS_PATH,
        DfsObjectPolicy::ReplicatedEventual,
    ));
    policies.push(DfsSubtreePolicy::legacy(
        SIFS_SERVICES_PATH,
        DfsObjectPolicy::ReplicatedEventual,
    ));
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
    let default_target_primary_node_id = default_metadata_shard_primary_node_id(
        &service_node_ids,
        &bootstrap_primary_node_id,
        primary_offset,
    );
    let mut desired = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.to_string(),
        primary_node_id: default_target_primary_node_id.clone(),
        replica_node_ids: default_metadata_shard_replica_node_ids(
            &service_node_ids,
            &default_target_primary_node_id,
        ),
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
    let connectivity_graph = host.connectivity_graph(state);
    desired.primary_node_id = default_metadata_shard_target_primary_node_id(
        connectivity_graph.as_ref(),
        state,
        &service_node_ids,
        default_target_primary_node_id,
        &current,
    );
    desired.replica_node_ids =
        default_metadata_shard_replica_node_ids(&service_node_ids, &desired.primary_node_id);
    if current.primary_node_id != desired.primary_node_id
        && let Some(replica_node_ids) =
            topology_preserving_metadata_shard_replicas(&current, &desired.primary_node_id)
    {
        // Authority movement and membership reconciliation are separate epoch
        // transitions. Keeping the current member ring here makes the
        // proof-carrying transfer exactly match the control-plane transaction;
        // a later bootstrap pass can reconcile newly added/removed replicas.
        desired.replica_node_ids = replica_node_ids;
    }
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
    let default_target_primary_node_id = default_metadata_shard_primary_node_id(
        &service_node_ids,
        &bootstrap_primary_node_id,
        primary_offset,
    );
    let mut desired = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.to_string(),
        primary_node_id: default_target_primary_node_id.clone(),
        replica_node_ids: default_metadata_shard_replica_node_ids(
            &service_node_ids,
            &default_target_primary_node_id,
        ),
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
    let connectivity_graph = host.connectivity_graph(state);
    desired.primary_node_id = default_metadata_shard_target_primary_node_id(
        connectivity_graph.as_ref(),
        state,
        &service_node_ids,
        default_target_primary_node_id,
        &current,
    );
    desired.replica_node_ids =
        default_metadata_shard_replica_node_ids(&service_node_ids, &desired.primary_node_id);
    if current.primary_node_id != desired.primary_node_id
        && let Some(replica_node_ids) =
            topology_preserving_metadata_shard_replicas(&current, &desired.primary_node_id)
    {
        desired.replica_node_ids = replica_node_ids;
    }
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

fn default_metadata_shard_target_primary_node_id(
    connectivity_graph: Option<&ClusterConnectivityGraph>,
    state: &ClusterBootstrapState,
    service_node_ids: &[NodeId],
    default_target_primary_node_id: NodeId,
    current: &DfsMetadataShardConfig,
) -> NodeId {
    if current.primary_node_id == state.local_node_id {
        return default_target_primary_node_id;
    }
    if !service_node_ids
        .iter()
        .any(|node_id| node_id == &state.local_node_id)
    {
        return default_target_primary_node_id;
    }
    if !current
        .replica_node_ids
        .iter()
        .any(|node_id| node_id == &state.local_node_id)
    {
        return default_target_primary_node_id;
    }
    let Some(graph) = connectivity_graph else {
        return default_target_primary_node_id;
    };
    if cluster_healthy_route_next_hop(graph, &state.local_node_id, &current.primary_node_id)
        .is_some()
    {
        return default_target_primary_node_id;
    }
    state.local_node_id.clone()
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

fn publish_dfs_service_advertisements_with_host(
    host: &mut impl DfsDefaultClusterBootstrapHost,
    state: &ClusterBootstrapState,
    local_zone: Option<&str>,
) -> Result<(), DfsError> {
    for member in dfs_service_members(state) {
        let metadata_key = metadata_service_key(&member.node_id);
        let content_key = content_service_key(&member.node_id);
        let is_local = member.node_id == state.local_node_id;
        let mut desired_metadata = DfsNodeAdvertisement::metadata(member.node_id.clone());
        let mut desired_content = DfsNodeAdvertisement::content(member.node_id.clone());
        if is_local {
            desired_metadata.zone = local_zone.map(str::to_string);
            desired_content.zone = local_zone.map(str::to_string);
        }
        let metadata = stable_dfs_advertisement(host, &metadata_key, desired_metadata, !is_local)?;
        let content = stable_dfs_advertisement(host, &content_key, desired_content, !is_local)?;
        host.put_node_advertisement(metadata_key, &metadata)?;
        host.put_node_advertisement(content_key, &content)?;
    }
    Ok(())
}

fn publish_local_dfs_service_advertisement_with_host(
    host: &mut impl DfsDefaultClusterBootstrapHost,
    state: &ClusterBootstrapState,
    local_zone: Option<&str>,
) -> Result<(), DfsError> {
    let member = dfs_service_members(state)
        .into_iter()
        .find(|member| member.node_id == state.local_node_id)
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "local node {} is not an eligible DFS service member",
                state.local_node_id
            ),
        })?;
    let metadata_key = metadata_service_key(&member.node_id);
    let content_key = content_service_key(&member.node_id);
    let mut metadata = DfsNodeAdvertisement::metadata(member.node_id.clone());
    let mut content = DfsNodeAdvertisement::content(member.node_id.clone());
    metadata.zone = local_zone.map(str::to_string);
    content.zone = local_zone.map(str::to_string);
    host.put_node_advertisement(metadata_key, &metadata)?;
    host.put_node_advertisement(content_key, &content)
}

async fn publish_dfs_service_advertisements_with_async_host(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    local_zone: Option<&str>,
) -> Result<(), DfsError> {
    for member in dfs_service_members(state) {
        let metadata_key = metadata_service_key(&member.node_id);
        let content_key = content_service_key(&member.node_id);
        let is_local = member.node_id == state.local_node_id;
        let mut desired_metadata = DfsNodeAdvertisement::metadata(member.node_id.clone());
        let mut desired_content = DfsNodeAdvertisement::content(member.node_id.clone());
        if is_local {
            desired_metadata.zone = local_zone.map(str::to_string);
            desired_content.zone = local_zone.map(str::to_string);
        }
        let metadata =
            stable_dfs_advertisement_async(host, metadata_key.clone(), desired_metadata, !is_local)
                .await?;
        let content =
            stable_dfs_advertisement_async(host, content_key.clone(), desired_content, !is_local)
                .await?;
        host.put_node_advertisement_async(metadata_key, metadata)
            .await?;
        host.put_node_advertisement_async(content_key, content)
            .await?;
    }
    Ok(())
}

async fn publish_local_dfs_service_advertisement_with_async_host(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    local_zone: Option<&str>,
) -> Result<(), DfsError> {
    let member = dfs_service_members(state)
        .into_iter()
        .find(|member| member.node_id == state.local_node_id)
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "local node {} is not an eligible DFS service member",
                state.local_node_id
            ),
        })?;
    let metadata_key = metadata_service_key(&member.node_id);
    let content_key = content_service_key(&member.node_id);
    let mut metadata = DfsNodeAdvertisement::metadata(member.node_id.clone());
    let mut content = DfsNodeAdvertisement::content(member.node_id.clone());
    metadata.zone = local_zone.map(str::to_string);
    content.zone = local_zone.map(str::to_string);
    host.put_node_advertisement_async(metadata_key, metadata)
        .await?;
    host.put_node_advertisement_async(content_key, content)
        .await
}

fn stable_dfs_advertisement(
    host: &impl DfsDefaultClusterBootstrapHost,
    key: &str,
    mut desired: DfsNodeAdvertisement,
    preserve_current_zone: bool,
) -> Result<DfsNodeAdvertisement, DfsError> {
    let Some(current) = host.current_node_advertisement(key)? else {
        return Ok(desired);
    };
    if preserve_current_zone {
        desired.zone.clone_from(&current.zone);
    }
    if current.node_id == desired.node_id
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
    mut desired: DfsNodeAdvertisement,
    preserve_current_zone: bool,
) -> Result<DfsNodeAdvertisement, DfsError> {
    let Some(current) = host.current_node_advertisement_async(key).await? else {
        return Ok(desired);
    };
    if preserve_current_zone {
        desired.zone.clone_from(&current.zone);
    }
    if current.node_id == desired.node_id
        && current.accepts_metadata == desired.accepts_metadata
        && current.accepts_content == desired.accepts_content
        && current.capacity_weight == desired.capacity_weight
        && current.zone == desired.zone
    {
        return Ok(current);
    }
    Ok(desired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
        DfsMetadataPrefixAckFloor, LocalSobjMetadataBackend,
    };
    use sinomo_core::ClusterTransportKind;
    use sinomo_core::network::{
        ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityState,
        ClusterEndpointId,
    };
    use sinomo_core::node::ClusterId;

    #[derive(Debug)]
    struct TestDefaultClusterBootstrapHost {
        current_shard: Option<DfsMetadataShardConfig>,
        connectivity_graph: Option<ClusterConnectivityGraph>,
        primary_transfer_ready: bool,
        primary_transfer_targets: Vec<NodeId>,
    }

    impl DfsDefaultClusterBootstrapHost for TestDefaultClusterBootstrapHost {
        fn connectivity_graph(
            &self,
            _state: &ClusterBootstrapState,
        ) -> Option<ClusterConnectivityGraph> {
            self.connectivity_graph.clone()
        }

        fn current_metadata_shard(
            &self,
            _file_system_id: &DfsFileSystemId,
            _shard_id: &str,
        ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
            Ok(self.current_shard.clone())
        }

        fn metadata_shard_primary_transfer_ready(
            &mut self,
            _current: &DfsMetadataShardConfig,
            target_primary_node_id: &NodeId,
        ) -> Result<bool, DfsError> {
            self.primary_transfer_targets
                .push(target_primary_node_id.clone());
            Ok(self.primary_transfer_ready)
        }

        fn put_file_system_config(
            &mut self,
            _config: &DfsFileSystemConfig,
        ) -> Result<(), DfsError> {
            Ok(())
        }

        fn put_metadata_shard(&mut self, _shard: &DfsMetadataShardConfig) -> Result<(), DfsError> {
            Ok(())
        }

        fn current_node_advertisement(
            &self,
            _key: &str,
        ) -> Result<Option<DfsNodeAdvertisement>, DfsError> {
            Ok(None)
        }

        fn put_node_advertisement(
            &mut self,
            _key: String,
            _advertisement: &DfsNodeAdvertisement,
        ) -> Result<(), DfsError> {
            Ok(())
        }

        fn materialize_default_sifs_metadata(
            &mut self,
            _file_system: &DfsFileSystem,
            _bootstrap_owner_node_ids: Vec<NodeId>,
            _fallback_owner_node_id: NodeId,
        ) -> Result<(), DfsError> {
            Ok(())
        }

        fn materialize_default_sifs_metadata_on_shards(
            &mut self,
            _file_system: &DfsFileSystem,
            _shards: Vec<DfsMetadataShardConfig>,
            _bootstrap_owner_node_ids: Vec<NodeId>,
            _fallback_owner_node_id: NodeId,
        ) -> Result<(), DfsError> {
            Ok(())
        }
    }

    #[test]
    fn control_plane_bootstrap_host_parks_primary_rotation_handoff() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-bootstrap-shard-cas");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let owner = NodeId::from_legacy_named("node_dfs_bootstrap_cas_owner").expect("owner node");
        let replacement = NodeId::from_legacy_named("node_dfs_bootstrap_cas_replacement")
            .expect("replacement node");
        let file_system_id =
            DfsFileSystemId::from_legacy_named("bootstrap-cas-fs").expect("filesystem id");
        let initial = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replacement.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut host = DfsControlPlaneDefaultClusterBootstrapHost::new(
            data_dir,
            cluster_paths,
            owner.clone(),
            Box::new(sinomo_core::kv::InProcessKvStore::new()),
        );
        host.put_metadata_shard(&initial).expect("create shard");

        let same_epoch_overwrite = DfsMetadataShardConfig {
            primary_node_id: replacement.clone(),
            ..initial.clone()
        };
        assert!(matches!(
            host.put_metadata_shard(&same_epoch_overwrite),
            Err(DfsError::Distributed { message }) if message.contains("stale metadata-shard bootstrap transition")
        ));
        assert!(
            DfsControlPlaneStore::new(host.control_plane.as_mut())
                .publish_metadata_shard_head_watermark(
                    &initial,
                    &DfsMetadataShardHeadWatermark {
                        file_system_id: file_system_id.clone(),
                        shard_id: "root".to_string(),
                        primary_node_id: owner.clone(),
                        epoch: initial.epoch,
                        journal_sequence: 1,
                        state_fingerprint: "blake3:bootstrap-sync".to_string(),
                        acknowledged_node_ids: vec![owner.clone(), replacement.clone()],
                    },
                )
                .expect("publish certified transfer head")
        );

        let next_epoch = DfsMetadataShardConfig {
            replica_node_ids: vec![owner],
            epoch: 2,
            ..same_epoch_overwrite
        };
        assert!(matches!(
            host.put_metadata_shard(&next_epoch),
            Err(DfsError::Distributed { message })
                if message.contains("membership or primary-role change")
                    && message.contains("fenced and parked")
                    && message.contains("exact DFS catch-up")
        ));
        assert_eq!(
            host.current_metadata_shard(&file_system_id, "root")
                .expect("read shard"),
            Some(initial)
        );
        let pending = DfsControlPlaneStore::new(host.control_plane.as_mut())
            .get_metadata_shard_membership_handoff(&file_system_id, "root")
            .expect("read primary-role handoff")
            .expect("primary-role handoff parked");
        assert_eq!(pending.desired_shard, next_epoch);
    }

    #[test]
    fn control_plane_bootstrap_parks_same_primary_membership_until_dfs_handoff() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-bootstrap-membership-handoff");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let owner =
            NodeId::from_legacy_named("node_dfs_bootstrap_membership_owner").expect("owner node");
        let old_replica =
            NodeId::from_legacy_named("node_dfs_bootstrap_membership_old").expect("old replica");
        let new_replica =
            NodeId::from_legacy_named("node_dfs_bootstrap_membership_new").expect("new replica");
        let file_system_id =
            DfsFileSystemId::from_legacy_named("bootstrap-membership-fs").expect("filesystem id");
        let initial = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![old_replica],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica],
            epoch: 2,
            ..initial.clone()
        };
        let mut host = DfsControlPlaneDefaultClusterBootstrapHost::new(
            data_dir,
            cluster_paths,
            owner,
            Box::new(sinomo_core::kv::InProcessKvStore::new()),
        );
        host.put_metadata_shard(&initial).expect("create shard");

        assert!(matches!(
            host.put_metadata_shard(&desired),
            Err(DfsError::Distributed { message })
                if message.contains("membership or primary-role change")
                    && message.contains("fenced and parked")
                    && message.contains("exact DFS catch-up")
        ));
        assert_eq!(
            host.current_metadata_shard(&file_system_id, "root")
                .expect("read retiring shard"),
            Some(initial)
        );
        assert!(
            DfsControlPlaneStore::new(host.control_plane.as_mut())
                .get_metadata_shard_membership_handoff(&file_system_id, "root")
                .expect("read parked handoff")
                .is_some()
        );
    }

    #[tokio::test]
    async fn async_control_plane_bootstrap_host_parks_primary_rotation_handoff() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-bootstrap-shard-async-cas");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let owner =
            NodeId::from_legacy_named("node_dfs_bootstrap_async_cas_owner").expect("owner node");
        let replacement = NodeId::from_legacy_named("node_dfs_bootstrap_async_cas_replacement")
            .expect("replacement node");
        let file_system_id =
            DfsFileSystemId::from_legacy_named("bootstrap-async-cas-fs").expect("filesystem id");
        let initial = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replacement.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut host = DfsControlPlaneDefaultClusterAsyncBootstrapHost::new(
            data_dir,
            cluster_paths,
            owner.clone(),
            Box::new(sinomo_core::kv::InProcessKvStore::new()),
        );
        host.put_metadata_shard_async(initial.clone())
            .await
            .expect("create shard");
        let same_epoch_overwrite = DfsMetadataShardConfig {
            primary_node_id: replacement.clone(),
            ..initial.clone()
        };
        assert!(matches!(
            host.put_metadata_shard_async(same_epoch_overwrite).await,
            Err(DfsError::Distributed { message })
                if message.contains("stale metadata-shard bootstrap transition")
        ));
        assert!(
            DfsControlPlaneAsyncStore::new(host.control_plane.as_mut())
                .publish_metadata_shard_head_watermark(
                    &initial,
                    &DfsMetadataShardHeadWatermark {
                        file_system_id: file_system_id.clone(),
                        shard_id: "root".to_string(),
                        primary_node_id: owner.clone(),
                        epoch: initial.epoch,
                        journal_sequence: 1,
                        state_fingerprint: "blake3:bootstrap-async".to_string(),
                        acknowledged_node_ids: vec![owner.clone(), replacement.clone()],
                    },
                )
                .await
                .expect("publish async certified transfer head")
        );
        let next_epoch = DfsMetadataShardConfig {
            primary_node_id: replacement,
            replica_node_ids: vec![owner],
            epoch: 2,
            ..initial.clone()
        };
        assert!(matches!(
            host.put_metadata_shard_async(next_epoch.clone()).await,
            Err(DfsError::Distributed { message })
                if message.contains("membership or primary-role change")
                    && message.contains("fenced and parked")
                    && message.contains("exact DFS catch-up")
        ));
        assert_eq!(
            host.current_metadata_shard_async(file_system_id.clone(), "root".to_string())
                .await
                .expect("read shard"),
            Some(initial)
        );
        let pending = DfsControlPlaneAsyncStore::new(host.control_plane.as_mut())
            .get_metadata_shard_membership_handoff(&file_system_id, "root")
            .await
            .expect("read async primary-role handoff")
            .expect("async primary-role handoff parked");
        assert_eq!(pending.desired_shard, next_epoch);
    }

    #[test]
    fn metadata_shard_failover_targets_local_replica_when_current_primary_unhealthy() {
        let state = test_cluster_state_with_members(
            "cluster_dfs_default_unhealthy_primary",
            "node_dfs_unhealthy_primary_c",
            &[
                "node_dfs_unhealthy_primary_a",
                "node_dfs_unhealthy_primary_b",
            ],
        );
        let service_nodes = stable_dfs_service_node_ids(&state);
        assert_eq!(service_nodes.len(), 3);
        let local_index = service_nodes
            .iter()
            .position(|node_id| node_id == &state.local_node_id)
            .expect("local service node");
        let unreachable_index = (local_index + 1) % service_nodes.len();
        let unreachable_primary = service_nodes[unreachable_index].clone();
        let bootstrap_primary = service_nodes
            .iter()
            .find(|node_id| *node_id != &state.local_node_id && *node_id != &unreachable_primary)
            .cloned()
            .expect("bootstrap peer");

        let file_system = default_cluster_sifs(&state);
        let current = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: unreachable_primary.clone(),
            replica_node_ids: vec![bootstrap_primary.clone(), state.local_node_id.clone()],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let graph = ClusterConnectivityGraph::from_records(
            state.local_node_id.clone(),
            vec![test_connectivity_record(
                &state,
                &state.local_node_id,
                &bootstrap_primary,
                "reachable-peer",
                ClusterConnectivityState::Healthy,
            )],
        );
        let mut host = TestDefaultClusterBootstrapHost {
            current_shard: Some(current.clone()),
            connectivity_graph: Some(graph),
            primary_transfer_ready: true,
            primary_transfer_targets: Vec::new(),
        };

        let shard = default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            unreachable_index,
        )
        .expect("services shard");

        assert_eq!(shard.primary_node_id, state.local_node_id);
        assert_eq!(shard.epoch, current.epoch + 1);
        assert_eq!(
            host.primary_transfer_targets,
            vec![state.local_node_id.clone()]
        );
        assert!(
            shard
                .replica_node_ids
                .iter()
                .any(|node_id| node_id == &unreachable_primary),
            "offline members stay in the replica set so they can catch up after recovery"
        );
    }

    #[test]
    fn metadata_primary_failover_matrix_matches_safety_rules() {
        let state = test_cluster_state_with_members(
            "cluster_dfs_quint_failover_matrix",
            "node_dfs_quint_failover_local",
            &[
                "node_dfs_quint_failover_current",
                "node_dfs_quint_failover_default",
            ],
        );
        let service_nodes = stable_dfs_service_node_ids(&state);
        let current_primary =
            NodeId::from_legacy_named("node_dfs_quint_failover_current").expect("current primary");
        let default_primary =
            NodeId::from_legacy_named("node_dfs_quint_failover_default").expect("default primary");
        let file_system = default_cluster_sifs(&state);
        let current = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: current_primary.clone(),
            replica_node_ids: vec![default_primary.clone(), state.local_node_id.clone()],
            epoch: 3,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let healthy_graph = ClusterConnectivityGraph::from_records(
            state.local_node_id.clone(),
            vec![test_connectivity_record(
                &state,
                &state.local_node_id,
                &current_primary,
                "current-primary",
                ClusterConnectivityState::Healthy,
            )],
        );
        let current_unreachable_graph = ClusterConnectivityGraph::from_records(
            state.local_node_id.clone(),
            vec![test_connectivity_record(
                &state,
                &state.local_node_id,
                &default_primary,
                "default-primary",
                ClusterConnectivityState::Healthy,
            )],
        );

        assert_eq!(
            default_metadata_shard_target_primary_node_id(
                Some(&healthy_graph),
                &state,
                &service_nodes,
                default_primary.clone(),
                &current,
            ),
            default_primary,
            "reachable current primary keeps normal default placement"
        );
        assert_eq!(
            default_metadata_shard_target_primary_node_id(
                None,
                &state,
                &service_nodes,
                default_primary.clone(),
                &current,
            ),
            default_primary,
            "missing connectivity graph is not enough evidence for local promotion"
        );
        assert_eq!(
            default_metadata_shard_target_primary_node_id(
                Some(&current_unreachable_graph),
                &state,
                &service_nodes,
                default_primary.clone(),
                &DfsMetadataShardConfig {
                    replica_node_ids: vec![default_primary.clone()],
                    ..current.clone()
                },
            ),
            default_primary,
            "non-replica local node cannot self-promote"
        );
        assert_eq!(
            default_metadata_shard_target_primary_node_id(
                Some(&current_unreachable_graph),
                &state,
                &service_nodes,
                default_primary,
                &current,
            ),
            state.local_node_id,
            "unreachable current primary lets the local replica become target primary"
        );
    }

    #[test]
    fn metadata_shard_local_replica_failover_ready_requires_local_shard_status() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-default-local-failover-ready");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("root layout");
        let local = NodeId::from_legacy_named("node_dfs_failover_ready_local").expect("local");
        let remote_primary =
            NodeId::from_legacy_named("node_dfs_failover_ready_remote").expect("remote");
        let non_replica =
            NodeId::from_legacy_named("node_dfs_failover_ready_non_replica").expect("non replica");
        let file_system = distributed_sifs(remote_primary.clone());
        let current = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: remote_primary.clone(),
            replica_node_ids: vec![local.clone()],
            epoch: 5,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let preliminary_watermark = DfsMetadataShardHeadWatermark {
            file_system_id: current.file_system_id.clone(),
            shard_id: current.shard_id.clone(),
            primary_node_id: remote_primary.clone(),
            epoch: current.epoch,
            journal_sequence: 1,
            state_fingerprint: "blake3:preliminary".to_string(),
            acknowledged_node_ids: vec![remote_primary.clone(), local.clone()],
        };

        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &data_dir,
                &local,
                &current,
                &local,
                Some(&preliminary_watermark),
            )
            .expect("empty local shard is not promotable")
        );
        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &data_dir, &local, &current, &local, None,
            )
            .expect("missing quorum head is not promotable")
        );
        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &data_dir,
                &local,
                &DfsMetadataShardConfig {
                    replica_node_ids: vec![non_replica],
                    ..current.clone()
                },
                &local,
                Some(&preliminary_watermark),
            )
            .expect("non replica cannot self-promote")
        );
        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &data_dir,
                &local,
                &current,
                &remote_primary,
                Some(&preliminary_watermark),
            )
            .expect("local replica only proves local promotion")
        );

        let mut backend =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("local root shard");
        backend.put_file_system(&file_system).expect("filesystem");
        drop(backend);

        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &data_dir,
                &local,
                &current,
                &local,
                Some(&preliminary_watermark),
            )
            .expect("locally originated shard state is not certified replica progress")
        );

        let replica_data_dir = SinomoDataDir::new(root.join("replica-data"));
        replica_data_dir
            .ensure_root_layout()
            .expect("replica root layout");
        let shard_id = DfsMetadataShardId::new("root");
        let mut primary_backend =
            LocalSobjMetadataBackend::new(shard_id.clone(), root.join("remote-primary-metadata"))
                .expect("remote primary backend");
        primary_backend
            .put_file_system(&file_system)
            .expect("primary filesystem");
        let batch = primary_backend
            .journal_batch_since_limited(
                0,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .expect("primary journal batch")
            .expect("primary journal entry");
        let mut replica_backend =
            open_local_metadata_shard_backend(&replica_data_dir, shard_id.clone())
                .expect("replica root shard");
        replica_backend
            .apply_journal_batch(&batch)
            .expect("apply certified primary batch");
        let certified_fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
            &replica_backend,
            &shard_id,
            batch.next_after_sequence,
        )
        .expect("certified replica fingerprint");
        replica_backend
            .put_metadata_prefix_ack_floor(&DfsMetadataPrefixAckFloor {
                shard_id: shard_id.clone(),
                journal_sequence: batch.next_after_sequence,
                state_fingerprint: certified_fingerprint.clone(),
            })
            .expect("persist certified replica checkpoint floor");
        drop(replica_backend);

        let certified_watermark = DfsMetadataShardHeadWatermark {
            journal_sequence: batch.next_after_sequence,
            state_fingerprint: certified_fingerprint,
            ..preliminary_watermark.clone()
        };
        let stale_epoch_watermark = DfsMetadataShardHeadWatermark {
            epoch: current.epoch.saturating_sub(1),
            ..certified_watermark.clone()
        };
        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &replica_data_dir,
                &local,
                &current,
                &local,
                Some(&stale_epoch_watermark),
            )
            .expect("stale-epoch watermark is not promotable")
        );
        let missing_local_ack = DfsMetadataShardHeadWatermark {
            acknowledged_node_ids: vec![remote_primary],
            ..certified_watermark.clone()
        };
        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &replica_data_dir,
                &local,
                &current,
                &local,
                Some(&missing_local_ack),
            )
            .expect("candidate absent from quorum acknowledgements is not promotable")
        );

        assert!(
            metadata_shard_local_replica_failover_ready_from_local_backend(
                &replica_data_dir,
                &local,
                &current,
                &local,
                Some(&certified_watermark),
            )
            .expect("quorum-certified primary head makes caught-up replica promotable")
        );
    }

    fn test_cluster_state_with_members(
        cluster_name: &str,
        local_name: &str,
        remote_names: &[&str],
    ) -> ClusterBootstrapState {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named(cluster_name).expect("cluster id");
        let local = NodeId::from_legacy_named(local_name).expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs default cluster test",
            local,
            "local",
            "pk-local",
            now,
        );
        for remote_name in remote_names {
            let node_id = NodeId::from_legacy_named(*remote_name).expect("remote node");
            state.members.push(ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id,
                *remote_name,
                format!("pk-{remote_name}"),
                now,
            ));
        }
        state
    }

    fn test_connectivity_record(
        state: &ClusterBootstrapState,
        source: &NodeId,
        target: &NodeId,
        endpoint_name: &str,
        connectivity_state: ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        let healthy = connectivity_state == ClusterConnectivityState::Healthy;
        ClusterConnectivityRecord {
            cluster_id: state.cluster_id.clone(),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint_id: ClusterEndpointId::derived(
                "sinomo.test.dfs.metadata-primary",
                format!("{}\0{}\0{endpoint_name}", source.as_str(), target.as_str()),
            ),
            transport: ClusterTransportKind::LanTcp,
            address: format!("{endpoint_name}.example.test:21353"),
            state: connectivity_state,
            latency_ms: healthy.then_some(2),
            consecutive_failures: if healthy { 0 } else { 1 },
            last_success_at: healthy.then(|| now.to_string()),
            last_failure_at: (!healthy).then(|| now.to_string()),
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(900),
        }
    }
}
