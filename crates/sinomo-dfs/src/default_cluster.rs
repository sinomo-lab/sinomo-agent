//! Default SIFS and metadata bootstrap for cluster-backed DFS.
//!
//! This module owns the default distributed SIFS topology, shard layout, and
//! service advertisements that make a cluster's DFS usable after bootstrap.

use serde::Serialize;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterPaths, cluster_healthy_route_next_hop,
    pooled_routed_relationship_is_active,
};
use sinomo_core::node::{
    DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY,
    DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY, NodeId,
};
use sinomo_core::{ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, DfsObjectPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

use crate::control_plane::topology_preserving_metadata_shard_replicas;
#[cfg(test)]
use crate::metadata_replication::materialize_default_sifs_metadata_on_active_primary_shards;
use crate::service_sync::rebind_completed_metadata_shard_membership_handoff;
use crate::{
    AsyncDfsControlPlaneKvClient, DfsControlPlaneAsyncStore, DfsControlPlaneKvClient,
    DfsControlPlaneStore, DfsError, DfsFileSystem, DfsFileSystemConfig, DfsFileSystemId,
    DfsMessageSender, DfsMetadataBackend, DfsMetadataJournalBatchSender,
    DfsMetadataNamespacePrepareCodec, DfsMetadataShardAssignment, DfsMetadataShardConfig,
    DfsMetadataShardHeadWatermark, DfsMetadataShardId, DfsMetadataShardMembershipHandoff,
    DfsMetadataShardMembershipHandoffProof, DfsMetadataSnapshotInstallSender, DfsNodeAdvertisement,
    DfsPeerTransport, DfsSmokeOptions, DfsSmokeReport, DfsSubtreePolicy,
    ResidentLocalShardedMetadataBackend, SIFS_SERVICES_PATH, SIFS_USERS_PATH, content_service_key,
    default_dfs_metadata_shards_root, dfs_message_sender_from_peer_transport,
    dfs_metadata_journal_batch_sender_from_peer_transport,
    dfs_metadata_snapshot_install_sender_from_peer_transport, distributed_sifs,
    file_system_config_key, local_dfs_metadata_shard_path, materialize_default_sifs_metadata,
    metadata_replication::materialize_default_sifs_metadata_on_bound_primary_shards,
    metadata_service_key, metadata_shard_key, metadata_shard_membership_handoff_key,
    metadata_shard_membership_handoff_proof_key, open_cluster_aware_dfs_metadata_backend,
    open_local_metadata_shard_backend, open_local_sharded_metadata_backend_for_replica_repair,
    run_cluster_dfs_smoke_with_message_sender, sifs_node_files_path,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDefaultClusterBootstrapOptions {
    pub publish_cluster_control: bool,
    pub materialize_local_metadata: bool,
    /// Failure domain advertised for this process's local DFS service.
    ///
    /// Each enabled DFS node owns only its local advertisement, so a bootstrap
    /// pass cannot manufacture or overwrite another node's topology.
    pub local_zone: Option<String>,
    /// Daemon-sampled aggregate content state for this node. The record may
    /// summarize several physical locations but always retains one node id.
    pub local_content_advertisement: Option<DfsNodeAdvertisement>,
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

type DfsDefaultMaterializationHandoffEvidence = Vec<(
    Option<DfsMetadataShardMembershipHandoff>,
    Option<DfsMetadataShardMembershipHandoffProof>,
)>;

#[derive(Clone, Debug)]
struct DfsDefaultMaterializationSignature {
    file_system: DfsFileSystem,
    shards: Vec<DfsMetadataShardConfig>,
    bootstrap_owner_node_ids: Vec<NodeId>,
    fallback_owner_node_id: NodeId,
    handoff_evidence: DfsDefaultMaterializationHandoffEvidence,
}

impl PartialEq for DfsDefaultMaterializationSignature {
    fn eq(&self, other: &Self) -> bool {
        crate::placement::dfs_file_system_semantically_eq(&self.file_system, &other.file_system)
            && self.shards == other.shards
            && self.bootstrap_owner_node_ids == other.bootstrap_owner_node_ids
            && self.fallback_owner_node_id == other.fallback_owner_node_id
            && self.handoff_evidence == other.handoff_evidence
    }
}

impl Eq for DfsDefaultMaterializationSignature {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsDefaultMaterializationCacheEntry {
    signature: DfsDefaultMaterializationSignature,
    awaiting_replica_progress: bool,
    invalidated_for_replica_progress: bool,
}

// Process-local successful-pass cache. The daemon's per-data-dir lifecycle gate
// serializes calls that share a metadata root, so the signature check and the
// materialization it guards cannot race there. Other callers may at worst do a
// redundant idempotent pass. We record only a fully scanned successful pass;
// process restart/crash clears the cache and forces recovery validation again.
static DFS_DEFAULT_MATERIALIZATION_SIGNATURES: OnceLock<
    Mutex<BTreeMap<PathBuf, DfsDefaultMaterializationCacheEntry>>,
> = OnceLock::new();
static DFS_DEFAULT_MATERIALIZATION_IMMEDIATE_RETRIES: OnceLock<Mutex<BTreeSet<PathBuf>>> =
    OnceLock::new();

const METADATA_PRIMARY_ROUTING_UNAVAILABLE_GRACE: Duration = Duration::from_secs(180);
type MetadataPrimaryRoutingSuspicionKey = (PathBuf, NodeId, NodeId);
static METADATA_PRIMARY_ROUTING_SUSPICIONS: OnceLock<
    Mutex<BTreeMap<MetadataPrimaryRoutingSuspicionKey, Instant>>,
> = OnceLock::new();

fn default_materialization_is_current(
    data_root: &Path,
    signature: &DfsDefaultMaterializationSignature,
) -> bool {
    DFS_DEFAULT_MATERIALIZATION_SIGNATURES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .is_ok_and(|signatures| {
            signatures.get(data_root).is_some_and(|cached| {
                cached.signature == *signature && !cached.invalidated_for_replica_progress
            })
        })
}

fn record_default_materialization(
    data_root: PathBuf,
    signature: DfsDefaultMaterializationSignature,
    awaiting_replica_progress: bool,
) {
    let mut establish_replica_subscription = false;
    if let Ok(mut signatures) = DFS_DEFAULT_MATERIALIZATION_SIGNATURES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        establish_replica_subscription = awaiting_replica_progress
            && signatures
                .get(&data_root)
                .is_none_or(|cached| !cached.awaiting_replica_progress);
        signatures.insert(
            data_root.clone(),
            DfsDefaultMaterializationCacheEntry {
                signature,
                awaiting_replica_progress,
                invalidated_for_replica_progress: false,
            },
        );
    }
    if let Ok(mut retries) = DFS_DEFAULT_MATERIALIZATION_IMMEDIATE_RETRIES
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
    {
        if establish_replica_subscription {
            // Establishing a dynamic watermark dependency needs one reactive
            // verification generation. A watermark can commit after this pass
            // sampled the old value but before the cache publishes
            // `awaiting_replica_progress`; the committed-snapshot observer
            // correctly ignored it while unsubscribed. The immediate pass
            // resamples the resident snapshot after the subscription bit is
            // visible, closing that false-to-true handoff without a polling
            // timer. Subsequent waiting passes remain subscribed and do not
            // self-wake.
            retries.insert(data_root.clone());
        } else {
            retries.remove(&data_root);
        }
    }
    tracing::trace!(
        target: "sinomo.dfs.reconciliation",
        awaiting_replica_progress,
        establish_replica_subscription,
        data_root = %data_root.display(),
        "recorded default DFS materialization generation"
    );
}

fn request_default_materialization_immediate_retry(data_root: PathBuf) {
    if let Ok(mut retries) = DFS_DEFAULT_MATERIALIZATION_IMMEDIATE_RETRIES
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
    {
        retries.insert(data_root);
    }
}

/// Consume an edge requesting one immediate materialization generation.
///
/// The edge is emitted when a bounded pass stops at its mutation cap or when
/// a pass first establishes a dynamic replica-progress dependency. The
/// daemon's sole reconciliation owner uses it to continue bounded work or
/// verify the new subscription against the resident snapshot without a
/// polling timer.
pub fn take_default_cluster_dfs_materialization_immediate_retry(data_dir: &SinomoDataDir) -> bool {
    DFS_DEFAULT_MATERIALIZATION_IMMEDIATE_RETRIES
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .is_ok_and(|mut retries| retries.remove(&data_dir.root()))
}

/// Invalidate a settled materialization pass only when it is waiting for an
/// exact replica generation to finish an authority-scoped shard migration.
/// Ordinary metadata replication therefore does not wake default-cluster
/// reconciliation in steady state.
pub fn invalidate_default_cluster_dfs_materialization_for_replica_progress(
    data_dir: &SinomoDataDir,
) -> bool {
    let Ok(mut signatures) = DFS_DEFAULT_MATERIALIZATION_SIGNATURES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    else {
        return false;
    };
    let data_root = data_dir.root();
    let Some(cached) = signatures.get_mut(&data_root) else {
        return false;
    };
    if !cached.awaiting_replica_progress || cached.invalidated_for_replica_progress {
        return false;
    }
    // Preserve the subscription bit while forcing the signature miss. A local
    // replica can advance between the batch wake and the next pass; removing
    // this entry would lose that edge and park migration indefinitely.
    cached.invalidated_for_replica_progress = true;
    true
}

/// Whether the settled/default materializer currently consumes exact replica
/// generations.
#[cfg(test)]
fn default_cluster_dfs_materialization_awaits_replica_progress(data_dir: &SinomoDataDir) -> bool {
    DFS_DEFAULT_MATERIALIZATION_SIGNATURES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .is_ok_and(|signatures| {
            signatures
                .get(&data_dir.root())
                .is_some_and(|cached| cached.awaiting_replica_progress)
        })
}

/// Prime the real process-local subscription state for cross-crate reactor
/// tests. This is compiled only when a consumer explicitly enables the
/// `test-support` feature; production callers establish the same state solely
/// through a completed materialization pass.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn test_establish_default_cluster_dfs_replica_progress_subscription(data_dir: &SinomoDataDir) {
    let local_node_id =
        NodeId::from_legacy_named("node_dfs_test_replica_subscription").expect("test node id");
    record_default_materialization(
        data_dir.root(),
        DfsDefaultMaterializationSignature {
            file_system: distributed_sifs(local_node_id.clone()),
            shards: Vec::new(),
            bootstrap_owner_node_ids: vec![local_node_id.clone()],
            fallback_owner_node_id: local_node_id,
            handoff_evidence: Vec::new(),
        },
        true,
    );
}

#[cfg(test)]
static DFS_DEFAULT_MATERIALIZATION_RUNS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();

#[cfg(test)]
fn record_default_materialization_run(data_root: &Path) {
    if let Ok(mut runs) = DFS_DEFAULT_MATERIALIZATION_RUNS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let count = runs.entry(data_root.to_path_buf()).or_default();
        *count = count.saturating_add(1);
    }
}

#[cfg(test)]
fn default_materialization_run_count(data_root: &Path) -> usize {
    DFS_DEFAULT_MATERIALIZATION_RUNS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|runs| runs.get(data_root).copied())
        .unwrap_or_default()
}

pub trait DfsDefaultClusterBootstrapHost {
    fn metadata_shard_node_routing_unavailable(
        &self,
        state: &ClusterBootstrapState,
        node_id: &NodeId,
    ) -> bool {
        let _ = (state, node_id);
        false
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
    fn metadata_shard_node_routing_unavailable(
        &self,
        state: &ClusterBootstrapState,
        node_id: &NodeId,
    ) -> bool {
        let _ = (state, node_id);
        false
    }

    fn metadata_service_node_ids(
        &self,
        state: &ClusterBootstrapState,
        service_node_ids: Vec<NodeId>,
    ) -> Vec<NodeId> {
        let _ = state;
        service_node_ids
    }

    fn metadata_service_node_ids_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        service_node_ids: Vec<NodeId>,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Vec<NodeId>> {
        let service_node_ids = self.metadata_service_node_ids(state, service_node_ids);
        Box::pin(async move { Ok(service_node_ids) })
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
            if let Some(pending) = pending.or(control.get_metadata_shard_membership_handoff(
                &current.file_system_id,
                &current.shard_id,
            )?) {
                return Err(DfsError::MetadataShardMembershipHandoffParked {
                    file_system_id: pending.retiring_shard.file_system_id.clone(),
                    shard_id: pending.retiring_shard.shard_id.clone(),
                    handoff_id: pending.handoff_id,
                    retiring_epoch: pending.retiring_shard.epoch,
                    desired_epoch: pending.desired_shard.epoch,
                });
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} changed concurrently while beginning its membership handoff; retry",
                    desired.shard_id
                ),
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
    resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
}

impl DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    #[cfg(test)]
    pub(crate) fn new(
        data_dir: SinomoDataDir,
        cluster_paths: ClusterPaths,
        local_node_id: NodeId,
        control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
    ) -> Self {
        Self::new_with_resident_metadata_backend(
            data_dir,
            cluster_paths,
            local_node_id,
            control_plane,
            None,
        )
    }

    pub fn new_with_resident_metadata_backend(
        data_dir: SinomoDataDir,
        cluster_paths: ClusterPaths,
        local_node_id: NodeId,
        control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
        resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
    ) -> Self {
        Self {
            data_dir,
            cluster_paths,
            local_node_id,
            control_plane,
            resident_metadata_backend,
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
            let pending = match pending {
                Some(pending) => Some(pending),
                None => self
                    .control_plane
                    .get_async(&metadata_shard_membership_handoff_key(
                        &current.file_system_id,
                        &current.shard_id,
                    ))
                    .await?
                    .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
                    .transpose()?,
            };
            if let Some(pending) = pending {
                return Err(DfsError::MetadataShardMembershipHandoffParked {
                    file_system_id: pending.retiring_shard.file_system_id.clone(),
                    shard_id: pending.retiring_shard.shard_id.clone(),
                    handoff_id: pending.handoff_id,
                    retiring_epoch: pending.retiring_shard.epoch,
                    desired_epoch: pending.desired_shard.epoch,
                });
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} changed concurrently while beginning its membership handoff; retry",
                    desired.shard_id
                ),
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
    fn metadata_shard_node_routing_unavailable(
        &self,
        state: &ClusterBootstrapState,
        node_id: &NodeId,
    ) -> bool {
        metadata_shard_node_routing_unavailable(
            &self.cluster_paths,
            state,
            &self.local_node_id,
            node_id,
        )
    }

    fn metadata_service_node_ids(
        &self,
        state: &ClusterBootstrapState,
        service_node_ids: Vec<NodeId>,
    ) -> Vec<NodeId> {
        let entries = self
            .control_plane
            .range_prefix(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
            .unwrap_or_default();
        discovered_metadata_service_node_ids(state, service_node_ids, entries)
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
        if metadata_shard_primary_transfer_ready_from_local_backend(
            &self.data_dir,
            &self.local_node_id,
            current,
            target_primary_node_id,
        )? {
            return Ok(true);
        }
        let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)
        else {
            return Ok(false);
        };
        if !metadata_shard_node_routing_unavailable(
            &self.cluster_paths,
            &state,
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
        DfsControlPlaneStore::new(self.control_plane.as_mut())
            .ensure_file_system_config_defaults(config)
            .map_err(DfsError::from)
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
        let mut active_primary_shard_ids = BTreeSet::new();
        let mut completion_evidence = Vec::with_capacity(shards.len());
        for active in &shards {
            let store = DfsControlPlaneStore::new(self.control_plane.as_mut());
            let pending = store
                .get_metadata_shard_membership_handoff(&active.file_system_id, &active.shard_id)?;
            let proof = store.get_metadata_shard_membership_handoff_proof(
                &active.file_system_id,
                &active.shard_id,
            )?;
            if active.primary_node_id == self.local_node_id && pending.is_none() {
                active_primary_shard_ids.insert(DfsMetadataShardId::new(active.shard_id.clone()));
            }
            completion_evidence.push((pending, proof));
        }
        if active_primary_shard_ids.is_empty() {
            return Ok(());
        }
        let metadata_root = default_dfs_metadata_shards_root(&self.data_dir);
        let mut backend = open_local_sharded_metadata_backend_for_replica_repair(
            metadata_root.clone(),
            shards.clone(),
            &self.local_node_id,
        )?;
        for (active, (pending, proof)) in shards.iter().zip(&completion_evidence) {
            if active_primary_shard_ids.contains(&DfsMetadataShardId::new(active.shard_id.clone()))
            {
                rebind_completed_metadata_shard_membership_handoff(
                    &mut backend,
                    &self.local_node_id,
                    active,
                    pending.as_ref(),
                    proof.as_ref(),
                )?;
            }
        }
        materialize_default_sifs_metadata_on_bound_primary_shards(
            metadata_root,
            &mut backend,
            file_system,
            &shards,
            &active_primary_shard_ids,
            &self.local_node_id,
            bootstrap_owner_node_ids,
            fallback_owner_node_id,
            OffsetDateTime::now_utc(),
        )?;
        Ok(())
    }
}

impl DfsDefaultClusterAsyncBootstrapHost for DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    fn metadata_shard_node_routing_unavailable(
        &self,
        state: &ClusterBootstrapState,
        node_id: &NodeId,
    ) -> bool {
        metadata_shard_node_routing_unavailable(
            &self.cluster_paths,
            state,
            &self.local_node_id,
            node_id,
        )
    }

    fn metadata_service_node_ids_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        service_node_ids: Vec<NodeId>,
    ) -> DfsDefaultClusterBootstrapFuture<'a, Vec<NodeId>> {
        Box::pin(async move {
            let entries = self
                .control_plane
                .range_prefix_async(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
                .await
                .unwrap_or_default();
            Ok(discovered_metadata_service_node_ids(
                state,
                service_node_ids,
                entries,
            ))
        })
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
            if metadata_shard_primary_transfer_ready_from_local_backend(
                &self.data_dir,
                &self.local_node_id,
                &current,
                &target_primary_node_id,
            )? {
                return Ok(true);
            }
            let Ok(state) =
                sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)
            else {
                return Ok(false);
            };
            if !metadata_shard_node_routing_unavailable(
                &self.cluster_paths,
                &state,
                &self.local_node_id,
                &current.primary_node_id,
            ) {
                return Ok(false);
            }
            let watermark = DfsControlPlaneAsyncStore::new(self.control_plane.as_mut())
                .get_metadata_shard_head_watermark(&current.file_system_id, &current.shard_id)
                .await?;
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
            DfsControlPlaneAsyncStore::new(self.control_plane.as_mut())
                .ensure_file_system_config_defaults(&config)
                .await
                .map_err(DfsError::from)
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
        mut bootstrap_owner_node_ids: Vec<NodeId>,
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
            bootstrap_owner_node_ids.sort();
            bootstrap_owner_node_ids.dedup();
            let signature = DfsDefaultMaterializationSignature {
                file_system: file_system.clone(),
                shards: shards.clone(),
                bootstrap_owner_node_ids: bootstrap_owner_node_ids.clone(),
                fallback_owner_node_id: fallback_owner_node_id.clone(),
                handoff_evidence: completion_evidence.clone(),
            };
            let data_root = self.data_dir.root();
            if default_materialization_is_current(&data_root, &signature) {
                return Ok(());
            }
            #[cfg(test)]
            record_default_materialization_run(&data_root);
            let mut active_primary_shard_ids = BTreeSet::new();
            active_primary_shard_ids.extend(
                shards
                    .iter()
                    .zip(&completion_evidence)
                    .filter(|&(active, (pending, _))| {
                        active.primary_node_id == self.local_node_id && pending.is_none()
                    })
                    .map(|(active, (_pending, _))| {
                        DfsMetadataShardId::new(active.shard_id.clone())
                    }),
            );
            if active_primary_shard_ids.is_empty() {
                record_default_materialization(data_root, signature, false);
                return Ok(());
            }
            let run_materialization = |backend: &mut crate::LocalShardedMetadataBackend| {
                for (active, (pending, proof)) in shards.iter().zip(&completion_evidence) {
                    if active_primary_shard_ids
                        .contains(&DfsMetadataShardId::new(active.shard_id.clone()))
                    {
                        rebind_completed_metadata_shard_membership_handoff(
                            backend,
                            &self.local_node_id,
                            active,
                            pending.as_ref(),
                            proof.as_ref(),
                        )?;
                    }
                }
                materialize_default_sifs_metadata_on_bound_primary_shards(
                    default_dfs_metadata_shards_root(&self.data_dir),
                    backend,
                    &file_system,
                    &shards,
                    &active_primary_shard_ids,
                    &self.local_node_id,
                    bootstrap_owner_node_ids,
                    fallback_owner_node_id,
                    OffsetDateTime::now_utc(),
                )
            };
            let metadata_root = default_dfs_metadata_shards_root(&self.data_dir);
            let resident = match self.resident_metadata_backend.as_ref() {
                Some(resident) if resident.matches_topology(&metadata_root, &shards)? => {
                    Some(resident)
                }
                Some(_) | None => None,
            };
            let materialization = if let Some(resident) = resident {
                let mut lease = resident.checkout_global()?;
                run_materialization(&mut lease)?
            } else {
                let mut backend = open_local_sharded_metadata_backend_for_replica_repair(
                    metadata_root,
                    shards.clone(),
                    &self.local_node_id,
                )?;
                run_materialization(&mut backend)?
            };
            if !materialization.migration.reached_limit {
                record_default_materialization(
                    data_root,
                    signature,
                    materialization.awaiting_replica_progress,
                );
            } else {
                request_default_materialization_immediate_retry(data_root);
            }
            Ok(())
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
    // Every visible DFS publication requires this target to acknowledge the
    // exact prepared prefix. The durable sender cursor is therefore the
    // DFS-native proof that it covers the current head; KV supplies only the
    // configured membership being changed.
    let target_cursor = backend.metadata_replication_cursor(target_primary_node_id, &shard_id)?;
    Ok(target_cursor >= journal_last_sequence)
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
    // Cursor files are checkpointed replay hints. Failover readiness must use
    // the backend's exact durable table-prefix proof, which remains current
    // between cursor checkpoints.
    let replayed_through_sequence = backend.metadata_replay_cursor(&shard_id)?;
    if watermark.journal_sequence == 0
        || status.journal_last_sequence < watermark.journal_sequence
        || replayed_through_sequence < watermark.journal_sequence
    {
        return Ok(false);
    }
    let authority_floor = backend.metadata_authority_ack_floor(&shard_id)?;
    let prefix_floor = backend.metadata_prefix_ack_floor(&shard_id)?;
    if authority_floor
        .as_ref()
        .is_none_or(|floor| floor.journal_sequence < watermark.journal_sequence)
        && prefix_floor
            .as_ref()
            .is_none_or(|floor| floor.journal_sequence < watermark.journal_sequence)
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
        && acknowledged_node_ids.len() > configured_node_ids.len() / 2
}

fn metadata_shard_node_routing_unavailable(
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    if target_node_id == local_node_id {
        return false;
    }
    let Ok(graph) = ClusterConnectivityGraph::try_read_for_local(cluster_paths, state) else {
        return false;
    };
    if cluster_healthy_route_next_hop(&graph, local_node_id, target_node_id).is_some() {
        clear_metadata_primary_routing_suspicion(cluster_paths, local_node_id, target_node_id);
        return false;
    }
    match pooled_routed_relationship_is_active(
        &state.cluster_id.to_string(),
        local_node_id,
        target_node_id,
    ) {
        Some(true) => {
            clear_metadata_primary_routing_suspicion(cluster_paths, local_node_id, target_node_id);
            false
        }
        None => false,
        Some(false) => metadata_primary_routing_suspicion_elapsed(
            cluster_paths,
            local_node_id,
            target_node_id,
            Instant::now(),
            METADATA_PRIMARY_ROUTING_UNAVAILABLE_GRACE,
        ),
    }
}

fn metadata_primary_routing_suspicion_key(
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> MetadataPrimaryRoutingSuspicionKey {
    (
        cluster_paths.root.clone(),
        local_node_id.clone(),
        target_node_id.clone(),
    )
}

fn clear_metadata_primary_routing_suspicion(
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) {
    if let Ok(mut suspicions) = METADATA_PRIMARY_ROUTING_SUSPICIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        suspicions.remove(&metadata_primary_routing_suspicion_key(
            cluster_paths,
            local_node_id,
            target_node_id,
        ));
    }
}

fn metadata_primary_routing_suspicion_elapsed(
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    now: Instant,
    grace: Duration,
) -> bool {
    let Ok(mut suspicions) = METADATA_PRIMARY_ROUTING_SUSPICIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    else {
        return false;
    };
    let first_observed = suspicions
        .entry(metadata_primary_routing_suspicion_key(
            cluster_paths,
            local_node_id,
            target_node_id,
        ))
        .or_insert(now);
    now.saturating_duration_since(*first_observed) >= grace
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
        publish_dfs_service_advertisements_with_host(
            host,
            state,
            options.local_zone.as_deref(),
            options.local_content_advertisement.as_ref(),
        )?;
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
        if options.local_zone.is_some() || options.local_content_advertisement.is_some() {
            publish_local_dfs_service_advertisement_with_host(
                host,
                state,
                options.local_zone.as_deref(),
                options.local_content_advertisement.as_ref(),
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
            options.local_content_advertisement.as_ref(),
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
        if options.local_zone.is_some() || options.local_content_advertisement.is_some() {
            publish_local_dfs_service_advertisement_with_async_host(
                host,
                state,
                options.local_zone.as_deref(),
                options.local_content_advertisement.as_ref(),
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

pub async fn ensure_default_cluster_dfs_async_with_resident_metadata(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
    resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
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
    let mut host =
        DfsControlPlaneDefaultClusterAsyncBootstrapHost::new_with_resident_metadata_backend(
            data_dir.clone(),
            cluster_paths.clone(),
            state.local_node_id.clone(),
            control_plane,
            resident_metadata_backend,
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
    let bootstrap_primary_node_id = service_node_ids
        .first()
        .cloned()
        .unwrap_or_else(|| state.local_node_id.clone());
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
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let Some(current) = host.current_metadata_shard(&file_system.file_system_id, shard_id)? else {
        desired.primary_node_id = bootstrap_primary_node_id.clone();
        desired.replica_node_ids =
            default_metadata_shard_replica_node_ids(&service_node_ids, &bootstrap_primary_node_id);
        let _ = select_metadata_namespace_prepare_codec(state, &mut desired, None);
        return Ok(desired);
    };
    // Default placement must be identical on every daemon. A process-local
    // routing view is not durable authority to promote that daemon's replica;
    // the transfer-readiness fence below retains the current primary until the
    // deterministic target can safely take over.
    desired.primary_node_id = default_target_primary_node_id;
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
    if !select_metadata_namespace_prepare_codec(state, &mut desired, Some(&current)) {
        return Ok(current);
    }
    if current.primary_node_id != desired.primary_node_id
        && !host.metadata_shard_primary_transfer_ready(&current, &desired.primary_node_id)?
    {
        let mut expanded = desired.clone();
        expanded.primary_node_id = current.primary_node_id.clone();
        expanded.replica_node_ids =
            default_metadata_shard_replica_node_ids(&service_node_ids, &current.primary_node_id);
        if !select_metadata_namespace_prepare_codec(state, &mut expanded, Some(&current)) {
            return Ok(current);
        }
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
    let service_node_ids = host
        .metadata_service_node_ids_async(state, stable_dfs_service_node_ids(state))
        .await?;
    let bootstrap_primary_node_id = service_node_ids
        .first()
        .cloned()
        .unwrap_or_else(|| state.local_node_id.clone());
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
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let Some(current) = host
        .current_metadata_shard_async(file_system.file_system_id.clone(), shard_id.to_string())
        .await?
    else {
        desired.primary_node_id = bootstrap_primary_node_id.clone();
        desired.replica_node_ids =
            default_metadata_shard_replica_node_ids(&service_node_ids, &bootstrap_primary_node_id);
        let _ = select_metadata_namespace_prepare_codec(state, &mut desired, None);
        return Ok(desired);
    };
    // See the synchronous planner above. Failover requires durable, shared
    // authority; a daemon's transient route suspicion cannot change placement.
    desired.primary_node_id = default_target_primary_node_id;
    desired.replica_node_ids =
        default_metadata_shard_replica_node_ids(&service_node_ids, &desired.primary_node_id);
    if current.primary_node_id != desired.primary_node_id
        && let Some(replica_node_ids) =
            topology_preserving_metadata_shard_replicas(&current, &desired.primary_node_id)
    {
        desired.replica_node_ids = replica_node_ids;
    }
    if !select_metadata_namespace_prepare_codec(state, &mut desired, Some(&current)) {
        return Ok(current);
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
        if !select_metadata_namespace_prepare_codec(state, &mut expanded, Some(&current)) {
            return Ok(current);
        }
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
        && left.namespace_prepare_codec == right.namespace_prepare_codec
        && left.default_policy == right.default_policy
}

fn select_metadata_namespace_prepare_codec(
    state: &ClusterBootstrapState,
    shard: &mut DfsMetadataShardConfig,
    current: Option<&DfsMetadataShardConfig>,
) -> bool {
    let assigned_node_ids = std::iter::once(&shard.primary_node_id)
        .chain(shard.replica_node_ids.iter())
        .collect::<BTreeSet<_>>();
    let all_assigned_support = |required_capability: &str| {
        !assigned_node_ids.is_empty()
            && assigned_node_ids.iter().all(|node_id| {
                state.members.iter().any(|member| {
                    member.node_id == **node_id
                        && member.state.is_authorized()
                        && member
                            .capabilities
                            .iter()
                            .any(|capability| capability.as_str() == required_capability)
                })
            })
    };
    let selected = if all_assigned_support(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY) {
        DfsMetadataNamespacePrepareCodec::PackedV2
    } else if all_assigned_support(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY) {
        DfsMetadataNamespacePrepareCodec::PackedV1
    } else {
        DfsMetadataNamespacePrepareCodec::Legacy
    };
    if let Some(current) = current
        && selected.rank() < current.namespace_prepare_codec.rank()
    {
        // Capability loss cannot prove that an older binary can reopen retained
        // packed journal history. Keep the exact active codec instead of
        // manufacturing an unsafe downgrade. Membership reconciliation can
        // resume after every desired member is capable (or the incapable
        // member has been removed); a future rollback protocol must provide a
        // compatible snapshot and replay floor first.
        shard.namespace_prepare_codec = current.namespace_prepare_codec;
        return false;
    }
    shard.namespace_prepare_codec = selected;
    true
}

fn discovered_metadata_service_node_ids(
    state: &ClusterBootstrapState,
    service_node_ids: Vec<NodeId>,
    entries: Vec<sinomo_core::kv::KvEntry>,
) -> Vec<NodeId> {
    if entries.is_empty() {
        return service_node_ids;
    }
    let Ok(instances) = sinomo_core::service::service_instances_from_entries(&entries) else {
        return service_node_ids;
    };
    let providers = sinomo_core::service::service_instances_providing_capability(
        instances,
        "dfs.metadata.replicate",
    )
    .into_iter()
    .map(|instance| instance.node_id)
    .collect::<BTreeSet<_>>();
    if providers.is_empty() {
        return service_node_ids;
    }
    let mut discovered = service_node_ids
        .into_iter()
        .filter(|node_id| node_id == &state.local_node_id || providers.contains(node_id))
        .collect::<Vec<_>>();
    if !discovered
        .iter()
        .any(|node_id| node_id == &state.local_node_id)
    {
        discovered.push(state.local_node_id.clone());
    }
    discovered.sort();
    discovered.dedup();
    discovered
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
    local_content_advertisement: Option<&DfsNodeAdvertisement>,
) -> Result<(), DfsError> {
    // A node may advertise only the service it actually materialized. Cluster
    // membership is not evidence that another member runs DFS; every enabled
    // DFS module publishes its own record during its local lifecycle.
    publish_local_dfs_service_advertisement_with_host(
        host,
        state,
        local_zone,
        local_content_advertisement,
    )
}

fn publish_local_dfs_service_advertisement_with_host(
    host: &mut impl DfsDefaultClusterBootstrapHost,
    state: &ClusterBootstrapState,
    local_zone: Option<&str>,
    local_content_advertisement: Option<&DfsNodeAdvertisement>,
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
    let mut content = local_content_advertisement
        .cloned()
        .unwrap_or_else(|| DfsNodeAdvertisement::content(member.node_id.clone()));
    if content.node_id != member.node_id {
        return Err(DfsError::Distributed {
            message: format!(
                "local DFS content advertisement names node {} but daemon owns {}",
                content.node_id, member.node_id
            ),
        });
    }
    content.accepts_metadata = false;
    metadata.zone = local_zone.map(str::to_string);
    content.zone = local_zone.map(str::to_string);
    let metadata = stable_dfs_advertisement(host, &metadata_key, metadata, false)?;
    let content = stable_dfs_advertisement(host, &content_key, content, false)?;
    host.put_node_advertisement(metadata_key, &metadata)?;
    host.put_node_advertisement(content_key, &content)
}

async fn publish_dfs_service_advertisements_with_async_host(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    local_zone: Option<&str>,
    local_content_advertisement: Option<&DfsNodeAdvertisement>,
) -> Result<(), DfsError> {
    publish_local_dfs_service_advertisement_with_async_host(
        host,
        state,
        local_zone,
        local_content_advertisement,
    )
    .await
}

async fn publish_local_dfs_service_advertisement_with_async_host(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    local_zone: Option<&str>,
    local_content_advertisement: Option<&DfsNodeAdvertisement>,
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
    let mut content = local_content_advertisement
        .cloned()
        .unwrap_or_else(|| DfsNodeAdvertisement::content(member.node_id.clone()));
    if content.node_id != member.node_id {
        return Err(DfsError::Distributed {
            message: format!(
                "local DFS content advertisement names node {} but daemon owns {}",
                content.node_id, member.node_id
            ),
        });
    }
    content.accepts_metadata = false;
    metadata.zone = local_zone.map(str::to_string);
    content.zone = local_zone.map(str::to_string);
    let metadata =
        stable_dfs_advertisement_async(host, metadata_key.clone(), metadata, false).await?;
    let content = stable_dfs_advertisement_async(host, content_key.clone(), content, false).await?;
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
    let lease_is_fresh = desired.expires_at.is_none()
        || current.expires_at.is_some_and(|expires_at| {
            expires_at > OffsetDateTime::now_utc() + time::Duration::seconds(30)
        });
    if lease_is_fresh
        && current.node_id == desired.node_id
        && current.accepts_metadata == desired.accepts_metadata
        && current.accepts_content == desired.accepts_content
        && current.capacity_weight == desired.capacity_weight
        && current.zone == desired.zone
        && current.storage_health == desired.storage_health
        && current.storage_freshness == desired.storage_freshness
        && current.usable_capacity_bytes == desired.usable_capacity_bytes
        && current.reserved_free_bytes == desired.reserved_free_bytes
        && current.content_location_count == desired.content_location_count
        && current.network_metering == desired.network_metering
        && current.advertisement_generation == desired.advertisement_generation
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
    let lease_is_fresh = desired.expires_at.is_none()
        || current.expires_at.is_some_and(|expires_at| {
            expires_at > OffsetDateTime::now_utc() + time::Duration::seconds(30)
        });
    if lease_is_fresh
        && current.node_id == desired.node_id
        && current.accepts_metadata == desired.accepts_metadata
        && current.accepts_content == desired.accepts_content
        && current.capacity_weight == desired.capacity_weight
        && current.zone == desired.zone
        && current.storage_health == desired.storage_health
        && current.storage_freshness == desired.storage_freshness
        && current.usable_capacity_bytes == desired.usable_capacity_bytes
        && current.reserved_free_bytes == desired.reserved_free_bytes
        && current.content_location_count == desired.content_location_count
        && current.network_metering == desired.network_metering
        && current.advertisement_generation == desired.advertisement_generation
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
        DfsMetadataAuthorityAckFloor, LocalSobjMetadataBackend,
    };
    use sinomo_core::node::ClusterId;

    #[test]
    fn metadata_service_nodes_exclude_members_without_discovered_dfs_metadata() {
        let state = test_cluster_state_with_members(
            "cluster_dfs_metadata_provider_filter",
            "node_dfs_metadata_provider_local",
            &[
                "node_dfs_metadata_provider_remote",
                "node_dfs_metadata_provider_disabled",
            ],
        );
        let remote = NodeId::from_legacy_named("node_dfs_metadata_provider_remote")
            .expect("remote provider");
        let disabled = NodeId::from_legacy_named("node_dfs_metadata_provider_disabled")
            .expect("disabled member");
        let now = OffsetDateTime::now_utc();
        let records = [
            {
                let mut record =
                    sinomo_core::service::ServiceInstanceRecord::new("dfs", remote.clone());
                record.routes = vec![crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.to_string()];
                record.provider_capabilities = vec!["dfs.metadata.replicate".to_string()];
                record.updated_at = now;
                record
            },
            {
                let mut record =
                    sinomo_core::service::ServiceInstanceRecord::new("daemon", disabled);
                record.routes = vec!["/daemon/status".to_string()];
                record.provider_capabilities = vec!["daemon.status.read".to_string()];
                record.updated_at = now;
                record
            },
        ];
        let entries = records
            .into_iter()
            .map(|record| sinomo_core::kv::KvEntry {
                key: sinomo_core::service::cluster_service_instance_key(
                    &record.service_id,
                    &record.node_id,
                ),
                value: sinomo_core::kv::encode_kv_value(&record).expect("encode service"),
                create_revision: sinomo_core::kv::KvRevision(1),
                modify_revision: sinomo_core::kv::KvRevision(1),
                version: sinomo_core::kv::KvVersion(1),
                lease_id: None,
            })
            .collect();

        let providers = discovered_metadata_service_node_ids(
            &state,
            stable_dfs_service_node_ids(&state),
            entries,
        );

        assert!(providers.contains(&state.local_node_id));
        assert!(providers.contains(&remote));
        assert_eq!(providers.len(), 2);
    }

    fn split_default_metadata_shards(
        file_system_id: &DfsFileSystemId,
        root_primary: NodeId,
        services_primary: NodeId,
        users_primary: NodeId,
    ) -> Vec<DfsMetadataShardConfig> {
        vec![
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: root_primary,
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: services_primary,
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: SIFS_SERVICES_PATH.to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "users".to_string(),
                primary_node_id: users_primary,
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: SIFS_USERS_PATH.to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ]
    }

    #[derive(Debug)]
    struct TestDefaultClusterBootstrapHost {
        current_shard: Option<DfsMetadataShardConfig>,
        unavailable_node_ids: BTreeSet<NodeId>,
        primary_transfer_ready: bool,
        primary_transfer_targets: Vec<NodeId>,
    }

    impl DfsDefaultClusterBootstrapHost for TestDefaultClusterBootstrapHost {
        fn metadata_shard_node_routing_unavailable(
            &self,
            _state: &ClusterBootstrapState,
            node_id: &NodeId,
        ) -> bool {
            self.unavailable_node_ids.contains(node_id)
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
    fn metadata_prepare_codec_stays_legacy_until_every_assigned_member_is_capable() {
        let mut state = test_cluster_state_with_members(
            "cluster_dfs_prepare_codec_mixed",
            "node_dfs_prepare_codec_local",
            &[
                "node_dfs_prepare_codec_remote_a",
                "node_dfs_prepare_codec_remote_b",
            ],
        );
        let file_system = default_cluster_sifs(&state);
        let mut host = TestDefaultClusterBootstrapHost {
            current_shard: None,
            unavailable_node_ids: BTreeSet::new(),
            primary_transfer_ready: false,
            primary_transfer_targets: Vec::new(),
        };

        let missing = default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("missing capability falls back to legacy");
        assert_eq!(
            missing.namespace_prepare_codec,
            DfsMetadataNamespacePrepareCodec::Legacy
        );

        let capability =
            sinomo_core::CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY)
                .expect("static capability");
        let member_count = state.members.len();
        for member in state.members.iter_mut().take(member_count - 1) {
            member.capabilities.push(capability.clone());
        }
        let mixed = default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("mixed capability falls back to legacy");
        assert_eq!(
            mixed.namespace_prepare_codec,
            DfsMetadataNamespacePrepareCodec::Legacy
        );
    }

    #[test]
    fn metadata_prepare_codec_activation_requires_every_assigned_member_and_is_one_way() {
        let mut state = test_cluster_state_with_members(
            "cluster_dfs_prepare_codec_transition",
            "node_dfs_prepare_codec_transition_local",
            &[
                "node_dfs_prepare_codec_transition_remote_a",
                "node_dfs_prepare_codec_transition_remote_b",
            ],
        );
        let file_system = default_cluster_sifs(&state);
        let mut host = TestDefaultClusterBootstrapHost {
            current_shard: None,
            unavailable_node_ids: BTreeSet::new(),
            primary_transfer_ready: false,
            primary_transfer_targets: Vec::new(),
        };
        let legacy = default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("legacy shard");
        assert_eq!(
            legacy.namespace_prepare_codec,
            DfsMetadataNamespacePrepareCodec::Legacy
        );

        let capability =
            sinomo_core::CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY)
                .expect("static capability");
        for member in &mut state.members {
            member.capabilities.push(capability.clone());
        }
        host.current_shard = Some(legacy.clone());

        let packed = default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("packed shard");
        assert_eq!(
            packed.namespace_prepare_codec,
            DfsMetadataNamespacePrepareCodec::PackedV1
        );
        assert_eq!(packed.epoch, legacy.epoch + 1);
        assert_eq!(packed.primary_node_id, legacy.primary_node_id);
        assert_eq!(packed.replica_node_ids, legacy.replica_node_ids);
        assert!(
            host.primary_transfer_targets.is_empty(),
            "a codec-only transition must not masquerade as a primary rotation"
        );

        state
            .members
            .last_mut()
            .expect("assigned member")
            .capabilities
            .retain(|candidate| candidate != &capability);
        host.current_shard = Some(packed.clone());
        let retained = default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("capability loss retains the packed generation");
        assert_eq!(
            retained, packed,
            "capability loss must fail closed instead of advertising an old-binary-safe downgrade"
        );
    }

    #[test]
    fn metadata_prepare_codec_prefers_v2_and_retains_it_after_capability_loss() {
        let mut state = test_cluster_state_with_members(
            "cluster_dfs_prepare_codec_v2",
            "node_dfs_prepare_codec_v2_local",
            &[
                "node_dfs_prepare_codec_v2_remote_a",
                "node_dfs_prepare_codec_v2_remote_b",
            ],
        );
        let file_system = default_cluster_sifs(&state);
        let mut host = TestDefaultClusterBootstrapHost {
            current_shard: None,
            unavailable_node_ids: BTreeSet::new(),
            primary_transfer_ready: false,
            primary_transfer_targets: Vec::new(),
        };
        let packed_v1 =
            sinomo_core::CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY)
                .expect("static capability");
        let packed_v2 =
            sinomo_core::CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY)
                .expect("static capability");
        for member in &mut state.members {
            member.capabilities.push(packed_v1.clone());
            member.capabilities.push(packed_v2.clone());
        }

        let selected = default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("v2-capable shard");
        assert_eq!(
            selected.namespace_prepare_codec,
            DfsMetadataNamespacePrepareCodec::PackedV2,
            "the newest codec supported by every assigned member wins"
        );

        state
            .members
            .last_mut()
            .expect("assigned member")
            .capabilities
            .retain(|candidate| candidate != &packed_v2);
        host.current_shard = Some(selected.clone());
        let retained = default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("v2 capability loss retains the current generation");
        assert_eq!(
            retained, selected,
            "falling back to packed-v1 still cannot reopen an active packed-v2 history"
        );
    }

    #[test]
    fn legacy_metadata_shard_config_without_codec_defaults_and_stays_compact() {
        let file_system_id =
            DfsFileSystemId::from_legacy_named("prepare-codec-serde-fs").expect("filesystem");
        let primary =
            NodeId::from_legacy_named("prepare-codec-serde-primary").expect("primary node");
        let mut packed = split_default_metadata_shards(
            &file_system_id,
            primary.clone(),
            primary.clone(),
            primary,
        )
        .remove(0);
        packed.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV1;
        let mut legacy_value = serde_json::to_value(packed).expect("encode packed config");
        assert!(
            legacy_value
                .as_object_mut()
                .expect("config object")
                .remove("namespace_prepare_codec")
                .is_some(),
            "packed configs must explicitly carry their codec"
        );

        let decoded: DfsMetadataShardConfig =
            serde_json::from_value(legacy_value).expect("decode legacy config");
        assert_eq!(
            decoded.namespace_prepare_codec,
            DfsMetadataNamespacePrepareCodec::Legacy
        );
        assert!(
            serde_json::to_value(decoded)
                .expect("re-encode legacy config")
                .get("namespace_prepare_codec")
                .is_none(),
            "legacy remains the compact omitted-field representation"
        );
    }

    #[test]
    fn control_plane_bootstrap_parks_codec_upgrade_for_exact_handoff() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-bootstrap-prepare-codec-upgrade");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let mut state = test_cluster_state_with_members(
            "cluster_dfs_bootstrap_prepare_codec_upgrade",
            "node_dfs_bootstrap_prepare_codec_upgrade_owner",
            &["node_dfs_bootstrap_prepare_codec_upgrade_replica"],
        );
        let capability =
            sinomo_core::CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY)
                .expect("static capability");
        for member in &mut state.members {
            member.capabilities.push(capability.clone());
        }
        let owner = state.local_node_id.clone();
        let replica = state.members[1].node_id.clone();
        let file_system_id =
            DfsFileSystemId::from_legacy_named("bootstrap-prepare-codec-upgrade-fs")
                .expect("filesystem id");
        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            epoch: 2,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            ..retiring.clone()
        };
        let mut kv = sinomo_core::kv::InProcessKvStore::new();
        put_cluster_member_records(&mut kv, &state.members);
        let mut host = DfsControlPlaneDefaultClusterBootstrapHost::new(
            data_dir,
            cluster_paths,
            owner,
            Box::new(kv),
        );
        host.put_metadata_shard(&retiring)
            .expect("create retiring shard");

        assert!(matches!(
            host.put_metadata_shard(&desired),
            Err(DfsError::MetadataShardMembershipHandoffParked {
                file_system_id: parked_file_system_id,
                shard_id,
                retiring_epoch: 1,
                desired_epoch: 2,
                ..
            }) if parked_file_system_id == file_system_id && shard_id == "root"
        ));
        assert_eq!(
            host.current_metadata_shard(&file_system_id, "root")
                .expect("read retiring shard"),
            Some(retiring.clone()),
            "the persisted format must not change before exact handoff completion"
        );
        let pending = DfsControlPlaneStore::new(host.control_plane.as_mut())
            .get_metadata_shard_membership_handoff(&file_system_id, "root")
            .expect("read codec handoff")
            .expect("codec handoff parked");
        assert_eq!(pending.retiring_shard, retiring);
        assert_eq!(pending.desired_shard, desired);
    }

    #[test]
    fn control_plane_bootstrap_rejects_packed_downgrade_without_replay_floor() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-bootstrap-prepare-codec-downgrade");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let mut state = test_cluster_state_with_members(
            "cluster_dfs_bootstrap_prepare_codec_downgrade",
            "node_dfs_bootstrap_prepare_codec_downgrade_owner",
            &["node_dfs_bootstrap_prepare_codec_downgrade_replica"],
        );
        let capability =
            sinomo_core::CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY)
                .expect("static capability");
        for member in &mut state.members {
            member.capabilities.push(capability.clone());
        }
        let owner = state.local_node_id.clone();
        let replica = state.members[1].node_id.clone();
        let file_system_id =
            DfsFileSystemId::from_legacy_named("bootstrap-prepare-codec-downgrade-fs")
                .expect("filesystem id");
        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            epoch: 2,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            ..retiring.clone()
        };
        let mut kv = sinomo_core::kv::InProcessKvStore::new();
        put_cluster_member_records(&mut kv, &state.members);
        let mut host = DfsControlPlaneDefaultClusterBootstrapHost::new(
            data_dir,
            cluster_paths,
            owner,
            Box::new(kv),
        );
        host.put_metadata_shard(&retiring)
            .expect("create packed retiring shard");

        assert!(matches!(
            host.put_metadata_shard(&desired),
            Err(DfsError::Distributed { message })
                if message.contains("legacy-readable snapshot")
                    && message.contains("replay-floor proof")
        ));
        assert_eq!(
            host.current_metadata_shard(&file_system_id, "root")
                .expect("read retained packed shard"),
            Some(retiring),
            "an unproven downgrade must leave the packed generation authoritative"
        );
        assert!(
            DfsControlPlaneStore::new(host.control_plane.as_mut())
                .get_metadata_shard_membership_handoff(&file_system_id, "root")
                .expect("read downgrade handoff")
                .is_none(),
            "an unsafe downgrade must be rejected before publishing a handoff"
        );
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
            namespace_prepare_codec: Default::default(),
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
        let next_epoch = DfsMetadataShardConfig {
            replica_node_ids: vec![owner],
            epoch: 2,
            ..same_epoch_overwrite
        };
        assert!(matches!(
            host.put_metadata_shard(&next_epoch),
            Err(DfsError::MetadataShardMembershipHandoffParked {
                file_system_id: parked_file_system_id,
                shard_id,
                retiring_epoch: 1,
                desired_epoch: 2,
                ..
            }) if parked_file_system_id == file_system_id && shard_id == "root"
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
            namespace_prepare_codec: Default::default(),
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

        let first_handoff_id = match host.put_metadata_shard(&desired) {
            Err(DfsError::MetadataShardMembershipHandoffParked {
                file_system_id: parked_file_system_id,
                shard_id,
                handoff_id,
                retiring_epoch: 1,
                desired_epoch: 2,
            }) if parked_file_system_id == file_system_id && shard_id == "root" => handoff_id,
            other => panic!("expected typed parked handoff, got {other:?}"),
        };
        let existing_handoff_id = match host.put_metadata_shard(&desired) {
            Err(DfsError::MetadataShardMembershipHandoffParked {
                handoff_id,
                retiring_epoch: 1,
                desired_epoch: 2,
                ..
            }) => handoff_id,
            other => panic!("expected existing typed parked handoff, got {other:?}"),
        };
        assert_eq!(
            existing_handoff_id, first_handoff_id,
            "revisiting a durable exact handoff must park on the same generation instead of becoming a generic concurrent failure"
        );
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
            namespace_prepare_codec: Default::default(),
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
        let next_epoch = DfsMetadataShardConfig {
            primary_node_id: replacement,
            replica_node_ids: vec![owner],
            epoch: 2,
            ..initial.clone()
        };
        assert!(matches!(
            host.put_metadata_shard_async(next_epoch.clone()).await,
            Err(DfsError::MetadataShardMembershipHandoffParked {
                file_system_id: parked_file_system_id,
                shard_id,
                retiring_epoch: 1,
                desired_epoch: 2,
                ..
            }) if parked_file_system_id == file_system_id && shard_id == "root"
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
    fn materialization_signature_ignores_distinct_policy_discovery_order() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-policy-order-signature");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let local = NodeId::from_legacy_named("node_dfs_policy_order_signature").expect("node");
        let file_system = distributed_sifs(local.clone());
        let signature = DfsDefaultMaterializationSignature {
            file_system: file_system.clone(),
            shards: Vec::new(),
            bootstrap_owner_node_ids: vec![local.clone()],
            fallback_owner_node_id: local.clone(),
            handoff_evidence: Vec::new(),
        };
        record_default_materialization(data_dir.root(), signature, false);

        let mut reordered = file_system;
        reordered.subtree_policies.reverse();
        assert!(default_materialization_is_current(
            &data_dir.root(),
            &DfsDefaultMaterializationSignature {
                file_system: reordered,
                shards: Vec::new(),
                bootstrap_owner_node_ids: vec![local.clone()],
                fallback_owner_node_id: local,
                handoff_evidence: Vec::new(),
            }
        ));
    }

    #[tokio::test]
    async fn replica_progress_invalidation_preserves_subscription_until_settled_pass() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-replica-subscription-cache");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let local = NodeId::from_legacy_named("node_dfs_replica_subscription").expect("node");
        let file_system = distributed_sifs(local.clone());
        let signature = DfsDefaultMaterializationSignature {
            file_system,
            shards: Vec::new(),
            bootstrap_owner_node_ids: vec![local.clone()],
            fallback_owner_node_id: local,
            handoff_evidence: Vec::new(),
        };
        record_default_materialization(data_dir.root(), signature.clone(), true);
        assert!(
            take_default_cluster_dfs_materialization_immediate_retry(&data_dir),
            "first entering a dynamic replica dependency must schedule one verification generation"
        );
        record_default_materialization(data_dir.root(), signature.clone(), true);
        assert!(
            !take_default_cluster_dfs_materialization_immediate_retry(&data_dir),
            "a settled waiting subscription must not self-wake repeatedly"
        );
        assert!(default_materialization_is_current(
            &data_dir.root(),
            &signature
        ));
        assert!(default_cluster_dfs_materialization_awaits_replica_progress(
            &data_dir
        ));

        assert!(invalidate_default_cluster_dfs_materialization_for_replica_progress(&data_dir));
        assert!(
            !default_materialization_is_current(&data_dir.root(), &signature),
            "replica progress must force the next materialization pass"
        );
        assert!(
            default_cluster_dfs_materialization_awaits_replica_progress(&data_dir),
            "the watermark subscription must survive invalidation to cover the commit race"
        );
        assert!(
            !invalidate_default_cluster_dfs_materialization_for_replica_progress(&data_dir),
            "one replica generation is consumed once"
        );

        record_default_materialization(data_dir.root(), signature.clone(), false);
        assert!(default_materialization_is_current(
            &data_dir.root(),
            &signature
        ));
        assert!(
            !default_cluster_dfs_materialization_awaits_replica_progress(&data_dir),
            "a settled pass must unsubscribe ordinary watermark traffic"
        );
    }

    #[tokio::test]
    async fn stable_async_cluster_publications_materialize_once() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-stable-publish-materialization");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_stable_publish")
                .expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_stable_publish_local").expect("local");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "stable publish",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let mut host = DfsControlPlaneDefaultClusterAsyncBootstrapHost::new(
            data_dir.clone(),
            cluster_paths,
            local,
            Box::new(sinomo_core::kv::InProcessKvStore::new()),
        );
        let options = DfsDefaultClusterBootstrapOptions {
            publish_cluster_control: true,
            materialize_local_metadata: true,
            local_zone: None,
            local_content_advertisement: None,
        };

        let first =
            ensure_default_distributed_dfs_with_async_host(&state, &mut host, options.clone())
                .await
                .expect("first default DFS publication");
        assert!(first.local_has_primary_metadata_shards);
        assert_eq!(default_materialization_run_count(&data_dir.root()), 1);

        let second =
            ensure_default_distributed_dfs_with_async_host(&state, &mut host, options.clone())
                .await
                .expect("stable default DFS publication");
        assert_eq!(second.metadata_shards, first.metadata_shards);
        assert_eq!(
            default_materialization_run_count(&data_dir.root()),
            1,
            "an unchanged publication must not reopen every shard and rescan the object catalog"
        );
    }

    #[tokio::test]
    async fn completed_handoff_materializes_through_exact_bound_backend_once() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-bound-handoff-materialization");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_dfs_bound_handoff_local").expect("local node");
        let root_primary =
            NodeId::from_legacy_named("node_dfs_bound_handoff_root").expect("root primary");
        let users_primary =
            NodeId::from_legacy_named("node_dfs_bound_handoff_users").expect("users primary");
        let file_system = distributed_sifs(local.clone());
        let mut shards = split_default_metadata_shards(
            &file_system.file_system_id,
            root_primary,
            local.clone(),
            users_primary,
        );
        let services_index = shards
            .iter()
            .position(|shard| shard.shard_id == "services")
            .expect("services shard");
        let retiring_services = shards[services_index].clone();
        shards[services_index].epoch = retiring_services.epoch.saturating_add(1);
        let active_services = shards[services_index].clone();
        let services_shard_id = DfsMetadataShardId::new("services");
        let handoff_id = "handoff-bound-default-materialization";

        let mut retiring_backend =
            open_local_metadata_shard_backend(&data_dir, services_shard_id.clone())
                .expect("retiring services backend");
        retiring_backend
            .put_file_system(&file_system)
            .expect("retiring filesystem");
        let freeze = retiring_backend
            .freeze_metadata_shard_for_membership_handoff(
                &services_shard_id,
                handoff_id,
                retiring_services.epoch,
            )
            .expect("freeze retiring services authority")
            .expect("local services shard");
        drop(retiring_backend);

        // This independently opened lane observes the marker but has not
        // validated the completed control-plane generation. It must remain
        // fenced even after another instance binds that generation.
        let mut stale_separate_backend =
            open_local_metadata_shard_backend(&data_dir, services_shard_id.clone())
                .expect("stale separate services backend");
        let proof = DfsMetadataShardMembershipHandoffProof {
            handoff_id: handoff_id.to_string(),
            retiring_shard: retiring_services,
            desired_shard: active_services.clone(),
            journal_sequence: freeze.journal_sequence,
            state_fingerprint: freeze.state_fingerprint,
            acknowledged_node_ids: vec![local.clone()],
        };
        let mut host = DfsControlPlaneDefaultClusterAsyncBootstrapHost::new(
            data_dir.clone(),
            cluster_paths,
            local.clone(),
            Box::new(sinomo_core::kv::InProcessKvStore::new()),
        );
        for shard in &shards {
            host.put_serde_if_changed(
                metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                shard,
            )
            .await
            .expect("publish active shard");
        }
        host.put_serde_if_changed(
            metadata_shard_membership_handoff_proof_key(
                &active_services.file_system_id,
                &active_services.shard_id,
            ),
            &proof,
        )
        .await
        .expect("publish completed handoff proof");

        host.materialize_default_sifs_metadata_on_shards_async(
            file_system.clone(),
            shards.clone(),
            vec![local.clone()],
            local.clone(),
        )
        .await
        .expect("the proof-bound backend materializes default metadata");
        assert_eq!(default_materialization_run_count(&data_dir.root()), 1);

        let materialized_backend = LocalSobjMetadataBackend::open_for_inspection(
            services_shard_id.clone(),
            local_dfs_metadata_shard_path(&data_dir, &services_shard_id),
        )
        .expect("inspect materialized services authority");
        let services_object = materialized_backend
            .get_object_by_path(&file_system.file_system_id, SIFS_SERVICES_PATH)
            .expect("read materialized services object")
            .expect("services object was materialized");
        let materialized_head = materialized_backend.journal_last_sequence();
        assert!(materialized_head > freeze.journal_sequence);

        let mut rejected_object = services_object;
        rejected_object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_bound_handoff_stale_writer")
                .expect("object id");
        rejected_object.path = "/services/stale-writer".to_string();
        rejected_object.version =
            crate::DfsVersion::from_legacy_named("version_bound_handoff_stale_writer")
                .expect("version");
        assert!(matches!(
            stale_separate_backend.put_object(&rejected_object),
            Err(crate::MetadataBackendError::MembershipHandoffFrozen {
                ref handoff_id,
                ..
            }) if handoff_id == "handoff-bound-default-materialization"
        ));
        let mut fresh_separate_backend =
            open_local_metadata_shard_backend(&data_dir, services_shard_id.clone())
                .expect("fresh separate services backend");
        assert_eq!(
            fresh_separate_backend
                .metadata_shard_write_freeze(&services_shard_id)
                .expect("read durable completed marker")
                .and_then(|marker| marker.activated_epoch),
            Some(active_services.epoch),
            "durable activation is evidence, not authority for a separately opened instance"
        );
        assert!(matches!(
            fresh_separate_backend.put_object(&rejected_object),
            Err(crate::MetadataBackendError::MembershipHandoffFrozen {
                ref handoff_id,
                ..
            }) if handoff_id == "handoff-bound-default-materialization"
        ));

        host.materialize_default_sifs_metadata_on_shards_async(
            file_system,
            shards,
            vec![local.clone()],
            local,
        )
        .await
        .expect("settled materialization rerun");
        assert_eq!(
            default_materialization_run_count(&data_dir.root()),
            1,
            "an unchanged completed handoff must remain idle"
        );
        assert_eq!(
            LocalSobjMetadataBackend::open_for_inspection(
                DfsMetadataShardId::new("services"),
                local_dfs_metadata_shard_path(&data_dir, &DfsMetadataShardId::new("services"),),
            )
            .expect("inspect settled services authority")
            .journal_last_sequence(),
            materialized_head,
            "the no-work rerun must not append metadata"
        );
    }

    #[test]
    fn default_materialization_writes_only_locally_active_primary_shards() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-local-primary-materialization");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_materialize_services").expect("local node");
        let root_primary =
            NodeId::from_legacy_named("node_dfs_materialize_root").expect("root primary");
        let users_primary =
            NodeId::from_legacy_named("node_dfs_materialize_users").expect("users primary");
        let file_system = distributed_sifs(local.clone());
        let shards = vec![
            DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: root_primary,
                replica_node_ids: vec![local.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: SIFS_SERVICES_PATH.to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "users".to_string(),
                primary_node_id: users_primary,
                replica_node_ids: vec![local.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: SIFS_USERS_PATH.to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ];
        let active_primary_shard_ids = BTreeSet::from([DfsMetadataShardId::new("services")]);

        materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &active_primary_shard_ids,
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("materialize local services authority");

        assert!(
            !local_dfs_metadata_shard_path(&data_dir, &DfsMetadataShardId::new("root")).exists(),
            "a replica must not append bootstrap records to the remote root primary's journal"
        );
        assert!(
            !local_dfs_metadata_shard_path(&data_dir, &DfsMetadataShardId::new("users")).exists(),
            "a replica must not append bootstrap records to the remote users primary's journal"
        );
        let services_path =
            local_dfs_metadata_shard_path(&data_dir, &DfsMetadataShardId::new("services"));
        assert!(services_path.exists());
        let backend =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("open materialized services shard");
        assert!(
            backend
                .get_object_by_path(&file_system.file_system_id, SIFS_SERVICES_PATH)
                .expect("read services object")
                .is_some()
        );
    }

    #[test]
    fn default_materialization_skips_a_local_primary_parked_for_handoff() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-pending-handoff-materialization");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_materialize_pending").expect("local node");
        let file_system = distributed_sifs(local.clone());
        let services = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };

        materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &[services],
            &BTreeSet::new(),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("skip parked local authority");

        assert!(
            !local_dfs_metadata_shard_path(&data_dir, &DfsMetadataShardId::new("services"))
                .exists(),
            "a pending handoff generation must stay frozen until activation"
        );
    }

    #[test]
    fn split_primary_migration_mutates_only_the_local_authority() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-split-primary-migration");
        let root_primary =
            NodeId::from_legacy_named("node_dfs_split_migration_root").expect("root primary");
        let services_primary = NodeId::from_legacy_named("node_dfs_split_migration_services")
            .expect("services primary");
        let users_primary =
            NodeId::from_legacy_named("node_dfs_split_migration_users").expect("users primary");
        let file_system = distributed_sifs(root_primary.clone());
        let mut shards = split_default_metadata_shards(
            &file_system.file_system_id,
            root_primary.clone(),
            services_primary.clone(),
            users_primary,
        );
        shards[0].replica_node_ids.push(services_primary.clone());
        shards[1].replica_node_ids.push(root_primary.clone());
        let mut legacy_object = crate::distributed_sifs_bootstrap_objects(root_primary.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        legacy_object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_split_primary_legacy_services")
                .expect("legacy object id");
        legacy_object.path = "/services/legacy.txt".to_string();

        // The services primary may adopt a misplaced object only from an exact
        // certified root replica. Its local root journal remains a replica and
        // must not receive the source tombstone.
        let target_data_dir = SinomoDataDir::new(root.join("target-data"));
        target_data_dir
            .ensure_root_layout()
            .expect("target data dir");
        let mut certified_root =
            open_local_metadata_shard_backend(&target_data_dir, DfsMetadataShardId::new("root"))
                .expect("target root replica");
        certified_root
            .put_file_system(&file_system)
            .expect("root replica filesystem");
        certified_root
            .put_object(&legacy_object)
            .expect("root replica legacy object");
        let target_root_sequence = certified_root.journal_last_sequence();
        certified_root
            .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
                shard_id: DfsMetadataShardId::new("root"),
                journal_sequence: target_root_sequence,
                durable_prefix_proof: None,
            })
            .expect("certify root replica");
        drop(certified_root);
        let target_report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&target_data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([DfsMetadataShardId::new("services")]),
            &services_primary,
            vec![root_primary.clone()],
            root_primary.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("adopt legacy object on services authority");
        assert_eq!(target_report.migration.migrated_objects, 1);
        let target_services = open_local_metadata_shard_backend(
            &target_data_dir,
            DfsMetadataShardId::new("services"),
        )
        .expect("target services authority");
        assert_eq!(
            target_services
                .get_object(&legacy_object.object_id)
                .expect("target services object"),
            Some(legacy_object.clone())
        );
        let target_root = LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new("root"),
            local_dfs_metadata_shard_path(&target_data_dir, &DfsMetadataShardId::new("root")),
        )
        .expect("inspect target root replica");
        assert_eq!(target_root.journal_last_sequence(), target_root_sequence);
        assert_eq!(
            target_root
                .get_object(&legacy_object.object_id)
                .expect("source replica object"),
            Some(legacy_object.clone())
        );

        // Once the root primary has an exact services replica containing the
        // adopted object, it may tombstone its own source journal. The remote
        // services replica remains read-only throughout this pass.
        let source_data_dir = SinomoDataDir::new(root.join("source-data"));
        source_data_dir
            .ensure_root_layout()
            .expect("source data dir");
        let mut source_root =
            open_local_metadata_shard_backend(&source_data_dir, DfsMetadataShardId::new("root"))
                .expect("source root authority");
        source_root
            .put_file_system(&file_system)
            .expect("source filesystem");
        source_root
            .put_object(&legacy_object)
            .expect("source legacy object");
        drop(source_root);
        let mut certified_services = open_local_metadata_shard_backend(
            &source_data_dir,
            DfsMetadataShardId::new("services"),
        )
        .expect("source services replica");
        certified_services
            .put_file_system(&file_system)
            .expect("services replica filesystem");
        certified_services
            .put_object(&legacy_object)
            .expect("services replica adopted object");
        let services_replica_sequence = certified_services.journal_last_sequence();
        certified_services
            .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
                shard_id: DfsMetadataShardId::new("services"),
                journal_sequence: services_replica_sequence,
                durable_prefix_proof: None,
            })
            .expect("certify services replica");
        drop(certified_services);
        let source_report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&source_data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([DfsMetadataShardId::new("root")]),
            &root_primary,
            vec![root_primary.clone()],
            root_primary.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("retire legacy object on root authority");
        assert_eq!(source_report.migration.migrated_objects, 1);
        let source_root =
            open_local_metadata_shard_backend(&source_data_dir, DfsMetadataShardId::new("root"))
                .expect("reopen source root authority");
        assert_eq!(
            source_root
                .get_object(&legacy_object.object_id)
                .expect("source tombstone")
                .map(|object| object.kind),
            Some(crate::DfsObjectKind::Tombstone)
        );
        let services_replica = LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new("services"),
            local_dfs_metadata_shard_path(&source_data_dir, &DfsMetadataShardId::new("services")),
        )
        .expect("inspect services replica");
        assert_eq!(
            services_replica.journal_last_sequence(),
            services_replica_sequence,
            "source cleanup must not append to the remote services primary's replica journal"
        );
    }

    #[test]
    fn split_primary_migration_rejects_same_id_on_wrong_target_path() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-split-migration-wrong-target");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let root_primary =
            NodeId::from_legacy_named("node_dfs_wrong_target_root").expect("root primary");
        let services_primary =
            NodeId::from_legacy_named("node_dfs_wrong_target_services").expect("services primary");
        let users_primary =
            NodeId::from_legacy_named("node_dfs_wrong_target_users").expect("users primary");
        let file_system = distributed_sifs(root_primary.clone());
        let mut shards = split_default_metadata_shards(
            &file_system.file_system_id,
            root_primary.clone(),
            services_primary,
            users_primary,
        );
        shards[1].replica_node_ids.push(root_primary.clone());
        let mut source_object = crate::distributed_sifs_bootstrap_objects(root_primary.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        source_object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_split_wrong_target_path")
                .expect("object id");
        source_object.path = "/services/source.txt".to_string();
        let mut wrong_target = source_object.clone();
        wrong_target.path = "/users/wrong-target.txt".to_string();

        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root authority");
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        source
            .put_object(&source_object)
            .expect("misplaced source object");
        drop(source);
        let mut target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("services replica");
        target
            .put_file_system(&file_system)
            .expect("target filesystem");
        target
            .put_object(&wrong_target)
            .expect("same-id wrong target object");
        let target_sequence = target.journal_last_sequence();
        target
            .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
                shard_id: DfsMetadataShardId::new("services"),
                journal_sequence: target_sequence,
                durable_prefix_proof: None,
            })
            .expect("certify wrong target replica");
        drop(target);
        let report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([DfsMetadataShardId::new("root")]),
            &root_primary,
            vec![root_primary.clone()],
            root_primary.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("defer wrong-routed target");

        assert_eq!(report.migration.migrated_objects, 0);
        assert!(report.awaiting_replica_progress);
        let source = open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
            .expect("reopen root authority");
        assert_eq!(
            source
                .get_object(&source_object.object_id)
                .expect("source object")
                .map(|object| object.kind),
            Some(source_object.kind)
        );
        let target = LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new("services"),
            local_dfs_metadata_shard_path(&data_dir, &DfsMetadataShardId::new("services")),
        )
        .expect("inspect services replica");
        assert_eq!(target.journal_last_sequence(), target_sequence);
    }

    #[test]
    fn authority_migration_selects_newest_duplicate_before_installing() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-migration-newest-duplicate");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_migration_newest").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = split_default_metadata_shards(
            &file_system.file_system_id,
            local.clone(),
            local.clone(),
            local.clone(),
        );
        let mut older = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        older.object_id =
            crate::DfsObjectId::from_legacy_named("obj_migration_duplicate").expect("object id");
        older.path = "/services/canonical-newest.txt".to_string();
        older.version =
            crate::DfsVersion::from_legacy_named("version_migration_old").expect("old version");
        older.changed_at_unix_nanos = 10;
        let mut newest = older.clone();
        newest.version =
            crate::DfsVersion::from_legacy_named("version_migration_new").expect("new version");
        newest.changed_at_unix_nanos = 30;
        newest.last_modified_by_event_id =
            Some(sinomo_core::Id::from_legacy_named("event-migration-newest").expect("event id"));
        let mut old_target = older.clone();
        old_target.changed_at_unix_nanos = 20;

        for (shard_id, object) in [
            (DfsMetadataShardId::new("root"), older),
            (DfsMetadataShardId::new("services"), old_target),
            (DfsMetadataShardId::new("users"), newest.clone()),
        ] {
            let mut backend =
                open_local_metadata_shard_backend(&data_dir, shard_id).expect("metadata shard");
            backend.put_file_system(&file_system).expect("filesystem");
            backend.put_object(&object).expect("duplicate object");
        }

        let report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([
                DfsMetadataShardId::new("root"),
                DfsMetadataShardId::new("services"),
                DfsMetadataShardId::new("users"),
            ]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("migrate newest duplicate");
        assert_eq!(report.migration.migrated_objects, 1);

        let target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("target shard")
                .get_object(&newest.object_id)
                .expect("target lookup")
                .expect("target object");
        assert_eq!(target.version, newest.version);
        assert_eq!(target.changed_at_unix_nanos, newest.changed_at_unix_nanos);
        assert_eq!(
            target.last_modified_by_event_id,
            newest.last_modified_by_event_id
        );
        for source_shard in ["root", "users"] {
            let tombstone =
                open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new(source_shard))
                    .expect("source shard")
                    .get_object(&newest.object_id)
                    .expect("source lookup")
                    .expect("source tombstone");
            assert_eq!(tombstone.kind, crate::DfsObjectKind::Tombstone);
            assert_eq!(tombstone.version, newest.version);
            assert_eq!(
                tombstone.last_modified_by_event_id,
                newest.last_modified_by_event_id
            );
        }
        let index = crate::metadata_shard_index::read_object_shard_index(
            &default_dfs_metadata_shards_root(&data_dir),
            &newest.object_id,
        )
        .expect("index lookup")
        .expect("target index");
        assert_eq!(index.shard_id, DfsMetadataShardId::new("services"));
        assert_eq!(index.path, "services/canonical-newest.txt");
    }

    #[test]
    fn authority_migration_preserves_newer_target_tombstone_over_stale_live_source() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-migration-target-tombstone");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_migration_target_tombstone").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = split_default_metadata_shards(
            &file_system.file_system_id,
            local.clone(),
            local.clone(),
            local.clone(),
        );
        let mut stale_live = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        stale_live.object_id =
            crate::DfsObjectId::from_legacy_named("obj_migration_deleted_duplicate")
                .expect("object id");
        stale_live.path = "/services/deleted-duplicate.txt".to_string();
        stale_live.version =
            crate::DfsVersion::from_legacy_named("version_before_delete").expect("version");
        stale_live.changed_at_unix_nanos = 10;
        let mut deleted = stale_live.clone();
        deleted.kind = crate::DfsObjectKind::Tombstone;
        deleted.version =
            crate::DfsVersion::from_legacy_named("version_authoritative_delete").expect("version");
        deleted.changed_at_unix_nanos = 30;
        deleted.last_modified_by_event_id = Some(
            sinomo_core::Id::from_legacy_named("event-authoritative-delete").expect("event id"),
        );

        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("stale source shard");
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        source.put_object(&stale_live).expect("stale live source");
        drop(source);
        let mut target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("target shard");
        target
            .put_file_system(&file_system)
            .expect("target filesystem");
        target.put_object(&deleted).expect("target deletion");
        drop(target);

        let report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([
                DfsMetadataShardId::new("root"),
                DfsMetadataShardId::new("services"),
                DfsMetadataShardId::new("users"),
            ]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("reconcile interrupted delete cleanup");
        assert_eq!(report.migration.migrated_objects, 1);

        let target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("reopen target")
                .get_object(&deleted.object_id)
                .expect("target lookup")
                .expect("target tombstone");
        assert_eq!(target, deleted, "a stale source must not revive the object");
        let source = open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
            .expect("reopen source")
            .get_object(&stale_live.object_id)
            .expect("source lookup")
            .expect("source tombstone");
        assert_eq!(source.kind, crate::DfsObjectKind::Tombstone);
        assert_eq!(source.version, deleted.version);

        let settled = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([
                DfsMetadataShardId::new("root"),
                DfsMetadataShardId::new("services"),
                DfsMetadataShardId::new("users"),
            ]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("settled reconciliation");
        assert_eq!(
            settled.migration.migrated_objects, 0,
            "the recovered deletion must not leave an infinite retry"
        );
    }

    #[test]
    fn authority_migration_uses_certified_compacted_tombstone_by_object_id() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-dfs-migration-certified-compacted-tombstone");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_migration_compacted_local").expect("node");
        let remote =
            NodeId::from_legacy_named("node_dfs_migration_compacted_remote").expect("node");
        let file_system = distributed_sifs(local.clone());
        let mut shards = split_default_metadata_shards(
            &file_system.file_system_id,
            local.clone(),
            remote.clone(),
            local.clone(),
        );
        shards[1].replica_node_ids.push(local.clone());

        let mut stale_live = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        stale_live.object_id =
            crate::DfsObjectId::from_legacy_named("obj_migration_compacted_deleted_duplicate")
                .expect("object id");
        stale_live.path = "/services/pre-rename-name.txt".to_string();
        stale_live.version =
            crate::DfsVersion::from_legacy_named("version_compacted_before_delete")
                .expect("version");
        stale_live.changed_at_unix_nanos = 10;
        let mut deleted = stale_live.clone();
        deleted.path = "/services/post-rename-name.txt".to_string();
        deleted.kind = crate::DfsObjectKind::Tombstone;
        deleted.version =
            crate::DfsVersion::from_legacy_named("version_compacted_authoritative_delete")
                .expect("version");
        deleted.changed_at_unix_nanos = 30;

        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("stale writable source");
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        source.put_object(&stale_live).expect("stale source object");
        drop(source);

        let services_shard_id = DfsMetadataShardId::new("services");
        let mut target = open_local_metadata_shard_backend(&data_dir, services_shard_id.clone())
            .expect("certified target replica");
        target
            .put_file_system(&file_system)
            .expect("target filesystem");
        target.put_object(&deleted).expect("target deletion");
        let compaction = target
            .compact_tombstones(
                crate::DfsTombstoneCompactionOptions::compact_eligible(
                    std::time::Duration::from_nanos(50),
                )
                .with_now_unix_nanos(100),
            )
            .expect("compact target tombstone");
        assert_eq!(compaction.compacted_tombstone_count, 1);
        assert!(
            target
                .get_object(&deleted.object_id)
                .expect("ordinary target lookup")
                .is_none(),
            "ordinary object paging must continue to hide compacted tombstones"
        );
        let target_sequence = target.journal_last_sequence();
        target
            .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
                shard_id: services_shard_id.clone(),
                journal_sequence: target_sequence,
                durable_prefix_proof: None,
            })
            .expect("certify compacted target");
        drop(target);
        let report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([
                DfsMetadataShardId::new("root"),
                DfsMetadataShardId::new("users"),
            ]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("reconcile compacted deletion evidence");
        assert_eq!(report.migration.migrated_objects, 1);
        assert!(
            !report.awaiting_replica_progress,
            "an exactly certified compacted deletion is already complete replica evidence"
        );
        let source = open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
            .expect("reopen source")
            .get_object(&stale_live.object_id)
            .expect("source lookup")
            .expect("source tombstone");
        assert_eq!(source.kind, crate::DfsObjectKind::Tombstone);
        assert_eq!(source.version, deleted.version);

        let settled = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([
                DfsMetadataShardId::new("root"),
                DfsMetadataShardId::new("users"),
            ]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("settled compacted deletion reconciliation");
        assert_eq!(settled.migration.migrated_objects, 0);
        assert!(!settled.awaiting_replica_progress);
    }

    #[test]
    fn authority_migration_repairs_index_pointing_at_missing_target() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-migration-stale-index");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_migration_stale_index").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = split_default_metadata_shards(
            &file_system.file_system_id,
            local.clone(),
            local.clone(),
            local.clone(),
        );
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_migration_stale_index").expect("object id");
        object.path = "/services/stale-index.txt".to_string();
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("source shard");
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        source.put_object(&object).expect("source object");
        drop(source);
        crate::metadata_shard_index::write_object_shard_index(
            &default_dfs_metadata_shards_root(&data_dir),
            &object,
            &DfsMetadataShardId::new("services"),
        )
        .expect("stale crash-residue index");

        let report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([
                DfsMetadataShardId::new("root"),
                DfsMetadataShardId::new("services"),
                DfsMetadataShardId::new("users"),
            ]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("repair stale index");
        assert_eq!(report.migration.migrated_objects, 1);
        assert!(
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"),)
                .expect("target shard")
                .get_object(&object.object_id)
                .expect("target lookup")
                .is_some(),
            "the derived index must not suppress the only durable source"
        );
    }

    #[test]
    fn unassigned_default_shards_do_not_create_impossible_replica_waits() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-unassigned-shard-wait");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_unassigned_local").expect("node");
        let services = NodeId::from_legacy_named("node_dfs_unassigned_services").expect("node");
        let users = NodeId::from_legacy_named("node_dfs_unassigned_users").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = split_default_metadata_shards(
            &file_system.file_system_id,
            local.clone(),
            services,
            users,
        );

        let report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([DfsMetadataShardId::new("root")]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("materialize local authority");
        assert!(
            !report.awaiting_replica_progress,
            "a node outside the replication factor cannot wait for evidence it will never receive"
        );
    }

    #[test]
    fn lagging_assigned_replica_without_misplaced_objects_does_not_subscribe_reconciliation() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-lagging-assigned-replica");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_lagging_replica_local").expect("node");
        let services_primary =
            NodeId::from_legacy_named("node_dfs_lagging_replica_services").expect("node");
        let users_primary =
            NodeId::from_legacy_named("node_dfs_lagging_replica_users").expect("node");
        let file_system = distributed_sifs(local.clone());
        let mut shards = split_default_metadata_shards(
            &file_system.file_system_id,
            local.clone(),
            services_primary,
            users_primary,
        );
        shards[1].replica_node_ids.push(local.clone());

        let mut correctly_routed = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        correctly_routed.object_id =
            crate::DfsObjectId::from_legacy_named("obj_lagging_services_replica")
                .expect("object id");
        correctly_routed.path = "/services/already-routed.txt".to_string();
        let mut services_replica =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("services replica");
        services_replica
            .put_file_system(&file_system)
            .expect("filesystem");
        services_replica
            .put_object(&correctly_routed)
            .expect("correctly routed replica object");

        let report = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([DfsMetadataShardId::new("root")]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("inspect lagging assigned replica");
        assert!(
            !report.awaiting_replica_progress,
            "ordinary lag on a correctly routed replica is not migration work"
        );
    }

    #[test]
    fn materialization_write_boundary_rejects_stale_authority_after_planning() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-materialization-authority-fence");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_materialization_fence").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = split_default_metadata_shards(
            &file_system.file_system_id,
            local.clone(),
            local.clone(),
            local.clone(),
        );
        let services_shard_id = DfsMetadataShardId::new("services");
        let mut services = open_local_metadata_shard_backend(&data_dir, services_shard_id.clone())
            .expect("services shard");
        services
            .put_file_system(&file_system)
            .expect("services filesystem");
        services
            .freeze_metadata_shard_for_membership_handoff(
                &services_shard_id,
                "handoff-materialization-race",
                1,
            )
            .expect("install authority fence")
            .expect("local services shard");
        drop(services);

        let error = materialize_default_sifs_metadata_on_active_primary_shards(
            default_dfs_metadata_shards_root(&data_dir),
            &file_system,
            &shards,
            &BTreeSet::from([
                DfsMetadataShardId::new("root"),
                services_shard_id,
                DfsMetadataShardId::new("users"),
            ]),
            &local,
            vec![local.clone()],
            local.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect_err("a stale planned primary must be fenced at the journal write boundary");
        assert!(
            error.to_string().contains("membership handoff"),
            "unexpected authority fence error: {error}"
        );
    }

    #[test]
    fn stable_local_replica_advertisements_do_not_refresh_timestamps_or_commit() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-stable-local-replica-advertisement");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_stable_local_replica").expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_stable_local_replica").expect("local");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "stable local replica",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let control_plane =
            crate::SharedDfsControlPlaneKvClient::new(sinomo_core::kv::InProcessKvStore::new());
        let mut host = DfsControlPlaneDefaultClusterBootstrapHost::new(
            data_dir,
            cluster_paths,
            local,
            Box::new(control_plane.clone()),
        );
        let options = DfsDefaultClusterBootstrapOptions {
            publish_cluster_control: false,
            materialize_local_metadata: false,
            local_zone: Some("rack-a".to_string()),
            local_content_advertisement: None,
        };

        ensure_default_distributed_dfs_with_host(&state, &mut host, options.clone())
            .expect("first local replica advertisement");
        let first_revision = control_plane.with_client(|store| store.revision());
        assert!(first_revision > sinomo_core::kv::KvRevision::ZERO);

        ensure_default_distributed_dfs_with_host(&state, &mut host, options)
            .expect("stable local replica advertisement");
        assert_eq!(
            control_plane.with_client(|store| store.revision()),
            first_revision,
            "a stable local advertisement must preserve its stored timestamp and avoid another transaction"
        );
    }

    #[test]
    fn metadata_shard_reconciliation_ignores_divergent_local_route_views() {
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
        let file_system = default_cluster_sifs(&state);
        let current = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: unreachable_primary.clone(),
            replica_node_ids: default_metadata_shard_replica_node_ids(
                &service_nodes,
                &unreachable_primary,
            ),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut unavailable_view = TestDefaultClusterBootstrapHost {
            current_shard: Some(current.clone()),
            unavailable_node_ids: BTreeSet::from([unreachable_primary.clone()]),
            primary_transfer_ready: true,
            primary_transfer_targets: Vec::new(),
        };
        let mut reachable_view = TestDefaultClusterBootstrapHost {
            current_shard: Some(current.clone()),
            unavailable_node_ids: BTreeSet::new(),
            primary_transfer_ready: true,
            primary_transfer_targets: Vec::new(),
        };

        let unavailable_plan = default_cluster_metadata_shard_with_host(
            &mut unavailable_view,
            &state,
            &file_system,
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            unreachable_index,
        )
        .expect("plan from unavailable route view");
        let reachable_plan = default_cluster_metadata_shard_with_host(
            &mut reachable_view,
            &state,
            &file_system,
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            unreachable_index,
        )
        .expect("plan from reachable route view");

        assert_eq!(unavailable_plan, current);
        assert_eq!(reachable_plan, current);
        assert_eq!(unavailable_plan, reachable_plan);
        assert!(unavailable_view.primary_transfer_targets.is_empty());
        assert!(reachable_view.primary_transfer_targets.is_empty());
    }

    #[test]
    fn metadata_primary_routing_unavailability_requires_sustained_evidence() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-routing-unavailability-grace");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_routing_grace_local").expect("local");
        let target = NodeId::from_legacy_named("node_dfs_routing_grace_target").expect("target");
        let started = Instant::now();
        let grace = Duration::from_secs(30);

        assert!(!metadata_primary_routing_suspicion_elapsed(
            &cluster_paths,
            &local,
            &target,
            started,
            grace,
        ));
        assert!(!metadata_primary_routing_suspicion_elapsed(
            &cluster_paths,
            &local,
            &target,
            started + grace - Duration::from_millis(1),
            grace,
        ));
        assert!(metadata_primary_routing_suspicion_elapsed(
            &cluster_paths,
            &local,
            &target,
            started + grace,
            grace,
        ));

        clear_metadata_primary_routing_suspicion(&cluster_paths, &local, &target);
        assert!(!metadata_primary_routing_suspicion_elapsed(
            &cluster_paths,
            &local,
            &target,
            started + grace + Duration::from_secs(1),
            grace,
        ));
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
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &data_dir, &local, &current, &local, None,
            )
            .expect("empty local shard is not promotable")
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
                None,
            )
            .expect("non replica cannot self-promote")
        );
        assert!(
            !metadata_shard_local_replica_failover_ready_from_local_backend(
                &data_dir,
                &local,
                &current,
                &remote_primary,
                None,
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
                &data_dir, &local, &current, &local, None,
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
        replica_backend
            .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
                shard_id: shard_id.clone(),
                journal_sequence: batch.next_after_sequence,
                durable_prefix_proof: None,
            })
            .expect("persist certified replica authority floor");
        let state_fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
            &replica_backend,
            &shard_id,
            batch.next_after_sequence,
        )
        .expect("replica state fingerprint");
        drop(replica_backend);
        let watermark = DfsMetadataShardHeadWatermark {
            file_system_id: current.file_system_id.clone(),
            shard_id: current.shard_id.clone(),
            primary_node_id: remote_primary,
            epoch: current.epoch,
            journal_sequence: batch.next_after_sequence,
            state_fingerprint,
            acknowledged_node_ids: vec![current.primary_node_id.clone(), local.clone()],
        };

        assert!(
            metadata_shard_local_replica_failover_ready_from_local_backend(
                &replica_data_dir,
                &local,
                &current,
                &local,
                Some(&watermark),
            )
            .expect("DFS-certified primary head makes caught-up replica promotable")
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

    fn put_cluster_member_records(
        kv: &mut sinomo_core::kv::InProcessKvStore,
        members: &[ClusterMemberRecord],
    ) {
        for member in members {
            DfsControlPlaneKvClient::put(
                kv,
                sinomo_core::node::cluster_member_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(member).expect("encode cluster member"),
            )
            .expect("put cluster member");
        }
    }
}
