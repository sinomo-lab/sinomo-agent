//! Default SIFS and metadata bootstrap for cluster-backed DFS.
//!
//! This module owns the default distributed SIFS topology, shard layout, and
//! service advertisements that make a cluster's DFS usable after bootstrap.

pub(crate) mod genesis;

use serde::Serialize;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterPaths, cluster_healthy_route_next_hop,
    pooled_routed_relationship_is_active,
};
use sinomo_core::node::{DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY, NodeId};
use sinomo_core::{ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, DfsObjectPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
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
    DfsSmokeReport, DfsSubtreePolicy, ResidentLocalShardedMetadataBackend, SIFS_SERVICES_PATH,
    SIFS_USERS_PATH, content_service_key, default_dfs_metadata_shards_root,
    dfs_message_sender_from_peer_transport, dfs_metadata_journal_batch_sender_from_peer_transport,
    dfs_metadata_snapshot_install_sender_from_peer_transport, distributed_sifs,
    file_system_config_key, local_dfs_metadata_shard_path, metadata_service_key,
    metadata_shard_key, metadata_shard_membership_handoff_key,
    metadata_shard_membership_handoff_proof_key, open_cluster_aware_dfs_metadata_backend,
    open_local_metadata_shard_backend, open_local_sharded_metadata_backend_for_replica_repair,
    run_cluster_dfs_smoke_with_runtime_client, sifs_node_files_path,
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
#[cfg(any(test, feature = "test-support"))]
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

    fn native_genesis_committed_async<'a>(
        &'a mut self,
        current: DfsMetadataShardConfig,
    ) -> DfsDefaultClusterBootstrapFuture<'a, bool>;

    fn put_file_system_config_async<'a>(
        &'a mut self,
        config: DfsFileSystemConfig,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()>;

    fn put_metadata_shard_async<'a>(
        &'a mut self,
        shard: DfsMetadataShardConfig,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()>;

    fn current_node_advertisements_async<'a>(
        &'a mut self,
        keys: [String; 2],
    ) -> DfsDefaultClusterBootstrapFuture<'a, [Option<DfsNodeAdvertisement>; 2]>;

    fn put_node_advertisement_async<'a>(
        &'a mut self,
        key: String,
        advertisement: DfsNodeAdvertisement,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()>;

    fn prepare_native_genesis_async<'a>(
        &'a mut self,
        file_system: DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        bootstrap_nodes: Vec<NodeId>,
        fallback_node: NodeId,
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
pub struct DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    local_node_id: NodeId,
    control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
    resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
    publication_owners: Arc<crate::DfsPublicationExecutionOwners>,
    genesis_backend: Option<crate::LocalShardedMetadataBackend>,
}

impl DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    pub fn new_with_resident_metadata_backend(
        data_dir: SinomoDataDir,
        cluster_paths: ClusterPaths,
        local_node_id: NodeId,
        control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
        resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
        publication_owners: Arc<crate::DfsPublicationExecutionOwners>,
    ) -> Self {
        Self {
            data_dir,
            cluster_paths,
            local_node_id,
            control_plane,
            resident_metadata_backend,
            publication_owners,
            genesis_backend: None,
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

    fn native_genesis_committed_async<'a>(
        &'a mut self,
        current: DfsMetadataShardConfig,
    ) -> DfsDefaultClusterBootstrapFuture<'a, bool> {
        Box::pin(async move {
            if current.epoch != 1 || !current.replica_node_ids.is_empty() {
                return Ok(true);
            }
            if current.primary_node_id == self.local_node_id {
                let shard_id = DfsMetadataShardId::new(current.shard_id.clone());
                if let Some(resident) = self.resident_metadata_backend.as_ref() {
                    let handle = resident.scope_shard(&current.file_system_id, &shard_id)?;
                    let backend = handle.checkout()?;
                    return genesis::native_genesis_committed(&backend, &current);
                }
                let backend = match self.publication_owners.selected_original_data_layout()? {
                    Some(layout) => crate::LocalSobjMetadataBackend::new_with_selected_layout(
                        current.file_system_id.clone(),
                        shard_id.clone(),
                        crate::local_dfs_metadata_shard_path(&self.data_dir, &shard_id),
                        layout,
                    )?,
                    None => open_local_metadata_shard_backend(&self.data_dir, shard_id)?,
                };
                return genesis::native_genesis_committed(&backend, &current);
            }
            let watermark = DfsControlPlaneAsyncStore::new(self.control_plane.as_mut())
                .get_metadata_shard_head_watermark(&current.file_system_id, &current.shard_id)
                .await?;
            Ok(watermark.is_some_and(|watermark| {
                watermark.epoch == current.epoch
                    && watermark.primary_node_id == current.primary_node_id
                    && watermark.journal_sequence >= 2
                    && !watermark.state_fingerprint.is_empty()
                    && watermark
                        .acknowledged_node_ids
                        .contains(&current.primary_node_id)
            }))
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

    fn current_node_advertisements_async<'a>(
        &'a mut self,
        keys: [String; 2],
    ) -> DfsDefaultClusterBootstrapFuture<'a, [Option<DfsNodeAdvertisement>; 2]> {
        Box::pin(async move {
            let entries = self
                .control_plane
                .get_consistent_async(keys.to_vec())
                .await?;
            let entries: [Option<sinomo_core::kv::KvEntry>; 2] =
                entries.try_into().map_err(|_| DfsError::Metadata {
                    message: "local DFS advertisement read returned an incomplete exact key set"
                        .into(),
                })?;
            Ok(entries
                .map(|entry| entry.and_then(|entry| entry.decode::<DfsNodeAdvertisement>().ok())))
        })
    }

    fn put_node_advertisement_async<'a>(
        &'a mut self,
        key: String,
        advertisement: DfsNodeAdvertisement,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move { self.put_serde_if_changed(key, &advertisement).await })
    }

    fn prepare_native_genesis_async<'a>(
        &'a mut self,
        file_system: DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        bootstrap_nodes: Vec<NodeId>,
        fallback_node: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move {
            self.prepare_native_genesis(&file_system, &shards, bootstrap_nodes, fallback_node)
                .await
        })
    }

    fn materialize_default_sifs_metadata_async<'a>(
        &'a mut self,
        file_system: DfsFileSystem,
        bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move {
            let shards = DfsControlPlaneAsyncStore::new(self.control_plane.as_mut())
                .list_metadata_shards(&file_system.file_system_id)
                .await?;
            self.materialize_default_sifs_metadata_on_shards_async(
                file_system,
                shards,
                bootstrap_owner_node_ids,
                fallback_owner_node_id,
            )
            .await
        })
    }

    /// sinomo:kv-configuration: Capture exact shard handoff configuration for normal bootstrap materialization.
    fn materialize_default_sifs_metadata_on_shards_async<'a>(
        &'a mut self,
        file_system: DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        mut bootstrap_owner_node_ids: Vec<NodeId>,
        fallback_owner_node_id: NodeId,
    ) -> DfsDefaultClusterBootstrapFuture<'a, ()> {
        Box::pin(async move {
            // Capture the same bounded configuration keys in one coherent view.
            // These entries retain their existing proof validation at publication.
            let keys: Vec<_> = shards
                .iter()
                .flat_map(|shard| {
                    [
                        metadata_shard_membership_handoff_key(
                            &shard.file_system_id,
                            &shard.shard_id,
                        ),
                        metadata_shard_membership_handoff_proof_key(
                            &shard.file_system_id,
                            &shard.shard_id,
                        ),
                    ]
                })
                .collect();
            let entries = self.control_plane.get_consistent_async(keys).await?;
            if entries.len() != shards.len() * 2 {
                return Err(DfsError::Metadata {
                    message: "native bootstrap handoff evidence changed its exact key cardinality"
                        .into(),
                });
            }
            let mut evidence = Vec::with_capacity(shards.len());
            for pair in entries.chunks_exact(2) {
                evidence.push((
                    pair[0]
                        .as_ref()
                        .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
                        .transpose()?,
                    pair[1]
                        .as_ref()
                        .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoffProof>())
                        .transpose()?,
                ));
            }
            bootstrap_owner_node_ids.sort();
            bootstrap_owner_node_ids.dedup();
            let signature = DfsDefaultMaterializationSignature {
                file_system: file_system.clone(),
                shards: shards.clone(),
                bootstrap_owner_node_ids: bootstrap_owner_node_ids.clone(),
                fallback_owner_node_id: fallback_owner_node_id.clone(),
                handoff_evidence: evidence.clone(),
            };
            if default_materialization_is_current(&self.data_dir.root(), &signature) {
                return Ok(());
            }
            let publication_provider =
                crate::runtime_host::bootstrap_publication_execution_provider(
                    &self.data_dir,
                    &self.cluster_paths,
                    &self.publication_owners,
                    &tokio::runtime::Handle::current(),
                )
                .await?;
            self.publish_current_bootstrap(
                publication_provider,
                &file_system,
                shards,
                bootstrap_owner_node_ids,
                fallback_owner_node_id,
                evidence,
            )?;
            record_default_materialization(self.data_dir.root(), signature, false);
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
    // Capture before local I/O so an authority or desired-placement change
    // invalidates this selection pass, including a not-yet-created backend.
    let selection = crate::default_reconciliation::capture_primary_transfer_selection(data_dir);
    let shard_path = local_dfs_metadata_shard_path(data_dir, &shard_id);
    if !shard_path.exists() {
        let creation = selection
            .as_ref()
            .and_then(|selection| selection.register(current, target_primary_node_id, 0));
        // Creation and a completed checkpoint can race the first existence
        // read. Register then recheck; a zero head only asks for a new selector
        // pass, never a prefix or activation certificate.
        if !shard_path.exists() {
            if let Some(creation) = creation {
                creation.keep_waiting();
            }
            return Ok(false);
        }
    }
    let backend = open_local_metadata_shard_backend(data_dir, shard_id.clone())?;
    let journal_last_sequence = backend
        .metadata_shard_statuses()?
        .into_iter()
        .find(|status| status.shard_id == shard_id)
        .map(|status| status.journal_last_sequence)
        .unwrap_or(0);
    // The sender cursor is a scheduling hint for choosing a transfer candidate.
    // A missing checkpoint delays selection until catch-up recovers it; this
    // observation never activates the new primary. The subsequent handoff
    // still requires the retiring fence and fresh exact-prefix certificates.
    // Register before sampling the hint. A checkpoint crossing this read
    // either wakes this exact dependency or is observed as ready below.
    let dependency = selection.and_then(|selection| {
        selection.register(current, target_primary_node_id, journal_last_sequence)
    });
    let target_cursor = backend.metadata_replication_cursor(target_primary_node_id, &shard_id)?;
    let ready = target_cursor >= journal_last_sequence;
    if !ready && let Some(dependency) = dependency {
        dependency.keep_waiting();
    }
    Ok(ready)
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
        host.prepare_native_genesis_async(
            file_system.clone(),
            metadata_shards.clone(),
            stable_dfs_service_node_ids(state),
            state.local_node_id.clone(),
        )
        .await?;
        let publishable_shards = metadata_shards.iter().filter(|shard| {
            local_has_primary_metadata_shards
                || matches!(shard.assignment, DfsMetadataShardAssignment::Root)
        });
        let mut parked_handoff = None;
        for shard in publishable_shards {
            match host.put_metadata_shard_async(shard.clone()).await {
                Ok(()) => {}
                Err(error @ DfsError::MetadataShardMembershipHandoffParked { .. }) => {
                    // Independent shards already passed their own genesis and
                    // current-member checks. Request every eligible transition
                    // through the same normal producer before waiting for the
                    // first handoff's exact fence/proof wake. Each assignment
                    // still activates only through its ordinary native protocol.
                    parked_handoff.get_or_insert(error);
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(error) = parked_handoff {
            // A pending request is not completion or materialization authority.
            return Err(error);
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
        let local_is_bootstrap_owner = default_cluster_sifs_owner(state) == state.local_node_id;
        if tracing::enabled!(target: "sinomo.dfs.reconciliation", tracing::Level::TRACE) {
            tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "nonpublisher-bootstrap-selection",
                local_node = %state.local_node_id, eligible_member_ids = ?stable_dfs_service_node_ids(state),
                local_has_primary_metadata_shards = local_is_bootstrap_owner,
                materialize_local_metadata = options.materialize_local_metadata,
                "default DFS nonpublisher used existing bootstrap membership read");
        }
        local_is_bootstrap_owner
    };
    if local_has_primary_metadata_shards
        && !options.publish_cluster_control
        && options.materialize_local_metadata
    {
        tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "nonpublisher-materialization-started",
            local_node = %state.local_node_id, "default DFS nonpublisher entering actual local materializer");
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

pub async fn ensure_default_cluster_dfs_async_with_resident_metadata(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    control_plane: Box<dyn AsyncDfsControlPlaneKvClient>,
    resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
    publication_owners: Arc<crate::DfsPublicationExecutionOwners>,
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
            publication_owners,
        );
    let result = ensure_default_distributed_dfs_with_async_host(&state, &mut host, options)
        .await
        .map(Some);
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "bootstrap-wrapper-returned",
        local_node = %state.local_node_id, data_root = %data_dir.root().display(),
        "default DFS bootstrap await returned before host drop");
    drop(host);
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "bootstrap-host-dropped",
        local_node = %state.local_node_id, data_root = %data_dir.root().display(),
        "default DFS bootstrap host dropped");
    result
}

pub fn run_default_cluster_dfs_smoke_with_peer_transport(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    options: DfsSmokeOptions,
) -> Result<Option<DfsSmokeReport>, DfsError> {
    // The normal configuration owner initializes current DFS. A diagnostic
    // client cannot create a competing bootstrap or reinterpret retained data.
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    run_cluster_dfs_smoke_with_runtime_client(
        data_dir,
        cluster_paths,
        dfs_metadata_journal_batch_sender_from_peer_transport(
            data_dir,
            Arc::clone(&peer_transport),
        ),
        dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
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
            DfsSubtreePolicy::new(
                sifs_node_files_path(&node_id),
                DfsObjectPolicy::LocalOwned,
                1,
                1,
            )
        })
        .collect::<Vec<_>>();
    if policies.is_empty() {
        policies.push(DfsSubtreePolicy::new(
            sifs_node_files_path(&state.local_node_id),
            DfsObjectPolicy::LocalOwned,
            1,
            1,
        ));
    }
    // Shared paths keep the normal candidate-derived default instead of
    // persisting a replica count observed during partial cluster formation.
    // Explicit placement Apply rules remain in the current configuration.
    policies
}

pub async fn default_cluster_metadata_shards_with_async_host(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    file_system: &DfsFileSystem,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    // One topology-selection pass consumes one discovery result for all three
    // shards. Their current assignments, genesis and handoff readiness remain
    // independently checked below.
    let service_node_ids = host
        .metadata_service_node_ids_async(state, stable_dfs_service_node_ids(state))
        .await?;
    Ok(vec![
        default_cluster_metadata_shard_with_service_nodes(
            host,
            state,
            file_system,
            &service_node_ids,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .await?,
        default_cluster_metadata_shard_with_service_nodes(
            host,
            state,
            file_system,
            &service_node_ids,
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            1,
        )
        .await?,
        default_cluster_metadata_shard_with_service_nodes(
            host,
            state,
            file_system,
            &service_node_ids,
            "users",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_USERS_PATH.to_string(),
            },
            2,
        )
        .await?,
    ])
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
    default_cluster_metadata_shard_with_service_nodes(
        host,
        state,
        file_system,
        &service_node_ids,
        shard_id,
        assignment,
        primary_offset,
    )
    .await
}

// The existing public shard-selection inputs plus one shared discovery result.
#[allow(clippy::too_many_arguments)]
async fn default_cluster_metadata_shard_with_service_nodes(
    host: &mut impl DfsDefaultClusterAsyncBootstrapHost,
    state: &ClusterBootstrapState,
    file_system: &DfsFileSystem,
    service_node_ids: &[NodeId],
    shard_id: &str,
    assignment: DfsMetadataShardAssignment,
    primary_offset: usize,
) -> Result<DfsMetadataShardConfig, DfsError> {
    let bootstrap_primary_node_id = dfs_service_members(state)
        .into_iter()
        .filter(|member| {
            member.role == ClusterMemberRole::Voter && service_node_ids.contains(&member.node_id)
        })
        .map(|member| member.node_id.clone())
        .min()
        .ok_or_else(|| DfsError::Metadata {
            message: "current DFS genesis requires an eligible native configuration voter".into(),
        })?;
    let default_target_primary_node_id = default_metadata_shard_primary_node_id(
        service_node_ids,
        &bootstrap_primary_node_id,
        primary_offset,
    );
    let mut desired = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.to_string(),
        primary_node_id: default_target_primary_node_id.clone(),
        replica_node_ids: default_metadata_shard_replica_node_ids(
            service_node_ids,
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
        desired.replica_node_ids.clear();
        require_current_metadata_members(state, &desired)?;
        return Ok(desired);
    };
    if current.file_system_id != file_system.file_system_id
        || current.shard_id != shard_id
        || current.assignment != desired.assignment
        || current.default_policy != desired.default_policy
        || current.epoch == 0
    {
        return Err(DfsError::Metadata {
            message: "current default metadata assignment changes its exact scope or policy".into(),
        });
    }
    require_current_metadata_members(state, &current)?;
    let initial_native_assignment = current.epoch == 1
        && current.primary_node_id == bootstrap_primary_node_id
        && current.replica_node_ids.is_empty();
    if !initial_native_assignment {
        // The authoritative active assignment is the persistent configuration.
        // Discovery supplies initial defaults, not permission to replace a
        // completed native membership or primary-role handoff. Further changes
        // use the same explicit guarded configuration handoff protocol.
        return Ok(current);
    }
    if !host.native_genesis_committed_async(current.clone()).await? {
        return Ok(current);
    }
    // Only the exact initial singleton expands after its native genesis commits.
    // Existing epoch, replica presence or a sorted discovery layout alone never
    // establish that a later assignment is bootstrap work.
    desired.primary_node_id = default_target_primary_node_id;
    desired.replica_node_ids =
        default_metadata_shard_replica_node_ids(service_node_ids, &desired.primary_node_id);
    if current.primary_node_id != desired.primary_node_id
        && let Some(replica_node_ids) =
            topology_preserving_metadata_shard_replicas(&current, &desired.primary_node_id)
    {
        desired.replica_node_ids = replica_node_ids;
    }
    require_current_metadata_members(state, &current)?;
    require_current_metadata_members(state, &desired)?;
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
            default_metadata_shard_replica_node_ids(service_node_ids, &current.primary_node_id);
        require_current_metadata_members(state, &expanded)?;
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

fn require_current_metadata_members(
    state: &ClusterBootstrapState,
    shard: &DfsMetadataShardConfig,
) -> Result<(), DfsError> {
    for node_id in std::iter::once(&shard.primary_node_id).chain(&shard.replica_node_ids) {
        if !state.members.iter().any(|member| {
            member.node_id == *node_id
                && member.state.is_authorized()
                && member.capabilities.iter().any(|capability| {
                    capability.as_str()
                        == sinomo_core::node::DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY
                })
        }) {
            return Err(DfsError::Metadata {
                message: format!(
                    "metadata member {node_id} must upgrade before using the current shard protocol"
                ),
            });
        }
    }
    Ok(())
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
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "local-advertisements-read-started", local_node = %state.local_node_id);
    let [current_metadata, current_content] = host
        .current_node_advertisements_async([metadata_key.clone(), content_key.clone()])
        .await?;
    let metadata = stable_dfs_advertisement(current_metadata, metadata);
    let content = stable_dfs_advertisement(current_content, content);
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "local-advertisements-read-completed", local_node = %state.local_node_id);
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "metadata-advertisement-put-started", local_node = %state.local_node_id, key = %metadata_key);
    host.put_node_advertisement_async(metadata_key.clone(), metadata)
        .await?;
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "metadata-advertisement-put-completed", local_node = %state.local_node_id, key = %metadata_key);
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "content-advertisement-put-started", local_node = %state.local_node_id, key = %content_key);
    host.put_node_advertisement_async(content_key.clone(), content)
        .await?;
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "content-advertisement-put-completed", local_node = %state.local_node_id, key = %content_key);
    Ok(())
}

fn stable_dfs_advertisement(
    current: Option<DfsNodeAdvertisement>,
    desired: DfsNodeAdvertisement,
) -> DfsNodeAdvertisement {
    let Some(current) = current else {
        return desired;
    };
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
        return current;
    }
    desired
}
