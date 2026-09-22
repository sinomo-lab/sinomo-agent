//! Cluster-facing DFS service construction helpers.
//!
//! This module keeps the crate-root service loop from also owning route
//! selection and local service assembly.

use sinomo_core::ClusterBootstrapState;
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ClusterPaths;
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::{
    AsyncDfsPeerTransport, DfsCommittedWorkTracker, DfsControlPlaneKvClient, DfsControlPlaneStore,
    DfsError, DfsMessageSender, DfsMetadataBackend, DfsNodeDetachedSendState, DfsNodeTransport,
    DfsPeerTransport, LocalContentStore, NetworkedDfsService, default_dfs_metadata_root,
    default_dfs_metadata_shards_root, open_cluster_aware_dfs_metadata_backend,
    open_local_sharded_metadata_backend_for_replica_repair, open_root_local_metadata_backend,
    sifs_file_system_id,
};

#[derive(Clone, Copy)]
enum ClusterDfsServiceHydration {
    Foreground,
    Lazy,
    CommitPromotionRecovery,
}

pub fn cluster_dfs_peer_node_ids(state: &ClusterBootstrapState) -> Vec<NodeId> {
    state
        .members
        .iter()
        .filter(|member| member.cluster_id == state.cluster_id && member.state.is_authorized())
        .map(|member| member.node_id.clone())
        .collect()
}

/// Resolves active DFS providers from the generic service-instance catalog.
///
/// An empty catalog is treated as a rolling-upgrade/legacy cluster and keeps
/// the membership-derived behavior. Once any service instances have been
/// published, the catalog is authoritative: a node which does not advertise
/// the DFS transfer capability is not a provider.
pub fn discovered_cluster_dfs_provider_node_ids(
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, DfsError> {
    // Callers can hold the bootstrap identity checkpoint while membership
    // advances in KV. Resolve protocol peers from the cluster-owned member
    // projection, retaining the legacy bootstrap fallback only when absent.
    let kv_members = sinomo_core::cluster::read_cluster_members_from_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    let members = if kv_members.is_empty() {
        &state.members
    } else {
        &kv_members
    };
    let entries = sinomo_core::kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX,
    )
    .map_err(DfsError::from)?;
    if entries.is_empty() {
        return Ok(members
            .iter()
            .filter(|member| member.cluster_id == state.cluster_id && member.state.is_authorized())
            .map(|member| member.node_id.clone())
            .collect());
    }
    let instances =
        sinomo_core::service::service_instances_from_entries(&entries).map_err(|source| {
            DfsError::Metadata {
                message: format!("cannot decode DFS service discovery records: {source}"),
            }
        })?;
    let provider_nodes = sinomo_core::service::service_instances_providing_capability(
        instances,
        "dfs.object.transfer",
    )
    .into_iter()
    .map(|instance| instance.node_id)
    .collect::<BTreeSet<_>>();
    let mut providers = members
        .iter()
        .filter(|member| {
            member.cluster_id == state.cluster_id
                && member.state.is_authorized()
                && provider_nodes.contains(&member.node_id)
        })
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    providers.sort();
    providers.dedup();
    Ok(providers)
}

/// Resolves DFS protocol peers for an already active local DFS runtime.
///
/// The local node is retained before its first service-instance publication
/// can commit; remote peers must be active providers from discovery.
pub fn discovered_cluster_dfs_peer_node_ids(
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, DfsError> {
    let mut peers = discovered_cluster_dfs_provider_node_ids(cluster_paths, state)?;
    if !peers.iter().any(|node_id| node_id == &state.local_node_id) {
        peers.push(state.local_node_id.clone());
        peers.sort();
    }
    Ok(peers)
}

pub(crate) fn open_cluster_dfs_service_with_message_sender(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    message_sender: DfsMessageSender,
    mut control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
    let metadata =
        open_service_metadata_backend(data_dir, cluster_paths, &local_node_id, &mut control_plane)?;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service = NetworkedDfsService::new(
        DfsNodeTransport::with_message_sender_for_nodes(
            local_node_id,
            peer_node_ids,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            message_sender,
        ),
        content,
    );
    if let Some(control_plane) = control_plane {
        service = service.with_boxed_control_plane(control_plane);
    }
    service.with_boxed_metadata_backend(metadata)
}

pub(crate) fn open_cluster_dfs_service_with_message_sender_lazy(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    message_sender: DfsMessageSender,
    mut control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
    let metadata =
        open_service_metadata_backend(data_dir, cluster_paths, &local_node_id, &mut control_plane)?;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service = NetworkedDfsService::new(
        DfsNodeTransport::with_message_sender_for_nodes(
            local_node_id,
            peer_node_ids,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            message_sender,
        ),
        content,
    );
    if let Some(control_plane) = control_plane {
        service = service.with_boxed_control_plane(control_plane);
    }
    service.with_boxed_metadata_backend_lazy(metadata)
}

pub(crate) fn open_cluster_dfs_service_with_peer_transports_for_nodes(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_detached_state(
        data_dir,
        cluster_paths,
        local_node_id,
        peer_node_ids,
        peer_transport,
        async_peer_transport,
        DfsNodeDetachedSendState::default(),
        control_plane,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn open_cluster_dfs_service_with_peer_transports_for_nodes_lazy(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_state(
        data_dir,
        cluster_paths,
        local_node_id,
        peer_node_ids,
        peer_transport,
        async_peer_transport,
        DfsNodeDetachedSendState::default(),
        control_plane,
        ClusterDfsServiceHydration::Lazy,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn open_cluster_dfs_service_with_peer_transports_for_nodes_and_detached_state(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: DfsNodeDetachedSendState,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_state(
        data_dir,
        cluster_paths,
        local_node_id,
        peer_node_ids,
        peer_transport,
        async_peer_transport,
        detached_send_state,
        control_plane,
        ClusterDfsServiceHydration::Foreground,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: DfsNodeDetachedSendState,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_state(
        data_dir,
        cluster_paths,
        local_node_id,
        peer_node_ids,
        peer_transport,
        async_peer_transport,
        detached_send_state,
        control_plane,
        ClusterDfsServiceHydration::CommitPromotionRecovery,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state_until_cancelled(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: DfsNodeDetachedSendState,
    mut control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    committed_work_tracker: Option<DfsCommittedWorkTracker>,
    should_cancel: &(dyn Fn() -> bool + Sync),
) -> Result<Option<NetworkedDfsService<DfsNodeTransport>>, DfsError> {
    if should_cancel() {
        return Ok(None);
    }
    let metadata =
        open_service_metadata_backend(data_dir, cluster_paths, &local_node_id, &mut control_plane)?;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service = NetworkedDfsService::new(
        DfsNodeTransport::with_peer_transports_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            peer_transport,
            async_peer_transport,
            detached_send_state,
        ),
        content,
    );
    if let Some(control_plane) = control_plane {
        service = service.with_boxed_control_plane(control_plane);
    }
    if let Some(committed_work_tracker) = committed_work_tracker {
        service.set_committed_work_tracker(committed_work_tracker);
    }
    service.with_boxed_metadata_backend_for_commit_promotion_recovery_until_cancelled(
        metadata,
        should_cancel,
    )
}

#[allow(clippy::too_many_arguments)]
fn open_cluster_dfs_service_with_peer_transports_for_nodes_and_state(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: DfsNodeDetachedSendState,
    mut control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    hydration: ClusterDfsServiceHydration,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
    let metadata =
        open_service_metadata_backend(data_dir, cluster_paths, &local_node_id, &mut control_plane)?;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service = NetworkedDfsService::new(
        DfsNodeTransport::with_peer_transports_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            peer_transport,
            async_peer_transport,
            detached_send_state,
        ),
        content,
    );
    if let Some(control_plane) = control_plane {
        service = service.with_boxed_control_plane(control_plane);
    }
    match hydration {
        ClusterDfsServiceHydration::Foreground => service.with_boxed_metadata_backend(metadata),
        ClusterDfsServiceHydration::Lazy => service.with_boxed_metadata_backend_lazy(metadata),
        ClusterDfsServiceHydration::CommitPromotionRecovery => {
            service.with_boxed_metadata_backend_for_commit_promotion_recovery(metadata)
        }
    }
}

fn open_service_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    control_plane: &mut Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
    if !cluster_paths.state.exists() {
        return open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths);
    }
    let Some(control_plane) = control_plane.as_mut() else {
        return open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths);
    };

    // The supplied control-plane client owns the process's authority boundary.
    // Reopening the daemon-owned resident Raft node here duplicates ReadIndex
    // work and either contends on its node lock or cold-replays durable state.
    // Reuse the exact topology scope this client has already validated instead.
    let shards = DfsControlPlaneStore::new(control_plane.as_mut())
        .list_metadata_shards(&sifs_file_system_id())
        .map_err(DfsError::from)?;
    if shards.is_empty() {
        return open_root_local_metadata_backend(default_dfs_metadata_root(data_dir))
            .map(|backend| Box::new(backend) as Box<dyn DfsMetadataBackend>)
            .map_err(Into::into);
    }
    open_local_sharded_metadata_backend_for_replica_repair(
        default_dfs_metadata_shards_root(data_dir),
        shards,
        local_node_id,
    )
    .map(|backend| Box::new(backend) as Box<dyn DfsMetadataBackend>)
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::{KvEntry, KvRevision, KvSnapshot, KvVersion};
    use time::OffsetDateTime;

    #[test]
    fn discovered_peers_exclude_authorized_members_without_dfs_service() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-discovered-peer-filter");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data root");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_discovered_peer_filter")
                .expect("cluster id");
        let local = NodeId::from_legacy_named("node_dfs_discovered_peer_local").expect("local");
        let provider =
            NodeId::from_legacy_named("node_dfs_discovered_peer_provider").expect("provider");
        let disabled =
            NodeId::from_legacy_named("node_dfs_discovered_peer_disabled").expect("disabled");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "DFS provider discovery",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members.extend([
            sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                provider.clone(),
                "provider",
                "pk-provider",
                now,
            ),
            sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id,
                disabled.clone(),
                "disabled",
                "pk-disabled",
                now,
            ),
        ]);
        let records = [
            {
                let mut record =
                    sinomo_core::service::ServiceInstanceRecord::new("dfs", provider.clone());
                record.routes = vec![crate::DFS_STATUS_ROUTE.to_string()];
                record.provider_capabilities = vec!["dfs.object.transfer".to_string()];
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
        let mut entries = records
            .into_iter()
            .map(|record| KvEntry {
                key: sinomo_core::service::cluster_service_instance_key(
                    &record.service_id,
                    &record.node_id,
                ),
                value: sinomo_core::kv::encode_kv_value(&record).expect("encode record"),
                create_revision: KvRevision(1),
                modify_revision: KvRevision(1),
                version: KvVersion(1),
                lease_id: None,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        sinomo_core::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &KvSnapshot {
                revision: KvRevision(1),
                compacted_revision: KvRevision(0),
                entries,
                leases: Vec::new(),
                events: Vec::new(),
                committed_requests: Vec::new(),
                expired_requests: Vec::new(),
            },
        )
        .expect("write service catalog");

        let peers = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)
            .expect("discover DFS peers");
        let providers = discovered_cluster_dfs_provider_node_ids(&cluster_paths, &state)
            .expect("discover DFS providers");

        assert!(peers.contains(&local));
        assert!(peers.contains(&provider));
        assert_eq!(peers.len(), 2);
        assert_eq!(providers, vec![provider]);
    }

    #[test]
    fn populated_catalog_with_no_dfs_capability_has_no_providers() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-no-discovered-provider");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data root");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_no_provider")
            .expect("cluster id");
        let local = NodeId::from_legacy_named("node_dfs_no_provider_local").expect("local");
        let disabled =
            NodeId::from_legacy_named("node_dfs_no_provider_disabled").expect("disabled");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "DFS provider discovery",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id,
                disabled.clone(),
                "disabled",
                "pk-disabled",
                now,
            ));
        let mut record = sinomo_core::service::ServiceInstanceRecord::new("daemon", disabled);
        record.routes = vec!["/daemon/status".to_string()];
        record.provider_capabilities = vec!["daemon.status.read".to_string()];
        record.updated_at = now;
        sinomo_core::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &KvSnapshot {
                revision: KvRevision(1),
                compacted_revision: KvRevision(0),
                entries: vec![KvEntry {
                    key: sinomo_core::service::cluster_service_instance_key(
                        &record.service_id,
                        &record.node_id,
                    ),
                    value: sinomo_core::kv::encode_kv_value(&record).expect("encode record"),
                    create_revision: KvRevision(1),
                    modify_revision: KvRevision(1),
                    version: KvVersion(1),
                    lease_id: None,
                }],
                leases: Vec::new(),
                events: Vec::new(),
                committed_requests: Vec::new(),
                expired_requests: Vec::new(),
            },
        )
        .expect("write service catalog");

        let providers = discovered_cluster_dfs_provider_node_ids(&cluster_paths, &state)
            .expect("discover DFS providers");
        let peers = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)
            .expect("discover DFS peers");

        assert!(providers.is_empty());
        assert_eq!(peers, vec![local]);
    }
}
