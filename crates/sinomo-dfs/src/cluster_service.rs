//! Cluster-facing DFS service construction helpers.
//!
//! This module keeps the crate-root service loop from also owning route
//! selection and local service assembly.

use sinomo_core::ClusterBootstrapState;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ClusterPaths;
use sinomo_core::node::NodeId;
use std::sync::Arc;

use crate::{
    AsyncDfsPeerTransport, DfsCommittedWorkTracker, DfsControlPlaneKvClient, DfsError,
    DfsMessageSender, DfsNodeDetachedSendState, DfsNodeTransport, DfsPeerTransport,
    LocalContentStore, NetworkedDfsService, open_cluster_aware_dfs_metadata_backend,
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

pub(crate) fn open_cluster_dfs_service_with_message_sender(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    message_sender: DfsMessageSender,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
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
    let metadata = open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
    service.with_boxed_metadata_backend(metadata)
}

pub(crate) fn open_cluster_dfs_service_with_message_sender_lazy(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    message_sender: DfsMessageSender,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
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
    let metadata = open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
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
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    committed_work_tracker: Option<DfsCommittedWorkTracker>,
    should_cancel: &(dyn Fn() -> bool + Sync),
) -> Result<Option<NetworkedDfsService<DfsNodeTransport>>, DfsError> {
    if should_cancel() {
        return Ok(None);
    }
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
    let metadata = open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
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
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    hydration: ClusterDfsServiceHydration,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsError> {
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
    let metadata = open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
    match hydration {
        ClusterDfsServiceHydration::Foreground => service.with_boxed_metadata_backend(metadata),
        ClusterDfsServiceHydration::Lazy => service.with_boxed_metadata_backend_lazy(metadata),
        ClusterDfsServiceHydration::CommitPromotionRecovery => {
            service.with_boxed_metadata_backend_for_commit_promotion_recovery(metadata)
        }
    }
}
