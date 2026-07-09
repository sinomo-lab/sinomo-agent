//! Cluster-facing DFS service construction helpers.
//!
//! This module keeps the crate-root service loop from also owning route
//! selection and local service assembly.

use sinomo_core::ClusterBootstrapState;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_core::node::NodeId;
use std::sync::Arc;

use crate::{
    AsyncDfsPeerTransport, DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT, DfsControlPlaneKvClient, DfsError,
    DfsMessageSender, DfsNodeTransport, DfsPeerTransport, LocalContentStore, NetworkedDfsService,
    open_cluster_aware_dfs_metadata_backend,
};

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
            DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT,
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
            DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT,
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
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service = NetworkedDfsService::new(
        DfsNodeTransport::with_peer_transports_for_nodes(
            local_node_id,
            peer_node_ids,
            DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT,
            peer_transport,
            async_peer_transport,
        ),
        content,
    );
    if let Some(control_plane) = control_plane {
        service = service.with_boxed_control_plane(control_plane);
    }
    let metadata = open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
    service.with_boxed_metadata_backend(metadata)
}

pub(crate) fn object_rpc_request_from_kv_payload(
    payload: sinomo_core::kv::ClusterKvRpcPayload,
) -> ObjectRpcRequest {
    ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
}
