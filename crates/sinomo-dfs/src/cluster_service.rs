//! Cluster-facing DFS service construction helpers.
//!
//! This module keeps the crate-root service loop from also owning endpoint
//! selection and local service assembly.

use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_core::node::NodeId;
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource,
    ClusterTransportKind,
};
use std::sync::Arc;

use crate::{
    AsyncDfsPeerTransport, DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT, DfsControlPlaneKvClient,
    DfsEndpointTransport, DfsError, DfsMessageSender, DfsPeerTransport, LocalContentStore,
    NetworkedDfsService, open_cluster_aware_dfs_metadata_backend,
};

pub fn cluster_dfs_data_endpoints(
    _cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    cluster_dfs_peer_node_ids(state)
        .into_iter()
        .map(cluster_dfs_logical_peer_endpoint)
        .collect()
}

pub fn cluster_dfs_peer_node_ids(state: &ClusterBootstrapState) -> Vec<NodeId> {
    state
        .members
        .iter()
        .filter(|member| member.cluster_id == state.cluster_id && member.state.is_authorized())
        .map(|member| member.node_id.clone())
        .collect()
}

fn cluster_dfs_logical_peer_endpoint(node_id: NodeId) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
            "sinomo.dfs.logical-peer",
            node_id.as_str(),
        ),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::InProcess,
        address: format!("cluster-node://{node_id}"),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 0,
        last_observed_latency_ms: None,
    }
}

pub(crate) fn open_cluster_dfs_service_with_message_sender(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    endpoints: Vec<ClusterEndpointRecord>,
    message_sender: DfsMessageSender,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsEndpointTransport>, DfsError> {
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service = NetworkedDfsService::new(
        DfsEndpointTransport::with_message_sender(
            local_node_id,
            endpoints,
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
    endpoints: Vec<ClusterEndpointRecord>,
    message_sender: DfsMessageSender,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<NetworkedDfsService<DfsEndpointTransport>, DfsError> {
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service = NetworkedDfsService::new(
        DfsEndpointTransport::with_message_sender(
            local_node_id,
            endpoints,
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
) -> Result<NetworkedDfsService<DfsEndpointTransport>, DfsError> {
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service = NetworkedDfsService::new(
        DfsEndpointTransport::with_peer_transports_for_nodes(
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
