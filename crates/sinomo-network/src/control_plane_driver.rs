use crate::ClusterPaths;
use crate::connectivity::{ClusterConnectivityRecord, ClusterConnectivityState};
use crate::control_plane_options::ClusterControlPlaneTimeouts;
use crate::error::NetworkError;
use crate::network_info::ClusterNetworkInfoRecord;
use crate::prelude::*;
use crate::rpc::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_kv::{
    ClusterKvReplicationRequest, ClusterKvReplicationResponse, ClusterKvStateAuthorityVoteRequest,
    ClusterKvStateAuthorityVoteResponse,
};
use std::time::Duration;

pub trait ClusterControlPlaneTransport {
    fn send_kv_replication(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterKvReplicationRequest,
        timeout: Duration,
    ) -> Result<ClusterKvReplicationResponse, NetworkError>;

    fn request_kv_state_authority_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterKvStateAuthorityVoteResponse, NetworkError>;

    fn probe_cluster_state_authority(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> bool;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn call_endpoint_object_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn published_network_info_matches(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) -> bool {
        false
    }

    fn remember_published_network_info(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) {
    }

    fn published_node_status_matches(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) -> bool {
        false
    }

    fn remember_published_node_status(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) {
    }

    fn published_connectivity_record_set_matches(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) -> bool {
        false
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) {
    }

    fn fan_out_committed_cluster_kv_command(
        &mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) {
    }

    fn on_kv_replication_sent(&mut self) {}
}

pub struct ClusterControlPlaneDriver<T> {
    pub(crate) transport: T,
    pub(crate) timeouts: ClusterControlPlaneTimeouts,
}

impl<T> ClusterControlPlaneDriver<T> {
    pub fn new(transport: T) -> Self {
        Self::with_timeouts(
            transport,
            ClusterControlPlaneTimeouts::default_daemon_transport(),
        )
    }

    pub fn with_timeouts(transport: T, timeouts: ClusterControlPlaneTimeouts) -> Self {
        Self {
            transport,
            timeouts,
        }
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn into_transport(self) -> T {
        self.transport
    }
}
