use crate::control_plane_options::ClusterControlPlaneTimeouts;
use crate::prelude::*;
use sinomo_kv::{
    ClusterKvReplicationRequest, ClusterKvReplicationResponse, ClusterKvRpcPayload,
    ClusterKvStateAuthorityVoteRequest, ClusterKvStateAuthorityVoteResponse,
};
#[allow(unused_imports)]
use sinomo_kv::{KvEvent, KvSnapshot};
use sinomo_network::ClusterNetworkInfoRecord;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ClusterConnectivityRecord, ClusterConnectivityState};
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
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

    fn send_kv_replication_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ClusterKvReplicationRequest,
        timeout: Duration,
    ) -> Result<ClusterKvReplicationResponse, NetworkError> {
        let rpc_request = cluster_raft_append_object_rpc_request(
            sinomo_kv::cluster_raft_append_rpc_payload(request).map_err(|source| {
                NetworkError::Io {
                    operation: "encode routed cluster raft append".to_string(),
                    detail: source.to_string(),
                }
            })?,
        );
        let response = self.call_node_object_rpc(state, target_node_id, &rpc_request, timeout)?;
        decode_cluster_raft_append_response(&response)
    }

    fn request_kv_state_authority_vote_from_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterKvStateAuthorityVoteResponse, NetworkError> {
        let rpc_request = cluster_raft_vote_object_rpc_request(
            sinomo_kv::cluster_raft_vote_rpc_payload(request).map_err(|source| {
                NetworkError::Io {
                    operation: "encode routed cluster raft vote".to_string(),
                    detail: source.to_string(),
                }
            })?,
        );
        let response = self.call_node_object_rpc(state, target_node_id, &rpc_request, timeout)?;
        decode_cluster_raft_vote_response(&response)
    }

    fn probe_cluster_state_authority(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> bool;

    fn probe_cluster_state_authority_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> bool {
        let Ok(payload) = sinomo_kv::cluster_kv_local_snapshot_rpc_payload() else {
            return false;
        };
        let request = cluster_kv_object_rpc_request(payload);
        self.call_node_object_rpc(state, target_node_id, &request, timeout)
            .and_then(|response| {
                response.error_detail()?;
                Ok(response)
            })
            .is_ok()
    }

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

fn cluster_kv_object_rpc_request(payload: ClusterKvRpcPayload) -> ObjectRpcRequest {
    ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
}

fn cluster_raft_append_object_rpc_request(payload: ClusterKvRpcPayload) -> ObjectRpcRequest {
    // AppendEntries is idempotent for an identical Raft request. Preserve the
    // 30s control-plane budget for snapshot and catch-up responses while the
    // transport still enforces shorter connect and no-progress timeouts.
    cluster_kv_object_rpc_request(payload)
        .with_response_retry_safe()
        .with_full_response_budget()
}

fn cluster_raft_vote_object_rpc_request(payload: ClusterKvRpcPayload) -> ObjectRpcRequest {
    // RequestVote is idempotent for an identical candidate/term. It may retry
    // another endpoint, but stays on the short ordinary route budget so the
    // endpoint order remains optimized for leader recovery latency.
    cluster_kv_object_rpc_request(payload).with_response_retry_safe()
}

fn decode_cluster_raft_append_response(
    response: &ObjectRpcResponse,
) -> Result<ClusterKvReplicationResponse, NetworkError> {
    response.error_detail()?;
    sinomo_kv::decode_cluster_raft_append_rpc_response_body(&response.body).map_err(|source| {
        NetworkError::Io {
            operation: "decode routed cluster raft append".to_string(),
            detail: source.to_string(),
        }
    })
}

fn decode_cluster_raft_vote_response(
    response: &ObjectRpcResponse,
) -> Result<ClusterKvStateAuthorityVoteResponse, NetworkError> {
    response.error_detail()?;
    sinomo_kv::decode_cluster_raft_vote_rpc_response_body(&response.body).map_err(|source| {
        NetworkError::Io {
            operation: "decode routed cluster raft vote".to_string(),
            detail: source.to_string(),
        }
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_payload(route: &'static str) -> ClusterKvRpcPayload {
        ClusterKvRpcPayload {
            route,
            category: "test.cluster.kv",
            body: Vec::new(),
        }
    }

    #[test]
    fn raft_requests_have_operation_specific_retry_and_budget_markers() {
        let append =
            cluster_raft_append_object_rpc_request(test_payload("/test/cluster/raft/append"));
        let vote = cluster_raft_vote_object_rpc_request(test_payload("/test/cluster/raft/vote"));
        let general = cluster_kv_object_rpc_request(test_payload("/test/cluster/kv"));

        assert!(append.is_response_retry_safe());
        assert!(append.uses_full_response_budget());
        assert!(vote.is_response_retry_safe());
        assert!(!vote.uses_full_response_budget());
        assert!(!general.is_response_retry_safe());
        assert!(!general.uses_full_response_budget());
    }
}
