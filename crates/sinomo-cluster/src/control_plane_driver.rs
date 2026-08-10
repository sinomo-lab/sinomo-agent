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
    /// Installs the cluster root used by routed operations for the duration of
    /// one driver call, returning the previous scope for nested restoration.
    /// Transports that do not need filesystem-backed routing may ignore it.
    fn replace_active_cluster_paths(
        &mut self,
        _paths: Option<ClusterPaths>,
    ) -> Option<ClusterPaths> {
        None
    }

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

    /// Concurrent synchronous ReadIndex heartbeat fanout. Ordinary transport
    /// implementations that do not serve authoritative reads fail closed here
    /// instead of silently serializing voters under one operation deadline.
    fn send_kv_read_index_probes(
        &mut self,
        _state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        _request: &ClusterKvReplicationRequest,
        _timeout: Duration,
        _required_acks: usize,
    ) -> Result<Vec<(NodeId, Result<ClusterKvReplicationResponse, NetworkError>)>, NetworkError>
    {
        Ok(targets
            .into_iter()
            .map(|target| {
                (
                    target,
                    Err(NetworkError::ClusterReadOnly {
                        detail: "synchronous ReadIndex batch transport is unavailable".to_string(),
                    }),
                )
            })
            .collect())
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

    /// Concurrent synchronous election fanout. Hosts that participate in
    /// elections must override this; failing closed avoids silently restoring
    /// a voter-by-voter WAN critical path.
    fn request_kv_state_authority_votes_from_nodes(
        &mut self,
        _state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        _request: &ClusterKvStateAuthorityVoteRequest,
        _timeout: Duration,
        _required_grants: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<ClusterKvStateAuthorityVoteResponse, NetworkError>,
        )>,
        NetworkError,
    > {
        Ok(targets
            .into_iter()
            .map(|target| {
                (
                    target,
                    Err(NetworkError::ClusterReadOnly {
                        detail: "synchronous Raft election batch transport is unavailable"
                            .to_string(),
                    }),
                )
            })
            .collect())
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
    // AppendEntries is idempotent for an identical Raft request.
    cluster_kv_object_rpc_request(payload).with_response_retry_safe()
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

impl<T> ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    pub(crate) fn with_active_cluster_paths<R>(
        &mut self,
        paths: &ClusterPaths,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let previous = self
            .transport
            .replace_active_cluster_paths(Some(paths.clone()));
        let result = operation(self);
        self.transport.replace_active_cluster_paths(previous);
        result
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
    fn raft_requests_have_operation_specific_retry_markers() {
        let append =
            cluster_raft_append_object_rpc_request(test_payload("/test/cluster/raft/append"));
        let vote = cluster_raft_vote_object_rpc_request(test_payload("/test/cluster/raft/vote"));
        let general = cluster_kv_object_rpc_request(test_payload("/test/cluster/kv"));

        assert!(append.is_response_retry_safe());
        assert!(vote.is_response_retry_safe());
        assert!(!general.is_response_retry_safe());
    }
}
