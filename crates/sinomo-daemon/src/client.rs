use crate::auth::CLUSTER_INVITE_CHALLENGE_PROTOCOL;
use crate::{
    ClusterInviteChallengeProof, ClusterInviteChallengeRequest, DaemonError, DaemonStatus,
    daemon_rpc_command, sign_daemon_rpc_request_for_local_node,
    verify_cluster_invite_challenge_proof,
};
use sinomo_core::{ClusterEndpointRecord, ClusterTransportKind};
use sinomo_kv::KvSnapshot;
use sinomo_network::{
    ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse, ClusterKvWatchEventsRequest,
    ClusterKvWatchEventsResponse, ClusterMemberPublishRequest, ClusterMemberPublishResponse,
    ClusterRaftAppendRequest, ClusterRaftAppendResponse, ClusterRaftVoteRequest,
    ClusterRaftVoteResponse, ObjectRpcRequest, random_url_secret,
};
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
};
use std::time::Duration;

pub fn send_daemon_rpc(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    sinomo_network::send_daemon_rpc_raw(
        address,
        &daemon_rpc_command(&request.route),
        Some(&signed),
        request,
        timeout,
    )
    .map_err(DaemonError::from)
}

pub fn send_daemon_rpc_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    sinomo_network::send_daemon_rpc_to_cluster_endpoint(
        endpoint,
        &daemon_rpc_command(&request.route),
        Some(&signed),
        request,
        timeout,
    )
    .map_err(DaemonError::from)
}

pub fn fetch_daemon_status_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        sinomo_network::DAEMON_STATUS_ROUTE,
        sinomo_network::DAEMON_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn fetch_daemon_status_from_address(
    address: &str,
    timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        sinomo_network::DAEMON_STATUS_ROUTE,
        sinomo_network::DAEMON_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc(address, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_shutdown_request(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        sinomo_network::DAEMON_SHUTDOWN_ROUTE,
        sinomo_network::DAEMON_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc(address, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn fetch_cluster_kv_snapshot(
    address: &str,
    timeout: Duration,
) -> Result<KvSnapshot, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_KV_SNAPSHOT_ROUTE,
        sinomo_network::CLUSTER_KV_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc(address, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_cluster_raft_append(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_RAFT_APPEND_ROUTE,
        sinomo_network::CLUSTER_RAFT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_cluster_raft_vote(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftVoteRequest,
    timeout: Duration,
) -> Result<ClusterRaftVoteResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_RAFT_VOTE_ROUTE,
        sinomo_network::CLUSTER_RAFT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_cluster_member_publish(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterMemberPublishRequest,
    timeout: Duration,
) -> Result<ClusterMemberPublishResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_cluster_join_registration(
    address: &str,
    request: &ClusterJoinRegistrationRequest,
    timeout: Duration,
) -> Result<ClusterJoinRegistrationResponse, DaemonError> {
    let challenge = random_url_secret(32)?;
    let challenge_request = ClusterInviteChallengeRequest {
        protocol: CLUSTER_INVITE_CHALLENGE_PROTOCOL.to_string(),
        invite_id: request.invite.invite_id.clone(),
        challenge: challenge.clone(),
    };
    let challenge_rpc = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        &challenge_request,
    )?;
    let proof_response = sinomo_network::send_daemon_rpc_raw(
        address,
        &daemon_rpc_command(sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE),
        None,
        &challenge_rpc,
        timeout,
    )?;
    let proof: ClusterInviteChallengeProof = proof_response.decode_body()?;
    verify_cluster_invite_challenge_proof(&request.invite, &challenge, &proof)?;
    let register_rpc = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    let signed = sign_daemon_rpc_request_for_local_node(&register_rpc)?;
    let response = sinomo_network::send_daemon_rpc_raw(
        address,
        &daemon_rpc_command(sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE),
        Some(&signed),
        &register_rpc,
        timeout,
    )?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_cluster_join_registration_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
    timeout: Duration,
) -> Result<ClusterJoinRegistrationResponse, DaemonError> {
    let challenge = random_url_secret(32)?;
    let challenge_request = ClusterInviteChallengeRequest {
        protocol: CLUSTER_INVITE_CHALLENGE_PROTOCOL.to_string(),
        invite_id: request.invite.invite_id.clone(),
        challenge: challenge.clone(),
    };
    let challenge_rpc = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        &challenge_request,
    )?;
    let proof_response = sinomo_network::send_daemon_rpc_to_cluster_endpoint(
        endpoint,
        &daemon_rpc_command(sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE),
        None,
        &challenge_rpc,
        timeout,
    )?;
    let proof: ClusterInviteChallengeProof = proof_response.decode_body()?;
    verify_cluster_invite_challenge_proof(&request.invite, &challenge, &proof)?;
    let register_rpc = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    let signed = sign_daemon_rpc_request_for_local_node(&register_rpc)?;
    let response = sinomo_network::send_daemon_rpc_to_cluster_endpoint(
        endpoint,
        &daemon_rpc_command(sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE),
        Some(&signed),
        &register_rpc,
        timeout,
    )?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_cluster_join_registration_to_channel(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
    tcp_timeout: Duration,
    iroh_timeout: Duration,
) -> Result<ClusterJoinRegistrationResponse, DaemonError> {
    match endpoint.transport {
        ClusterTransportKind::Iroh => {
            send_cluster_join_registration_to_endpoint(endpoint, request, iroh_timeout)
        }
        _ => send_cluster_join_registration(&endpoint.address, request, tcp_timeout),
    }
}

pub fn send_cluster_kv_watch_events(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterKvWatchEventsRequest,
    timeout: Duration,
) -> Result<ClusterKvWatchEventsResponse, DaemonError> {
    if request.events.is_empty() {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: true,
            detail: "no watch events to fan out".to_string(),
            stored_events: 0,
        });
    }
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_KV_WATCH_EVENTS_ROUTE,
        sinomo_network::CLUSTER_KV_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_binary(
    address: &str,
    request: &ClusterRolloutBinaryRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::ROLLOUT_BINARY_ROUTE,
        sinomo_network::ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc(address, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_build(
    address: &str,
    request: &ClusterRolloutBuildRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::ROLLOUT_BUILD_ROUTE,
        sinomo_network::ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc(address, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_finalize_shadow_rollout_request(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        sinomo_network::ROLLOUT_FINALIZE_SHADOW_ROUTE,
        sinomo_network::ROLLOUT_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc(address, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_diagnostics(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        sinomo_network::ROLLOUT_DIAGNOSTICS_ROUTE,
        sinomo_network::ROLLOUT_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc(address, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}
