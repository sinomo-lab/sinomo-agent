use crate::auth::CLUSTER_INVITE_CHALLENGE_PROTOCOL;
use crate::{
    ClusterInviteChallengeProof, ClusterInviteChallengeRequest, DaemonDfsInspectionRequest,
    DaemonDfsInspectionResponse, DaemonDfsSmokeReport, DaemonError, DaemonStatus,
    daemon_rpc_command, sign_daemon_rpc_request_for_local_node,
    verify_cluster_invite_challenge_proof,
};
use sinomo_core::{ClusterEndpointRecord, ClusterTransportKind};
use sinomo_dfs::{
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsOperatorStatus,
    DfsProtocolFrameCommand, DfsProtocolFrameResponse,
};
use sinomo_kv::{KvSnapshot, KvTxn, KvTxnResponse};
use sinomo_network::{
    ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse, ClusterKvWatchEventsRequest,
    ClusterKvWatchEventsResponse, ClusterMemberPublishRequest, ClusterMemberPublishResponse,
    ClusterRaftAppendRequest, ClusterRaftAppendResponse, ClusterRaftVoteRequest,
    ClusterRaftVoteResponse, DaemonControlRequest, DaemonControlResponse, DaemonControlStream,
    ObjectRpcRequest, random_url_secret,
};
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
};
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

pub fn send_daemon_rpc(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    let command = daemon_rpc_command(&request.route);
    match daemon_rpc_address_transport(address) {
        DaemonRpcAddressTransport::Iroh => sinomo_network::send_iroh_daemon_rpc_raw(
            address,
            &command,
            Some(&signed),
            request,
            timeout,
        ),
        DaemonRpcAddressTransport::Tcp => {
            sinomo_network::send_daemon_rpc_raw(address, &command, Some(&signed), request, timeout)
        }
    }
    .map_err(DaemonError::from)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonRpcAddressTransport {
    Tcp,
    Iroh,
}

fn daemon_rpc_address_transport(address: &str) -> DaemonRpcAddressTransport {
    if address.trim_start().starts_with("iroh://") {
        DaemonRpcAddressTransport::Iroh
    } else {
        DaemonRpcAddressTransport::Tcp
    }
}

pub fn send_daemon_rpc_stream(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
    mut on_response: impl FnMut(sinomo_network::ObjectRpcResponse) -> Result<bool, DaemonError>,
) -> Result<(), DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    let control_request = DaemonControlRequest::authenticated_rpc(
        daemon_rpc_command(&request.route),
        request.clone(),
        signed,
    );
    let mut stream = DaemonControlStream::connect(address).map_err(DaemonError::from)?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(DaemonError::from)?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(DaemonError::from)?;
    sinomo_network::write_object_frame_blocking(&mut stream, &control_request)
        .map_err(DaemonError::from)?;
    stream.flush().map_err(|source| DaemonError::Network {
        address: address.to_string(),
        detail: source.to_string(),
    })?;
    stream.shutdown_write();
    loop {
        let response: sinomo_network::DaemonControlResponse =
            sinomo_network::read_object_frame_blocking(
                &mut stream,
                sinomo_network::IROH_DAEMON_COMMAND_MAX_BYTES,
            )
            .map_err(DaemonError::from)?;
        let rpc = response.rpc.ok_or_else(|| DaemonError::Network {
            address: address.to_string(),
            detail: "daemon returned non-rpc stream frame".to_string(),
        })?;
        if !on_response(rpc)? {
            return Ok(());
        }
    }
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

pub fn fetch_dfs_status_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<DfsOperatorStatus, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::DFS_STATUS_ROUTE,
        sinomo_network::DFS_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn fetch_dfs_inspection_from_address(
    address: &str,
    request: &DaemonDfsInspectionRequest,
    timeout: Duration,
) -> Result<DaemonDfsInspectionResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::DFS_INSPECTION_ROUTE,
        sinomo_network::DFS_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc(address, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn run_dfs_smoke_via_daemon(
    address: &str,
    timeout: Duration,
) -> Result<DaemonDfsSmokeReport, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::DFS_SMOKE_ROUTE,
        sinomo_network::DFS_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc(address, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_dfs_metadata_journal_batch_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &DfsMetadataJournalBatchCommand,
    timeout: Duration,
) -> Result<DfsMetadataJournalBatchResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        sinomo_network::DFS_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_dfs_protocol_frame_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &DfsProtocolFrameCommand,
    timeout: Duration,
) -> Result<DfsProtocolFrameResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::DFS_PROTOCOL_FRAME_ROUTE,
        sinomo_network::DFS_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
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

pub fn fetch_cluster_kv_snapshot_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<KvSnapshot, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_KV_SNAPSHOT_ROUTE,
        sinomo_network::CLUSTER_KV_RPC_CATEGORY,
        &(),
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_cluster_kv_txn(
    endpoint: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
) -> Result<KvTxnResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_KV_TXN_ROUTE,
        sinomo_network::CLUSTER_KV_RPC_CATEGORY,
        txn,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
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
    send_cluster_join_registration_single_connection(address, request, timeout)
}

fn send_cluster_join_registration_single_connection(
    address: &str,
    request: &ClusterJoinRegistrationRequest,
    timeout: Duration,
) -> Result<ClusterJoinRegistrationResponse, DaemonError> {
    let mut stream = connect_daemon_control_stream(address, timeout)?;
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
    let challenge_control = DaemonControlRequest::rpc(
        daemon_rpc_command(sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE),
        challenge_rpc,
    );
    sinomo_network::write_object_frame_blocking(&mut stream, &challenge_control)?;
    let proof_response = read_join_rpc_response(
        &mut stream,
        address,
        sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE,
    )?;
    let proof: ClusterInviteChallengeProof = proof_response.decode_body()?;
    verify_cluster_invite_challenge_proof(&request.invite, &challenge, &proof)?;
    let register_rpc = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    let signed = sign_daemon_rpc_request_for_local_node(&register_rpc)?;
    let register_control = DaemonControlRequest::authenticated_rpc(
        daemon_rpc_command(sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE),
        register_rpc,
        signed,
    );
    sinomo_network::write_object_frame_blocking(&mut stream, &register_control)?;
    stream.shutdown_write();
    let response = read_join_rpc_response(
        &mut stream,
        address,
        sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE,
    )?;
    response.decode_body().map_err(DaemonError::from)
}

fn connect_daemon_control_stream(
    address: &str,
    timeout: Duration,
) -> Result<DaemonControlStream, DaemonError> {
    let socket_address = address
        .to_socket_addrs()
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: format!("failed to resolve join introducer: {source}"),
        })?
        .next()
        .ok_or_else(|| DaemonError::Network {
            address: address.to_string(),
            detail: "failed to resolve join introducer: no socket addresses".to_string(),
        })?;
    let stream = TcpStream::connect_timeout(&socket_address, timeout).map_err(|source| {
        DaemonError::Network {
            address: address.to_string(),
            detail: format!("failed to connect join introducer: {source}"),
        }
    })?;
    let stream = DaemonControlStream::new(stream);
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    Ok(stream)
}

fn read_join_rpc_response(
    stream: &mut DaemonControlStream,
    address: &str,
    route: &str,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let response: DaemonControlResponse = sinomo_network::read_object_frame_blocking(
        stream,
        sinomo_network::IROH_DAEMON_COMMAND_MAX_BYTES,
    )
    .map_err(|source| DaemonError::Network {
        address: address.to_string(),
        detail: format!(
            "join RPC `{route}` closed before a response: {source}. The introducer daemon may have rejected the invite or join signature before it could return a structured response; check the introducer daemon log for the matching client error."
        ),
    })?;
    response.rpc.ok_or_else(|| DaemonError::Network {
        address: address.to_string(),
        detail: format!("join RPC `{route}` returned a non-rpc response"),
    })
}

pub fn send_cluster_join_registration_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
    timeout: Duration,
) -> Result<ClusterJoinRegistrationResponse, DaemonError> {
    let deadline = Instant::now() + timeout;
    let remaining = || {
        deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| Duration::from_millis(1))
    };
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
        remaining(),
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
        remaining(),
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

pub fn send_daemon_rollout_binary_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::ROLLOUT_BINARY_ROUTE,
        sinomo_network::ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
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

pub fn send_daemon_rollout_build_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBuildRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::ROLLOUT_BUILD_ROUTE,
        sinomo_network::ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
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

#[cfg(test)]
mod tests {
    use super::{DaemonRpcAddressTransport, daemon_rpc_address_transport};

    #[test]
    fn daemon_rpc_address_transport_detects_iroh_urls() {
        assert_eq!(
            daemon_rpc_address_transport("iroh://public-key?relay=https://relay.example./"),
            DaemonRpcAddressTransport::Iroh
        );
        assert_eq!(
            daemon_rpc_address_transport("  iroh://public-key"),
            DaemonRpcAddressTransport::Iroh
        );
        assert_eq!(
            daemon_rpc_address_transport("127.0.0.1:21353"),
            DaemonRpcAddressTransport::Tcp
        );
    }
}
