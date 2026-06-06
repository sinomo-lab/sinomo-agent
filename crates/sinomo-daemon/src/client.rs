use crate::auth::CLUSTER_INVITE_CHALLENGE_PROTOCOL;
use crate::{
    ClusterHealthRow, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DaemonDfsSmokeReport, DaemonError,
    DaemonStatus, daemon_rpc_command, sign_daemon_rpc_request_for_local_node,
    verify_cluster_invite_challenge_proof,
};
use sinomo_core::{ClusterEndpointRecord, ClusterTransportKind};
use sinomo_dfs::{
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse, DfsOperatorStatus,
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
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse,
    ClusterRolloutBinaryUploadChunkRequest, ClusterRolloutBinaryUploadChunkResponse,
    ClusterRolloutBinaryUploadFinalizeRequest, ClusterRolloutBuildRequest, ROLLOUT_BINARY_ROUTE,
    ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE, ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE, ROLLOUT_BUILD_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_FINALIZE_SHADOW_ROUTE,
    ROLLOUT_RPC_CATEGORY,
};
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const MAX_DAEMON_CONTROL_DNS_WORKERS: usize = 4;
static DAEMON_CONTROL_DNS_WORKERS: AtomicUsize = AtomicUsize::new(0);

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
    let deadline = DaemonRpcAttemptDeadline::new(timeout);
    let mut stream = connect_daemon_control_stream(address, &deadline, "daemon control address")?;
    configure_daemon_control_stream_deadline(&stream, &deadline, false, true)?;
    sinomo_network::write_object_frame_blocking(&mut stream, &control_request)
        .map_err(DaemonError::from)?;
    stream.flush().map_err(|source| DaemonError::Network {
        address: address.to_string(),
        detail: source.to_string(),
    })?;
    stream.shutdown_write();
    stream.set_read_timeout(Some(timeout))?;
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
    let command = daemon_rpc_command(&request.route);
    let deadline = DaemonRpcAttemptDeadline::new(timeout);
    if let Some(response) =
        route_endpoint_request_via_local_daemon(endpoint, request, &signed, &command, &deadline)
    {
        return response;
    }
    // If we have no healthy direct edge to this endpoint but DO have a healthy relay route
    // (e.g. a cross-segment voter reachable only through a bridge), skip the direct dial -- it
    // would otherwise burn the entire timeout on a connect that cannot succeed, often past the
    // caller's patience -- and relay straight away. Unprobed peers (no route known) still try a
    // direct dial first.
    if !crate::cluster::cluster_endpoint_prefers_relay(endpoint) {
        let direct_timeout = crate::cluster::cluster_endpoint_fair_command_timeout(
            endpoint,
            deadline.remaining(),
            1,
        );
        match sinomo_network::send_daemon_rpc_to_cluster_endpoint(
            endpoint,
            &command,
            Some(&signed),
            request,
            direct_timeout,
        ) {
            Ok(response) => return Ok(response),
            Err(direct_error) => {
                // Direct dial failed. If the connectivity graph offers a multi-hop route to the
                // target node through a reachable bridge, relay the (already signed) request one
                // hop closer; otherwise surface the original error.
                return match crate::cluster::relay_request_to_endpoint(
                    endpoint,
                    request,
                    &signed,
                    &command,
                    deadline.remaining(),
                ) {
                    Some(result) => result,
                    None => Err(DaemonError::from(direct_error)),
                };
            }
        }
    }
    match crate::cluster::relay_request_to_endpoint(
        endpoint,
        request,
        &signed,
        &command,
        deadline.remaining(),
    ) {
        Some(result) => result,
        None => Err(DaemonError::Network {
            address: endpoint.address.clone(),
            detail: format!(
                "endpoint {} is unreachable and no relay route is available",
                endpoint.node_id
            ),
        }),
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DaemonRpcAttemptDeadline {
    started: Instant,
    timeout: Duration,
}

impl DaemonRpcAttemptDeadline {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.timeout
            .checked_sub(self.started.elapsed())
            .unwrap_or_else(|| Duration::from_millis(1))
    }
}

fn route_endpoint_request_via_local_daemon(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: &[u8],
    request_command: &str,
    deadline: &DaemonRpcAttemptDeadline,
) -> Option<Result<sinomo_network::ObjectRpcResponse, DaemonError>> {
    if request.route == sinomo_network::CLUSTER_RELAY_ROUTE {
        return None;
    }
    let status = match crate::default_main_daemon_status(deadline.remaining()) {
        Ok(Some(status)) => status,
        Ok(None) | Err(_) => return None,
    };
    if !local_daemon_can_gateway_endpoint(
        status.pid,
        status.cluster.as_ref(),
        endpoint,
        std::process::id(),
    ) {
        return None;
    }

    let cluster = status.cluster.as_ref()?;
    let inner = DaemonControlRequest::authenticated_rpc(
        request_command.to_string(),
        request.clone(),
        request_auth.to_vec(),
    );
    let outer = match sinomo_network::RelayEnvelope::new(
        cluster.cluster_id.clone(),
        endpoint.node_id.clone(),
        inner,
    )
    .into_request()
    {
        Ok(outer) => outer,
        Err(error) => return Some(Err(DaemonError::from(error))),
    };
    match send_daemon_rpc(&status.address, &outer, deadline.remaining()) {
        Ok(response) if is_relay_routing_error(&response) => None,
        Ok(response) => Some(Ok(response)),
        Err(_) => None,
    }
}

fn local_daemon_can_gateway_endpoint(
    daemon_pid: u32,
    daemon_cluster: Option<&crate::DaemonClusterStatus>,
    endpoint: &ClusterEndpointRecord,
    current_pid: u32,
) -> bool {
    daemon_pid != current_pid
        && daemon_cluster.is_some_and(|cluster| {
            endpoint.node_id != cluster.node_id
                && endpoint.node_id != sinomo_node::shadow_daemon_node_id(&cluster.node_id)
        })
}

fn is_relay_routing_error(response: &sinomo_network::ObjectRpcResponse) -> bool {
    response.route == sinomo_network::CLUSTER_RELAY_ROUTE && response.error_detail().is_err()
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
        sinomo_dfs::DFS_STATUS_ROUTE,
        sinomo_dfs::DFS_RPC_CATEGORY,
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
        sinomo_dfs::DFS_INSPECTION_ROUTE,
        sinomo_dfs::DFS_RPC_CATEGORY,
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
        sinomo_dfs::DFS_SMOKE_ROUTE,
        sinomo_dfs::DFS_RPC_CATEGORY,
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
        sinomo_dfs::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        sinomo_dfs::DFS_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_dfs_metadata_snapshot_install_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &DfsMetadataSnapshotInstallCommand,
    timeout: Duration,
) -> Result<DfsMetadataSnapshotInstallResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_dfs::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
        sinomo_dfs::DFS_RPC_CATEGORY,
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
        sinomo_dfs::DFS_PROTOCOL_FRAME_ROUTE,
        sinomo_dfs::DFS_RPC_CATEGORY,
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

pub fn fetch_cluster_health_rows(
    address: &str,
    timeout: Duration,
) -> Result<Vec<ClusterHealthRow>, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_HEALTH_ROUTE,
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
    let deadline = DaemonRpcAttemptDeadline::new(timeout);
    let mut stream = connect_daemon_control_stream(address, &deadline, "join introducer")?;
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
    configure_daemon_control_stream_deadline(&stream, &deadline, false, true)?;
    sinomo_network::write_object_frame_blocking(&mut stream, &challenge_control)?;
    let proof_response = read_join_rpc_response(
        &mut stream,
        address,
        sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE,
        &deadline,
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
    configure_daemon_control_stream_deadline(&stream, &deadline, false, true)?;
    sinomo_network::write_object_frame_blocking(&mut stream, &register_control)?;
    let response = read_join_rpc_response(
        &mut stream,
        address,
        sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE,
        &deadline,
    )?;
    response.decode_body().map_err(DaemonError::from)
}

fn connect_daemon_control_stream(
    address: &str,
    deadline: &DaemonRpcAttemptDeadline,
    resolution_target: &'static str,
) -> Result<DaemonControlStream, DaemonError> {
    let socket_address =
        resolve_daemon_control_socket_address(address, deadline, resolution_target)?;
    let stream =
        TcpStream::connect_timeout(&socket_address, deadline.remaining()).map_err(|source| {
            DaemonError::Network {
                address: address.to_string(),
                detail: format!("failed to connect {resolution_target}: {source}"),
            }
        })?;
    let stream = DaemonControlStream::new(stream);
    configure_daemon_control_stream_deadline(&stream, deadline, true, true)?;
    Ok(stream)
}

struct DaemonControlDnsWorkerPermit;

impl Drop for DaemonControlDnsWorkerPermit {
    fn drop(&mut self) {
        DAEMON_CONTROL_DNS_WORKERS.fetch_sub(1, Ordering::AcqRel);
    }
}

fn try_acquire_daemon_control_dns_worker() -> Option<DaemonControlDnsWorkerPermit> {
    let mut current = DAEMON_CONTROL_DNS_WORKERS.load(Ordering::Acquire);
    loop {
        if current >= MAX_DAEMON_CONTROL_DNS_WORKERS {
            return None;
        }
        match DAEMON_CONTROL_DNS_WORKERS.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Some(DaemonControlDnsWorkerPermit),
            Err(actual) => current = actual,
        }
    }
}

#[cfg(test)]
fn daemon_control_dns_worker_count_for_test() -> usize {
    DAEMON_CONTROL_DNS_WORKERS.load(Ordering::Acquire)
}

fn resolve_daemon_control_socket_address(
    address: &str,
    deadline: &DaemonRpcAttemptDeadline,
    resolution_target: &'static str,
) -> Result<SocketAddr, DaemonError> {
    if let Ok(socket_address) = address.parse::<SocketAddr>() {
        return Ok(socket_address);
    }
    let Some(permit) = try_acquire_daemon_control_dns_worker() else {
        return Err(DaemonError::Network {
            address: address.to_string(),
            detail: format!(
                "{resolution_target} DNS worker limit reached ({MAX_DAEMON_CONTROL_DNS_WORKERS})"
            ),
        });
    };

    let address = address.to_string();
    let worker_address = address.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _permit = permit;
        let resolved = std::net::ToSocketAddrs::to_socket_addrs(&worker_address)
            .map_err(|source| format!("failed to resolve {resolution_target}: {source}"))
            .and_then(|mut addresses| {
                addresses.next().ok_or_else(|| {
                    format!("failed to resolve {resolution_target}: no socket addresses")
                })
            });
        let _ = sender.send(resolved);
    });

    match receiver.recv_timeout(deadline.remaining()) {
        Ok(Ok(socket_address)) => Ok(socket_address),
        Ok(Err(detail)) => Err(DaemonError::Network { address, detail }),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(DaemonError::Network {
            address,
            detail: format!(
                "failed to resolve {resolution_target} before deadline: timed out after {}ms",
                deadline.timeout.as_millis()
            ),
        }),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(DaemonError::Network {
            address,
            detail: format!(
                "{resolution_target} resolver worker disconnected before returning an address"
            ),
        }),
    }
}

fn configure_daemon_control_stream_deadline(
    stream: &DaemonControlStream,
    deadline: &DaemonRpcAttemptDeadline,
    read: bool,
    write: bool,
) -> Result<Duration, DaemonError> {
    let timeout = deadline.remaining();
    if read {
        stream.set_read_timeout(Some(timeout))?;
    }
    if write {
        stream.set_write_timeout(Some(timeout))?;
    }
    Ok(timeout)
}

fn read_join_rpc_response(
    stream: &mut DaemonControlStream,
    address: &str,
    route: &str,
    deadline: &DaemonRpcAttemptDeadline,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    configure_daemon_control_stream_deadline(stream, deadline, true, false)?;
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
    let deadline = DaemonRpcAttemptDeadline::new(timeout);
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
        deadline.remaining(),
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
        deadline.remaining(),
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
    let rpc_request =
        ObjectRpcRequest::encode_body(ROLLOUT_BINARY_ROUTE, ROLLOUT_RPC_CATEGORY, request)?;
    let response = send_daemon_rpc(address, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_binary_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let rpc_request =
        ObjectRpcRequest::encode_body(ROLLOUT_BINARY_ROUTE, ROLLOUT_RPC_CATEGORY, request)?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_binary_upload_chunk(
    address: &str,
    request: &ClusterRolloutBinaryUploadChunkRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryUploadChunkResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc(address, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_binary_upload_chunk_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryUploadChunkRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryUploadChunkResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_binary_upload_finalize(
    address: &str,
    request: &ClusterRolloutBinaryUploadFinalizeRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
    )?;
    let response = send_daemon_rpc(address, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_binary_upload_finalize_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryUploadFinalizeRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
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
    let rpc_request =
        ObjectRpcRequest::encode_body(ROLLOUT_BUILD_ROUTE, ROLLOUT_RPC_CATEGORY, request)?;
    let response = send_daemon_rpc(address, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_build_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBuildRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let rpc_request =
        ObjectRpcRequest::encode_body(ROLLOUT_BUILD_ROUTE, ROLLOUT_RPC_CATEGORY, request)?;
    let response = send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_finalize_shadow_rollout_request(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let request =
        ObjectRpcRequest::encode_body(ROLLOUT_FINALIZE_SHADOW_ROUTE, ROLLOUT_RPC_CATEGORY, &())?;
    let response = send_daemon_rpc(address, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_clear_status(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let request =
        ObjectRpcRequest::encode_body(ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_RPC_CATEGORY, &())?;
    let response = send_daemon_rpc(address, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

pub fn send_daemon_rollout_diagnostics(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let request =
        ObjectRpcRequest::encode_body(ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_RPC_CATEGORY, &())?;
    let response = send_daemon_rpc(address, &request, timeout)?;
    response.decode_body().map_err(DaemonError::from)
}

#[cfg(test)]
mod tests {
    use super::{
        DaemonRpcAddressTransport, DaemonRpcAttemptDeadline, daemon_rpc_address_transport,
        is_relay_routing_error, local_daemon_can_gateway_endpoint,
    };
    use crate::DaemonClusterStatus;
    use sinomo_core::{
        ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterMemberRole,
        ClusterMemberState, ClusterTransportKind,
    };
    use sinomo_network::{ClusterId, DaemonControlStream, ObjectRpcResponse};
    use sinomo_node::NodeId;
    use sinomo_rollout::ROLLOUT_BUILD_ROUTE;
    use std::time::{Duration, Instant};

    fn node_id(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn endpoint(node_id: NodeId) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: sinomo_core::Id::generate(),
            node_id,
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.2:21353".to_string(),
            source: ClusterEndpointSource::KvMembership,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        }
    }

    fn cluster_status(node_id: NodeId) -> DaemonClusterStatus {
        DaemonClusterStatus {
            cluster_id: ClusterId::from_legacy_named("cluster_gateway_test").expect("cluster id"),
            node_id,
            process: "daemon-main".to_string(),
            member_role: ClusterMemberRole::Voter,
            member_state: ClusterMemberState::Voter,
        }
    }

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

    #[test]
    fn local_daemon_gateway_only_handles_remote_endpoints_from_client_processes() {
        let local = node_id("node_gateway_local");
        let local_shadow = sinomo_node::shadow_daemon_node_id(&local);
        let remote = node_id("node_gateway_remote");
        let cluster = cluster_status(local.clone());
        assert!(local_daemon_can_gateway_endpoint(
            42,
            Some(&cluster),
            &endpoint(remote),
            7
        ));
        assert!(!local_daemon_can_gateway_endpoint(
            42,
            Some(&cluster),
            &endpoint(local),
            7
        ));
        assert!(!local_daemon_can_gateway_endpoint(
            42,
            Some(&cluster),
            &endpoint(local_shadow),
            7
        ));
        assert!(!local_daemon_can_gateway_endpoint(
            42,
            Some(&cluster),
            &endpoint(node_id("node_gateway_other")),
            42
        ));
        assert!(!local_daemon_can_gateway_endpoint(
            42,
            None,
            &endpoint(node_id("node_gateway_other")),
            7
        ));
    }

    #[test]
    fn relay_routing_errors_are_gateway_fallbacks() {
        let relay_error =
            ObjectRpcResponse::encode_error(sinomo_network::CLUSTER_RELAY_ROUTE, "no route")
                .expect("relay error");
        assert!(is_relay_routing_error(&relay_error));

        let target_error = ObjectRpcResponse::encode_error(ROLLOUT_BUILD_ROUTE, "build failed")
            .expect("target error");
        assert!(!is_relay_routing_error(&target_error));
    }

    #[test]
    fn daemon_rpc_attempt_deadline_shrinks_fallback_budget() {
        let deadline = DaemonRpcAttemptDeadline {
            started: Instant::now() - Duration::from_millis(40),
            timeout: Duration::from_millis(100),
        };

        let remaining = deadline.remaining();

        assert!(
            remaining < Duration::from_millis(100),
            "fallback attempts must not receive the original timeout"
        );
        assert!(
            remaining <= Duration::from_millis(80),
            "expected elapsed time to reduce the remaining budget, got {remaining:?}"
        );
    }

    #[test]
    fn daemon_rpc_attempt_deadline_bounds_elapsed_budget_to_short_tail() {
        let deadline = DaemonRpcAttemptDeadline {
            started: Instant::now() - Duration::from_millis(100),
            timeout: Duration::from_millis(10),
        };

        assert_eq!(deadline.remaining(), Duration::from_millis(1));
    }

    #[test]
    fn join_rpc_deadline_refreshes_blocking_stream_timeouts() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("local address");
        let accepted = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept");
            std::thread::sleep(Duration::from_millis(250));
        });
        let stream =
            DaemonControlStream::new(std::net::TcpStream::connect(address).expect("connect"));
        let deadline = DaemonRpcAttemptDeadline::new(Duration::from_millis(500));

        let initial =
            super::configure_daemon_control_stream_deadline(&stream, &deadline, true, true)
                .expect("initial timeout configuration");
        let initial_read_timeout = stream
            .read_timeout()
            .expect("read timeout")
            .expect("read timeout set");
        let initial_write_timeout = stream
            .write_timeout()
            .expect("write timeout")
            .expect("write timeout set");

        std::thread::sleep(Duration::from_millis(100));
        let refreshed =
            super::configure_daemon_control_stream_deadline(&stream, &deadline, true, false)
                .expect("refreshed timeout configuration");
        let refreshed_read_timeout = stream
            .read_timeout()
            .expect("read timeout")
            .expect("read timeout set");

        assert!(
            refreshed < initial,
            "join RPC phases must share elapsed budget instead of reusing the original timeout"
        );
        assert!(
            refreshed_read_timeout < initial_read_timeout,
            "blocking join RPC reads must use the remaining join budget"
        );
        assert_eq!(
            stream
                .write_timeout()
                .expect("write timeout")
                .expect("write timeout set"),
            initial_write_timeout,
            "refreshing the read phase must not lengthen the write timeout"
        );

        drop(stream);
        accepted.join().expect("accepted thread");
    }

    #[test]
    fn daemon_control_resolver_uses_socket_addresses_and_bounds_dns_workers() {
        let deadline = DaemonRpcAttemptDeadline::new(Duration::from_millis(10));
        let mut permits = Vec::new();
        for _ in 0..super::MAX_DAEMON_CONTROL_DNS_WORKERS {
            permits.push(
                super::try_acquire_daemon_control_dns_worker()
                    .expect("reserve daemon control DNS worker"),
            );
        }

        let resolved = super::resolve_daemon_control_socket_address(
            "127.0.0.1:21353",
            &deadline,
            "join introducer",
        )
        .expect("socket address resolves without DNS");

        assert_eq!(
            resolved,
            "127.0.0.1:21353"
                .parse::<std::net::SocketAddr>()
                .expect("socket address")
        );
        assert_eq!(
            super::daemon_control_dns_worker_count_for_test(),
            super::MAX_DAEMON_CONTROL_DNS_WORKERS,
            "already-normalized daemon control addresses must not consume DNS workers"
        );

        let error = super::resolve_daemon_control_socket_address(
            "localhost:21353",
            &deadline,
            "join introducer",
        )
        .expect_err("dns worker saturation");

        match error {
            crate::DaemonError::Network { address, detail } => {
                assert_eq!(address, "localhost:21353");
                assert_eq!(
                    detail,
                    format!(
                        "join introducer DNS worker limit reached ({})",
                        super::MAX_DAEMON_CONTROL_DNS_WORKERS
                    )
                );
            }
            error => panic!("unexpected error: {error}"),
        }

        let error = super::connect_daemon_control_stream(
            "localhost:21353",
            &deadline,
            "daemon control address",
        )
        .expect_err("stream setup must reject saturated DNS before connect");
        match error {
            crate::DaemonError::Network { address, detail } => {
                assert_eq!(address, "localhost:21353");
                assert_eq!(
                    detail,
                    format!(
                        "daemon control address DNS worker limit reached ({})",
                        super::MAX_DAEMON_CONTROL_DNS_WORKERS
                    )
                );
            }
            error => panic!("unexpected error: {error}"),
        }

        drop(permits);
        assert_eq!(super::daemon_control_dns_worker_count_for_test(), 0);
    }
}
