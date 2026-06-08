use crate::{
    DaemonError, DaemonStatus, daemon_rpc_command, sign_daemon_rpc_request_for_local_node,
};
use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE,
    ConfigCatalogRequest, ConfigCatalogResponse, ConfigClient, ConfigDefinition, ConfigError,
    ConfigGetRequest, ConfigGetResponse, ConfigResolution, ConfigSetRequest, ConfigSetResponse,
    ConfigWriteResult, ConfigWriteTarget,
};
use sinomo_core::ClusterEndpointRecord;
use sinomo_dfs::{
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse, DfsProtocolFrameCommand,
    DfsProtocolFrameResponse,
};
use sinomo_kv::{
    ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, KvSnapshot, KvTxn, KvTxnResponse,
};
use sinomo_network::{
    ClusterMemberPublishRequest, ClusterMemberPublishResponse, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse, ClusterRaftVoteRequest, ClusterRaftVoteResponse,
    DaemonControlRequest, DaemonControlStream, ObjectRpcRequest,
};
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse,
    ClusterRolloutBinaryUploadChunkRequest, ClusterRolloutBinaryUploadChunkResponse,
    ClusterRolloutBinaryUploadFinalizeRequest, ClusterRolloutBuildRequest, ROLLOUT_BINARY_ROUTE,
    ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE, ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE, ROLLOUT_BUILD_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_RPC_CATEGORY,
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

/// A [`ConfigClient`] backed by RPC to a running daemon's `/config/*` service over the daemon
/// control protocol. The daemon resolves each request with its own cluster access, so a remote
/// caller (e.g. the desktop app) sees exactly the same configuration as the daemon and CLI —
/// keeping every component on one source of truth.
#[derive(Clone)]
pub struct RemoteConfigClient {
    address: String,
    timeout: Duration,
}

impl RemoteConfigClient {
    pub fn new(address: impl Into<String>, timeout: Duration) -> Self {
        Self {
            address: address.into(),
            timeout,
        }
    }

    fn rpc<Req, Resp>(&self, route: &str, body: &Req) -> Result<Resp, ConfigError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let client_error = |detail: String| ConfigError::Client { detail };
        let request = ObjectRpcRequest::encode_body(route, CONFIG_RPC_CATEGORY, body)
            .map_err(|error| client_error(error.to_string()))?;
        let response = send_daemon_rpc(&self.address, &request, self.timeout)
            .map_err(|error| client_error(error.to_string()))?;
        response
            .decode_body()
            .map_err(|error| client_error(error.to_string()))
    }
}

impl ConfigClient for RemoteConfigClient {
    fn catalog(&self, owner: Option<&str>) -> Result<Vec<ConfigDefinition>, ConfigError> {
        let response: ConfigCatalogResponse = self.rpc(
            CONFIG_CATALOG_ROUTE,
            &ConfigCatalogRequest {
                owner: owner.map(str::to_string),
            },
        )?;
        Ok(response.definitions)
    }

    fn get(&self, key: &str) -> Result<ConfigResolution, ConfigError> {
        let response: ConfigGetResponse = self.rpc(
            CONFIG_GET_ROUTE,
            &ConfigGetRequest {
                key: key.to_string(),
            },
        )?;
        Ok(response.resolution)
    }

    fn set(
        &self,
        key: &str,
        value: serde_json::Value,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let response: ConfigSetResponse = self.rpc(
            CONFIG_SET_ROUTE,
            &ConfigSetRequest {
                key: key.to_string(),
                value,
                target,
            },
        )?;
        Ok(response.result)
    }
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
    send_daemon_rpc_to_endpoint_with_timeout_policy(
        endpoint,
        request,
        timeout,
        DaemonEndpointRpcTimeoutPolicy::FairControlPlane,
    )
}

fn send_daemon_large_rpc_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    send_daemon_rpc_to_endpoint_with_timeout_policy(
        endpoint,
        request,
        timeout,
        DaemonEndpointRpcTimeoutPolicy::FullRequestBudget,
    )
}

fn send_daemon_rpc_to_endpoint_with_timeout_policy(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    timeout_policy: DaemonEndpointRpcTimeoutPolicy,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    let command = daemon_rpc_command(&request.route);
    let mut host = DaemonSignedEndpointRpcHost::new(timeout_policy);
    sinomo_network::send_signed_rpc_to_endpoint_with_host(
        endpoint,
        request,
        &signed,
        &command,
        &daemon_rpc_command(sinomo_network::CLUSTER_RELAY_ROUTE),
        sinomo_network::ClusterSignedEndpointRpcOptions { timeout },
        &mut host,
    )
    .map_err(DaemonError::from)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonEndpointRpcTimeoutPolicy {
    FairControlPlane,
    FullRequestBudget,
}

struct DaemonSignedEndpointRpcHost {
    timeout_policy: DaemonEndpointRpcTimeoutPolicy,
}

impl DaemonSignedEndpointRpcHost {
    fn new(timeout_policy: DaemonEndpointRpcTimeoutPolicy) -> Self {
        Self { timeout_policy }
    }

    fn endpoint_timeout(&self, endpoint: &ClusterEndpointRecord, remaining: Duration) -> Duration {
        match self.timeout_policy {
            DaemonEndpointRpcTimeoutPolicy::FairControlPlane => {
                crate::cluster::cluster_voter_reachability_options()
                    .fair_command_timeout(endpoint, remaining)
            }
            DaemonEndpointRpcTimeoutPolicy::FullRequestBudget => remaining,
        }
    }
}

fn send_daemon_rpc_body<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    body: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, body)?;
    send_daemon_rpc_request_with_decoder(address, &request, timeout, |response| {
        response.decode_body()
    })
}

fn send_daemon_rpc_body_to_endpoint<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    body: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    send_daemon_rpc_body_to_endpoint_with_timeout_policy(
        endpoint,
        route,
        category,
        body,
        timeout,
        DaemonEndpointRpcTimeoutPolicy::FairControlPlane,
    )
}

fn send_daemon_large_rpc_body_to_endpoint<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    body: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    send_daemon_rpc_body_to_endpoint_with_timeout_policy(
        endpoint,
        route,
        category,
        body,
        timeout,
        DaemonEndpointRpcTimeoutPolicy::FullRequestBudget,
    )
}

fn send_daemon_rpc_body_to_endpoint_with_timeout_policy<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    body: &Req,
    timeout: Duration,
    timeout_policy: DaemonEndpointRpcTimeoutPolicy,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, body)?;
    send_daemon_rpc_request_to_endpoint_with_decoder_with_timeout_policy(
        endpoint,
        &request,
        timeout,
        timeout_policy,
        |response| response.decode_body(),
    )
}

fn send_daemon_rpc_request_with_decoder<Resp, DecodeError>(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
    decode: impl FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
) -> Result<Resp, DaemonError>
where
    DecodeError: Into<DaemonError>,
{
    let response = send_daemon_rpc(address, request, timeout)?;
    decode(&response).map_err(Into::into)
}

fn send_daemon_rpc_request_to_endpoint_with_decoder<Resp, DecodeError>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    decode: impl FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
) -> Result<Resp, DaemonError>
where
    DecodeError: Into<DaemonError>,
{
    send_daemon_rpc_request_to_endpoint_with_decoder_with_timeout_policy(
        endpoint,
        request,
        timeout,
        DaemonEndpointRpcTimeoutPolicy::FairControlPlane,
        decode,
    )
}

fn send_daemon_rpc_request_to_endpoint_with_decoder_with_timeout_policy<Resp, DecodeError>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    timeout_policy: DaemonEndpointRpcTimeoutPolicy,
    decode: impl FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
) -> Result<Resp, DaemonError>
where
    DecodeError: Into<DaemonError>,
{
    let response = match timeout_policy {
        DaemonEndpointRpcTimeoutPolicy::FairControlPlane => {
            send_daemon_rpc_to_endpoint(endpoint, request, timeout)?
        }
        DaemonEndpointRpcTimeoutPolicy::FullRequestBudget => {
            send_daemon_large_rpc_to_endpoint(endpoint, request, timeout)?
        }
    };
    decode(&response).map_err(Into::into)
}

impl sinomo_network::ClusterSignedEndpointRpcHost for DaemonSignedEndpointRpcHost {
    fn try_gateway_signed_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        request_auth: &[u8],
        request_command: &str,
        timeout: Duration,
    ) -> Result<Option<sinomo_network::ObjectRpcResponse>, sinomo_network::NetworkError> {
        let deadline = DaemonRpcAttemptDeadline::new(timeout);
        match route_endpoint_request_via_local_daemon(
            endpoint,
            request,
            request_auth,
            request_command,
            &deadline,
        ) {
            Some(Ok(response)) => Ok(Some(response)),
            Some(Err(error)) => Err(daemon_client_network_error(
                "cluster endpoint gateway",
                error,
            )),
            None => Ok(None),
        }
    }

    fn endpoint_prefers_relay(
        &mut self,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<bool, sinomo_network::NetworkError> {
        Ok(cluster_paths_from_env()
            .as_ref()
            .is_some_and(|paths| sinomo_network::cluster_endpoint_prefers_relay(paths, endpoint)))
    }

    fn direct_endpoint_timeout(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        self.endpoint_timeout(endpoint, remaining)
    }

    fn send_signed_endpoint_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        command: &str,
        auth: &[u8],
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
        sinomo_network::send_daemon_rpc_to_cluster_endpoint(
            endpoint,
            command,
            Some(auth),
            request,
            timeout,
        )
    }

    fn relay_signed_endpoint_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        request_auth: &[u8],
        request_command: &str,
        relay_command: &str,
        timeout: Duration,
    ) -> Result<Option<sinomo_network::ObjectRpcResponse>, sinomo_network::NetworkError> {
        let Some(paths) = cluster_paths_from_env() else {
            return Ok(None);
        };
        sinomo_network::relay_rpc_to_endpoint_from_paths_with_host(
            &paths,
            endpoint,
            request,
            request_auth.to_vec(),
            request_command.to_string(),
            relay_command,
            sinomo_network::ClusterSignedRelayRpcOptions { timeout },
            self,
        )
    }
}

impl sinomo_network::ClusterSignedRelayRpcHost for DaemonSignedEndpointRpcHost {
    fn sign_relay_rpc_request(
        &mut self,
        request: &sinomo_network::ObjectRpcRequest,
    ) -> Result<Vec<u8>, sinomo_network::NetworkError> {
        sign_daemon_rpc_request_for_local_node(request)
            .map_err(|error| daemon_client_network_error("cluster relay signing", error))
    }

    fn send_signed_relay_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        command: &str,
        auth: &[u8],
        request: &sinomo_network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
        let timeout = self.endpoint_timeout(endpoint, timeout);
        sinomo_network::send_daemon_rpc_to_cluster_endpoint(
            endpoint,
            command,
            Some(auth),
            request,
            timeout,
        )
    }
}

fn cluster_paths_from_env() -> Option<sinomo_network::ClusterPaths> {
    let data_dir = sinomo_local::SinomoDataDir::from_env_or_home().ok()?;
    Some(sinomo_network::ClusterPaths::new(&data_dir))
}

fn daemon_client_network_error(
    operation: &str,
    error: DaemonError,
) -> sinomo_network::NetworkError {
    match error {
        DaemonError::Network { address, detail } => sinomo_network::NetworkError::Io {
            operation: format!("{operation} {address}"),
            detail,
        },
        other => sinomo_network::NetworkError::Io {
            operation: operation.to_string(),
            detail: other.to_string(),
        },
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

    pub(crate) fn checked_remaining(&self) -> Option<Duration> {
        self.timeout
            .checked_sub(self.started.elapsed())
            .filter(|remaining| !remaining.is_zero())
    }

    #[cfg(test)]
    pub(crate) fn timeout_error(&self, address: &str) -> DaemonError {
        DaemonError::Network {
            address: address.to_string(),
            detail: format!("timed out after {}ms", self.timeout.as_millis()),
        }
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
    let status = match crate::default_main_daemon_status(deadline.checked_remaining()?) {
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
    let outer = match sinomo_network::relay_request_for_signed_rpc(
        &cluster.cluster_id,
        &endpoint.node_id,
        request,
        request_auth.to_vec(),
        request_command.to_string(),
        std::iter::empty::<sinomo_node::NodeId>(),
    ) {
        Ok(outer) => outer,
        Err(error) => return Some(Err(DaemonError::from(error))),
    };
    match send_daemon_rpc(&status.address, &outer, deadline.checked_remaining()?) {
        Ok(response) if sinomo_network::relay_response_is_routing_error(&response) => None,
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

pub fn fetch_daemon_status_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    send_daemon_rpc_body_to_endpoint(
        endpoint,
        sinomo_network::DAEMON_STATUS_ROUTE,
        sinomo_network::DAEMON_RPC_CATEGORY,
        &(),
        timeout,
    )
}

pub(crate) fn send_dfs_metadata_journal_batch_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &DfsMetadataJournalBatchCommand,
    timeout: Duration,
) -> Result<DfsMetadataJournalBatchResponse, DaemonError> {
    let rpc_request = sinomo_dfs::dfs_metadata_journal_batch_rpc_request(request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &rpc_request, timeout, |response| {
        sinomo_dfs::decode_dfs_metadata_journal_batch_rpc_response(response)
    })
}

pub(crate) fn send_dfs_metadata_snapshot_install_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &DfsMetadataSnapshotInstallCommand,
    timeout: Duration,
) -> Result<DfsMetadataSnapshotInstallResponse, DaemonError> {
    let rpc_request = sinomo_dfs::dfs_metadata_snapshot_install_rpc_request(request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &rpc_request, timeout, |response| {
        sinomo_dfs::decode_dfs_metadata_snapshot_install_rpc_response(response)
    })
}

pub(crate) fn send_dfs_protocol_frame_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &DfsProtocolFrameCommand,
    timeout: Duration,
) -> Result<DfsProtocolFrameResponse, DaemonError> {
    let rpc_request = sinomo_dfs::dfs_protocol_frame_rpc_request(request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &rpc_request, timeout, |response| {
        sinomo_dfs::decode_dfs_protocol_frame_rpc_response(response)
    })
}

pub fn fetch_daemon_status_from_address(
    address: &str,
    timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    send_daemon_rpc_body(
        address,
        sinomo_network::DAEMON_STATUS_ROUTE,
        sinomo_network::DAEMON_RPC_CATEGORY,
        &(),
        timeout,
    )
}

pub fn send_daemon_shutdown_request(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rpc_body(
        address,
        sinomo_network::DAEMON_SHUTDOWN_ROUTE,
        sinomo_network::DAEMON_RPC_CATEGORY,
        &(),
        timeout,
    )
}

pub fn fetch_cluster_kv_snapshot(
    address: &str,
    timeout: Duration,
) -> Result<KvSnapshot, DaemonError> {
    let request = sinomo_network::cluster_kv_snapshot_rpc_request()?;
    send_daemon_rpc_request_with_decoder(address, &request, timeout, |response| {
        sinomo_network::decode_cluster_kv_snapshot_rpc_response(response)
    })
}

pub fn fetch_cluster_kv_snapshot_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<KvSnapshot, DaemonError> {
    let rpc_request = sinomo_network::cluster_kv_snapshot_rpc_request()?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &rpc_request, timeout, |response| {
        sinomo_network::decode_cluster_kv_snapshot_rpc_response(response)
    })
}

pub(crate) fn send_cluster_kv_txn(
    endpoint: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
) -> Result<KvTxnResponse, DaemonError> {
    let rpc_request = sinomo_network::cluster_kv_txn_rpc_request(txn)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &rpc_request, timeout, |response| {
        sinomo_network::decode_cluster_kv_txn_rpc_response(response)
    })
}

pub(crate) fn send_cluster_raft_append(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    let rpc_request = sinomo_network::cluster_raft_append_rpc_request(request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &rpc_request, timeout, |response| {
        sinomo_network::decode_cluster_raft_append_rpc_response(response)
    })
}

pub(crate) fn send_cluster_raft_vote(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftVoteRequest,
    timeout: Duration,
) -> Result<ClusterRaftVoteResponse, DaemonError> {
    let rpc_request = sinomo_network::cluster_raft_vote_rpc_request(request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &rpc_request, timeout, |response| {
        sinomo_network::decode_cluster_raft_vote_rpc_response(response)
    })
}

pub(crate) fn send_cluster_member_publish(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterMemberPublishRequest,
    timeout: Duration,
) -> Result<ClusterMemberPublishResponse, DaemonError> {
    send_daemon_rpc_body_to_endpoint(
        endpoint,
        sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
        timeout,
    )
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

pub(crate) fn send_cluster_kv_watch_events(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterKvWatchEventsRequest,
    timeout: Duration,
) -> Result<ClusterKvWatchEventsResponse, DaemonError> {
    if let Some(response) = sinomo_kv::cluster_kv_watch_events_noop_response(request) {
        return Ok(response);
    }
    let rpc_request = sinomo_network::cluster_kv_watch_events_rpc_request(request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &rpc_request, timeout, |response| {
        sinomo_network::decode_cluster_kv_watch_events_rpc_response(response)
    })
}

pub fn send_daemon_rollout_binary(
    address: &str,
    request: &ClusterRolloutBinaryRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body(
        address,
        ROLLOUT_BINARY_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_binary_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_large_rpc_body_to_endpoint(
        endpoint,
        ROLLOUT_BINARY_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_binary_upload_chunk(
    address: &str,
    request: &ClusterRolloutBinaryUploadChunkRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryUploadChunkResponse, DaemonError> {
    send_daemon_rpc_body(
        address,
        ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_binary_upload_chunk_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryUploadChunkRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryUploadChunkResponse, DaemonError> {
    send_daemon_large_rpc_body_to_endpoint(
        endpoint,
        ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_binary_upload_finalize(
    address: &str,
    request: &ClusterRolloutBinaryUploadFinalizeRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body(
        address,
        ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_binary_upload_finalize_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryUploadFinalizeRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_large_rpc_body_to_endpoint(
        endpoint,
        ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_build(
    address: &str,
    request: &ClusterRolloutBuildRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body(
        address,
        ROLLOUT_BUILD_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_build_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBuildRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_large_rpc_body_to_endpoint(
        endpoint,
        ROLLOUT_BUILD_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_clear_status(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rpc_body(
        address,
        ROLLOUT_CLEAR_STATUS_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        &(),
        timeout,
    )
}

pub fn send_daemon_rollout_diagnostics(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rpc_body(
        address,
        ROLLOUT_DIAGNOSTICS_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        &(),
        timeout,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        DaemonEndpointRpcTimeoutPolicy, DaemonRpcAddressTransport, DaemonRpcAttemptDeadline,
        DaemonSignedEndpointRpcHost, daemon_rpc_address_transport,
        local_daemon_can_gateway_endpoint,
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

    fn iroh_endpoint(node_id: NodeId) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: sinomo_core::Id::generate(),
            node_id,
            transport: ClusterTransportKind::Iroh,
            address: "iroh://example?relay=https://relay.example./".to_string(),
            source: ClusterEndpointSource::KvMembership,
            privacy: ClusterEndpointPrivacy::Relayed,
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
        assert!(sinomo_network::relay_response_is_routing_error(
            &relay_error
        ));

        let target_error = ObjectRpcResponse::encode_error(ROLLOUT_BUILD_ROUTE, "build failed")
            .expect("target error");
        assert!(!sinomo_network::relay_response_is_routing_error(
            &target_error
        ));
    }

    #[test]
    fn daemon_endpoint_rpc_control_plane_policy_caps_iroh_attempts() {
        let endpoint = iroh_endpoint(node_id("node_endpoint_timeout_control"));
        let host =
            DaemonSignedEndpointRpcHost::new(DaemonEndpointRpcTimeoutPolicy::FairControlPlane);

        assert_eq!(
            host.endpoint_timeout(&endpoint, Duration::from_secs(60)),
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn daemon_endpoint_rpc_large_policy_uses_full_request_budget() {
        let endpoint = iroh_endpoint(node_id("node_endpoint_timeout_rollout"));
        let host =
            DaemonSignedEndpointRpcHost::new(DaemonEndpointRpcTimeoutPolicy::FullRequestBudget);

        assert_eq!(
            host.endpoint_timeout(&endpoint, Duration::from_secs(60)),
            Duration::from_secs(60)
        );
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
        assert_eq!(deadline.checked_remaining(), None);
        let error = deadline.timeout_error("iroh://example");
        match error {
            crate::DaemonError::Network { address, detail } => {
                assert_eq!(address, "iroh://example");
                assert_eq!(detail, "timed out after 10ms");
            }
            error => panic!("unexpected error: {error}"),
        }
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
