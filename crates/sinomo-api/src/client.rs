//! Typed client for the running daemon's control API.

#[cfg(test)]
use crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE;
use crate::{
    DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE, DAEMON_OBJECT_RPC_FORWARD_ROUTE, DAEMON_RPC_CATEGORY,
    DAEMON_SHUTDOWN_ROUTE, DAEMON_STATUS_ROUTE, DaemonApiError,
    DaemonLocalConnectivityReloadRequest, DaemonLocalConnectivityReloadResponse,
    DaemonObjectRpcForwardDeliveryState, DaemonObjectRpcForwardRequest,
    DaemonObjectRpcForwardResponse, DaemonPaths, DaemonProbeStatus, DaemonStatus,
    probe_daemon_status,
};
use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE,
    ConfigCatalogRequest, ConfigCatalogResponse, ConfigClient, ConfigDefinition, ConfigError,
    ConfigGetRequest, ConfigGetResponse, ConfigResolution, ConfigSetRequest, ConfigSetResponse,
    ConfigWriteResult, ConfigWriteTarget,
};
use sinomo_core::cluster::{
    ClusterControlInitRequest, ClusterControlInitResponse, ClusterControlInviteRequest,
    ClusterControlInviteResponse, ClusterControlJoinRequest, ClusterControlJoinResponse,
    ClusterControlLeaveRequest, ClusterControlLeaveResponse, ClusterControlRemoveRequest,
    ClusterControlRemoveResponse, ClusterControlRenameRequest, ClusterControlRenameResponse,
    ClusterControlRoleRequest, ClusterControlRoleResponse, ClusterControlRpcPayload,
    ClusterHealthRow,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ObjectRpcRequest, ObjectRpcResponse, ObjectRpcSessionCall, ObjectRpcSessionResult,
    ObjectRpcUnarySessionAdapter,
};
use sinomo_core::node::NodeId;
use std::time::Duration;

/// How long daemon discovery waits for the status probe.
pub const DAEMON_API_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
/// Compatibility name for the canonical daemon-control no-progress timeout.
/// Canonical loopback sessions now report authenticated progress while a
/// cluster-control handler is running, so they need no wider idle exception.
pub const DAEMON_API_CLUSTER_CONTROL_TIMEOUT: Duration =
    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
/// Maximum value materialized by the daemon API's legacy unary compatibility
/// surface. Canonical session bodies remain aggregate-unbounded.
const DAEMON_API_UNARY_COMPATIBILITY_MAX_BODY_BYTES: usize = 256 * 1024 * 1024;

/// Typed daemon API client: daemon discovery plus signed object RPC.
#[derive(Clone, Debug)]
pub struct DaemonApiClient {
    address: String,
    timeout: Duration,
}

impl DaemonApiClient {
    /// Connects to the running main daemon of the default data directory
    /// (`SINOMO_DATA_DIR` or the home fallback).
    pub fn connect() -> Result<Self, DaemonApiError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        data_dir.ensure_root_layout()?;
        Self::connect_for_paths(&DaemonPaths::new(&data_dir))
    }

    /// Connects to the running daemon owning `paths` (main or shadow),
    /// verifying liveness through the status/probe flow.
    pub fn connect_for_paths(paths: &DaemonPaths) -> Result<Self, DaemonApiError> {
        Self::connect_for_paths_with_timeout(paths, DAEMON_API_PROBE_TIMEOUT)
    }

    /// Connects to the daemon while bounding discovery by the caller's
    /// remaining operation budget.
    pub fn connect_for_paths_with_timeout(
        paths: &DaemonPaths,
        probe_timeout: Duration,
    ) -> Result<Self, DaemonApiError> {
        match probe_daemon_status(paths, probe_timeout) {
            DaemonProbeStatus::Running(status) => Ok(Self::at_address(status.address)),
            DaemonProbeStatus::Locked { .. } => Err(DaemonApiError::NotRunning {
                detail: format!(
                    "daemon at {} is locked but did not answer its status probe",
                    paths.root.display()
                ),
            }),
            DaemonProbeStatus::NotRunning => Err(DaemonApiError::NotRunning {
                detail: format!(
                    "no daemon is running for {}; start it with `sinomo daemon start`",
                    paths.root.display()
                ),
            }),
        }
    }

    /// Client for a known daemon control address, skipping discovery.
    pub fn at_address(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        }
    }

    /// Overrides connection establishment and each no-progress interval.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn cluster_control_timeouts(&self) -> sinomo_core::network::DaemonRpcTimeouts {
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout)
    }

    fn object_rpc_forward_control_timeouts(
        &self,
        _effective_inner_timeout: Option<Duration>,
    ) -> sinomo_core::network::DaemonRpcTimeouts {
        // The loopback wrapper is a canonical session: authenticated
        // keepalives report transport health while the daemon waits for the
        // inner routed call, so its semantic deadline must not become an outer
        // no-progress timeout.
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout)
    }

    /// The daemon control address this client talks to.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Generic signed object-RPC passthrough for routes without a typed
    /// wrapper. `body` must already be encoded for the route's codec.
    pub fn call(
        &self,
        route: &str,
        category: &str,
        body: Vec<u8>,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let request = ObjectRpcRequest::from_encoded_body(route, category, body);
        send_daemon_rpc(&self.address, &request, self.timeout)
    }

    /// Generic signed object-RPC passthrough for an already built request.
    pub fn call_request(
        &self,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let response = send_daemon_rpc(&self.address, request, self.timeout)?;
        response.error_detail()?;
        Ok(response)
    }

    /// Sends a signed request through the connected daemon to another cluster
    /// node using the normal relay route.
    pub fn relay_signed_request_to_node_from_data_dir(
        &self,
        data_dir: &SinomoDataDir,
        target_node_id: &NodeId,
        request: ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let paths = sinomo_core::network::ClusterPaths::new(data_dir);
        let state = sinomo_core::cluster::read_cluster_state(&paths)?;
        let auth = sinomo_core::network::sign_daemon_rpc_request_from_data_dir(data_dir, &request)?;
        let envelope = sinomo_core::network::RelayEnvelope::new(
            state.cluster_id,
            target_node_id.clone(),
            request.with_auth(auth),
        );
        let relay = envelope.into_request()?;
        let response = send_daemon_rpc(&self.address, &relay, timeout)?;
        response.error_detail()?;
        Ok(response)
    }

    /// Sends an object-RPC from inside the running local daemon.
    ///
    /// This is intentionally daemon-mediated rather than a client-side routed
    /// send so CLI/app callers reuse the daemon's persistent cluster transports.
    pub fn object_rpc_to_target(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        self.object_rpc_to_target_with_deadline(
            target_node_id,
            request,
            connect_timeout,
            None,
            observe_connectivity,
        )
    }

    /// Sends an object-RPC through the running local daemon with an explicit
    /// route-wide semantic deadline. The loopback session remains healthy via
    /// authenticated progress while the daemon waits for the terminal response.
    pub fn object_rpc_to_target_with_deadline(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let call = ObjectRpcUnarySessionAdapter::encode_request(request.clone())?;
        block_on_daemon_client(async {
            let result = self
                .object_rpc_session_to_target_with_deadline_async(
                    target_node_id,
                    call,
                    connect_timeout,
                    overall_timeout,
                    observe_connectivity,
                )
                .await?;
            decode_unary_object_rpc_session_response(result).await
        })
    }

    /// Sends one canonical object-RPC session through the running local daemon
    /// without adding an operation-wide semantic deadline.
    pub async fn object_rpc_session_to_target_async(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        connect_timeout: Duration,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcSessionResult, DaemonApiError> {
        self.object_rpc_session_to_target_with_deadline_async(
            target_node_id,
            call,
            connect_timeout,
            None,
            observe_connectivity,
        )
        .await
    }

    /// Sends one canonical object-RPC session through the running local daemon.
    /// The request and response bodies remain streams; callers that expose a
    /// unary `Vec<u8>` API must opt into [`ObjectRpcUnarySessionAdapter`].
    pub async fn object_rpc_session_to_target_with_deadline_async(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcSessionResult, DaemonApiError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        let runtime = tokio::runtime::Handle::current();
        send_daemon_object_rpc_session_async(
            data_dir,
            &self.address,
            target_node_id,
            call,
            connect_timeout,
            overall_timeout,
            observe_connectivity,
            self.object_rpc_forward_control_timeouts(overall_timeout),
            None,
            &runtime,
        )
        .await
    }

    /// Sends a DFS object-RPC from inside the running local daemon.
    pub fn dfs_object_rpc_to_target(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        self.dfs_object_rpc_to_target_with_options(
            target_node_id,
            request,
            sinomo_dfs::DfsObjectRpcSendOptions::new(connect_timeout)
                .with_observe_connectivity(observe_connectivity),
        )
    }

    /// Sends a DFS object-RPC while keeping transport phase limits separate
    /// from an optional DFS operation-wide deadline.
    pub fn dfs_object_rpc_to_target_with_options(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        let address = self.address.clone();
        let outer_timeouts =
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout);
        let target_node_id = target_node_id.clone();
        let request = request.clone();
        block_on_daemon_client(async move {
            let runtime = tokio::runtime::Handle::current();
            let call = ObjectRpcUnarySessionAdapter::encode_request(request)?;
            let result = send_daemon_object_rpc_session_async(
                data_dir,
                &address,
                &target_node_id,
                call,
                options.connect_timeout,
                options.overall_timeout,
                options.observe_connectivity,
                outer_timeouts,
                None,
                &runtime,
            )
            .await?;
            decode_unary_object_rpc_session_response(result).await
        })
    }

    fn call_control_payload(
        &self,
        payload: ClusterControlRpcPayload,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let request =
            ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
        let response = send_daemon_rpc_with_timeouts(
            &self.address,
            &request,
            self.cluster_control_timeouts(),
        )?;
        response.error_detail()?;
        Ok(response)
    }

    /// Fetches the daemon's live status.
    pub fn status(&self) -> Result<DaemonStatus, DaemonApiError> {
        let request = ObjectRpcRequest::encode_body(DAEMON_STATUS_ROUTE, DAEMON_RPC_CATEGORY, &())?;
        self.call_request(&request)?
            .decode_body()
            .map_err(DaemonApiError::from)
    }

    /// Requests a daemon shutdown; returns the daemon's acknowledgement line.
    pub fn shutdown(&self) -> Result<String, DaemonApiError> {
        let request =
            ObjectRpcRequest::encode_body(DAEMON_SHUTDOWN_ROUTE, DAEMON_RPC_CATEGORY, &())?;
        self.call_request(&request)?
            .decode_body()
            .map_err(DaemonApiError::from)
    }

    /// Drops the daemon process's cached local connectivity snapshot so its
    /// next route read reloads state written through the shared data directory.
    pub fn reload_local_connectivity(
        &self,
    ) -> Result<DaemonLocalConnectivityReloadResponse, DaemonApiError> {
        let request = ObjectRpcRequest::encode_body(
            DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
            DAEMON_RPC_CATEGORY,
            &DaemonLocalConnectivityReloadRequest,
        )?;
        self.call_request(&request)?
            .decode_body()
            .map_err(DaemonApiError::from)
    }

    /// Initializes a fresh single-node cluster on the daemon's node.
    pub fn cluster_init(
        &self,
        request: ClusterControlInitRequest,
    ) -> Result<ClusterControlInitResponse, DaemonApiError> {
        let response = self.call_control_payload(
            sinomo_core::cluster::cluster_control_init_rpc_payload(&request)?,
        )?;
        sinomo_core::cluster::decode_cluster_control_init_rpc_response_body(&response.body)
            .map_err(DaemonApiError::from)
    }

    /// Joins an existing cluster from an invite.
    pub fn cluster_join(
        &self,
        request: ClusterControlJoinRequest,
    ) -> Result<ClusterControlJoinResponse, DaemonApiError> {
        let response = self.call_control_payload(
            sinomo_core::cluster::cluster_control_join_rpc_payload(&request)?,
        )?;
        sinomo_core::cluster::decode_cluster_control_join_rpc_response_body(&response.body)
            .map_err(DaemonApiError::from)
    }

    /// Creates and publishes a cluster join invite.
    pub fn cluster_invite(&self) -> Result<ClusterControlInviteResponse, DaemonApiError> {
        self.cluster_invite_with_request(ClusterControlInviteRequest::default())
    }

    /// Creates and publishes a cluster join invite with explicit request options.
    pub fn cluster_invite_with_request(
        &self,
        request: ClusterControlInviteRequest,
    ) -> Result<ClusterControlInviteResponse, DaemonApiError> {
        let response = self.call_control_payload(
            sinomo_core::cluster::cluster_control_invite_rpc_payload(&request)?,
        )?;
        sinomo_core::cluster::decode_cluster_control_invite_rpc_response_body(&response.body)
            .map_err(DaemonApiError::from)
    }

    /// Renames a cluster member (the daemon's local node by default).
    pub fn cluster_rename(
        &self,
        request: ClusterControlRenameRequest,
    ) -> Result<ClusterControlRenameResponse, DaemonApiError> {
        let response = self.call_control_payload(
            sinomo_core::cluster::cluster_control_rename_rpc_payload(&request)?,
        )?;
        sinomo_core::cluster::decode_cluster_control_rename_rpc_response_body(&response.body)
            .map_err(DaemonApiError::from)
    }

    /// Changes a cluster member's node role.
    pub fn cluster_role(
        &self,
        request: ClusterControlRoleRequest,
    ) -> Result<ClusterControlRoleResponse, DaemonApiError> {
        let response = self.call_control_payload(
            sinomo_core::cluster::cluster_control_role_rpc_payload(&request)?,
        )?;
        sinomo_core::cluster::decode_cluster_control_role_rpc_response_body(&response.body)
            .map_err(DaemonApiError::from)
    }

    /// Removes cluster members by node id.
    pub fn cluster_remove(
        &self,
        request: ClusterControlRemoveRequest,
    ) -> Result<ClusterControlRemoveResponse, DaemonApiError> {
        let response = self.call_control_payload(
            sinomo_core::cluster::cluster_control_remove_rpc_payload(&request)?,
        )?;
        sinomo_core::cluster::decode_cluster_control_remove_rpc_response_body(&response.body)
            .map_err(DaemonApiError::from)
    }

    /// Leaves the current cluster; the node re-initializes as a fresh
    /// single-node cluster.
    pub fn cluster_leave(&self) -> Result<ClusterControlLeaveResponse, DaemonApiError> {
        let response =
            self.call_control_payload(sinomo_core::cluster::cluster_control_leave_rpc_payload(
                &ClusterControlLeaveRequest {},
            )?)?;
        sinomo_core::cluster::decode_cluster_control_leave_rpc_response_body(&response.body)
            .map_err(DaemonApiError::from)
    }

    /// Fetches cluster health rows from the daemon's KV-backed `/cluster/health`
    /// view.
    ///
    /// Background connectivity probes publish node status and reachability into
    /// cluster KV. Foreground status readers should use that source of truth
    /// rather than spending their command budget on a live network sweep.
    pub fn cluster_health(&self) -> Result<Vec<ClusterHealthRow>, DaemonApiError> {
        let request = cluster_health_snapshot_request()?;
        let response = self.call_request(&request)?;
        sinomo_core::cluster::decode_cluster_health_rpc_response(&response)
            .map_err(DaemonApiError::from)
    }

    /// Fetches cluster health rows with live member probes and peer-local view
    /// merging. This is useful for diagnostics but can be slower than the
    /// KV-backed snapshot used by [`Self::cluster_health`].
    pub fn cluster_health_with_peer_views(&self) -> Result<Vec<ClusterHealthRow>, DaemonApiError> {
        let request = cluster_health_live_request()?;
        let response = self.call_request(&request)?;
        sinomo_core::cluster::decode_cluster_health_rpc_response(&response)
            .map_err(DaemonApiError::from)
    }
}

fn cluster_health_snapshot_request() -> Result<ObjectRpcRequest, DaemonApiError> {
    sinomo_core::cluster::cluster_health_rpc_request_without_peer_views().map_err(Into::into)
}

fn cluster_health_live_request() -> Result<ObjectRpcRequest, DaemonApiError> {
    sinomo_core::cluster::cluster_health_rpc_request().map_err(Into::into)
}

fn object_rpc_forward_request(
    route: &str,
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    connect_timeout: Duration,
    overall_timeout: Option<Duration>,
    observe_connectivity: bool,
) -> Result<ObjectRpcRequest, DaemonApiError> {
    let payload = DaemonObjectRpcForwardRequest {
        target_node_id: target_node_id.clone(),
        request: request.clone(),
        connect_timeout_millis: u64::try_from(connect_timeout.as_millis()).unwrap_or(u64::MAX),
        overall_timeout_millis: overall_timeout
            .map(|timeout| u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)),
        observe_connectivity,
    };
    ObjectRpcRequest::encode_body(route, DAEMON_RPC_CATEGORY, &payload).map_err(Into::into)
}

#[cfg(test)]
fn decode_daemon_object_rpc_forward_response(
    response: &ObjectRpcResponse,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    match response.decode_body::<DaemonObjectRpcForwardResponse>() {
        Ok(DaemonObjectRpcForwardResponse::Success { response }) => Ok(response),
        Ok(DaemonObjectRpcForwardResponse::NetworkError {
            detail,
            delivery_state,
        }) => Err(DaemonApiError::ForwardNetwork {
            detail,
            delivery_state: delivery_state.map(|state| match state {
                DaemonObjectRpcForwardDeliveryState::DefinitelyNotSent => {
                    sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent
                }
                DaemonObjectRpcForwardDeliveryState::PossiblyApplied => {
                    sinomo_core::network::NetworkDeliveryState::PossiblyApplied
                }
            }),
        }),
        // Rolling upgrades may talk to a daemon that still encodes the inner
        // ObjectRpcResponse directly. Accept that legacy body until every node
        // runs the new envelope format.
        Err(_) => response.decode_body().map_err(DaemonApiError::from),
    }
}

fn daemon_object_rpc_forward_error(
    detail: String,
    delivery_state: Option<DaemonObjectRpcForwardDeliveryState>,
) -> DaemonApiError {
    DaemonApiError::ForwardNetwork {
        detail,
        delivery_state: delivery_state.map(|state| match state {
            DaemonObjectRpcForwardDeliveryState::DefinitelyNotSent => {
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent
            }
            DaemonObjectRpcForwardDeliveryState::PossiblyApplied => {
                sinomo_core::network::NetworkDeliveryState::PossiblyApplied
            }
        }),
    }
}

async fn decode_daemon_object_rpc_forward_session_response(
    mut result: ObjectRpcSessionResult,
) -> Result<ObjectRpcSessionResult, DaemonApiError> {
    // The forward envelope is itself a unary control value. When the inner
    // response has no semantic body (notably permission and service errors),
    // the daemon's unary compatibility layer moves that envelope into the
    // session lane. Materialize only that explicitly marked envelope; preserve
    // an unmarked inner response stream end to end.
    if ObjectRpcUnarySessionAdapter::response_is_encoded(&result)? {
        result = ObjectRpcUnarySessionAdapter::decode_response(
            result,
            DAEMON_API_UNARY_COMPATIBILITY_MAX_BODY_BYTES,
        )
        .await?;
    }
    result.response.error_detail()?;
    match result
        .response
        .decode_body::<DaemonObjectRpcForwardResponse>()
    {
        Ok(DaemonObjectRpcForwardResponse::Success { response }) => {
            Ok(ObjectRpcSessionResult::new(response, result.body))
        }
        Ok(DaemonObjectRpcForwardResponse::NetworkError {
            detail,
            delivery_state,
        }) => Err(daemon_object_rpc_forward_error(detail, delivery_state)),
        Err(source) => Err(DaemonApiError::Parse {
            detail: format!("invalid daemon object RPC session-forward response: {source}"),
        }),
    }
}

async fn decode_unary_object_rpc_session_response(
    result: ObjectRpcSessionResult,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    let result = ObjectRpcUnarySessionAdapter::decode_response(
        result,
        DAEMON_API_UNARY_COMPATIBILITY_MAX_BODY_BYTES,
    )
    .await?;
    result.response.error_detail()?;
    Ok(result.response)
}

async fn call_loopback_daemon_session_async(
    data_dir: SinomoDataDir,
    address: &str,
    call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcSessionResult, DaemonApiError> {
    if !sinomo_core::network::is_loopback_connect_address(address) {
        return Err(DaemonApiError::Network {
            address: address.to_string(),
            detail: "daemon-mediated object RPC requires the local daemon loopback address"
                .to_string(),
        });
    }
    let mut host = DaemonApiSessionHost {
        data_dir: data_dir.clone(),
        runtime: runtime.clone(),
    };
    let ObjectRpcSessionCall { mut request, body } = call;
    request.auth = None;
    sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
        .call_loopback_session(
            &data_dir,
            address,
            ObjectRpcSessionCall::new(request, body),
            options,
        )
        .await
        .map_err(Into::into)
}

async fn send_daemon_object_rpc_session_async(
    data_dir: SinomoDataDir,
    address: &str,
    target_node_id: &NodeId,
    call: ObjectRpcSessionCall,
    connect_timeout: Duration,
    overall_timeout: Option<Duration>,
    observe_connectivity: bool,
    outer_timeouts: sinomo_core::network::DaemonRpcTimeouts,
    known_local_node_id: Option<&NodeId>,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcSessionResult, DaemonApiError> {
    let effective_overall_timeout =
        sinomo_core::network::cluster_object_rpc_effective_route_timeout(
            &call.request,
            overall_timeout,
        );
    let target_is_local = match known_local_node_id {
        Some(local_node_id) => target_node_id == local_node_id,
        None => {
            let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            target_node_id == &state.local_node_id
        }
    };
    if target_is_local {
        let mut local_options = sinomo_core::network::ClusterObjectRpcOptions::new(outer_timeouts);
        if let Some(overall_timeout) = effective_overall_timeout {
            local_options = local_options.with_overall_timeout(overall_timeout);
        }
        return call_loopback_daemon_session_async(data_dir, address, call, local_options, runtime)
            .await;
    }
    let forward_request = object_rpc_forward_request(
        DAEMON_OBJECT_RPC_FORWARD_ROUTE,
        target_node_id,
        &call.request,
        connect_timeout,
        effective_overall_timeout,
        observe_connectivity,
    )?;
    let outer = ObjectRpcSessionCall::new(forward_request, call.body);
    let result = call_loopback_daemon_session_async(
        data_dir,
        address,
        outer,
        sinomo_core::network::ClusterObjectRpcOptions::new(outer_timeouts),
        runtime,
    )
    .await?;
    decode_daemon_object_rpc_forward_session_response(result).await
}

/// DFS peer transport that deliberately terminates at the running local
/// daemon. The daemon owns endpoint selection, retries, authentication, and
/// canonical TCP tunnels; mounted clients never open a competing cluster
/// router or fall back to direct advertised endpoints.
#[derive(Clone, Debug)]
pub struct DaemonDfsPeerTransport {
    data_dir: SinomoDataDir,
    address: String,
    local_node_id: NodeId,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
}

impl DaemonDfsPeerTransport {
    /// Discovers and probes the main daemon for `data_dir` once, when a mounted
    /// service is opened. Clustered mounts fail closed when no local daemon is
    /// available instead of silently changing transport semantics.
    pub fn connect(data_dir: SinomoDataDir) -> Result<Self, DaemonApiError> {
        data_dir.ensure_root_layout()?;
        let client = DaemonApiClient::connect_for_paths(&DaemonPaths::new(&data_dir))?;
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let local_node_id =
            sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?.local_node_id;
        let runtime = daemon_dfs_peer_runtime()?;
        Ok(Self {
            data_dir,
            address: client.address,
            local_node_id,
            runtime,
        })
    }

    #[cfg(test)]
    pub fn at_address(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            data_dir,
            address: address.into(),
            local_node_id,
            runtime: daemon_dfs_peer_runtime().expect("daemon DFS peer runtime"),
        }
    }

    async fn call_session_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<ObjectRpcSessionResult, DaemonApiError> {
        send_daemon_object_rpc_session_async(
            self.data_dir.clone(),
            &self.address,
            target_node_id,
            call,
            options.connect_timeout,
            options.overall_timeout,
            options.observe_connectivity,
            sinomo_core::network::DaemonRpcTimeouts::default(),
            Some(&self.local_node_id),
            runtime,
        )
        .await
    }
}

fn daemon_dfs_peer_runtime() -> Result<std::sync::Arc<tokio::runtime::Runtime>, DaemonApiError> {
    static SHARED: std::sync::OnceLock<Result<std::sync::Arc<tokio::runtime::Runtime>, String>> =
        std::sync::OnceLock::new();
    SHARED
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("sinomo-daemon-dfs-peer")
                .enable_all()
                .build()
                .map(std::sync::Arc::new)
                .map_err(|source| source.to_string())
        })
        .clone()
        .map_err(|detail| DaemonApiError::Network {
            address: "daemon DFS peer runtime".to_string(),
            detail,
        })
}

impl sinomo_dfs::DfsPeerTransport for DaemonDfsPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResponse, sinomo_dfs::DfsPeerTransportError> {
        let call = ObjectRpcUnarySessionAdapter::encode_request(request.clone())
            .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))?;
        let runtime = self.runtime.handle().clone();
        block_on_daemon_client_on_owner_runtime(&runtime, async {
            let result = self
                .call_session_inner(target_node_id, call, options, &runtime)
                .await?;
            decode_unary_object_rpc_session_response(result).await
        })
        .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))
    }

    fn call_object_rpc_session(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcSessionResult, sinomo_dfs::DfsPeerTransportError> {
        let runtime = self.runtime.handle().clone();
        block_on_daemon_client_on_owner_runtime(&runtime, async {
            self.call_session_inner(target_node_id, call, options, &runtime)
                .await
        })
        .and_then(|result| {
            result.response.error_detail()?;
            Ok(result)
        })
        .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))
    }
}

impl sinomo_dfs::AsyncDfsPeerTransport for DaemonDfsPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        request: &'a ObjectRpcRequest,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<
        'a,
        Result<ObjectRpcResponse, sinomo_dfs::DfsPeerTransportError>,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))?;
            let call = ObjectRpcUnarySessionAdapter::encode_request(request.clone())
                .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))?;
            let result = self
                .call_session_inner(target_node_id, call, options, &runtime)
                .await
                .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))?;
            decode_unary_object_rpc_session_response(result)
                .await
                .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))
        })
    }

    fn call_object_rpc_session_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcSessionCall,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<
        'a,
        Result<ObjectRpcSessionResult, sinomo_dfs::DfsPeerTransportError>,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))?;
            let result = self
                .call_session_inner(target_node_id, call, options, &runtime)
                .await
                .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))?;
            result
                .response
                .error_detail()
                .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))?;
            Ok(result)
        })
    }
}

pub fn send_daemon_rpc(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    send_daemon_rpc_with_timeouts(
        address,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )
}

pub fn send_daemon_rpc_on_runtime(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_rpc_async(
            address,
            request,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            &runtime_for_future,
        ),
    )
}

pub async fn send_daemon_rpc_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    if sinomo_core::network::is_loopback_connect_address(address) {
        return send_daemon_rpc_loopback_session_async(
            SinomoDataDir::from_env_or_home()?,
            address,
            request,
            timeouts,
            runtime,
        )
        .await;
    }
    let route = request.route.clone();
    let category = request.category.clone();
    let body_sha256 = sinomo_core::network::daemon_rpc_body_sha256(request);
    let signed = runtime
        .spawn_blocking(move || {
            sinomo_core::network::sign_daemon_rpc_request_digest_for_local_node(
                &route,
                &category,
                body_sha256,
            )
        })
        .await
        .map_err(|source| DaemonApiError::Network {
            address: address.to_string(),
            detail: format!("daemon RPC signing task failed: {source}"),
        })??;
    let control_request = request.clone().with_auth(signed);
    let response = sinomo_core::network::send_daemon_control_rpc_request_async(
        address,
        &control_request,
        timeouts,
        sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
    )
    .await?;
    Ok(response)
}

pub fn send_daemon_rpc_raw(
    address: &str,
    auth: Option<&[u8]>,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    send_daemon_rpc_raw_with_timeouts(
        address,
        auth,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )
}

pub fn send_daemon_rpc_with_timeouts(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    if sinomo_core::network::is_loopback_connect_address(address) {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        return block_on_daemon_client(async {
            let runtime = tokio::runtime::Handle::current();
            send_daemon_rpc_loopback_session_async(data_dir, address, request, timeouts, &runtime)
                .await
        });
    }
    let signed = sinomo_core::network::sign_daemon_rpc_request_for_local_node(request)?;
    send_daemon_rpc_raw_with_timeouts(address, Some(&signed), request, timeouts)
}

async fn send_daemon_rpc_loopback_session_async(
    data_dir: SinomoDataDir,
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    let mut host = DaemonApiSessionHost {
        data_dir: data_dir.clone(),
        runtime: runtime.clone(),
    };
    let mut request = request.clone();
    request.auth = None;
    let call = ObjectRpcUnarySessionAdapter::encode_request(request)?;
    let result = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
        .call_loopback_session(
            &data_dir,
            address,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
        )
        .await?;
    let result = ObjectRpcUnarySessionAdapter::decode_response(
        result,
        DAEMON_API_UNARY_COMPATIBILITY_MAX_BODY_BYTES,
    )
    .await?;
    Ok(result.response)
}

pub fn send_daemon_rpc_raw_with_timeouts(
    address: &str,
    auth: Option<&[u8]>,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    block_on_daemon_client(async {
        let control_request = match auth {
            Some(auth) => request.clone().with_auth(auth.to_vec()),
            None => request.clone(),
        };
        sinomo_core::network::send_daemon_control_rpc_request_async(
            address,
            &control_request,
            timeouts,
            sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
        )
        .await
        .map_err(DaemonApiError::from)
    })
}

/// A [`ConfigClient`] backed by RPC to a running daemon's `/config/*` service.
#[derive(Clone)]
pub struct RemoteConfigClient {
    address: String,
    timeout: Duration,
    runtime: Option<tokio::runtime::Handle>,
}

impl RemoteConfigClient {
    pub fn new(address: impl Into<String>, timeout: Duration) -> Self {
        Self {
            address: address.into(),
            timeout,
            runtime: None,
        }
    }

    pub fn new_on_runtime(
        address: impl Into<String>,
        timeout: Duration,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        Self {
            address: address.into(),
            timeout,
            runtime: Some(runtime.clone()),
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
        let response = match &self.runtime {
            Some(runtime) => {
                send_daemon_rpc_on_runtime(&self.address, &request, self.timeout, runtime)
                    .map_err(|error| client_error(error.to_string()))?
            }
            None => send_daemon_rpc(&self.address, &request, self.timeout)
                .map_err(|error| client_error(error.to_string()))?,
        };
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

pub fn send_daemon_rpc_stream(
    address: &str,
    request: &ObjectRpcRequest,
    _operation_timeout: Duration,
    mut on_response: impl FnMut(ObjectRpcResponse) -> Result<bool, DaemonApiError>,
) -> Result<(), DaemonApiError> {
    block_on_daemon_client_stream(send_daemon_rpc_stream_async(
        address,
        request,
        daemon_rpc_stream_timeouts(),
        &mut on_response,
    ))
}

fn daemon_rpc_stream_timeouts() -> sinomo_core::network::DaemonRpcTimeouts {
    // Session keepalives prove transport progress independently of whether the
    // remote process is currently producing application output.
    sinomo_core::network::DaemonRpcTimeouts::default()
}

pub async fn send_daemon_rpc_stream_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    on_response: impl FnMut(ObjectRpcResponse) -> Result<bool, DaemonApiError>,
) -> Result<(), DaemonApiError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    send_daemon_rpc_stream_from_data_dir_async(data_dir, address, request, timeouts, on_response)
        .await
}

pub async fn send_daemon_rpc_stream_from_data_dir_async(
    data_dir: SinomoDataDir,
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    mut on_response: impl FnMut(ObjectRpcResponse) -> Result<bool, DaemonApiError>,
) -> Result<(), DaemonApiError> {
    let runtime = tokio::runtime::Handle::current();
    let mut host = DaemonApiSessionHost {
        data_dir: data_dir.clone(),
        runtime: runtime.clone(),
    };
    let mut request = request.clone();
    request.auth = None;
    let call = ObjectRpcUnarySessionAdapter::encode_request(request)?;
    let options = sinomo_core::network::ClusterObjectRpcOptions::new(timeouts);
    let result = if sinomo_core::network::is_loopback_connect_address(address) {
        let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
        client
            .call_loopback_session(&data_dir, address, call, options)
            .await?
    } else {
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
        let endpoint = daemon_api_session_endpoint_for_address(&state, address)?;
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
            &mut host,
            &runtime,
            &cluster_paths,
        );
        client
            .call_endpoint_session(&state, &endpoint, call, options)
            .await?
    };
    result.response.error_detail()?;
    let mut body = result.body;
    loop {
        let rpc = sinomo_core::network::read_object_frame_async(
            &mut body,
            sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
            sinomo_core::network::ObjectFrameIoOptions::none(),
        )
        .await?;
        if !on_response(rpc)? {
            return Ok(());
        }
    }
}

struct DaemonApiSessionHost {
    data_dir: SinomoDataDir,
    runtime: tokio::runtime::Handle,
}

impl sinomo_core::network::ClusterSignedObjectRpcAsyncHost for DaemonApiSessionHost {
    fn sign_object_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> sinomo_core::network::ClusterSignedObjectRpcFuture<'a, Vec<u8>> {
        let data_dir = self.data_dir.clone();
        let request = request.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    sinomo_core::network::sign_daemon_rpc_request_from_data_dir(&data_dir, &request)
                })
                .await
                .map_err(|source| sinomo_core::network::NetworkError::Io {
                    operation: "daemon API session signing".to_string(),
                    detail: format!("session signing task failed: {source}"),
                })?
        })
    }
}

fn daemon_api_session_endpoint_for_address(
    state: &sinomo_core::ClusterBootstrapState,
    address: &str,
) -> Result<sinomo_core::ClusterEndpointRecord, DaemonApiError> {
    if let Some(endpoint) = state
        .members
        .iter()
        .flat_map(|member| member.endpoints.iter())
        .find(|endpoint| endpoint.address == address)
    {
        return Ok(endpoint.clone());
    }
    Err(DaemonApiError::Network {
        address: address.to_string(),
        detail: "daemon session endpoint is not authorized in local cluster state".to_string(),
    })
}

fn block_on_daemon_client<F, T>(future: F) -> Result<T, DaemonApiError>
where
    F: std::future::Future<Output = Result<T, DaemonApiError>> + Send,
    T: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            Err(DaemonApiError::Network {
                address: "daemon client runtime".to_string(),
                detail: "daemon RPC sync client cannot run inside a current-thread Tokio runtime; use the async or owner-runtime API".to_string(),
            })
        };
    }
    block_on_daemon_client_on_ephemeral_sync_runtime(future)
}

fn block_on_daemon_client_stream<F, T>(future: F) -> Result<T, DaemonApiError>
where
    F: std::future::Future<Output = Result<T, DaemonApiError>>,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(DaemonApiError::Network {
            address: "daemon client runtime".to_string(),
            detail: "streaming daemon RPC sync client cannot run inside a Tokio runtime; use send_daemon_rpc_stream_async".to_string(),
        });
    }
    block_on_daemon_client_on_ephemeral_sync_runtime(future)
}

fn block_on_daemon_client_on_owner_runtime<F, T>(
    runtime: &tokio::runtime::Handle,
    future: F,
) -> Result<T, DaemonApiError>
where
    F: std::future::Future<Output = Result<T, DaemonApiError>> + Send,
    T: Send,
{
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        return if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| runtime.block_on(future))
        } else {
            Err(DaemonApiError::Network {
                address: "daemon client runtime".to_string(),
                detail: "daemon client owner-runtime bridge cannot run inside a current-thread Tokio runtime; use the async daemon client API".to_string(),
            })
        };
    }
    runtime.block_on(future)
}

fn block_on_daemon_client_on_ephemeral_sync_runtime<F, T>(future: F) -> Result<T, DaemonApiError>
where
    F: std::future::Future<Output = Result<T, DaemonApiError>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DaemonApiError::Network {
            address: "daemon client runtime".to_string(),
            detail: source.to_string(),
        })?;
    let output = runtime.block_on(future);
    runtime.shutdown_timeout(Duration::from_millis(250));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_api_client_discovery_fails_without_running_daemon() {
        let root = sinomo_testdir::TestDir::new("sinomo-api-client-discovery");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        let paths = DaemonPaths::new(&data_dir);
        let error = DaemonApiClient::connect_for_paths(&paths)
            .expect_err("no daemon is running in the test data dir");
        assert!(matches!(error, DaemonApiError::NotRunning { .. }));
    }

    #[test]
    fn daemon_api_client_accepts_a_caller_bounded_discovery_timeout() {
        let root = sinomo_testdir::TestDir::new("sinomo-api-client-bounded-discovery");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        let paths = DaemonPaths::new(&data_dir);
        let error =
            DaemonApiClient::connect_for_paths_with_timeout(&paths, Duration::from_millis(10))
                .expect_err("no daemon is running in the test data dir");

        assert!(matches!(error, DaemonApiError::NotRunning { .. }));
    }

    #[test]
    fn daemon_api_client_address_and_timeout_are_configurable() {
        let client =
            DaemonApiClient::at_address("127.0.0.1:21353").with_timeout(Duration::from_secs(5));
        assert_eq!(client.address(), "127.0.0.1:21353");
        assert_eq!(client.timeout, Duration::from_secs(5));
    }

    #[test]
    fn cluster_control_uses_default_network_health_timeout() {
        let client = DaemonApiClient::at_address("127.0.0.1:21353");
        let timeouts = client.cluster_control_timeouts();
        assert_eq!(
            timeouts.connect_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn daemon_rpc_stream_keeps_default_network_health_timeout() {
        let timeouts = daemon_rpc_stream_timeouts();
        assert_eq!(
            timeouts.connect_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn cluster_health_live_request_keeps_peer_views_enabled() {
        let request = cluster_health_live_request().expect("cluster health request");
        assert_eq!(request.route, sinomo_core::network::CLUSTER_HEALTH_ROUTE);
        assert!(
            !request
                .metadata
                .contains_key(sinomo_core::network::CLUSTER_HEALTH_INCLUDE_PEER_VIEWS_METADATA_KEY),
            "live cluster health must not opt out of peer views"
        );
    }

    #[test]
    fn cluster_health_snapshot_request_disables_peer_views() {
        let request = cluster_health_snapshot_request().expect("cluster health request");
        assert_eq!(request.route, sinomo_core::network::CLUSTER_HEALTH_ROUTE);
        assert_eq!(
            request
                .metadata
                .get(sinomo_core::network::CLUSTER_HEALTH_INCLUDE_PEER_VIEWS_METADATA_KEY)
                .map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn forwarded_object_rpc_control_timeout_uses_client_budget() {
        let client =
            DaemonApiClient::at_address("127.0.0.1:21353").with_timeout(Duration::from_secs(6));
        let timeouts = client.object_rpc_forward_control_timeouts(Some(Duration::from_secs(10)));
        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(6)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(6)));
    }

    #[test]
    fn forwarded_object_rpc_deadline_does_not_change_network_health_timeout() {
        let client = DaemonApiClient::at_address("127.0.0.1:21353");
        let timeouts = client.object_rpc_forward_control_timeouts(Some(Duration::from_secs(10)));
        assert_eq!(
            timeouts.connect_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn forwarded_unbounded_rpc_uses_default_network_health_timeout() {
        let client = DaemonApiClient::at_address("127.0.0.1:21353");
        let request = ObjectRpcRequest::from_encoded_body("/test/data", "test", Vec::new())
            .with_full_response_budget();
        let inner_timeout =
            sinomo_core::network::cluster_object_rpc_effective_route_timeout(&request, None);
        let timeouts = client.object_rpc_forward_control_timeouts(inner_timeout);
        assert_eq!(inner_timeout, None);
        assert_eq!(
            timeouts.connect_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn forwarded_buffered_object_rpc_honors_explicit_client_timeout() {
        let client =
            DaemonApiClient::at_address("127.0.0.1:21353").with_timeout(Duration::from_secs(6));
        let timeouts = client.object_rpc_forward_control_timeouts(Some(Duration::from_secs(60)));
        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(6)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(6)));
    }

    #[test]
    fn forwarded_bounded_object_rpc_uses_default_network_health_timeout() {
        let client = DaemonApiClient::at_address("127.0.0.1:21353");
        let timeouts = client.object_rpc_forward_control_timeouts(Some(Duration::from_secs(15)));
        assert_eq!(
            timeouts.connect_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn signed_loopback_rpc_survives_long_handler_and_rehydrates_large_response() {
        let root = sinomo_testdir::TestDir::new("sinomo-api-loopback-session-progress");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let response_body = vec![0x5a_u8; 96 * 1024];
        let server_body = response_body.clone();
        let server_data_dir = data_dir.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept loopback session");
            let (request, responder) = sinomo_core::network::accept_object_rpc_server_session(
                Box::new(stream.into_inner()),
                server_data_dir,
                sinomo_core::network::NetworkCallPolicy::default(),
                |_request, _node_id, _public_key| Ok(()),
            )
            .await
            .expect("accept object RPC session");
            assert_eq!(request.request.route, "/test/long-loopback-response");
            tokio::time::sleep(Duration::from_secs(12)).await;
            let response =
                ObjectRpcResponse::from_encoded_body("/test/long-loopback-response", server_body);
            let response = ObjectRpcUnarySessionAdapter::encode_response(
                sinomo_core::network::ObjectRpcSessionResponse::buffered(response),
            )
            .expect("adapt buffered unary response");
            responder
                .respond(response)
                .await
                .expect("respond with adapted buffered body");
        });
        let request = ObjectRpcRequest::encode_body("/test/long-loopback-response", "test", &())
            .expect("request");

        let response = send_daemon_rpc_loopback_session_async(
            data_dir,
            &address,
            &request,
            sinomo_core::network::DaemonRpcTimeouts::default(),
            &tokio::runtime::Handle::current(),
        )
        .await
        .expect("authenticated keepalives preserve the long loopback RPC");
        server.await.expect("server task");

        assert_eq!(response.body, response_body);
        assert!(
            !response
                .metadata
                .contains_key("sinomo.rpc-session.buffered-response-body")
        );
    }

    #[test]
    fn dfs_forward_request_uses_dfs_route() {
        let target_node_id = NodeId::from_legacy_named("node_dfs_forward").expect("node id");
        let inner_request =
            ObjectRpcRequest::encode_body("/dfs/test/stream", "test", &()).expect("inner request");
        let request = object_rpc_forward_request(
            DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            &target_node_id,
            &inner_request,
            Duration::from_secs(5),
            Some(Duration::from_secs(30)),
            true,
        )
        .expect("forward request");
        assert_eq!(request.route, DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE);
        let payload: DaemonObjectRpcForwardRequest =
            request.decode_body().expect("forward payload");
        assert_eq!(payload.target_node_id, target_node_id);
        assert_eq!(payload.request.route, "/dfs/test/stream");
        assert_eq!(payload.connect_timeout_millis, 5_000);
        assert_eq!(payload.overall_timeout_millis, Some(30_000));
        assert!(payload.observe_connectivity);
    }

    #[test]
    fn generic_forward_request_carries_the_requested_overall_deadline() {
        let target_node_id = NodeId::from_legacy_named("node_generic_forward").expect("node id");
        let inner_request = ObjectRpcRequest::encode_body("/cluster/raft/append", "cluster", &())
            .expect("inner request");
        let request = object_rpc_forward_request(
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            &target_node_id,
            &inner_request,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            Some(Duration::from_secs(10)),
            true,
        )
        .expect("forward request");
        let payload: DaemonObjectRpcForwardRequest =
            request.decode_body().expect("forward payload");

        assert_eq!(payload.target_node_id, target_node_id);
        assert_eq!(payload.connect_timeout_millis, 5_000);
        assert_eq!(payload.overall_timeout_millis, Some(10_000));
    }

    #[test]
    fn generic_forward_request_preserves_an_unbounded_overall_deadline() {
        let target_node_id =
            NodeId::from_legacy_named("node_generic_forward_unbounded").expect("node id");
        let inner_request =
            ObjectRpcRequest::encode_body("/test/unbounded", "test", &()).expect("inner request");
        let request = object_rpc_forward_request(
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            &target_node_id,
            &inner_request,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            None,
            true,
        )
        .expect("forward request");
        let payload: DaemonObjectRpcForwardRequest =
            request.decode_body().expect("forward payload");

        assert_eq!(payload.overall_timeout_millis, None);
    }

    #[test]
    fn generic_forward_moves_a_large_unary_value_out_of_the_control_head() {
        let target_node_id =
            NodeId::from_legacy_named("node_generic_forward_large_unary").expect("node id");
        let request = ObjectRpcRequest::from_encoded_body(
            "/cluster/raft/append",
            "cluster.raft",
            vec![0x5a; 344_208],
        );
        let call = ObjectRpcUnarySessionAdapter::encode_request(request)
            .expect("adapt large unary request");
        let outer = object_rpc_forward_request(
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            &target_node_id,
            &call.request,
            Duration::from_secs(5),
            None,
            false,
        )
        .expect("encode generic forward head");

        assert_eq!(call.body.total_bytes(), Some(344_208));
        assert!(call.request.body.is_empty());
        assert!(
            outer.body.len() < sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
            "the generic forward control head must remain bounded independently of the unary value"
        );
    }

    #[test]
    fn forward_response_envelope_preserves_trusted_delivery_state() {
        for (wire_state, network_state) in [
            (
                DaemonObjectRpcForwardDeliveryState::DefinitelyNotSent,
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            ),
            (
                DaemonObjectRpcForwardDeliveryState::PossiblyApplied,
                sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
            ),
        ] {
            let outer = ObjectRpcResponse::encode_body(
                DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                &DaemonObjectRpcForwardResponse::NetworkError {
                    detail: "forward failed".to_string(),
                    delivery_state: Some(wire_state),
                },
            )
            .expect("outer response");
            let error = decode_daemon_object_rpc_forward_response(&outer)
                .expect_err("forward response must fail");

            assert!(matches!(
                error,
                DaemonApiError::ForwardNetwork {
                    delivery_state: Some(state),
                    ..
                } if state == network_state
            ));
        }
    }

    #[tokio::test]
    async fn forward_session_response_preserves_trusted_delivery_state() {
        let outer = ObjectRpcResponse::encode_body(
            DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            &DaemonObjectRpcForwardResponse::NetworkError {
                detail: "session forward failed".to_string(),
                delivery_state: Some(DaemonObjectRpcForwardDeliveryState::PossiblyApplied),
            },
        )
        .expect("outer response");
        let encoded = ObjectRpcUnarySessionAdapter::encode_response(
            sinomo_core::network::ObjectRpcSessionResponse::buffered(outer),
        )
        .expect("stream outer network-error envelope");
        let body = encoded
            .body
            .collect_to_vec(DAEMON_API_UNARY_COMPATIBILITY_MAX_BODY_BYTES)
            .await
            .expect("collect encoded outer network-error envelope");
        let error = decode_daemon_object_rpc_forward_session_response(ObjectRpcSessionResult::new(
            encoded.response,
            sinomo_core::network::ObjectRpcIncomingBody::bytes(body),
        ))
        .await
        .expect_err("forward session response must fail");

        assert!(matches!(
            error,
            DaemonApiError::ForwardNetwork {
                delivery_state: Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn forward_session_response_decodes_streamed_unary_outer_envelope() {
        let inner = ObjectRpcResponse::encode_error(
            "/remote-management/system/info",
            "device authorization was denied",
        )
        .expect("inner error response");
        let outer = ObjectRpcResponse::encode_body(
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            &DaemonObjectRpcForwardResponse::Success {
                response: inner.clone(),
            },
        )
        .expect("outer response");
        let encoded = ObjectRpcUnarySessionAdapter::encode_response(
            sinomo_core::network::ObjectRpcSessionResponse::buffered(outer),
        )
        .expect("stream outer unary envelope");
        let body = encoded
            .body
            .collect_to_vec(DAEMON_API_UNARY_COMPATIBILITY_MAX_BODY_BYTES)
            .await
            .expect("collect encoded outer envelope");
        let decoded =
            decode_daemon_object_rpc_forward_session_response(ObjectRpcSessionResult::new(
                encoded.response,
                sinomo_core::network::ObjectRpcIncomingBody::bytes(body),
            ))
            .await
            .expect("decode streamed outer envelope");

        assert_eq!(decoded.response, inner);
        assert!(decoded.body.is_empty());
    }

    #[test]
    fn forward_response_decoder_accepts_legacy_inner_response_body() {
        let inner = ObjectRpcResponse::encode_body("/test/legacy", &"ok").expect("inner response");
        let outer = ObjectRpcResponse::encode_body(DAEMON_OBJECT_RPC_FORWARD_ROUTE, &inner)
            .expect("legacy outer response");

        assert_eq!(
            decode_daemon_object_rpc_forward_response(&outer).expect("legacy response"),
            inner
        );
    }

    #[test]
    fn dfs_forward_session_preserves_the_client_outer_health_timeout() {
        let client =
            DaemonApiClient::at_address("127.0.0.1:21353").with_timeout(Duration::from_secs(9));
        let timeouts = client.object_rpc_forward_control_timeouts(None);

        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(9)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(9)));
    }

    #[tokio::test]
    async fn daemon_dfs_peer_transport_dispatches_local_target_directly_with_bodies() {
        let root = sinomo_testdir::TestDir::new("sinomo-api-dfs-local-target-session");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main identity");
        let local_node =
            NodeId::from_legacy_named("node_api_dfs_local_target_session").expect("local node");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_api_dfs_local_target_session")
                .expect("cluster id"),
            "API DFS local target session",
            local_node.clone(),
            "local",
            identity.public_key,
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let request_body = vec![0x41_u8; 512 * 1024 + 31];
        let response_body = vec![0x52_u8; 768 * 1024 + 47];
        let expected_request = request_body.clone();
        let server_response = response_body.clone();
        let server_data_dir = data_dir.clone();
        let server = tokio::spawn(async move {
            let stream = listener
                .accept()
                .await
                .expect("accept local daemon session");
            let (incoming, responder) = sinomo_core::network::accept_object_rpc_server_session(
                Box::new(stream.into_inner()),
                server_data_dir,
                sinomo_core::network::NetworkCallPolicy::default(),
                |_request, _node_id, _public_key| Ok(()),
            )
            .await
            .expect("accept authenticated session");
            assert_eq!(
                incoming.request.route,
                sinomo_core::kv::CLUSTER_KV_TXN_ROUTE
            );
            assert_eq!(
                incoming
                    .body
                    .collect_to_vec(1024 * 1024)
                    .await
                    .expect("local request body"),
                expected_request
            );
            responder
                .respond(sinomo_core::network::ObjectRpcSessionResponse {
                    response: ObjectRpcResponse::encode_body(
                        sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
                        &"local-ok",
                    )
                    .expect("local response head"),
                    body: sinomo_core::network::ObjectRpcBodySource::bytes(server_response),
                })
                .await
                .expect("local response");
        });

        let transport = DaemonDfsPeerTransport::at_address(data_dir, address, local_node.clone());
        let request = ObjectRpcRequest::encode_body(
            sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &"local-request",
        )
        .expect("local request head");
        let result = sinomo_dfs::AsyncDfsPeerTransport::call_object_rpc_session_async(
            &transport,
            &local_node,
            ObjectRpcSessionCall::new(
                request,
                sinomo_core::network::ObjectRpcBodySource::bytes(request_body),
            ),
            sinomo_dfs::DfsObjectRpcSendOptions::default()
                .with_overall_timeout(Duration::from_secs(30)),
        )
        .await
        .expect("local target session");
        server.await.expect("server task");

        assert_eq!(
            result
                .response
                .decode_body::<String>()
                .expect("local response"),
            "local-ok"
        );
        assert_eq!(
            result
                .body
                .collect_to_vec(1024 * 1024)
                .await
                .expect("local response body"),
            response_body
        );
    }
}
