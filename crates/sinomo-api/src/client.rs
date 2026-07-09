//! Typed client for the running daemon's control API.

use crate::{
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE, DAEMON_OBJECT_RPC_FORWARD_ROUTE, DAEMON_RPC_CATEGORY,
    DAEMON_SHUTDOWN_ROUTE, DAEMON_STATUS_ROUTE, DaemonApiError, DaemonObjectRpcForwardRequest,
    DaemonPaths, DaemonProbeStatus, DaemonStatus, probe_daemon_status,
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
use sinomo_core::network::{AsyncDaemonControlStream, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_core::node::NodeId;
use std::time::Duration;

/// How long daemon discovery waits for the status probe.
pub const DAEMON_API_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
/// Default connect+idle deadline for daemon API calls.
pub const DAEMON_API_DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Connect+idle deadline for cluster-control calls. The daemon-side handlers
/// apply their remote budgets sequentially, so the client must wait with
/// headroom; no response bytes arrive until the handler finishes.
pub const DAEMON_API_CLUSTER_CONTROL_TIMEOUT: Duration = Duration::from_secs(150);
const OBJECT_RPC_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Typed daemon API client: daemon discovery plus signed object RPC.
#[derive(Clone, Debug)]
pub struct DaemonApiClient {
    address: String,
    timeout: Duration,
    /// Set by [`Self::with_timeout`]: an explicit deadline also replaces the
    /// wider cluster-control default.
    timeout_overridden: bool,
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
        match probe_daemon_status(paths, DAEMON_API_PROBE_TIMEOUT) {
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
            timeout: DAEMON_API_DEFAULT_TIMEOUT,
            timeout_overridden: false,
        }
    }

    /// Overrides the connect+idle deadline used for every call, including
    /// cluster-control calls.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self.timeout_overridden = true;
        self
    }

    fn cluster_control_timeout(&self) -> Duration {
        if self.timeout_overridden {
            self.timeout
        } else {
            DAEMON_API_CLUSTER_CONTROL_TIMEOUT
        }
    }

    fn object_rpc_forward_control_timeouts(&self) -> sinomo_core::network::DaemonRpcTimeouts {
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout)
    }

    fn object_rpc_forward_stream_control_timeouts(
        &self,
    ) -> sinomo_core::network::DaemonRpcTimeouts {
        let idle_timeout = if self.timeout_overridden {
            self.timeout
        } else {
            OBJECT_RPC_TRANSFER_IDLE_TIMEOUT
        };
        sinomo_core::network::DaemonRpcTimeouts::connect_only(self.timeout)
            .with_idle_timeout(Some(idle_timeout))
    }

    fn dfs_object_rpc_forward_control_timeouts(
        &self,
        connect_timeout: Duration,
    ) -> sinomo_core::network::DaemonRpcTimeouts {
        let idle_timeout = if self.timeout_overridden {
            self.timeout
        } else {
            connect_timeout
        };
        sinomo_core::network::DaemonRpcTimeouts::connect_only(self.timeout)
            .with_idle_timeout(Some(idle_timeout))
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
        if sinomo_core::network::object_rpc_request_stream_mode(request)
            .expects_raw_streamed_response()
        {
            return self.object_rpc_to_target_raw_response(
                DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                target_node_id,
                request,
                connect_timeout,
                observe_connectivity,
                self.object_rpc_forward_stream_control_timeouts(),
            );
        }
        let request = object_rpc_forward_request(
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            target_node_id,
            request,
            connect_timeout,
            observe_connectivity,
            false,
        )?;
        let response = send_daemon_rpc_with_timeouts(
            &self.address,
            &request,
            self.object_rpc_forward_control_timeouts(),
        )?;
        response.error_detail()?;
        response.decode_body().map_err(DaemonApiError::from)
    }

    fn object_rpc_to_target_raw_response(
        &self,
        forward_route: &str,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        observe_connectivity: bool,
        control_timeouts: sinomo_core::network::DaemonRpcTimeouts,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let request = object_rpc_forward_request(
            forward_route,
            target_node_id,
            request,
            connect_timeout,
            observe_connectivity,
            true,
        )?;
        let response = send_daemon_rpc_with_timeouts(&self.address, &request, control_timeouts)?;
        response.error_detail()?;
        Ok(response)
    }

    /// Sends a DFS object-RPC from inside the running local daemon.
    pub fn dfs_object_rpc_to_target(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        if sinomo_core::network::object_rpc_request_stream_mode(request)
            .expects_raw_streamed_response()
        {
            return self.object_rpc_to_target_raw_response(
                DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
                target_node_id,
                request,
                connect_timeout,
                observe_connectivity,
                self.dfs_object_rpc_forward_control_timeouts(connect_timeout),
            );
        }
        let request = object_rpc_forward_request(
            DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            target_node_id,
            request,
            connect_timeout,
            observe_connectivity,
            false,
        )?;
        let response = send_daemon_rpc_with_timeouts(
            &self.address,
            &request,
            self.dfs_object_rpc_forward_control_timeouts(connect_timeout),
        )?;
        response.error_detail()?;
        response.decode_body().map_err(DaemonApiError::from)
    }

    fn call_control_payload(
        &self,
        payload: ClusterControlRpcPayload,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let request =
            ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
        let response = send_daemon_rpc(&self.address, &request, self.cluster_control_timeout())?;
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
    observe_connectivity: bool,
    raw_response: bool,
) -> Result<ObjectRpcRequest, DaemonApiError> {
    let payload = DaemonObjectRpcForwardRequest {
        target_node_id: target_node_id.clone(),
        request: request.clone(),
        connect_timeout_millis: u64::try_from(connect_timeout.as_millis()).unwrap_or(u64::MAX),
        observe_connectivity,
        raw_response,
    };
    let mut request = ObjectRpcRequest::encode_body(route, DAEMON_RPC_CATEGORY, &payload)?;
    if raw_response {
        sinomo_core::network::set_object_rpc_request_stream_mode(
            &mut request,
            sinomo_core::network::ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        );
    }
    Ok(request)
}

pub fn send_daemon_rpc(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    let signed = sinomo_core::network::sign_daemon_rpc_request_for_local_node(request)?;
    send_daemon_rpc_raw(address, Some(&signed), request, timeout)
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
    let signed = sinomo_core::network::sign_daemon_rpc_request_for_local_node(request)?;
    send_daemon_rpc_raw_with_timeouts(address, Some(&signed), request, timeouts)
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
    timeout: Duration,
    mut on_response: impl FnMut(ObjectRpcResponse) -> Result<bool, DaemonApiError>,
) -> Result<(), DaemonApiError> {
    block_on_daemon_client_stream(send_daemon_rpc_stream_async(
        address,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_only(timeout),
        &mut on_response,
    ))
}

pub async fn send_daemon_rpc_stream_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    mut on_response: impl FnMut(ObjectRpcResponse) -> Result<bool, DaemonApiError>,
) -> Result<(), DaemonApiError> {
    let route = request.route.clone();
    let category = request.category.clone();
    let body_sha256 = sinomo_core::network::daemon_rpc_body_sha256(request);
    let signed = tokio::task::spawn_blocking(move || {
        sinomo_core::network::sign_daemon_rpc_request_digest_for_local_node(
            &route,
            &category,
            body_sha256,
        )
    })
    .await
    .map_err(|source| DaemonApiError::Network {
        address: address.to_string(),
        detail: format!("daemon streaming RPC signing task failed: {source}"),
    })??;
    let control_request = request.clone().with_auth(signed);
    let mut stream = AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
    let frame_options =
        sinomo_core::network::ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    stream
        .write_request(address, &control_request, frame_options)
        .await?;
    stream.shutdown_write(address).await?;
    loop {
        let rpc = stream
            .read_response(
                address,
                sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
                frame_options,
            )
            .await?;
        if !on_response(rpc)? {
            return Ok(());
        }
    }
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
    fn daemon_api_client_address_and_timeout_are_configurable() {
        let client =
            DaemonApiClient::at_address("127.0.0.1:21353").with_timeout(Duration::from_secs(5));
        assert_eq!(client.address(), "127.0.0.1:21353");
        assert_eq!(client.timeout, Duration::from_secs(5));
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
        let timeouts = client.object_rpc_forward_control_timeouts();
        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(6)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(6)));
    }

    #[test]
    fn forwarded_streamed_object_rpc_uses_transfer_idle_by_default() {
        let client = DaemonApiClient::at_address("127.0.0.1:21353");
        let timeouts = client.object_rpc_forward_stream_control_timeouts();
        assert_eq!(timeouts.connect_timeout, Some(DAEMON_API_DEFAULT_TIMEOUT));
        assert_eq!(
            timeouts.idle_timeout,
            Some(OBJECT_RPC_TRANSFER_IDLE_TIMEOUT)
        );
    }

    #[test]
    fn forwarded_streamed_object_rpc_honors_explicit_client_timeout() {
        let client =
            DaemonApiClient::at_address("127.0.0.1:21353").with_timeout(Duration::from_secs(6));
        let timeouts = client.object_rpc_forward_stream_control_timeouts();
        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(6)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(6)));
    }

    #[test]
    fn forwarded_dfs_object_rpc_uses_route_idle_by_default() {
        let client = DaemonApiClient::at_address("127.0.0.1:21353");
        let timeouts = client.dfs_object_rpc_forward_control_timeouts(Duration::from_secs(60));
        assert_eq!(timeouts.connect_timeout, Some(DAEMON_API_DEFAULT_TIMEOUT));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(60)));
    }

    #[test]
    fn forwarded_dfs_object_rpc_honors_explicit_client_timeout() {
        let client =
            DaemonApiClient::at_address("127.0.0.1:21353").with_timeout(Duration::from_secs(6));
        let timeouts = client.dfs_object_rpc_forward_control_timeouts(Duration::from_secs(60));
        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(6)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(6)));
    }

    #[test]
    fn dfs_raw_forward_request_uses_dfs_route() {
        let target_node_id = NodeId::from_legacy_named("node_dfs_forward").expect("node id");
        let inner_request =
            ObjectRpcRequest::encode_body("/dfs/test/stream", "test", &()).expect("inner request");
        let request = object_rpc_forward_request(
            DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            &target_node_id,
            &inner_request,
            Duration::from_secs(5),
            true,
            true,
        )
        .expect("forward request");
        assert_eq!(request.route, DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE);
        assert!(
            sinomo_core::network::object_rpc_request_stream_mode(&request)
                .expects_raw_streamed_response()
        );
        let payload: DaemonObjectRpcForwardRequest =
            request.decode_body().expect("forward payload");
        assert_eq!(payload.target_node_id, target_node_id);
        assert_eq!(payload.request.route, "/dfs/test/stream");
        assert_eq!(payload.connect_timeout_millis, 5_000);
        assert!(payload.observe_connectivity);
        assert!(payload.raw_response);
    }
}
