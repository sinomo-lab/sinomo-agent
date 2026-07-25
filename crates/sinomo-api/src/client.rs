//! Typed client for the running daemon's control API.

use crate::{
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE, DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_NETWORK_CHUNK_METRICS_ROUTE, DAEMON_OBJECT_RPC_FORWARD_ROUTE, DAEMON_RPC_CATEGORY,
    DAEMON_SHUTDOWN_ROUTE, DAEMON_STATUS_ROUTE, DaemonApiError,
    DaemonLocalConnectivityReloadRequest, DaemonLocalConnectivityReloadResponse,
    DaemonNetworkChunkMetricsResponse, DaemonObjectRpcForwardDeliveryState,
    DaemonObjectRpcForwardRequest, DaemonObjectRpcForwardResponse, DaemonPaths, DaemonProbeStatus,
    DaemonStatus, probe_daemon_status,
};
use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE,
    ConfigCatalogRequest, ConfigCatalogResponse, ConfigClient, ConfigDefinition, ConfigError,
    ConfigGetRequest, ConfigGetResponse, ConfigResolution, ConfigSetRequest, ConfigSetResponse,
    ConfigWriteResult, ConfigWriteTarget,
};
use sinomo_core::cluster::{
    CLUSTER_CONTROL_INIT_ROUTE, CLUSTER_CONTROL_INVITE_ROUTE, CLUSTER_CONTROL_JOIN_ROUTE,
    CLUSTER_CONTROL_LEAVE_ROUTE, CLUSTER_CONTROL_REMOVE_ROUTE, CLUSTER_CONTROL_RENAME_ROUTE,
    CLUSTER_CONTROL_ROLE_ROUTE, CLUSTER_CONTROL_RPC_CATEGORY, ClusterControlInitRequest,
    ClusterControlInitResponse, ClusterControlInviteRequest, ClusterControlInviteResponse,
    ClusterControlJoinRequest, ClusterControlJoinResponse, ClusterControlLeaveRequest,
    ClusterControlLeaveResponse, ClusterControlRemoveRequest, ClusterControlRemoveResponse,
    ClusterControlRenameRequest, ClusterControlRenameResponse, ClusterControlRoleRequest,
    ClusterControlRoleResponse, ClusterHealthRow,
};
use sinomo_core::kv::{
    CLUSTER_KV_REQUEST_OUTCOME_ROUTE, CLUSTER_KV_RPC_CATEGORY, ClusterKvRequestOutcomeRead,
    ClusterKvRequestOutcomeRequest, KvRequestId, KvRequestOutcome,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY, ObjectRpcCall, ObjectRpcRequest, ObjectRpcResponse,
    ObjectRpcResult,
};
use sinomo_core::node::NodeId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long daemon discovery waits for the status probe.
pub const DAEMON_API_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
/// Typed daemon API client: daemon discovery plus object RPC.
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
        // Authenticated keepalives report transport health while the daemon
        // waits for the inner routed call, so its semantic deadline must not
        // become an outer no-progress timeout.
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout)
    }

    /// The daemon control address this client talks to.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Samples the running daemon's process-local ownership-native network
    /// allocator. This is a local-command diagnostic; it does not query or
    /// aggregate remote cluster members.
    pub async fn network_chunk_metrics_async(
        &self,
    ) -> Result<DaemonNetworkChunkMetricsResponse, DaemonApiError> {
        let request = ObjectRpcRequest::encode_body(
            DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
            DAEMON_RPC_CATEGORY,
            &(),
        )?;
        let runtime = tokio::runtime::Handle::current();
        send_daemon_rpc_result_async(
            &self.address,
            &request,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout),
            &runtime,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    }

    /// Generic object-RPC passthrough for routes without a typed
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

    /// Generic object-RPC passthrough for an already built request.
    pub fn call_request(
        &self,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let response = send_daemon_rpc(&self.address, request, self.timeout)?;
        response.error_detail()?;
        Ok(response)
    }

    fn call_request_value<T>(
        &self,
        request: &ObjectRpcRequest,
        timeouts: sinomo_core::network::DaemonRpcTimeouts,
    ) -> Result<T, DaemonApiError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        send_daemon_rpc_value(&self.address, request, timeouts)
    }

    fn call_value<T>(&self, request: &ObjectRpcRequest) -> Result<T, DaemonApiError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.call_request_value(
            request,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout),
        )
    }

    fn call_encoded_value<Req, Resp>(
        &self,
        route: &str,
        category: &str,
        value: Req,
        timeouts: sinomo_core::network::DaemonRpcTimeouts,
    ) -> Result<Resp, DaemonApiError>
    where
        Req: serde::Serialize + Send + 'static,
        Resp: serde::de::DeserializeOwned + Send + 'static,
    {
        send_daemon_typed_value(&self.address, route, category, value, timeouts)
    }

    /// Resolves one cluster-KV request id after the daemon establishes a fresh
    /// current-term quorum barrier.
    pub fn cluster_kv_request_outcome(
        &self,
        request_id: &KvRequestId,
    ) -> Result<ClusterKvRequestOutcomeRead, DaemonApiError> {
        let mut call = ObjectRpcCall::encode_value(
            CLUSTER_KV_REQUEST_OUTCOME_ROUTE,
            CLUSTER_KV_RPC_CATEGORY,
            ClusterKvRequestOutcomeRequest {
                request_id: request_id.clone(),
            },
        )?;
        call.request = call.request.with_response_retry_safe();
        let read = send_daemon_call_value(
            &self.address,
            call,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout),
        )?;
        validate_cluster_kv_request_outcome_response(read, request_id)
    }

    /// Sends an object RPC through the connected daemon to another cluster
    /// node using the stream-native routing path.
    pub fn object_rpc_to_target_from_data_dir(
        &self,
        data_dir: &SinomoDataDir,
        target_node_id: &NodeId,
        request: ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let address = self.address.clone();
        let data_dir = data_dir.clone();
        let target_node_id = target_node_id.clone();
        block_on_daemon_client(async move {
            let runtime = tokio::runtime::Handle::current();
            let result = send_daemon_object_rpc_call_async(
                data_dir,
                &address,
                DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                &target_node_id,
                ObjectRpcCall::value(request),
                timeout,
                None,
                Some(timeout),
                false,
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
                None,
                &runtime,
            )
            .await?;
            decode_object_rpc_value_response(result).await
        })
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
    /// route-wide semantic deadline. The same wall-clock budget bounds both
    /// the daemon's forwarded request and the caller's enclosing loopback
    /// session; authenticated keepalives remain transport-health signals and
    /// cannot extend that budget.
    pub fn object_rpc_to_target_with_deadline(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        block_on_daemon_client(self.object_rpc_to_target_with_deadline_async(
            target_node_id,
            request,
            connect_timeout,
            overall_timeout,
            observe_connectivity,
        ))
    }

    /// Asynchronously sends an object-RPC value through the running local
    /// daemon. The stream-native protocol carries the value independently of
    /// its size and restores it into the response value before returning.
    pub async fn object_rpc_to_target_with_deadline_async(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let call = ObjectRpcCall::value(request.clone());
        let result = self
            .object_rpc_call_to_target_with_deadline_async(
                target_node_id,
                call,
                connect_timeout,
                overall_timeout,
                observe_connectivity,
            )
            .await?;
        decode_object_rpc_value_response(result).await
    }

    /// Sends a value RPC through the local daemon and returns the elapsed time
    /// measured around the daemon's inner routed call. The duration excludes
    /// the loopback proxy request and is intended for transport diagnostics
    /// where the cluster path, rather than CLI-to-daemon IPC, is under test.
    pub async fn object_rpc_to_target_with_forward_elapsed_async(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
    ) -> Result<(ObjectRpcResponse, Duration), DaemonApiError> {
        let call = ObjectRpcCall::value(request.clone());
        let (result, forward_elapsed) = self
            .object_rpc_call_to_target_with_forward_elapsed_async(
                target_node_id,
                call,
                connect_timeout,
                overall_timeout,
                observe_connectivity,
            )
            .await?;
        let response = decode_object_rpc_value_response(result).await?;
        Ok((response, forward_elapsed))
    }

    /// Sends one stream-native object-RPC call through the local daemon and
    /// returns the elapsed time measured around the daemon's inner routed
    /// call. The result retains its owned response body so callers can decode
    /// a large typed value incrementally.
    pub async fn object_rpc_call_to_target_with_forward_elapsed_async(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
    ) -> Result<(ObjectRpcResult, Duration), DaemonApiError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        let runtime = tokio::runtime::Handle::current();
        let (result, forward_elapsed) = send_daemon_object_rpc_call_with_forward_elapsed_async(
            data_dir,
            &self.address,
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            target_node_id,
            call,
            connect_timeout,
            None,
            overall_timeout,
            observe_connectivity,
            self.object_rpc_forward_control_timeouts(overall_timeout),
            None,
            &runtime,
        )
        .await?;
        let forward_elapsed = forward_elapsed.ok_or_else(|| DaemonApiError::Parse {
            detail: "forward elapsed time is unavailable for a local daemon RPC".to_string(),
        })?;
        Ok((result, forward_elapsed))
    }

    /// Sends one stream-native object-RPC call through the running local daemon
    /// without adding an operation-wide semantic deadline.
    pub async fn object_rpc_call_to_target_async(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        connect_timeout: Duration,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, DaemonApiError> {
        self.object_rpc_call_to_target_with_deadline_async(
            target_node_id,
            call,
            connect_timeout,
            None,
            observe_connectivity,
        )
        .await
    }

    /// Sends one stream-native object-RPC call through the running local daemon.
    /// Ordinary values are represented by [`ObjectRpcCall::value`];
    /// explicit attachments remain streams.
    pub async fn object_rpc_call_to_target_with_deadline_async(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, DaemonApiError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        let runtime = tokio::runtime::Handle::current();
        send_daemon_object_rpc_call_async(
            data_dir,
            &self.address,
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            target_node_id,
            call,
            connect_timeout,
            None,
            overall_timeout,
            observe_connectivity,
            self.object_rpc_forward_control_timeouts(overall_timeout),
            None,
            &runtime,
        )
        .await
    }

    /// Sends one stream-native object-RPC call through the running local daemon
    /// with distinct connection and no-progress budgets. Both transport
    /// limits must be present; `overall_timeout` remains an independent
    /// semantic deadline and may be unbounded.
    pub async fn object_rpc_call_to_target_with_transport_timeouts_async(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        transport_timeouts: sinomo_core::network::DaemonRpcTimeouts,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, DaemonApiError> {
        let connect_timeout =
            transport_timeouts
                .connect_timeout
                .ok_or_else(|| DaemonApiError::Parse {
                    detail: "daemon-forwarded object RPC requires a connection timeout".to_string(),
                })?;
        let idle_timeout =
            transport_timeouts
                .idle_timeout
                .ok_or_else(|| DaemonApiError::Parse {
                    detail: "daemon-forwarded object RPC requires a no-progress timeout"
                        .to_string(),
                })?;
        let data_dir = SinomoDataDir::from_env_or_home()?;
        let runtime = tokio::runtime::Handle::current();
        send_daemon_object_rpc_call_async(
            data_dir,
            &self.address,
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            target_node_id,
            call,
            connect_timeout,
            Some(idle_timeout),
            overall_timeout,
            observe_connectivity,
            transport_timeouts,
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
            let call = ObjectRpcCall::value(request);
            let result = send_daemon_object_rpc_call_async(
                data_dir,
                &address,
                DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
                &target_node_id,
                call,
                options.connect_timeout,
                None,
                options.overall_timeout,
                options.observe_connectivity,
                outer_timeouts,
                None,
                &runtime,
            )
            .await?;
            decode_object_rpc_value_response(result).await
        })
    }

    fn call_control_value<Req, Resp>(&self, route: &str, value: Req) -> Result<Resp, DaemonApiError>
    where
        Req: serde::Serialize + Send + 'static,
        Resp: serde::de::DeserializeOwned + Send + 'static,
    {
        self.call_encoded_value(
            route,
            CLUSTER_CONTROL_RPC_CATEGORY,
            value,
            self.cluster_control_timeouts(),
        )
    }

    /// Fetches the daemon's live status.
    pub fn status(&self) -> Result<DaemonStatus, DaemonApiError> {
        self.call_encoded_value(
            DAEMON_STATUS_ROUTE,
            DAEMON_RPC_CATEGORY,
            (),
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout),
        )
    }

    /// Requests a daemon shutdown; returns the daemon's acknowledgement line.
    pub fn shutdown(&self) -> Result<String, DaemonApiError> {
        self.call_encoded_value(
            DAEMON_SHUTDOWN_ROUTE,
            DAEMON_RPC_CATEGORY,
            (),
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout),
        )
    }

    /// Drops the daemon process's cached local connectivity snapshot so its
    /// next route read reloads state written through the shared data directory.
    pub fn reload_local_connectivity(
        &self,
    ) -> Result<DaemonLocalConnectivityReloadResponse, DaemonApiError> {
        self.call_encoded_value(
            DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
            DAEMON_RPC_CATEGORY,
            DaemonLocalConnectivityReloadRequest,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout),
        )
    }

    /// Initializes a fresh single-node cluster on the daemon's node.
    pub fn cluster_init(
        &self,
        request: ClusterControlInitRequest,
    ) -> Result<ClusterControlInitResponse, DaemonApiError> {
        self.call_control_value(CLUSTER_CONTROL_INIT_ROUTE, request)
    }

    /// Joins an existing cluster from an invite.
    pub fn cluster_join(
        &self,
        request: ClusterControlJoinRequest,
    ) -> Result<ClusterControlJoinResponse, DaemonApiError> {
        self.call_control_value(CLUSTER_CONTROL_JOIN_ROUTE, request)
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
        self.call_control_value(CLUSTER_CONTROL_INVITE_ROUTE, request)
    }

    /// Renames a cluster member (the daemon's local node by default).
    pub fn cluster_rename(
        &self,
        request: ClusterControlRenameRequest,
    ) -> Result<ClusterControlRenameResponse, DaemonApiError> {
        self.call_control_value(CLUSTER_CONTROL_RENAME_ROUTE, request)
    }

    /// Changes a cluster member's node role.
    pub fn cluster_role(
        &self,
        request: ClusterControlRoleRequest,
    ) -> Result<ClusterControlRoleResponse, DaemonApiError> {
        self.call_control_value(CLUSTER_CONTROL_ROLE_ROUTE, request)
    }

    /// Removes cluster members by node id.
    pub fn cluster_remove(
        &self,
        request: ClusterControlRemoveRequest,
    ) -> Result<ClusterControlRemoveResponse, DaemonApiError> {
        self.call_control_value(CLUSTER_CONTROL_REMOVE_ROUTE, request)
    }

    /// Leaves the current cluster; the node re-initializes as a fresh
    /// single-node cluster.
    pub fn cluster_leave(&self) -> Result<ClusterControlLeaveResponse, DaemonApiError> {
        self.call_control_value(CLUSTER_CONTROL_LEAVE_ROUTE, ClusterControlLeaveRequest {})
    }

    /// Fetches cluster health rows from the daemon's KV-backed `/cluster/health`
    /// view.
    ///
    /// Background connectivity probes publish node status and reachability into
    /// cluster KV. Foreground status readers should use that source of truth
    /// rather than spending their command budget on a live network sweep.
    pub fn cluster_health(&self) -> Result<Vec<ClusterHealthRow>, DaemonApiError> {
        let request = cluster_health_snapshot_request()?;
        self.call_value(&request)
    }

    /// Fetches cluster health rows with live member probes and peer-local view
    /// merging. This is useful for diagnostics but can be slower than the
    /// KV-backed snapshot used by [`Self::cluster_health`].
    pub fn cluster_health_with_peer_views(&self) -> Result<Vec<ClusterHealthRow>, DaemonApiError> {
        let request = cluster_health_live_request()?;
        self.call_value(&request)
    }
}

fn validate_cluster_kv_request_outcome_response(
    read: ClusterKvRequestOutcomeRead,
    request_id: &KvRequestId,
) -> Result<ClusterKvRequestOutcomeRead, DaemonApiError> {
    if let KvRequestOutcome::Committed(record) = &read.outcome
        && record.request_id != *request_id
    {
        return Err(DaemonApiError::Parse {
            detail: format!(
                "cluster KV outcome response is for request ID {}, expected {}",
                record.request_id, request_id
            ),
        });
    }
    Ok(read)
}

fn cluster_health_snapshot_request() -> Result<ObjectRpcRequest, DaemonApiError> {
    sinomo_core::cluster::cluster_health_rpc_request_without_peer_views().map_err(Into::into)
}

fn cluster_health_live_request() -> Result<ObjectRpcRequest, DaemonApiError> {
    sinomo_core::cluster::cluster_health_rpc_request().map_err(Into::into)
}

#[cfg(test)]
fn object_rpc_forward_request(
    route: &str,
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    connect_timeout: Duration,
    idle_timeout: Option<Duration>,
    overall_timeout: Option<Duration>,
    observe_connectivity: bool,
) -> Result<ObjectRpcRequest, DaemonApiError> {
    let payload = object_rpc_forward_payload(
        target_node_id,
        request,
        false,
        connect_timeout,
        idle_timeout,
        overall_timeout,
        observe_connectivity,
    );
    let outer = ObjectRpcRequest::encode_body(route, DAEMON_RPC_CATEGORY, &payload)
        .map_err(DaemonApiError::from)?;
    Ok(with_object_rpc_forward_trace_metadata(outer, request))
}

fn object_rpc_forward_payload(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    streamed_value_body: bool,
    connect_timeout: Duration,
    idle_timeout: Option<Duration>,
    overall_timeout: Option<Duration>,
    observe_connectivity: bool,
) -> DaemonObjectRpcForwardRequest {
    DaemonObjectRpcForwardRequest {
        target_node_id: target_node_id.clone(),
        request: request.clone(),
        streamed_value_body,
        connect_timeout_millis: u64::try_from(connect_timeout.as_millis()).unwrap_or(u64::MAX),
        idle_timeout_millis: idle_timeout
            .map(|timeout| u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)),
        overall_timeout_millis: overall_timeout
            .map(|timeout| u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)),
        observe_connectivity,
    }
}

fn with_object_rpc_forward_trace_metadata(
    mut outer: ObjectRpcRequest,
    request: &ObjectRpcRequest,
) -> ObjectRpcRequest {
    if let Some(traceparent) = request.traceparent() {
        outer = outer.with_traceparent(traceparent.to_string());
    }
    if let Some(tracestate) = request.tracestate() {
        outer = outer.with_tracestate(tracestate.to_string());
    }
    if request
        .metadata
        .contains_key(OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY)
    {
        outer = outer.with_observe_sampled(request.observe_sampled());
    }
    outer
}

#[cfg(test)]
fn decode_daemon_object_rpc_forward_response(
    response: &ObjectRpcResponse,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    match response.decode_body::<DaemonObjectRpcForwardResponse>()? {
        DaemonObjectRpcForwardResponse::Success { response, .. } => Ok(response),
        DaemonObjectRpcForwardResponse::NetworkError {
            detail,
            delivery_state,
        } => Err(DaemonApiError::ForwardNetwork {
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

#[cfg(test)]
async fn decode_daemon_object_rpc_forward_call_response(
    result: ObjectRpcResult,
) -> Result<ObjectRpcResult, DaemonApiError> {
    decode_daemon_object_rpc_forward_call_response_with_elapsed(result)
        .await
        .map(|(result, _)| result)
}

async fn decode_daemon_object_rpc_forward_call_response_with_elapsed(
    result: ObjectRpcResult,
) -> Result<(ObjectRpcResult, Duration), DaemonApiError> {
    let (envelope, body) = if result.has_streamed_value() {
        (
            result
                .decode_body::<DaemonObjectRpcForwardResponse>()
                .await?,
            sinomo_core::network::ObjectRpcIncomingBody::empty(),
        )
    } else {
        let (response, body) = result.into_parts();
        response.error_detail()?;
        (
            response.decode_body::<DaemonObjectRpcForwardResponse>()?,
            body,
        )
    };
    match envelope {
        DaemonObjectRpcForwardResponse::Success {
            response,
            streamed_value_body,
            forward_elapsed_micros,
        } => Ok((
            ObjectRpcResult::from_forwarded_body(response, body, streamed_value_body)?,
            Duration::from_micros(forward_elapsed_micros),
        )),
        DaemonObjectRpcForwardResponse::NetworkError {
            detail,
            delivery_state,
        } => Err(daemon_object_rpc_forward_error(detail, delivery_state)),
    }
}

async fn decode_object_rpc_value_response(
    result: ObjectRpcResult,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    result
        .into_materialized_value_response(usize::MAX)
        .await
        .map_err(Into::into)
}

async fn call_loopback_daemon_rpc_async(
    data_dir: SinomoDataDir,
    address: &str,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, DaemonApiError> {
    if !sinomo_core::network::is_loopback_connect_address(address) {
        return Err(DaemonApiError::Network {
            address: address.to_string(),
            detail: "daemon-mediated object RPC requires the local daemon loopback address"
                .to_string(),
        });
    }
    call_daemon_address_rpc_async(data_dir, address, call, options, runtime).await
}

async fn call_loopback_daemon_rpc_with_overall_deadline_async(
    data_dir: SinomoDataDir,
    address: &str,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    overall_deadline: Option<(tokio::time::Instant, Duration)>,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, DaemonApiError> {
    let pending = call_loopback_daemon_rpc_async(data_dir, address, call, options, runtime);
    let Some((deadline_at, timeout)) = overall_deadline else {
        return pending.await;
    };
    tokio::pin!(pending);
    let deadline = tokio::time::sleep_until(deadline_at);
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        _ = &mut deadline => Err(sinomo_core::network::NetworkError::timeout(
            "daemon-mediated object RPC outer call",
            sinomo_core::network::NetworkTimeoutPhase::Overall,
            timeout,
            // The local daemon may already have dispatched the inner request.
            sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
        ).into()),
        result = &mut pending => result,
    }
}

async fn call_daemon_address_rpc_async(
    data_dir: SinomoDataDir,
    address: &str,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    _runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, DaemonApiError> {
    let mut host = DaemonApiRpcHost;
    let mut call = call;
    call.request.auth = None;
    if sinomo_core::network::is_loopback_connect_address(address) {
        return sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
            .call_loopback(&data_dir, address, call, options)
            .await
            .map_err(Into::into);
    }
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let endpoint = daemon_api_rpc_endpoint_for_address(&state, address)?;
    sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths)
        .call_endpoint(&state, &endpoint, call, options)
        .await
        .map_err(Into::into)
}

async fn send_daemon_object_rpc_call_async(
    data_dir: SinomoDataDir,
    address: &str,
    forward_route: &'static str,
    target_node_id: &NodeId,
    call: ObjectRpcCall,
    connect_timeout: Duration,
    forwarded_idle_timeout: Option<Duration>,
    overall_timeout: Option<Duration>,
    observe_connectivity: bool,
    outer_timeouts: sinomo_core::network::DaemonRpcTimeouts,
    known_local_node_id: Option<&NodeId>,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, DaemonApiError> {
    send_daemon_object_rpc_call_with_forward_elapsed_async(
        data_dir,
        address,
        forward_route,
        target_node_id,
        call,
        connect_timeout,
        forwarded_idle_timeout,
        overall_timeout,
        observe_connectivity,
        outer_timeouts,
        known_local_node_id,
        runtime,
    )
    .await
    .map(|(result, _)| result)
}

async fn send_daemon_object_rpc_call_with_forward_elapsed_async(
    data_dir: SinomoDataDir,
    address: &str,
    forward_route: &'static str,
    target_node_id: &NodeId,
    call: ObjectRpcCall,
    connect_timeout: Duration,
    forwarded_idle_timeout: Option<Duration>,
    overall_timeout: Option<Duration>,
    observe_connectivity: bool,
    outer_timeouts: sinomo_core::network::DaemonRpcTimeouts,
    known_local_node_id: Option<&NodeId>,
    runtime: &tokio::runtime::Handle,
) -> Result<(ObjectRpcResult, Option<Duration>), DaemonApiError> {
    let overall_deadline =
        overall_timeout.map(|timeout| (tokio::time::Instant::now() + timeout, timeout));
    let target_is_local = match known_local_node_id {
        Some(local_node_id) => target_node_id == local_node_id,
        None => target_node_id == &cached_daemon_api_local_node_id(&data_dir)?,
    };
    let mut outer_options = sinomo_core::network::ClusterObjectRpcOptions::new(outer_timeouts);
    if let Some(overall_timeout) = overall_timeout {
        outer_options = outer_options.with_overall_timeout(overall_timeout);
    }
    if target_is_local {
        return call_loopback_daemon_rpc_with_overall_deadline_async(
            data_dir,
            address,
            call,
            outer_options,
            overall_deadline,
            runtime,
        )
        .await
        .map(|result| (result, None));
    }
    let forward_payload = object_rpc_forward_payload(
        target_node_id,
        &call.request,
        call.has_streamed_value_body(),
        connect_timeout,
        forwarded_idle_timeout,
        overall_timeout,
        observe_connectivity,
    );
    let outer = if call.body.is_empty() {
        let mut outer =
            ObjectRpcCall::encode_value(forward_route, DAEMON_RPC_CATEGORY, forward_payload)?;
        outer.request = with_object_rpc_forward_trace_metadata(outer.request, &call.request);
        outer
    } else {
        let forward_request = with_object_rpc_forward_trace_metadata(
            ObjectRpcRequest::encode_body(forward_route, DAEMON_RPC_CATEGORY, &forward_payload)?,
            &call.request,
        );
        ObjectRpcCall::new(forward_request, call.body)
    };
    let result = call_loopback_daemon_rpc_with_overall_deadline_async(
        data_dir,
        address,
        outer,
        outer_options,
        overall_deadline,
        runtime,
    )
    .await?;
    let (result, elapsed) =
        decode_daemon_object_rpc_forward_call_response_with_elapsed(result).await?;
    Ok((result, Some(elapsed)))
}

/// DFS peer transport that deliberately terminates at the running local
/// daemon. The daemon owns endpoint selection, retries, authentication, and
/// cluster streams; mounted clients never open a competing cluster
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

    async fn call_rpc_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<ObjectRpcResult, DaemonApiError> {
        send_daemon_object_rpc_call_async(
            self.data_dir.clone(),
            &self.address,
            DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            target_node_id,
            call,
            options.connect_timeout,
            None,
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
        call: ObjectRpcCall,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, sinomo_dfs::DfsPeerTransportError> {
        let runtime = self.runtime.handle().clone();
        block_on_daemon_client_on_owner_runtime(&runtime, async {
            self.call_rpc_inner(target_node_id, call, options, &runtime)
                .await
        })
        .and_then(|result| {
            result.response.error_detail()?;
            Ok(result)
        })
        .map_err(|source| {
            sinomo_dfs::DfsPeerTransportError::with_delivery_state(
                source.to_string(),
                source.delivery_state(),
            )
        })
    }
}

impl sinomo_dfs::AsyncDfsPeerTransport for DaemonDfsPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<
        'a,
        Result<ObjectRpcResult, sinomo_dfs::DfsPeerTransportError>,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|source| sinomo_dfs::DfsPeerTransportError::new(source.to_string()))?;
            let result = self
                .call_rpc_inner(target_node_id, call, options, &runtime)
                .await
                .map_err(|source| {
                    sinomo_dfs::DfsPeerTransportError::with_delivery_state(
                        source.to_string(),
                        source.delivery_state(),
                    )
                })?;
            result
                .response
                .error_detail()
                .map_err(sinomo_dfs::DfsPeerTransportError::from_network)?;
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
    let result = send_daemon_rpc_result_async(address, request, timeouts, runtime).await?;
    decode_object_rpc_value_response(result).await
}

async fn send_daemon_rpc_result_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, DaemonApiError> {
    call_daemon_address_rpc_async(
        SinomoDataDir::from_env_or_home()?,
        address,
        ObjectRpcCall::value(request.clone()),
        sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
        runtime,
    )
    .await
}

fn send_daemon_rpc_value<T>(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<T, DaemonApiError>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_daemon_rpc_result_async(address, request, timeouts, &runtime)
            .await?
            .decode_body()
            .await
            .map_err(Into::into)
    })
}

fn send_daemon_typed_value<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    value: Req,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<Resp, DaemonApiError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_value(route, category, value)?;
    send_daemon_call_value(address, call, timeouts)
}

fn send_daemon_call_value<Resp>(
    address: &str,
    call: ObjectRpcCall,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<Resp, DaemonApiError>
where
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        call_daemon_address_rpc_async(
            SinomoDataDir::from_env_or_home()?,
            address,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
            &runtime,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn send_daemon_typed_value_on_runtime<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    value: Req,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<Resp, DaemonApiError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_value(route, category, value)?;
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async {
        call_daemon_address_rpc_async(
            SinomoDataDir::from_env_or_home()?,
            address,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
            &runtime_for_future,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

pub fn send_daemon_rpc_with_timeouts(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<ObjectRpcResponse, DaemonApiError> {
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_daemon_rpc_async(address, request, timeouts, &runtime).await
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

    fn rpc<Req, Resp>(&self, route: &str, body: Req) -> Result<Resp, ConfigError>
    where
        Req: serde::Serialize + Send + 'static,
        Resp: serde::de::DeserializeOwned + Send + 'static,
    {
        let client_error = |detail: String| ConfigError::Client { detail };
        let timeouts = sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout);
        match &self.runtime {
            Some(runtime) => send_daemon_typed_value_on_runtime(
                &self.address,
                route,
                CONFIG_RPC_CATEGORY,
                body,
                timeouts,
                runtime,
            ),
            None => {
                send_daemon_typed_value(&self.address, route, CONFIG_RPC_CATEGORY, body, timeouts)
            }
        }
        .map_err(|error| client_error(error.to_string()))
    }

    /// Sets configuration with a stable cluster-KV request identity that the
    /// caller can later reconcile through [`DaemonApiClient::cluster_kv_request_outcome`].
    pub fn set_with_request_id(
        &self,
        key: &str,
        value: serde_json::Value,
        target: ConfigWriteTarget,
        request_id: KvRequestId,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let response: ConfigSetResponse = self.rpc(
            CONFIG_SET_ROUTE,
            ConfigSetRequest {
                request_id: Some(request_id),
                key: key.to_string(),
                value,
                target,
            },
        )?;
        Ok(response.result)
    }
}

impl ConfigClient for RemoteConfigClient {
    fn catalog(&self, owner: Option<&str>) -> Result<Vec<ConfigDefinition>, ConfigError> {
        let response: ConfigCatalogResponse = self.rpc(
            CONFIG_CATALOG_ROUTE,
            ConfigCatalogRequest {
                owner: owner.map(str::to_string),
            },
        )?;
        Ok(response.definitions)
    }

    fn get(&self, key: &str) -> Result<ConfigResolution, ConfigError> {
        let response: ConfigGetResponse = self.rpc(
            CONFIG_GET_ROUTE,
            ConfigGetRequest {
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
        self.set_with_request_id(key, value, target, KvRequestId::generate())
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
    let mut request = request.clone();
    request.auth = None;
    let call = ObjectRpcCall::value(request);
    let options = sinomo_core::network::ClusterObjectRpcOptions::new(timeouts);
    let result = call_daemon_address_rpc_async(data_dir, address, call, options, &runtime).await?;
    result.response.error_detail()?;
    let mut body = result.body;
    loop {
        let rpc = sinomo_core::network::read_object_frame_async(
            &mut body,
            sinomo_core::network::ObjectFrameIoOptions::none(),
        )
        .await?;
        if !on_response(rpc)? {
            return Ok(());
        }
    }
}

struct DaemonApiRpcHost;

const DAEMON_API_CANONICAL_ROOT_CACHE_MAX_ENTRIES: usize = 64;

#[derive(Clone)]
struct DaemonApiCanonicalRootCacheEntry {
    canonical: PathBuf,
    last_used: u64,
}

#[derive(Default)]
struct DaemonApiCanonicalRootCache {
    entries: HashMap<PathBuf, DaemonApiCanonicalRootCacheEntry>,
    sequence: u64,
}

impl DaemonApiCanonicalRootCache {
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.wrapping_add(1);
        self.sequence
    }

    fn get(&mut self, root: &PathBuf) -> Option<PathBuf> {
        let sequence = self.next_sequence();
        let entry = self.entries.get_mut(root)?;
        entry.last_used = sequence;
        Some(entry.canonical.clone())
    }

    fn insert(&mut self, root: PathBuf, canonical: PathBuf) {
        let sequence = self.next_sequence();
        if !self.entries.contains_key(&root)
            && self.entries.len() >= DAEMON_API_CANONICAL_ROOT_CACHE_MAX_ENTRIES
        {
            let candidate = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(root, _)| root.clone());
            if let Some(candidate) = candidate {
                self.entries.remove(&candidate);
            }
        }
        self.entries.insert(
            root,
            DaemonApiCanonicalRootCacheEntry {
                canonical,
                last_used: sequence,
            },
        );
    }
}

fn daemon_api_canonical_root_cache() -> &'static Mutex<DaemonApiCanonicalRootCache> {
    static ROOTS: OnceLock<Mutex<DaemonApiCanonicalRootCache>> = OnceLock::new();
    ROOTS.get_or_init(|| Mutex::new(DaemonApiCanonicalRootCache::default()))
}

fn canonical_daemon_api_data_dir_root(data_dir: &SinomoDataDir) -> PathBuf {
    let root = data_dir.root();
    if let Ok(mut roots) = daemon_api_canonical_root_cache().lock()
        && let Some(canonical) = roots.get(&root)
    {
        return canonical;
    }
    let canonical = std::fs::canonicalize(&root).unwrap_or_else(|_| root.clone());
    if let Ok(mut roots) = daemon_api_canonical_root_cache().lock() {
        roots.insert(root, canonical.clone());
    }
    canonical
}

const DAEMON_API_LOCAL_NODE_ID_CACHE_MAX_ENTRIES: usize = 64;
const DAEMON_API_LOCAL_NODE_ID_CACHE_MAX_AGE: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct DaemonApiLocalNodeIdCacheEntry {
    state_mark: sinomo_core::network::ClusterFileMark,
    node_id: NodeId,
    checked_at: Instant,
    last_used: u64,
}

#[derive(Default)]
struct DaemonApiLocalNodeIdCache {
    entries: HashMap<PathBuf, DaemonApiLocalNodeIdCacheEntry>,
    sequence: u64,
}

impl DaemonApiLocalNodeIdCache {
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.wrapping_add(1);
        self.sequence
    }

    fn remove_lru(&mut self) {
        let candidate = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(root, _)| root.clone());
        if let Some(candidate) = candidate {
            self.entries.remove(&candidate);
        }
    }
}

fn daemon_api_local_node_id_cache() -> &'static Mutex<DaemonApiLocalNodeIdCache> {
    static CACHE: OnceLock<Mutex<DaemonApiLocalNodeIdCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(DaemonApiLocalNodeIdCache::default()))
}

/// Returns the local cluster identity without reopening and decoding the
/// bootstrap object on every forwarded RPC. Ready calls remain syscall-free;
/// the durable file mark is sampled at the same bounded cadence as the daemon
/// signing-identity cache.
fn cached_daemon_api_local_node_id(
    data_dir: &SinomoDataDir,
) -> Result<NodeId, sinomo_core::network::NetworkError> {
    let root = canonical_daemon_api_data_dir_root(data_dir);
    let checked_at = Instant::now();
    {
        let mut cache = daemon_api_local_node_id_cache().lock().map_err(|_| {
            sinomo_core::network::NetworkError::Io {
                operation: "lock daemon API local node identity cache".to_string(),
                detail: "local node identity cache lock was poisoned".to_string(),
            }
        })?;
        let sequence = cache.next_sequence();
        if let Some(entry) = cache.entries.get_mut(&root)
            && entry.state_mark.exists
            && checked_at.duration_since(entry.checked_at) < DAEMON_API_LOCAL_NODE_ID_CACHE_MAX_AGE
        {
            entry.last_used = sequence;
            return Ok(entry.node_id.clone());
        }
    }

    let cluster_paths = sinomo_core::network::ClusterPaths::new(data_dir);
    let state_mark = sinomo_core::network::cluster_file_mark(&cluster_paths.state);
    if let Some(state_mark) = state_mark {
        let mut cache = daemon_api_local_node_id_cache().lock().map_err(|_| {
            sinomo_core::network::NetworkError::Io {
                operation: "lock daemon API local node identity cache".to_string(),
                detail: "local node identity cache lock was poisoned".to_string(),
            }
        })?;
        let sequence = cache.next_sequence();
        if let Some(entry) = cache.entries.get_mut(&root) {
            if entry.state_mark == state_mark && state_mark.exists {
                entry.last_used = sequence;
                return Ok(entry.node_id.clone());
            }
            cache.entries.remove(&root);
        }
    } else {
        let mut cache = daemon_api_local_node_id_cache().lock().map_err(|_| {
            sinomo_core::network::NetworkError::Io {
                operation: "lock daemon API local node identity cache".to_string(),
                detail: "local node identity cache lock was poisoned".to_string(),
            }
        })?;
        cache.entries.remove(&root);
    }

    let state = read_daemon_api_cluster_state_without_kv(data_dir, &cluster_paths)?;
    let node_id = state.local_node_id;
    let after_mark = sinomo_core::network::cluster_file_mark(&cluster_paths.state);
    if let Some(stable_mark) = state_mark.filter(|mark| Some(*mark) == after_mark && mark.exists) {
        let mut cache = daemon_api_local_node_id_cache().lock().map_err(|_| {
            sinomo_core::network::NetworkError::Io {
                operation: "lock daemon API local node identity cache".to_string(),
                detail: "local node identity cache lock was poisoned".to_string(),
            }
        })?;
        let sequence = cache.next_sequence();
        while cache.entries.len() >= DAEMON_API_LOCAL_NODE_ID_CACHE_MAX_ENTRIES
            && !cache.entries.contains_key(&root)
        {
            cache.remove_lru();
        }
        cache.entries.insert(
            root,
            DaemonApiLocalNodeIdCacheEntry {
                state_mark: stable_mark,
                node_id: node_id.clone(),
                checked_at,
                last_used: sequence,
            },
        );
    }
    Ok(node_id)
}

fn read_daemon_api_cluster_state_without_kv(
    data_dir: &SinomoDataDir,
    cluster_paths: &sinomo_core::network::ClusterPaths,
) -> Result<sinomo_core::ClusterBootstrapState, sinomo_core::network::NetworkError> {
    #[cfg(test)]
    {
        let root = canonical_daemon_api_data_dir_root(data_dir);
        let mut reads = daemon_api_cluster_state_reads()
            .lock()
            .expect("daemon API cluster-state read counter");
        *reads.entry(root).or_default() += 1;
    }
    #[cfg(not(test))]
    let _ = data_dir;
    sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
}

#[cfg(test)]
fn daemon_api_cluster_state_reads() -> &'static Mutex<HashMap<PathBuf, usize>> {
    static READS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    READS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn daemon_api_cluster_state_read_count(data_dir: &SinomoDataDir) -> usize {
    let root = canonical_daemon_api_data_dir_root(data_dir);
    daemon_api_cluster_state_reads()
        .lock()
        .expect("daemon API cluster-state read counter")
        .get(&root)
        .copied()
        .unwrap_or_default()
}

impl sinomo_core::network::ClusterObjectRpcAsyncHost for DaemonApiRpcHost {}

fn daemon_api_rpc_endpoint_for_address(
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
        detail: "daemon RPC endpoint is not authorized in local cluster state".to_string(),
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
    block_on_daemon_client_on_process_runtime(future)
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
    block_on_daemon_client_on_process_runtime(future)
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

fn daemon_api_sync_runtime() -> Result<&'static tokio::runtime::Runtime, DaemonApiError> {
    static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
    match RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("sinomo-api-sync")
            .enable_all()
            .build()
            .map_err(|source| source.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(detail) => Err(DaemonApiError::Network {
            address: "daemon client runtime".to_string(),
            detail: detail.clone(),
        }),
    }
}

fn block_on_daemon_client_on_process_runtime<F, T>(future: F) -> Result<T, DaemonApiError>
where
    F: std::future::Future<Output = Result<T, DaemonApiError>>,
{
    daemon_api_sync_runtime()?.block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn request_outcome_client_rejects_a_committed_record_for_another_request() {
        let submitted = KvRequestId::generate();
        let returned = KvRequestId::generate();
        let read = ClusterKvRequestOutcomeRead {
            authority: sinomo_core::kv::KvAuthorityProof {
                term: 2,
                commit_index: 7,
                revision: sinomo_core::kv::KvRevision::ZERO,
                basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
            outcome: KvRequestOutcome::Committed(sinomo_core::kv::KvCommittedRequestRecord {
                request_id: returned,
                command_digest: sinomo_core::kv::KvCommandDigest([0; 32]),
                term: 2,
                log_index: 7,
                revision: sinomo_core::kv::KvRevision::ZERO,
                txn_response: None,
            }),
        };
        let error = validate_cluster_kv_request_outcome_response(read, &submitted)
            .expect_err("request ID mismatch must fail closed");

        assert!(matches!(
            error,
            DaemonApiError::Parse { detail }
                if detail.contains("outcome response is for request ID")
        ));
    }

    fn test_channel_data_dir(label: &str) -> (sinomo_testdir::TestDir, SinomoDataDir) {
        let root = sinomo_testdir::TestDir::new(label);
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("test data dir");
        (root, data_dir)
    }

    async fn accept_test_stream_native_carrier(
        stream: sinomo_core::network::AsyncDaemonControlStream,
    ) -> Result<sinomo_core::network::RoutedStreamAcceptor, sinomo_core::network::NetworkError>
    {
        let mut stream = stream.into_inner();
        let context =
            sinomo_core::network::read_local_object_rpc_carrier_prelude(&mut stream).await?;
        sinomo_core::network::accept_local_object_rpc_stream_carrier(Box::new(stream), &context)
    }

    async fn serve_test_peer_stream_native_service<F, Fut>(
        stream: sinomo_core::network::PeerStream,
        handler: F,
    ) -> Result<(), sinomo_core::network::NetworkError>
    where
        F: FnOnce(
            sinomo_core::network::ObjectRpcIncomingRequest,
            sinomo_core::network::RpcPeerContext,
        ) -> Fut,
        Fut: std::future::Future<
                Output = Result<
                    sinomo_core::network::ObjectRpcOutgoingResponse,
                    sinomo_core::network::NetworkError,
                >,
            >,
    {
        let context = sinomo_core::network::RpcPeerContext::unauthenticated_local(
            stream.session_id.clone(),
            stream.channel_binding,
        );
        sinomo_core::network::serve_service_rpc_peer_stream(
            stream,
            &context,
            sinomo_core::network::ObjectFrameIoOptions::default(),
            handler,
        )
        .await
    }

    async fn serve_test_logical_stream_native_service<F, Fut>(
        acceptor: &mut sinomo_core::network::RoutedStreamAcceptor,
        handler: F,
    ) -> Result<(), sinomo_core::network::NetworkError>
    where
        F: FnOnce(
            sinomo_core::network::ObjectRpcIncomingRequest,
            sinomo_core::network::RpcPeerContext,
        ) -> Fut,
        Fut: std::future::Future<
                Output = Result<
                    sinomo_core::network::ObjectRpcOutgoingResponse,
                    sinomo_core::network::NetworkError,
                >,
            >,
    {
        let stream =
            acceptor
                .accept()
                .await?
                .ok_or_else(|| sinomo_core::network::NetworkError::Io {
                    operation: "accept test local RPC logical stream".to_string(),
                    detail: "local RPC carrier closed before the logical stream arrived"
                        .to_string(),
                })?;
        serve_test_peer_stream_native_service(stream, handler).await
    }

    async fn serve_test_stream_native_service<F, Fut>(
        stream: sinomo_core::network::AsyncDaemonControlStream,
        handler: F,
    ) -> Result<(), sinomo_core::network::NetworkError>
    where
        F: FnOnce(
            sinomo_core::network::ObjectRpcIncomingRequest,
            sinomo_core::network::RpcPeerContext,
        ) -> Fut,
        Fut: std::future::Future<
                Output = Result<
                    sinomo_core::network::ObjectRpcOutgoingResponse,
                    sinomo_core::network::NetworkError,
                >,
            >,
    {
        let mut acceptor = accept_test_stream_native_carrier(stream).await?;
        serve_test_logical_stream_native_service(&mut acceptor, handler).await
    }

    #[test]
    fn canonical_root_cache_is_bounded_and_evicts_lru() {
        let mut cache = DaemonApiCanonicalRootCache::default();
        for index in 0..DAEMON_API_CANONICAL_ROOT_CACHE_MAX_ENTRIES {
            cache.insert(
                PathBuf::from(format!("root-{index}")),
                PathBuf::from(format!("canonical-{index}")),
            );
        }
        assert_eq!(
            cache.get(&PathBuf::from("root-0")),
            Some(PathBuf::from("canonical-0"))
        );
        cache.insert(
            PathBuf::from("root-overflow"),
            PathBuf::from("canonical-overflow"),
        );
        assert_eq!(
            cache.entries.len(),
            DAEMON_API_CANONICAL_ROOT_CACHE_MAX_ENTRIES
        );
        assert!(cache.entries.contains_key(&PathBuf::from("root-0")));
        assert!(!cache.entries.contains_key(&PathBuf::from("root-1")));
    }

    fn remove_test_local_node_id_cache(data_dir: &SinomoDataDir) {
        let root = canonical_daemon_api_data_dir_root(data_dir);
        daemon_api_local_node_id_cache()
            .lock()
            .expect("local node identity cache")
            .entries
            .remove(&root);
        daemon_api_cluster_state_reads()
            .lock()
            .expect("cluster-state read counter")
            .remove(&root);
    }

    fn write_test_cluster_state(
        data_dir: &SinomoDataDir,
        label: &str,
        local_node_id: NodeId,
        public_key: String,
    ) {
        let cluster_paths = sinomo_core::network::ClusterPaths::new(data_dir);
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(format!("cluster_{label}"))
                .expect("cluster id"),
            format!("API test cluster {label}"),
            local_node_id,
            "local",
            public_key,
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
    }

    #[test]
    fn local_node_id_cache_refreshes_after_identity_transition() {
        let (_root, data_dir) = test_channel_data_dir("sinomo-api-local-node-id-transition");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main identity");
        let first = NodeId::from_legacy_named("node_api_local_first").expect("first node id");
        let second = NodeId::from_legacy_named("node_api_local_second").expect("second node id");
        write_test_cluster_state(
            &data_dir,
            "api_local_node_transition",
            first.clone(),
            identity.public_key.clone(),
        );

        assert_eq!(
            cached_daemon_api_local_node_id(&data_dir).expect("first cached identity"),
            first
        );
        let reads_after_first = daemon_api_cluster_state_read_count(&data_dir);
        assert_eq!(reads_after_first, 1);
        assert_eq!(
            cached_daemon_api_local_node_id(&data_dir).expect("ready cached identity"),
            first
        );
        assert_eq!(
            daemon_api_cluster_state_read_count(&data_dir),
            reads_after_first,
            "an unchanged ready-path lookup must not reopen cluster state"
        );

        write_test_cluster_state(
            &data_dir,
            "api_local_node_transition",
            second.clone(),
            identity.public_key,
        );
        let cache_root = canonical_daemon_api_data_dir_root(&data_dir);
        daemon_api_local_node_id_cache()
            .lock()
            .expect("local node identity cache")
            .entries
            .get_mut(&cache_root)
            .expect("cached local node identity")
            .checked_at = Instant::now()
            .checked_sub(DAEMON_API_LOCAL_NODE_ID_CACHE_MAX_AGE + Duration::from_millis(1))
            .expect("expired cache timestamp");
        assert_eq!(
            cached_daemon_api_local_node_id(&data_dir).expect("transitioned cached identity"),
            second
        );
        assert_eq!(
            daemon_api_cluster_state_read_count(&data_dir),
            reads_after_first + 1,
            "a durable identity transition must invalidate and reload the cache"
        );
        remove_test_local_node_id_cache(&data_dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_forwarded_stream_native_calls_do_not_reopen_cluster_state() {
        const CALLS: usize = 3;
        let (_root, data_dir) = test_channel_data_dir("sinomo-api-forward-local-id-cache");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main identity");
        let local_node =
            NodeId::from_legacy_named("node_api_forward_cache_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_api_forward_cache_remote").expect("remote node");
        write_test_cluster_state(
            &data_dir,
            "api_forward_cache",
            local_node,
            identity.public_key,
        );

        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("address");
        let expected_target = remote_node.clone();
        let accepts = Arc::new(AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        let expected_request_value = vec![0x5a; 256 * 1024 + 37];
        let server_request_value = expected_request_value.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept RPC carrier");
            server_accepts.fetch_add(1, Ordering::SeqCst);
            let mut acceptor = accept_test_stream_native_carrier(stream)
                .await
                .expect("accept stream-native RPC carrier");
            for _ in 0..CALLS {
                let expected_target = expected_target.clone();
                let expected_request_value = server_request_value.clone();
                serve_test_logical_stream_native_service(
                    &mut acceptor,
                    move |mut inbound, _peer| async move {
                        assert_eq!(inbound.request.route, DAEMON_OBJECT_RPC_FORWARD_ROUTE);
                        let forward = inbound
                            .request
                            .decode_body::<DaemonObjectRpcForwardRequest>()
                            .expect("forward request");
                        assert_eq!(forward.target_node_id, expected_target);
                        assert!(forward.streamed_value_body);
                        assert!(inbound.body.is_attachment());
                        let encoded_request = std::mem::take(&mut inbound.body)
                            .collect_to_vec(512 * 1024)
                            .await?;
                        let rebuilt = ObjectRpcCall::from_forwarded_streamed_value(
                            forward.request.clone(),
                            sinomo_core::network::ObjectRpcIncomingBody::bytes(
                                encoded_request.clone(),
                            ),
                        )?;
                        assert!(rebuilt.has_streamed_value_body());
                        let actual_request = ObjectRpcRequest::from_encoded_body(
                            "/test/api-forward-cache",
                            "test",
                            encoded_request,
                        )
                        .decode_body::<Vec<u8>>()?;
                        assert_eq!(actual_request, expected_request_value);
                        let response = ObjectRpcResponse::encode_body(
                            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                            &DaemonObjectRpcForwardResponse::Success {
                                response: ObjectRpcResponse::from_encoded_body(
                                    forward.request.route,
                                    Vec::new(),
                                ),
                                streamed_value_body: true,
                                forward_elapsed_micros: 0,
                            },
                        )
                        .expect("forward response");
                        Ok(sinomo_core::network::ObjectRpcOutgoingResponse::new(
                            response,
                            sinomo_core::network::ObjectRpcBodySource::sobj_borrowed_value(
                                &"pong".to_string(),
                            )?,
                        ))
                    },
                )
                .await
                .expect("serve stream-native forwarded request");
            }
        });

        let runtime = tokio::runtime::Handle::current();
        let timeouts =
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(2));
        let mut reads_after_first = 0;
        for call_index in 0..CALLS {
            let result = send_daemon_object_rpc_call_async(
                data_dir.clone(),
                &address,
                DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                &remote_node,
                ObjectRpcCall::encode_borrowed_value(
                    "/test/api-forward-cache",
                    "test",
                    &expected_request_value,
                )
                .expect("streamed inner request"),
                Duration::from_secs(2),
                None,
                Some(Duration::from_secs(2)),
                false,
                timeouts,
                None,
                &runtime,
            )
            .await
            .expect("forwarded call");
            assert!(result.has_streamed_value());
            assert_eq!(
                result
                    .decode_body::<String>()
                    .await
                    .expect("inner response body"),
                "pong"
            );
            if call_index == 0 {
                reads_after_first = daemon_api_cluster_state_read_count(&data_dir);
                assert_eq!(reads_after_first, 1);
            }
        }
        assert_eq!(
            daemon_api_cluster_state_read_count(&data_dir),
            reads_after_first,
            "repeated stream-native calls must perform zero cluster-state reads"
        );
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "repeated migrated calls must reuse one stream-native RPC carrier"
        );

        server.await.expect("server task");
        remove_test_local_node_id_cache(&data_dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cluster_control_join_stream_native_call_outlives_idle_window() {
        const CALL_IDLE_TIMEOUT: Duration = Duration::from_millis(50);
        const JOIN_HANDLER_DELAY: Duration = Duration::from_millis(150);

        let (_root, data_dir) = test_channel_data_dir("sinomo-api-stream-native-cluster-join");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("address");
        let expected = ClusterControlJoinResponse {
            cluster_id: sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_api_stream_native_join",
            )
            .expect("cluster id"),
            node_id: NodeId::from_legacy_named("node_api_stream_native_join").expect("node id"),
            accepted_endpoint: Some("bridge.test:21353".to_string()),
            migration: None,
        };
        let server_expected = expected.clone();
        let server = tokio::spawn(async move {
            let carrier = listener.accept().await.expect("accept RPC carrier");
            let mut acceptor = accept_test_stream_native_carrier(carrier)
                .await
                .expect("accept stream-native RPC carrier");
            serve_test_logical_stream_native_service(&mut acceptor, |warmup, _peer| async move {
                assert_eq!(warmup.request.route, "/test/warm-cluster-control-channel");
                assert_eq!(
                    warmup
                        .decode_body::<String>()
                        .await
                        .expect("warmup request"),
                    "warm"
                );
                sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                    "/test/warm-cluster-control-channel",
                    &"warm".to_string(),
                )
            })
            .await
            .expect("serve warmup service stream");

            serve_test_logical_stream_native_service(
                &mut acceptor,
                move |join, _peer| async move {
                    assert_eq!(
                        join.request.route,
                        sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE
                    );
                    let _: ClusterControlJoinRequest =
                        join.decode_body().await.expect("join request");
                    tokio::time::sleep(JOIN_HANDLER_DELAY).await;
                    sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                        sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
                        &server_expected,
                    )
                },
            )
            .await
            .expect("serve join service stream");
        });

        let runtime = tokio::runtime::Handle::current();
        let warmup = ObjectRpcCall::encode_borrowed_value(
            "/test/warm-cluster-control-channel",
            "test",
            &"warm".to_string(),
        )
        .expect("warmup request");
        let warmup_result = call_daemon_address_rpc_async(
            data_dir.clone(),
            &address,
            warmup,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            ),
            &runtime,
        )
        .await
        .expect("warm stream-native service path");
        assert_eq!(
            warmup_result
                .decode_body::<String>()
                .await
                .expect("warmup response"),
            "warm"
        );

        let join_call = ObjectRpcCall::encode_borrowed_value(
            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_RPC_CATEGORY,
            &ClusterControlJoinRequest {
                invite: "sinomo-join://test-stream-native".to_string(),
                node_display_name: Some("candidate".to_string()),
                node_role: None,
            },
        )
        .expect("join request");
        let join_result = call_daemon_address_rpc_async(
            data_dir.clone(),
            &address,
            join_call,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(CALL_IDLE_TIMEOUT),
            ),
            &runtime,
        )
        .await;

        server.await.expect("server task");
        let result = join_result
            .expect("stream-native liveness must preserve a healthy long-running cluster join");
        let actual = result
            .decode_body::<ClusterControlJoinResponse>()
            .await
            .expect("join response");
        assert_eq!(actual, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_cluster_control_joins_are_isolated_on_stream_native_logical_streams() {
        const JOINERS: usize = 2;
        let (_root, data_dir) =
            test_channel_data_dir("sinomo-api-concurrent-stream-native-cluster-join");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("address");
        let server = tokio::spawn(async move {
            let carrier = listener.accept().await.expect("accept RPC carrier");
            let mut acceptor = accept_test_stream_native_carrier(carrier)
                .await
                .expect("accept stream-native RPC carrier");
            serve_test_logical_stream_native_service(&mut acceptor, |warmup, _peer| async move {
                let route = warmup.request.route.clone();
                assert_eq!(
                    warmup
                        .decode_body::<String>()
                        .await
                        .expect("warmup request"),
                    "warm"
                );
                sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                    route,
                    &"warm".to_string(),
                )
            })
            .await
            .expect("serve warmup service stream");

            let mut responders = Vec::with_capacity(JOINERS);
            for _ in 0..JOINERS {
                let stream = acceptor
                    .accept()
                    .await
                    .expect("accept concurrent join logical stream")
                    .expect("RPC carrier remains open");
                responders.push(tokio::spawn(async move {
                    serve_test_peer_stream_native_service(stream, |inbound, _peer| async move {
                        assert_eq!(
                            inbound.request.route,
                            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE
                        );
                        let request = inbound
                            .decode_body::<ClusterControlJoinRequest>()
                            .await
                            .expect("join request");
                        let suffix = request
                            .node_display_name
                            .as_deref()
                            .expect("candidate name")
                            .to_string();
                        if suffix.ends_with('a') {
                            tokio::time::sleep(Duration::from_millis(30)).await;
                        }
                        let response = ClusterControlJoinResponse {
                            cluster_id: sinomo_core::node::ClusterId::from_legacy_named(
                                "cluster_api_concurrent_join",
                            )
                            .expect("cluster id"),
                            node_id: NodeId::from_legacy_named(format!(
                                "node_api_concurrent_join_{suffix}"
                            ))
                            .expect("node id"),
                            accepted_endpoint: Some("bridge.test:21353".to_string()),
                            migration: None,
                        };
                        sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
                            &response,
                        )
                    })
                    .await
                    .expect("serve concurrent join stream");
                }));
            }
            for responder in responders {
                responder.await.expect("join responder");
            }
        });

        let runtime = tokio::runtime::Handle::current();
        let warmup = ObjectRpcCall::encode_borrowed_value(
            "/test/warm-concurrent-cluster-control-channel",
            "test",
            &"warm".to_string(),
        )
        .expect("warmup request");
        call_daemon_address_rpc_async(
            data_dir.clone(),
            &address,
            warmup,
            sinomo_core::network::ClusterObjectRpcOptions::default(),
            &runtime,
        )
        .await
        .expect("warm stream-native service path");

        let calls = ["candidate-a", "candidate-b"].map(|candidate| {
            let data_dir = data_dir.clone();
            let address = address.clone();
            let runtime = runtime.clone();
            async move {
                let call = ObjectRpcCall::encode_borrowed_value(
                    sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
                    sinomo_core::cluster::CLUSTER_CONTROL_RPC_CATEGORY,
                    &ClusterControlJoinRequest {
                        invite: format!("sinomo-join://{candidate}"),
                        node_display_name: Some(candidate.to_string()),
                        node_role: None,
                    },
                )
                .expect("join request");
                call_daemon_address_rpc_async(
                    data_dir,
                    &address,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::new(
                        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(
                            Duration::from_secs(1),
                        ),
                    ),
                    &runtime,
                )
                .await
            }
        });
        let [first_call, second_call] = calls;
        let (first, second) = tokio::join!(first_call, second_call);
        for (result, expected_candidate) in [(first, "candidate-a"), (second, "candidate-b")] {
            let result = result.expect("concurrent join call");
            let response = result
                .decode_body::<ClusterControlJoinResponse>()
                .await
                .expect("join response");
            assert_eq!(
                response.accepted_endpoint.as_deref(),
                Some("bridge.test:21353")
            );
            assert_eq!(
                response.node_id,
                NodeId::from_legacy_named(format!("node_api_concurrent_join_{expected_candidate}"))
                    .expect("expected node id")
            );
        }

        server.await.expect("server task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn loopback_service_call_uses_exactly_one_stream_native_connection() {
        let root = sinomo_testdir::TestDir::new("sinomo-api-stream-native-single-connection");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("address");
        let accepts = Arc::new(AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("service stream accept");
            server_accepts.fetch_add(1, Ordering::SeqCst);
            let mut prefix = [0_u8; 4];
            assert_eq!(
                stream.peek_prefix(&mut prefix).await.expect("peek prelude"),
                4
            );
            assert!(
                sinomo_core::network::local_object_rpc_carrier_prelude_prefix_matches(&prefix,),
                "the first connection must begin with the local object-RPC carrier prelude"
            );
            serve_test_stream_native_service(stream, |request, _peer| async move {
                assert_eq!(
                    request.request.route,
                    "/test/stream-native-single-connection"
                );
                assert_eq!(
                    request.decode_body::<String>().await.expect("request body"),
                    "request"
                );
                sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                    "/test/stream-native-single-connection",
                    &"stream-native".to_string(),
                )
            })
            .await
            .expect("serve stream-native request");
        });
        let runtime = tokio::runtime::Handle::current();
        let call = ObjectRpcCall::encode_borrowed_value(
            "/test/stream-native-single-connection",
            "test",
            &"request".to_string(),
        )
        .expect("request");
        let result = call_daemon_address_rpc_async(
            data_dir.clone(),
            &address,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::default(),
            &runtime,
        )
        .await
        .expect("stream-native call");
        assert_eq!(
            result.decode_body::<String>().await.expect("response body"),
            "stream-native"
        );
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "the stream-native loopback path uses exactly one persistent carrier connection"
        );
        server.await.expect("server task");
    }

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
    fn forwarded_value_object_rpc_honors_explicit_client_timeout() {
        let client =
            DaemonApiClient::at_address("127.0.0.1:21353").with_timeout(Duration::from_secs(6));
        let timeouts = client.object_rpc_forward_control_timeouts(Some(Duration::from_secs(60)));
        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(6)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(6)));
    }

    #[test]
    fn forwarded_deadline_object_rpc_uses_default_network_health_timeout() {
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
    async fn signed_loopback_rpc_survives_long_handler_and_rehydrates_value_response() {
        let root = sinomo_testdir::TestDir::new("sinomo-api-loopback-session-progress");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let runtime = tokio::runtime::Handle::current();
        let response_body = vec![0x5a_u8; 173_219];
        let server_body = response_body.clone();
        let server = tokio::spawn(async move {
            let carrier = listener.accept().await.expect("accept RPC carrier");
            let mut acceptor = accept_test_stream_native_carrier(carrier)
                .await
                .expect("accept stream-native RPC carrier");
            serve_test_logical_stream_native_service(&mut acceptor, |request, _peer| async move {
                assert_eq!(request.request.route, "/test/warm-long-loopback-response");
                Ok(sinomo_core::network::ObjectRpcOutgoingResponse::value(
                    ObjectRpcResponse::encode_body("/test/warm-long-loopback-response", &"warm")
                        .expect("warmup response"),
                ))
            })
            .await
            .expect("serve warmup request");
            serve_test_logical_stream_native_service(
                &mut acceptor,
                move |request, _peer| async move {
                    assert_eq!(request.request.route, "/test/long-loopback-response");
                    tokio::time::sleep(Duration::from_secs(12)).await;
                    let response = ObjectRpcResponse::from_encoded_body(
                        "/test/long-loopback-response",
                        server_body,
                    );
                    Ok(sinomo_core::network::ObjectRpcOutgoingResponse::value(
                        response,
                    ))
                },
            )
            .await
            .expect("serve long stream-native request");
        });
        let warmup =
            ObjectRpcRequest::encode_body("/test/warm-long-loopback-response", "test", &"warm")
                .expect("warmup request");
        call_daemon_address_rpc_async(
            data_dir.clone(),
            &address,
            ObjectRpcCall::value(warmup),
            sinomo_core::network::ClusterObjectRpcOptions::default(),
            &runtime,
        )
        .await
        .expect("warm persistent loopback carrier");
        let request = ObjectRpcRequest::encode_body("/test/long-loopback-response", "test", &())
            .expect("request");

        let result = call_daemon_address_rpc_async(
            data_dir.clone(),
            &address,
            ObjectRpcCall::value(request),
            sinomo_core::network::ClusterObjectRpcOptions::default(),
            &runtime,
        )
        .await
        .expect("stream-native liveness preserves the long loopback RPC");
        let response = decode_object_rpc_value_response(result)
            .await
            .expect("decode response value");
        server.await.expect("server task");

        assert_eq!(response.body, response_body);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forwarded_rpc_outer_deadline_is_not_extended_by_loopback_keepalives() {
        const OUTER_IDLE_TIMEOUT: Duration = Duration::from_millis(100);
        const OVERALL_TIMEOUT: Duration = Duration::from_millis(250);
        const HANDLER_DELAY: Duration = Duration::from_millis(750);

        let root = sinomo_testdir::TestDir::new("sinomo-api-forward-outer-deadline");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let local_node =
            NodeId::from_legacy_named("node_api_forward_deadline_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_api_forward_deadline_target").expect("target node");
        write_test_cluster_state(
            &data_dir,
            "forward_outer_deadline",
            local_node,
            identity.public_key,
        );

        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let runtime = tokio::runtime::Handle::current();
        let server = tokio::spawn(async move {
            let carrier = listener.accept().await.expect("accept RPC carrier");
            let mut acceptor = accept_test_stream_native_carrier(carrier)
                .await
                .expect("accept stream-native RPC carrier");
            serve_test_logical_stream_native_service(&mut acceptor, |incoming, _peer| async move {
                assert_eq!(incoming.request.route, "/test/warm-forward-deadline");
                let _: String = incoming.decode_body().await.expect("warmup request");
                sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                    "/test/warm-forward-deadline",
                    &"warm",
                )
            })
            .await
            .expect("serve warmup request");
            let _ = serve_test_logical_stream_native_service(
                &mut acceptor,
                |incoming, _peer| async move {
                    assert_eq!(incoming.request.route, DAEMON_OBJECT_RPC_FORWARD_ROUTE);
                    let payload: DaemonObjectRpcForwardRequest = incoming
                        .decode_body()
                        .await
                        .expect("forward request payload");
                    assert_eq!(payload.overall_timeout_millis, Some(250));

                    // Stream liveness preserves transport health without extending
                    // the caller's operation-wide deadline.
                    tokio::time::sleep(HANDLER_DELAY).await;
                    sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                        DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                        &DaemonObjectRpcForwardResponse::NetworkError {
                            detail: "response after caller deadline".to_string(),
                            delivery_state: None,
                        },
                    )
                },
            )
            .await;
        });
        let warmup = ObjectRpcRequest::encode_body("/test/warm-forward-deadline", "test", &"warm")
            .expect("warmup request");
        call_daemon_address_rpc_async(
            data_dir.clone(),
            &address,
            ObjectRpcCall::value(warmup),
            sinomo_core::network::ClusterObjectRpcOptions::default(),
            &runtime,
        )
        .await
        .expect("warm persistent loopback carrier");
        let request =
            ObjectRpcRequest::encode_body("/test/forward-outer-deadline", "test", &"request")
                .expect("inner request");
        let started_at = tokio::time::Instant::now();

        let error = send_daemon_object_rpc_call_async(
            data_dir.clone(),
            &address,
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            &target_node,
            ObjectRpcCall::value(request),
            Duration::from_secs(2),
            None,
            Some(OVERALL_TIMEOUT),
            false,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(OUTER_IDLE_TIMEOUT),
            None,
            &runtime,
        )
        .await
        .expect_err("outer loopback session must honor the caller deadline");

        let elapsed = tokio::time::Instant::now().duration_since(started_at);
        assert!(
            elapsed >= OVERALL_TIMEOUT && elapsed < OVERALL_TIMEOUT + Duration::from_millis(100),
            "the outer deadline fired after {elapsed:?}, expected {OVERALL_TIMEOUT:?}"
        );
        assert!(
            matches!(
                &error,
                DaemonApiError::NetworkDelivery {
                    delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    ..
                }
            ),
            "unexpected forwarded deadline error: {error:?}"
        );
        assert!(
            error.to_string().contains("Overall"),
            "forwarded deadline must remain an overall timeout: {error}"
        );

        server.abort();
        let _ = server.await;
        remove_test_local_node_id_cache(&data_dir);
    }

    /// Opt-in loopback probe for TCP connect plus the stream-native object-RPC
    /// API. It intentionally has no timing assertion because CI
    /// hosts are not stable benchmark environments.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "diagnostic microbenchmark; run explicitly with --ignored --nocapture"]
    async fn diagnostic_loopback_small_rpc_cost() {
        const WARMUP: usize = 100;
        const ITERATIONS: usize = 1_000;
        let root = sinomo_testdir::TestDir::new("sinomo-api-loopback-cost-diagnostic");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        // The signer cache tracks both durable identity and cluster-state
        // file marks. This probe needs only the mark because its server-side
        // callback deliberately omits cluster authorization.
        std::fs::write(&cluster_paths.state, b"diagnostic-state-mark")
            .expect("diagnostic cluster state mark");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let runtime = tokio::runtime::Handle::current();
        let server = tokio::spawn(async move {
            let carrier = listener.accept().await.expect("accept diagnostic carrier");
            let mut acceptor = accept_test_stream_native_carrier(carrier)
                .await
                .expect("accept diagnostic RPC carrier");
            for _ in 0..WARMUP + ITERATIONS {
                serve_test_logical_stream_native_service(
                    &mut acceptor,
                    |request, _peer| async move {
                        assert_eq!(request.request.route, "/test/loopback-cost-diagnostic");
                        Ok(sinomo_core::network::ObjectRpcOutgoingResponse::value(
                            ObjectRpcResponse::encode_body(
                                "/test/loopback-cost-diagnostic",
                                &"small response",
                            )
                            .expect("diagnostic response"),
                        ))
                    },
                )
                .await
                .expect("serve diagnostic stream-native RPC");
            }
        });
        let request = ObjectRpcRequest::encode_body(
            "/test/loopback-cost-diagnostic",
            "test",
            &"small request",
        )
        .expect("diagnostic request");
        let mut latency_us = Vec::with_capacity(ITERATIONS);
        for iteration in 0..WARMUP + ITERATIONS {
            let started = std::time::Instant::now();
            let result = call_daemon_address_rpc_async(
                data_dir.clone(),
                &address,
                ObjectRpcCall::value(request.clone()),
                sinomo_core::network::ClusterObjectRpcOptions::default(),
                &runtime,
            )
            .await
            .expect("diagnostic loopback RPC");
            decode_object_rpc_value_response(result)
                .await
                .expect("diagnostic response value");
            if iteration >= WARMUP {
                latency_us.push(started.elapsed().as_micros() as u64);
            }
        }
        server.await.expect("diagnostic loopback server");
        latency_us.sort_unstable();
        let percentile = |percent: usize| {
            let rank = latency_us.len().saturating_mul(percent).saturating_add(99) / 100;
            latency_us[rank.saturating_sub(1).min(latency_us.len() - 1)]
        };
        eprintln!(
            "loopback-small-rpc us: min={} p50={} p95={} max={}",
            latency_us[0],
            percentile(50),
            percentile(95),
            latency_us[latency_us.len() - 1],
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
            Some(Duration::from_secs(9)),
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
        assert_eq!(payload.idle_timeout_millis, Some(9_000));
        assert_eq!(payload.overall_timeout_millis, Some(30_000));
        assert!(payload.observe_connectivity);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn daemon_dfs_peer_transport_sends_the_dfs_forward_route() {
        let (_root, data_dir) = test_channel_data_dir("sinomo-api-dfs-forward-route");
        let local_node =
            NodeId::from_legacy_named("node_api_dfs_forward_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_api_dfs_forward_target").expect("target node");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let expected_target = target_node.clone();
        let server = tokio::spawn(async move {
            let carrier = listener.accept().await.expect("accept local RPC carrier");
            let mut acceptor = accept_test_stream_native_carrier(carrier)
                .await
                .expect("accept stream-native local RPC carrier");
            serve_test_logical_stream_native_service(
                &mut acceptor,
                move |inbound, _peer| async move {
                    assert_eq!(
                        inbound.request.route,
                        DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
                        "the real DFS peer transport must not fall back to the generic daemon forward route"
                    );
                    let forward = inbound
                        .decode_body::<DaemonObjectRpcForwardRequest>()
                        .await
                        .expect("forward request");
                    assert_eq!(forward.target_node_id, expected_target);
                    let inner = ObjectRpcResponse::encode_body(forward.request.route, &"dfs-pong")
                        .expect("inner response");
                    sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                        DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
                        &DaemonObjectRpcForwardResponse::Success {
                            response: inner,
                            streamed_value_body: false,
                            forward_elapsed_micros: 0,
                        },
                    )
                },
            )
            .await
            .expect("serve stream-native DFS forward session");
        });

        let transport =
            DaemonDfsPeerTransport::at_address(data_dir.clone(), address.clone(), local_node);
        let request = ObjectRpcRequest::encode_body("/dfs/test/actual-forward", "dfs", &())
            .expect("DFS request");
        let result = transport
            .call_rpc_inner(
                &target_node,
                ObjectRpcCall::value(request),
                sinomo_dfs::DfsObjectRpcSendOptions::new(Duration::from_secs(2))
                    .with_overall_timeout(Duration::from_secs(2)),
                &tokio::runtime::Handle::current(),
            )
            .await
            .expect("DFS forwarded call");
        assert_eq!(
            result
                .response
                .decode_body::<String>()
                .expect("inner response"),
            "dfs-pong"
        );
        server.await.expect("server task");
    }

    #[test]
    fn forward_request_propagates_trace_context_to_the_outer_rpc() {
        let target_node_id = NodeId::from_legacy_named("node_forward_trace").expect("node id");
        let inner_request = ObjectRpcRequest::encode_body("/test/timeline", "test", &())
            .expect("inner request")
            .with_traceparent("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
            .with_tracestate("sinomo=timeline")
            .with_observe_sampled(true);
        let request = object_rpc_forward_request(
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            &target_node_id,
            &inner_request,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            Some(Duration::from_secs(10)),
            false,
        )
        .expect("forward request");

        assert_eq!(request.traceparent(), inner_request.traceparent());
        assert_eq!(request.tracestate(), inner_request.tracestate());
        assert!(request.observe_sampled());
        let payload: DaemonObjectRpcForwardRequest =
            request.decode_body().expect("forward payload");
        assert_eq!(payload.request, inner_request);
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
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            Some(Duration::from_secs(10)),
            true,
        )
        .expect("forward request");
        let payload: DaemonObjectRpcForwardRequest =
            request.decode_body().expect("forward payload");

        assert_eq!(payload.target_node_id, target_node_id);
        assert_eq!(payload.connect_timeout_millis, 5_000);
        assert_eq!(payload.idle_timeout_millis, Some(5_000));
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
            Some(Duration::from_secs(310)),
            None,
            true,
        )
        .expect("forward request");
        let payload: DaemonObjectRpcForwardRequest =
            request.decode_body().expect("forward payload");

        assert_eq!(payload.idle_timeout_millis, Some(310_000));
        assert_eq!(payload.overall_timeout_millis, None);
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
    async fn forward_call_response_preserves_trusted_delivery_state() {
        let outer = ObjectRpcResponse::encode_body(
            DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            &DaemonObjectRpcForwardResponse::NetworkError {
                detail: "session forward failed".to_string(),
                delivery_state: Some(DaemonObjectRpcForwardDeliveryState::PossiblyApplied),
            },
        )
        .expect("outer response");
        let error = decode_daemon_object_rpc_forward_call_response(ObjectRpcResult::value(outer))
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
    async fn forward_call_response_decodes_value_outer_envelope() {
        let inner = ObjectRpcResponse::encode_error(
            "/remote-management/system/info",
            "device authorization was denied",
        )
        .expect("inner error response");
        let outer = ObjectRpcResponse::encode_body(
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            &DaemonObjectRpcForwardResponse::Success {
                response: inner.clone(),
                streamed_value_body: false,
                forward_elapsed_micros: 0,
            },
        )
        .expect("outer response");
        let decoded = decode_daemon_object_rpc_forward_call_response(ObjectRpcResult::value(outer))
            .await
            .expect("decode outer value envelope");

        assert_eq!(decoded.response, inner);
        assert!(decoded.body.is_empty());
    }

    #[tokio::test]
    async fn forward_call_response_restores_a_large_streamed_inner_value() {
        let expected = vec![0x5a; 344_208];
        let encoded = ObjectRpcResponse::encode_body("/cluster/network/ping", &expected)
            .expect("encode inner value")
            .body;
        let inner = ObjectRpcResponse::from_encoded_body("/cluster/network/ping", Vec::new());
        let outer = ObjectRpcResponse::encode_body(
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            &DaemonObjectRpcForwardResponse::Success {
                response: inner,
                streamed_value_body: true,
                forward_elapsed_micros: 0,
            },
        )
        .expect("outer response");

        let forwarded = decode_daemon_object_rpc_forward_call_response(ObjectRpcResult::new(
            outer,
            sinomo_core::network::ObjectRpcIncomingBody::bytes(encoded),
        ))
        .await
        .expect("unwrap forward envelope");
        assert!(forwarded.has_streamed_value());
        let decoded = decode_object_rpc_value_response(forwarded)
            .await
            .expect("restore inner RPC value");

        assert_eq!(
            decoded
                .decode_body::<Vec<u8>>()
                .expect("decode inner value"),
            expected
        );
    }

    #[tokio::test]
    async fn value_response_rejects_an_explicit_attachment() {
        let result = ObjectRpcResult::new(
            ObjectRpcResponse::from_encoded_body("/test/value", vec![1, 2, 3]),
            sinomo_core::network::ObjectRpcIncomingBody::bytes(vec![4, 5, 6]),
        );
        let error = decode_object_rpc_value_response(result)
            .await
            .expect_err("value-only API must not discard an attachment");
        assert!(error.to_string().contains("semantic attachment"));
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
        let server = tokio::spawn(async move {
            let stream = listener
                .accept()
                .await
                .expect("accept local service stream");
            serve_test_stream_native_service(stream, move |incoming, _peer| async move {
                assert_eq!(
                    incoming.request.route,
                    sinomo_core::kv::CLUSTER_KV_TXN_ROUTE
                );
                let request_body = incoming
                    .body
                    .collect_to_vec(1024 * 1024)
                    .await
                    .expect("local request body");
                assert_eq!(request_body, expected_request);
                Ok(sinomo_core::network::ObjectRpcOutgoingResponse {
                    response: ObjectRpcResponse::encode_body(
                        sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
                        &"local-ok",
                    )
                    .expect("local response head"),
                    body: sinomo_core::network::ObjectRpcBodySource::bytes(server_response),
                })
            })
            .await
            .expect("serve local stream-native request");
        });

        let transport = DaemonDfsPeerTransport::at_address(
            data_dir.clone(),
            address.clone(),
            local_node.clone(),
        );
        let request = ObjectRpcRequest::encode_body(
            sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &"local-request",
        )
        .expect("local request head");
        let result = sinomo_dfs::AsyncDfsPeerTransport::call_object_rpc_async(
            &transport,
            &local_node,
            ObjectRpcCall::new(
                request,
                sinomo_core::network::ObjectRpcBodySource::bytes(request_body),
            ),
            sinomo_dfs::DfsObjectRpcSendOptions::default()
                .with_overall_timeout(Duration::from_secs(30)),
        )
        .await
        .expect("local target RPC");

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
        server.await.expect("server task");
    }
}
