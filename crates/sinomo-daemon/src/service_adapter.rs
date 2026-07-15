//! Bridges the daemon's control loop to the async, host-agnostic service seam
//! (`sinomo-service`).
//!
//! Routes owned by the [`ServiceRegistry`] are served here; everything else falls
//! through to the daemon's own dispatch. Sandbox live output is produced by the
//! daemon and carried as standard object frames inside a canonical session body;
//! the synchronous command runners remain isolated behind a bounded producer.
//!
//! The registry mixes host-agnostic services from `sinomo-service` and native modules
//! such as `sinomo-rollout` with **daemon-foundational** services defined here
//! ([`DaemonControlService`], [`ClusterKvService`], [`ClusterKvConsensusService`],
//! [`ClusterHealthService`], [`ClusterRelayService`]) that own daemon state
//! (their [`DaemonPaths`], which encode whether this is the main or shadow daemon)
//! and call the daemon's own cluster/control handlers. Consuming services receive a real
//! [`DaemonClusterAccess`] via [`ServiceContext::cluster`] when the node is clustered.

#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt as _;

use crate::registry::network_module_surfaces;
use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_SEARCH_ROUTE, CONFIG_SET_ROUTE, ConfigRegistry,
    ConfigRegistryBuilder, register_services as register_config_services,
};
use sinomo_core::kv::{
    CLUSTER_KV_LOCAL_READ_WATCH_ROUTES, CLUSTER_KV_RPC_ROUTE_POLICIES, CLUSTER_KV_RPC_ROUTES,
    CLUSTER_KV_SNAPSHOT_ROUTE, CLUSTER_KV_SNAPSHOT_RPC_MAX_PAYLOAD_BYTES, CLUSTER_KV_TXN_ROUTE,
    CLUSTER_KV_WATCH_EVENTS_ROUTE, CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_RPC_ROUTE_POLICIES,
    CLUSTER_RAFT_RPC_ROUTES, ClusterKvRpcRoutePolicy, ClusterKvWatchCancelRequest,
    ClusterKvWatchCancelResponse, ClusterKvWatchDrainRequest, ClusterKvWatchDrainResponse,
    ClusterKvWatchPrefixRequest, ClusterKvWatchRegistration, KvEntry, KvError, KvOp, KvSnapshot,
    KvTxn, KvTxnResponse,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    AsyncDaemonControlStream, CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_CHALLENGE_ROUTE,
    CLUSTER_JOIN_REGISTER_ROUTE, CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY,
    CLUSTER_MEMBERSHIP_RPC_ROUTE_METADATA, CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
    CLUSTER_NETWORK_PING_ROUTE, CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_ROUTE_METADATA,
    ClusterPaths, DaemonControlRequest, NETWORK_OBJECT_RPC_ROUTES, NetworkError,
    ObjectRouteDescriptor, ObjectRpcAuthorizationLayer, ObjectRpcBodySource,
    ObjectRpcCancelRequest, ObjectRpcCancelResponse, ObjectRpcRequest, ObjectRpcResponse,
    ObjectRpcRouteCatalog, ObjectRpcRoutePolicyLayer, ObjectRpcRouteRegistry, ObjectRpcRouter,
    ObjectRpcSessionCall, ObjectRpcSessionRequest, ObjectRpcSessionResponse, ServiceCatalog,
};
use sinomo_core::service::{
    ClusterAccess, ResponseSink, Service, ServiceBodyMode, ServiceContext, ServiceDisposition,
    ServiceError, ServiceFuture, ServiceRegistry, ServiceRegistryBuilder, ServiceRegistrySnapshot,
    ServiceRegistrySnapshotStore, ServiceRoute, ServiceSigner,
};
use sinomo_dfs::register_services as register_dfs_services;
use sinomo_module::{MODULE_NATIVE_DYLIB_CANCEL_ROUTE, MODULE_RPC_CATEGORY};
use sinomo_rollout::{
    ClusterRolloutBinaryResponse, ROLLOUT_BINARY_ROUTE, ROLLOUT_BUILD_ROUTE,
    ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE, register_services as register_rollout_services,
    rollout_module_surfaces,
};
use sinomo_sandbox::{
    LocalSandboxBackend, SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE, SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
    SandboxAddToolchainArtifactRequest, SandboxAddToolchainArtifactResponse,
    SandboxAddToolchainCommandRequest, SandboxAddToolchainCommandResponse, SandboxBackend,
    SandboxBackendError, SandboxBackendFactory, SandboxCancellationToken, SandboxCheckpointRequest,
    SandboxCheckpointResponse, SandboxCreateInstanceRequest, SandboxCreateInstanceResponse,
    SandboxDestroyRequest, SandboxDestroyResponse, SandboxHttpFetchFileResult,
    SandboxHttpFetchToFileRequest, SandboxHttpRequest, SandboxHttpResponse,
    SandboxImportBusyBoxToolchainRequest, SandboxImportBusyBoxToolchainResponse,
    SandboxImportToolchainPackageFromPathRequest, SandboxImportToolchainPackageRequest,
    SandboxImportToolchainPackageResponse, SandboxListRequest, SandboxListResponse,
    SandboxLocalCommandOutcome, SandboxProcessPlanOutcome, SandboxReadRequest, SandboxReadResponse,
    SandboxRebootRequest, SandboxRebootResponse, SandboxResumeRequest, SandboxResumeResponse,
    SandboxRunProcessPlanRequest, SandboxRunToolchainRequest, SandboxStatusRequest,
    SandboxStatusResponse, SandboxUpdateInstanceRequest, SandboxUpdateInstanceResponse,
    SandboxWriteRequest, SandboxWriteResponse,
};
use tower_layer::Layer;

use crate::auth::{AuthenticatedDaemonCommand, DaemonSignerAuthority};
use crate::cluster::{
    DaemonClusterJoinRegistrationHost, DaemonClusterMemberMutationHost, DaemonClusterStatusProbe,
    cluster_paths_from_daemon_paths,
    forward_cluster_invite_challenge_to_cluster_state_authority_async_on_runtime,
    handle_daemon_cluster_control_init_on_runtime, handle_daemon_cluster_control_invite_on_runtime,
    handle_daemon_cluster_control_join_on_runtime, handle_daemon_cluster_control_leave_on_runtime,
    handle_daemon_cluster_control_remove_on_runtime,
    handle_daemon_cluster_control_rename_on_runtime, handle_daemon_cluster_control_role_on_runtime,
    handle_daemon_cluster_kv_snapshot_async, handle_daemon_cluster_kv_txn_request_async_on_runtime,
    handle_daemon_cluster_kv_txn_request_async_on_runtime_with_timeouts,
    handle_daemon_cluster_relay_request_on_runtime,
    handle_forwarded_daemon_cluster_kv_txn_request_async_on_runtime,
};
use crate::control::{
    ACCEPTED_ROLLOUT_RESPONSE_DRAIN_GRACE, accepted_rollout_should_stop_current_daemon,
    daemon_data_dir_path, daemon_status_response_async,
    handle_daemon_sandbox_checkpoint_on_runtime, handle_daemon_sandbox_create_on_runtime,
    handle_daemon_sandbox_destroy_on_runtime, handle_daemon_sandbox_http_fetch,
    handle_daemon_sandbox_http_fetch_to_file_on_runtime,
    handle_daemon_sandbox_import_busybox_on_runtime, handle_daemon_sandbox_list_on_runtime,
    handle_daemon_sandbox_read_on_runtime, handle_daemon_sandbox_reboot_on_runtime,
    handle_daemon_sandbox_resume_on_runtime, handle_daemon_sandbox_run_process_plan_on_runtime,
    handle_daemon_sandbox_run_process_plan_with_cancellation_on_runtime,
    handle_daemon_sandbox_run_toolchain_on_runtime,
    handle_daemon_sandbox_run_toolchain_with_cancellation_on_runtime,
    handle_daemon_sandbox_status_on_runtime,
    handle_daemon_sandbox_toolchain_add_artifact_on_runtime,
    handle_daemon_sandbox_toolchain_add_command_on_runtime,
    handle_daemon_sandbox_toolchain_import_package_from_path_on_runtime,
    handle_daemon_sandbox_toolchain_import_package_on_runtime,
    handle_daemon_sandbox_update_on_runtime, handle_daemon_sandbox_write_on_runtime,
};
use crate::dfs::{
    DaemonDfsDataPlaneHost, daemon_dfs_async_peer_transport,
    daemon_dfs_blocking_peer_transport_on_runtime,
};
use crate::error::DaemonError;
use crate::event_stream::{DaemonEventHub, DaemonEventService};
use crate::paths::DaemonPaths;
use crate::rollout::{
    handle_daemon_rollout_binary_request, handle_daemon_rollout_build_request,
    handle_daemon_rollout_native_dylib_module_request,
};
use crate::{
    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE, DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
    DAEMON_DFS_BROWSER_DELETE_ROUTE, DAEMON_DFS_BROWSER_LIST_ROUTE,
    DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE, DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE, DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILES_ROUTE, DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_RENAME_ROUTE, DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_ROUTES, DAEMON_DFS_BROWSER_STAT_ROUTE, DAEMON_DFS_BROWSER_STATUS_ROUTE,
    DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE, DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE, DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE, DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_ROUTE, DAEMON_RPC_CATEGORY, DAEMON_SHUTDOWN_ROUTE,
    DAEMON_STATUS_ROUTE,
};

static DAEMON_CLUSTER_NETWORK_PING_NODE_IDS: OnceLock<
    Mutex<HashMap<PathBuf, sinomo_core::node::NodeId>>,
> = OnceLock::new();

fn daemon_error(error: DaemonError) -> ServiceError {
    ServiceError::Service(error.to_string())
}

fn require_service_runtime<'a>(
    runtime: Option<&'a tokio::runtime::Handle>,
    operation: &str,
) -> Result<&'a tokio::runtime::Handle, ServiceError> {
    runtime.ok_or_else(|| {
        ServiceError::Service(format!(
            "{operation} requires the daemon-owned Tokio runtime"
        ))
    })
}

async fn forward_object_rpc_from_daemon_control(
    paths: &crate::DaemonPaths,
    payload: crate::DaemonObjectRpcForwardRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<crate::DaemonObjectRpcForwardResponse, ServiceError> {
    let transport_timeouts = sinomo_core::network::DaemonRpcTimeouts::default()
        .with_connect_timeout(Some(Duration::from_millis(payload.connect_timeout_millis)));
    let mut options = sinomo_core::network::ClusterObjectRpcOptions::new(transport_timeouts);
    if let Some(overall_timeout_millis) = payload.overall_timeout_millis {
        options = options.with_overall_timeout(Duration::from_millis(overall_timeout_millis));
    }
    match crate::client::send_daemon_rpc_to_target_async_with_daemon_paths_and_network_error(
        paths,
        &payload.target_node_id,
        &payload.request,
        options,
        runtime,
        payload.observe_connectivity,
    )
    .await
    {
        Ok(response) => Ok(crate::DaemonObjectRpcForwardResponse::Success { response }),
        Err(error) => Ok(crate::DaemonObjectRpcForwardResponse::NetworkError {
            detail: error.to_string(),
            delivery_state: error.delivery_state().map(|state| match state {
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent => {
                    crate::DaemonObjectRpcForwardDeliveryState::DefinitelyNotSent
                }
                sinomo_core::network::NetworkDeliveryState::PossiblyApplied => {
                    crate::DaemonObjectRpcForwardDeliveryState::PossiblyApplied
                }
            }),
        }),
    }
}

fn daemon_object_rpc_forward_network_error(
    error: NetworkError,
) -> crate::DaemonObjectRpcForwardResponse {
    crate::DaemonObjectRpcForwardResponse::NetworkError {
        detail: error.to_string(),
        delivery_state: error.delivery_state().map(|state| match state {
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent => {
                crate::DaemonObjectRpcForwardDeliveryState::DefinitelyNotSent
            }
            sinomo_core::network::NetworkDeliveryState::PossiblyApplied => {
                crate::DaemonObjectRpcForwardDeliveryState::PossiblyApplied
            }
        }),
    }
}

async fn forward_object_rpc_session_from_daemon_control(
    paths: &crate::DaemonPaths,
    payload: crate::DaemonObjectRpcForwardRequest,
    request_body: sinomo_core::network::ObjectRpcIncomingBody,
    runtime: &tokio::runtime::Handle,
) -> Result<(crate::DaemonObjectRpcForwardResponse, ObjectRpcBodySource), ServiceError> {
    let transport_timeouts = sinomo_core::network::DaemonRpcTimeouts::default()
        .with_connect_timeout(Some(Duration::from_millis(payload.connect_timeout_millis)));
    let mut options = sinomo_core::network::ClusterObjectRpcOptions::new(transport_timeouts);
    if let Some(overall_timeout_millis) = payload.overall_timeout_millis {
        options = options.with_overall_timeout(Duration::from_millis(overall_timeout_millis));
    }

    // This is a trusted proxy boundary, not a signed relay. Rebind both body
    // finish proofs to the route signed on their next hop instead of carrying
    // the outer daemon-forward proof into the inner DFS call (or vice versa).
    let request_body = if request_body.is_empty() {
        ObjectRpcBodySource::empty()
    } else {
        let total_bytes = request_body.total_bytes();
        ObjectRpcBodySource::async_reader(request_body, total_bytes)
    };
    let call = ObjectRpcSessionCall::new(payload.request, request_body);
    match crate::client::send_object_rpc_session_to_target_async_with_daemon_paths_and_network_error(
        paths,
        &payload.target_node_id,
        call,
        options,
        runtime,
        payload.observe_connectivity,
    )
    .await
    {
        Ok(result) => {
            let (response, body) = result.into_parts();
            let body = if body.is_empty() {
                ObjectRpcBodySource::empty()
            } else {
                let total_bytes = body.total_bytes();
                ObjectRpcBodySource::async_reader(body, total_bytes)
            };
            Ok((
                crate::DaemonObjectRpcForwardResponse::Success { response },
                body,
            ))
        }
        Err(error) => Ok((
            daemon_object_rpc_forward_network_error(error),
            ObjectRpcBodySource::empty(),
        )),
    }
}

/// Real cluster-KV access for services that consume it. Reads come from the local
/// cluster-KV snapshot; commits go through the daemon's quorum-replicated txn path
/// (forwarding to the leader when needed).
pub(crate) struct DaemonClusterAccess {
    paths: DaemonPaths,
    cluster_paths: ClusterPaths,
    runtime: Option<tokio::runtime::Handle>,
    control_plane_timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
}

impl DaemonClusterAccess {
    pub(crate) fn new(
        paths: DaemonPaths,
        data_dir: &SinomoDataDir,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> Self {
        Self {
            cluster_paths: ClusterPaths::new(data_dir),
            paths,
            runtime: runtime.cloned(),
            control_plane_timeouts:
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport(),
        }
    }

    pub(crate) fn new_with_control_plane_timeouts(
        paths: DaemonPaths,
        data_dir: &SinomoDataDir,
        runtime: Option<&tokio::runtime::Handle>,
        control_plane_timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
    ) -> Self {
        Self {
            cluster_paths: ClusterPaths::new(data_dir),
            paths,
            runtime: runtime.cloned(),
            control_plane_timeouts,
        }
    }

    fn runtime(&self) -> Result<&tokio::runtime::Handle, ServiceError> {
        require_service_runtime(self.runtime.as_ref(), "daemon cluster access")
    }
}

impl ClusterAccess for DaemonClusterAccess {
    fn kv_get(&self, key: &str) -> Result<Option<KvEntry>, ServiceError> {
        sinomo_core::kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
            .map_err(ServiceError::from)
    }

    fn kv_range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, ServiceError> {
        sinomo_core::kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
            .map_err(ServiceError::from)
    }

    fn kv_commit_async<'a>(
        &'a self,
        txn: KvTxn,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<KvTxnResponse, ServiceError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            handle_daemon_cluster_kv_txn_request_async_on_runtime_with_timeouts(
                &self.paths,
                txn,
                runtime,
                self.control_plane_timeouts,
            )
            .await
            .map_err(daemon_error)
        })
    }
}

struct DaemonClusterKvLocalReadWatchHost<'a> {
    paths: &'a ClusterPaths,
}

impl sinomo_core::kv::ClusterKvLocalReadWatchHost for DaemonClusterKvLocalReadWatchHost<'_> {
    type Error = ServiceError;

    fn kv_error(error: sinomo_core::kv::KvError) -> Self::Error {
        ServiceError::from(error)
    }

    fn range_prefix(&mut self, prefix: &str) -> Result<Vec<KvEntry>, Self::Error> {
        sinomo_core::kv::read_cluster_kv_prefix_entries(&self.paths.kv_paths(), prefix)
            .map_err(ServiceError::from)
    }

    fn create_watch(
        &mut self,
        request: ClusterKvWatchPrefixRequest,
    ) -> Result<ClusterKvWatchRegistration, Self::Error> {
        let node_id = cluster_kv_local_node_id(self.paths)?;
        sinomo_core::kv::create_cluster_kv_watch_for_node(&self.paths.kv_paths(), node_id, request)
            .map_err(ServiceError::from)
    }

    fn drain_watch(
        &mut self,
        request: ClusterKvWatchDrainRequest,
    ) -> Result<ClusterKvWatchDrainResponse, Self::Error> {
        let node_id = cluster_kv_local_node_id(self.paths)?;
        sinomo_core::kv::drain_cluster_kv_watch_for_node(&self.paths.kv_paths(), node_id, request)
            .map_err(ServiceError::from)
    }

    fn cancel_watch(
        &mut self,
        request: ClusterKvWatchCancelRequest,
    ) -> Result<ClusterKvWatchCancelResponse, Self::Error> {
        sinomo_core::kv::cancel_cluster_kv_watch(&self.paths.kv_paths(), request)
            .map_err(ServiceError::from)
    }
}

fn cluster_kv_local_node_id(
    paths: &ClusterPaths,
) -> Result<sinomo_core::node::NodeId, ServiceError> {
    sinomo_core::cluster::read_cluster_state_without_kv(paths)
        .map(|state| state.local_node_id)
        .map_err(ServiceError::from)
}

fn object_rpc_response_from_kv_payload(
    payload: sinomo_core::kv::ClusterKvRpcPayload,
) -> ObjectRpcResponse {
    ObjectRpcResponse::from_encoded_body(payload.route, payload.body)
}

fn handle_cluster_kv_local_read_watch_request(
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
) -> Result<ObjectRpcResponse, ServiceError> {
    let mut host = DaemonClusterKvLocalReadWatchHost { paths };
    let payload = sinomo_core::kv::handle_cluster_kv_local_read_watch_rpc_payload(
        request.route.as_str(),
        &request.body,
        &mut host,
    )?
    .ok_or_else(|| ServiceError::UnknownRoute(request.route.clone()))?;
    Ok(object_rpc_response_from_kv_payload(payload))
}

fn handle_cluster_kv_watch_events_request(
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
) -> Result<ObjectRpcResponse, ServiceError> {
    let payload = sinomo_core::kv::decode_cluster_kv_watch_events_rpc_request_body(&request.body)?;
    let state = sinomo_core::cluster::read_cluster_state(paths)?;
    let response =
        sinomo_core::kv::handle_cluster_kv_watch_events(&paths.kv_paths(), &state, payload)?;
    let payload = sinomo_core::kv::cluster_kv_watch_events_response_rpc_payload(&response)?;
    Ok(object_rpc_response_from_kv_payload(payload))
}

fn encode_cluster_kv_snapshot_response(
    snapshot: &KvSnapshot,
) -> Result<ObjectRpcResponse, sinomo_core::network::NetworkError> {
    encode_cluster_kv_snapshot_response_limited(snapshot, CLUSTER_KV_SNAPSHOT_RPC_MAX_PAYLOAD_BYTES)
}

fn encode_cluster_kv_snapshot_response_limited(
    snapshot: &KvSnapshot,
    max_payload_bytes: usize,
) -> Result<ObjectRpcResponse, sinomo_core::network::NetworkError> {
    let response = object_rpc_response_from_kv_payload(
        sinomo_core::kv::cluster_kv_snapshot_response_rpc_payload(snapshot)
            .map_err(sinomo_core::network::NetworkError::directory)?,
    );
    if response.body.len() > max_payload_bytes {
        return ObjectRpcResponse::encode_error(
            CLUSTER_KV_SNAPSHOT_ROUTE,
            format!(
                "cluster kv snapshot response is {} bytes; max is {} bytes",
                response.body.len(),
                max_payload_bytes
            ),
        );
    }
    Ok(response)
}

async fn run_blocking_service_task<T>(
    route: impl Into<String>,
    label: &'static str,
    task: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError>
where
    T: Send + 'static,
{
    let route = route.into();
    tokio::task::spawn_blocking(task).await.map_err(|source| {
        ServiceError::Service(format!("{label} route {route} task failed: {source}"))
    })?
}

fn service_route_categories(
    routes: &'static [&'static str],
    category: &'static str,
) -> Vec<ServiceRoute> {
    routes
        .iter()
        .copied()
        .map(|route| ServiceRoute::new(route, category))
        .collect()
}

fn service_routes_from_metadata(
    metadata: &'static [(&'static str, &'static str)],
) -> Vec<ServiceRoute> {
    metadata
        .iter()
        .map(|(route, category)| ServiceRoute::new(*route, *category))
        .collect()
}

fn service_routes_from_kv_policies(
    policies: &'static [ClusterKvRpcRoutePolicy],
) -> Vec<ServiceRoute> {
    policies
        .iter()
        .map(|policy| {
            ServiceRoute::from_route_metadata(
                policy.route,
                policy.category,
                policy.auth.as_scope(),
                policy.required_capabilities.iter().copied(),
            )
        })
        .collect()
}

fn service_routes_from_cluster_control_policies(
    policies: &'static [sinomo_core::cluster::ClusterControlRoutePolicy],
) -> Vec<ServiceRoute> {
    policies
        .iter()
        .map(|policy| {
            ServiceRoute::from_route_metadata(
                policy.route,
                policy.category,
                policy.auth.as_scope(),
                policy.required_capabilities.iter().copied(),
            )
        })
        .collect()
}

fn object_rpc_response_from_cluster_control_payload(
    payload: sinomo_core::cluster::ClusterControlRpcPayload,
) -> ObjectRpcResponse {
    ObjectRpcResponse::from_encoded_body(payload.route, payload.body)
}

fn network_module_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = network_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("network module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown network service route {route}"));
    ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
}

fn rollout_binary_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = rollout_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("rollout module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown rollout binary service route {route}"));
    ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
}

/// Daemon-foundational service: daemon status + shutdown. Holds this daemon's paths so
/// status reflects the right (main/shadow) process; shutdown reports `Stop`.
struct DaemonControlService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl Service for DaemonControlService {
    fn name(&self) -> &'static str {
        "daemon-control"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[
            DAEMON_STATUS_ROUTE,
            DAEMON_SHUTDOWN_ROUTE,
            DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
            DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
            DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
        ]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        let mut routes = service_route_categories(self.routes(), DAEMON_RPC_CATEGORY);
        for route in &mut routes {
            if route.route == DAEMON_OBJECT_RPC_FORWARD_ROUTE
                || route.route == DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
                || route.route == DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE
            {
                route.auth_scope = Some("local-command".to_string());
            }
            if route.route == DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE {
                route.request_body_mode = ServiceBodyMode::Optional;
                route.response_body_mode = ServiceBodyMode::Optional;
            }
        }
        routes
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            if request.route == DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE {
                let payload: crate::DaemonObjectRpcForwardRequest = request.decode_body()?;
                let runtime =
                    require_service_runtime(runtime.as_ref(), "daemon DFS object RPC forward")?
                        .clone();
                let request_body = ctx.take_request_body();
                let (response, response_body) = forward_object_rpc_session_from_daemon_control(
                    &paths,
                    payload,
                    request_body,
                    &runtime,
                )
                .await?;
                ctx.respond_with_body(
                    ObjectRpcResponse::encode_body(DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE, &response)?,
                    response_body,
                )?;
                return Ok(ServiceDisposition::Continue);
            }
            let (response, disposition) = match request.route.as_str() {
                DAEMON_STATUS_ROUTE => {
                    let _: () = request.decode_body()?;
                    let status = daemon_status_response_async(&paths, runtime.as_ref())
                        .await
                        .map_err(daemon_error)?;
                    (
                        ObjectRpcResponse::encode_body(DAEMON_STATUS_ROUTE, &status)?,
                        ServiceDisposition::Continue,
                    )
                }
                DAEMON_SHUTDOWN_ROUTE => {
                    let _: () = request.decode_body()?;
                    crate::append_operational_event_best_effort(
                        &paths,
                        crate::OperationalSeverity::Info,
                        "daemon-control",
                        "daemon.lifecycle.shutdown_requested",
                        "daemon shutdown requested",
                        None,
                        serde_json::json!({
                            "route": DAEMON_SHUTDOWN_ROUTE,
                        }),
                    );
                    (
                        ObjectRpcResponse::encode_body(
                            DAEMON_SHUTDOWN_ROUTE,
                            &"sinomo daemon stopping".to_string(),
                        )?,
                        ServiceDisposition::Stop,
                    )
                }
                DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE => {
                    let _: crate::DaemonClusterMaintenanceRefreshRequest = request.decode_body()?;
                    crate::control::request_daemon_cluster_maintenance_refresh();
                    crate::dfs::notify_daemon_dfs_maintenance_work();
                    if let Some(runtime) = runtime.as_ref() {
                        if let Err(error) =
                            crate::dfs::schedule_daemon_dfs_metadata_publish_on_runtime(
                                &paths,
                                runtime,
                                request.traceparent().map(ToString::to_string),
                            )
                        {
                            tracing::warn!(
                                "sinomo daemon DFS metadata publish could not be scheduled after maintenance refresh: {error}"
                            );
                        }
                    }
                    tracing::info!("sinomo daemon cluster maintenance refresh requested by RPC");
                    (
                        ObjectRpcResponse::encode_body(
                            DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
                            &crate::DaemonClusterMaintenanceRefreshResponse,
                        )?,
                        ServiceDisposition::Continue,
                    )
                }
                DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE => {
                    let _: crate::DaemonLocalConnectivityReloadRequest = request.decode_body()?;
                    let cluster_paths =
                        cluster_paths_from_daemon_paths(&paths).map_err(daemon_error)?;
                    sinomo_core::network::invalidate_local_cluster_connectivity_cache(
                        &cluster_paths,
                    )?;
                    (
                        ObjectRpcResponse::encode_body(
                            DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
                            &crate::DaemonLocalConnectivityReloadResponse,
                        )?,
                        ServiceDisposition::Continue,
                    )
                }
                DAEMON_OBJECT_RPC_FORWARD_ROUTE => {
                    let payload: crate::DaemonObjectRpcForwardRequest = request.decode_body()?;
                    let runtime =
                        require_service_runtime(runtime.as_ref(), "daemon object RPC forward")?
                            .clone();
                    let response =
                        forward_object_rpc_from_daemon_control(&paths, payload, &runtime).await?;
                    (
                        ObjectRpcResponse::encode_body(request.route.as_str(), &response)?,
                        ServiceDisposition::Continue,
                    )
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(disposition)
        })
    }
}

/// Daemon-founded service adapter for cluster KV object-RPC routes. KV owns
/// route decoding and local read/watch semantics; the daemon provides the host
/// and uses network only for cluster authority checks and transport-aware paths.
struct ClusterKvService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl Service for ClusterKvService {
    fn name(&self) -> &'static str {
        "cluster-kv"
    }

    fn routes(&self) -> &'static [&'static str] {
        CLUSTER_KV_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        service_routes_from_kv_policies(CLUSTER_KV_RPC_ROUTE_POLICIES)
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let cluster_paths = ClusterPaths::new(ctx.data_dir.as_ref());
            let route = request.route.clone();
            if CLUSTER_KV_LOCAL_READ_WATCH_ROUTES.contains(&route.as_str()) {
                let request_for_task = request.clone();
                let cluster_paths_for_task = cluster_paths.clone();
                let response = run_blocking_service_task(
                    route.clone(),
                    "cluster kv local read/watch",
                    move || {
                        handle_cluster_kv_local_read_watch_request(
                            &cluster_paths_for_task,
                            &request_for_task,
                        )
                    },
                )
                .await?;
                ctx.respond(response)?;
                return Ok(ServiceDisposition::Continue);
            }
            if route == CLUSTER_KV_WATCH_EVENTS_ROUTE {
                let request_for_task = request.clone();
                let cluster_paths_for_task = cluster_paths.clone();
                let response = run_blocking_service_task(
                    route.clone(),
                    "cluster kv watch-event fanout",
                    move || {
                        handle_cluster_kv_watch_events_request(
                            &cluster_paths_for_task,
                            &request_for_task,
                        )
                    },
                )
                .await?;
                ctx.respond(response)?;
                return Ok(ServiceDisposition::Continue);
            }
            let response = match route.as_str() {
                CLUSTER_KV_SNAPSHOT_ROUTE => {
                    sinomo_core::kv::decode_cluster_kv_snapshot_rpc_request_body(&request.body)?;
                    let snapshot = handle_daemon_cluster_kv_snapshot_async(&paths)
                        .await
                        .map_err(daemon_error)?;
                    encode_cluster_kv_snapshot_response(&snapshot)?
                }
                CLUSTER_KV_TXN_ROUTE => {
                    let txn =
                        sinomo_core::kv::decode_cluster_kv_txn_rpc_request_body(&request.body)?;
                    let txn_paths = paths.clone();
                    let local_signer = ctx.signer.is_local();
                    let runtime =
                        require_service_runtime(runtime.as_ref(), "cluster kv transaction")?;
                    let result = if local_signer {
                        handle_daemon_cluster_kv_txn_request_async_on_runtime(
                            &txn_paths, txn, runtime,
                        )
                        .await
                    } else {
                        handle_forwarded_daemon_cluster_kv_txn_request_async_on_runtime(
                            &txn_paths, txn, runtime,
                        )
                        .await
                    }
                    .map_err(daemon_error)?;
                    let payload = sinomo_core::kv::cluster_kv_txn_response_rpc_payload(&result)?;
                    object_rpc_response_from_kv_payload(payload)
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Daemon adapter for the cluster health RPC. `sinomo-network` owns health aggregation;
/// this service only supplies daemon-local probing through the existing cluster host code.
struct ClusterHealthService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl Service for ClusterHealthService {
    fn name(&self) -> &'static str {
        "cluster-network"
    }

    fn routes(&self) -> &'static [&'static str] {
        NETWORK_OBJECT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        NETWORK_OBJECT_RPC_ROUTES
            .iter()
            .copied()
            .map(network_module_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let cluster_paths = ClusterPaths::new(ctx.data_dir.as_ref());
            if request.route == CLUSTER_NETWORK_PING_ROUTE {
                let responder_node_id =
                    cached_cluster_network_ping_node_id(&paths, ctx.data_dir.as_ref())?;
                let response =
                    sinomo_core::cluster::handle_cluster_network_ping_rpc_request_for_node(
                        &responder_node_id,
                        request,
                        time::OffsetDateTime::now_utc(),
                    )
                    .map_err(ServiceError::from)?;
                ctx.respond(response)?;
                return Ok(ServiceDisposition::Continue);
            }
            if request.route == CLUSTER_NETWORK_CONNECTIVITY_ROUTE {
                let _: () = request.decode_body()?;
                let response = run_blocking_service_task(
                    CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
                    "cluster network connectivity",
                    move || {
                        let state =
                            sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
                                .map_err(ServiceError::from)?;
                        let records =
                            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                                &cluster_paths,
                                &state.local_node_id,
                            )
                            .map_err(ServiceError::from)?;
                        ObjectRpcResponse::encode_body(CLUSTER_NETWORK_CONNECTIVITY_ROUTE, &records)
                            .map_err(ServiceError::from)
                    },
                )
                .await?;
                ctx.respond(response)?;
                return Ok(ServiceDisposition::Continue);
            }
            let runtime = require_service_runtime(runtime.as_ref(), "cluster health")?;
            let probe = DaemonClusterStatusProbe::new(&cluster_paths, runtime);
            let response = sinomo_core::cluster::handle_current_cluster_health_rpc_request_async(
                runtime,
                &cluster_paths,
                &probe,
                request,
                time::OffsetDateTime::now_utc(),
            )
            .await
            .map_err(ServiceError::from)?;
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub(crate) fn cached_cluster_network_ping_node_id(
    paths: &DaemonPaths,
    data_dir: &SinomoDataDir,
) -> Result<sinomo_core::node::NodeId, ServiceError> {
    let cache = DAEMON_CLUSTER_NETWORK_PING_NODE_IDS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(node_id) = cache
        .lock()
        .map_err(|source| ServiceError::Service(source.to_string()))?
        .get(&paths.root)
        .cloned()
    {
        return Ok(node_id);
    }

    let cluster_paths = ClusterPaths::new(data_dir);
    let node_id = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map(|state| state.local_node_id)
        .map_err(ServiceError::from)?;
    let mut guard = cache
        .lock()
        .map_err(|source| ServiceError::Service(source.to_string()))?;
    Ok(guard
        .entry(paths.root.clone())
        .or_insert_with(|| node_id)
        .clone())
}

pub(crate) fn mark_daemon_rpc_response_responder(
    paths: &DaemonPaths,
    response: &mut ObjectRpcResponse,
) {
    if response.responder_node_id().is_some() {
        return;
    }
    let Ok(data_dir_path) = daemon_data_dir_path(paths) else {
        return;
    };
    let data_dir = SinomoDataDir::new(data_dir_path);
    let Ok(node_id) = cached_cluster_network_ping_node_id(paths, &data_dir) else {
        return;
    };
    response.mark_responder_node_id(node_id.to_string());
}

/// Daemon adapter for one-hop cluster relay routing. `sinomo-network` owns the relay
/// decision; the daemon supplies local delivery by reinjecting through its control port.
struct ClusterRelayService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl Service for ClusterRelayService {
    fn name(&self) -> &'static str {
        "cluster-relay"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[CLUSTER_RELAY_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        service_routes_from_metadata(CLUSTER_ROUTING_RPC_ROUTE_METADATA)
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let response =
                handle_daemon_cluster_relay_request_on_runtime(&paths, &request, runtime.as_ref())
                    .await
                    .map_err(daemon_error)?;
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Daemon adapter for the KV-owned consensus routes. Request identity flows through
/// `ServiceContext::signer`; `sinomo-network` validates that identity against cluster
/// membership and applies the transport policy.
struct ClusterKvConsensusService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl Service for ClusterKvConsensusService {
    fn name(&self) -> &'static str {
        "cluster-kv-consensus"
    }

    fn routes(&self) -> &'static [&'static str] {
        CLUSTER_RAFT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        service_routes_from_kv_policies(CLUSTER_RAFT_RPC_ROUTE_POLICIES)
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let route = request.route.clone();
            if route == CLUSTER_RAFT_APPEND_ROUTE
                && let Some(stats) = crate::process_idle_stats()
            {
                stats.incr_raft_appends_recv();
            }
            let cluster_paths = cluster_paths_from_daemon_paths(&paths).map_err(daemon_error)?;
            let authenticated_peer = cluster_authenticated_peer(&ctx.signer, &route)?;
            let runtime = require_service_runtime(runtime.as_ref(), "cluster kv consensus rpc")?;
            let response = sinomo_core::cluster::handle_cluster_kv_consensus_rpc_request_async(
                runtime,
                &cluster_paths,
                request,
                &authenticated_peer,
            )
            .await
            .map_err(ServiceError::from)?;
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn cluster_authenticated_peer(
    signer: &ServiceSigner,
    route: &str,
) -> Result<sinomo_core::cluster::ClusterAuthenticatedPeer, ServiceError> {
    let (node_id, public_key) = service_signer_cluster_identity(signer, route)?;
    Ok(sinomo_core::cluster::ClusterAuthenticatedPeer {
        node_id,
        public_key,
    })
}

fn service_signer_cluster_identity(
    signer: &ServiceSigner,
    route: &str,
) -> Result<(String, String), ServiceError> {
    let Some((node_id, public_key)) = signer.cluster_identity() else {
        return Err(ServiceError::Service(format!(
            "cluster route {route} requires an authenticated cluster signer identity"
        )));
    };
    Ok((node_id.to_string(), public_key.to_string()))
}

/// Daemon adapter for cluster membership object-RPC routes. The domain join decision
/// lives in `sinomo-network`; the daemon handler still supplies host-local member refresh,
/// authority forwarding, and cluster state persistence.
/// Server-side budgets for join handling, deliberately below the joiner's
/// 30s idle deadline so a stuck step yields a structured error for the
/// one-shot join attempt.
const CLUSTER_JOIN_CHALLENGE_BUDGET: std::time::Duration = std::time::Duration::from_secs(20);
const CLUSTER_JOIN_REGISTRATION_BUDGET: std::time::Duration = std::time::Duration::from_secs(25);
static CLUSTER_JOIN_REGISTRATION_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn remember_accepted_join_response_state_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    accepted_state: sinomo_core::ClusterBootstrapState,
) -> Result<(usize, usize), NetworkError> {
    let paths = paths.clone();
    runtime
        .spawn_blocking(move || {
            let mut local_state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
            if local_state.cluster_id != accepted_state.cluster_id {
                return Err(NetworkError::Io {
                    operation: "remember accepted cluster join".to_string(),
                    detail: format!(
                        "accepted cluster {} does not match local cluster {}",
                        accepted_state.cluster_id, local_state.cluster_id
                    ),
                });
            }
            let before_members = local_state.members.len();
            sinomo_core::cluster::merge_cluster_join_registration(&mut local_state, accepted_state);
            let after_members = local_state.members.len();
            sinomo_core::cluster::write_cluster_state(&paths, &local_state)?;
            Ok((before_members, after_members))
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "remember accepted cluster join".to_string(),
            detail: format!("accepted cluster join state task failed: {source}"),
        })?
}

struct ClusterMembershipService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl Service for ClusterMembershipService {
    fn name(&self) -> &'static str {
        "cluster-membership"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[
            CLUSTER_JOIN_CHALLENGE_ROUTE,
            CLUSTER_JOIN_REGISTER_ROUTE,
            CLUSTER_INVITE_PUBLISH_ROUTE,
            CLUSTER_MEMBER_PUBLISH_ROUTE,
        ]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        service_routes_from_metadata(CLUSTER_MEMBERSHIP_RPC_ROUTE_METADATA)
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            if request.route == CLUSTER_JOIN_CHALLENGE_ROUTE {
                let challenge: sinomo_core::cluster::ClusterInviteChallengeRequest =
                    request.decode_body()?;
                crate::cluster_join_trace(format_args!(
                    "challenge invite={} received",
                    challenge.invite_id
                ));
                // The whole proof acquisition (local attempt + authority forward)
                // runs under a hard budget: a hung step must surface an error
                // to the joiner — never hold its stream open silently, or the
                // one-shot join command can wedge for its whole window.
                let proof_paths = paths.clone();
                let proof_runtime = runtime.clone();
                let proof_challenge = challenge.clone();
                let proof = tokio::time::timeout(CLUSTER_JOIN_CHALLENGE_BUDGET, async move {
                    let challenge_paths = proof_paths.clone();
                    let challenge_for_local = proof_challenge.clone();
                    let local_started = std::time::Instant::now();
                    let local_proof = tokio::task::spawn_blocking(move || {
                        crate::auth::create_cluster_invite_challenge_proof(
                            &challenge_paths,
                            &challenge_for_local,
                        )
                    })
                    .await
                    .map_err(|source| {
                        ServiceError::Service(format!(
                            "cluster join challenge route {CLUSTER_JOIN_CHALLENGE_ROUTE} task failed: {source}"
                        ))
                    })?;
                    match local_proof {
                        Ok(proof) => {
                            crate::cluster_join_trace(format_args!(
                                "challenge invite={} local proof ok in {:?}",
                                proof_challenge.invite_id,
                                local_started.elapsed()
                            ));
                            Ok(proof)
                        }
                        Err(local_error) => {
                            crate::cluster_join_trace(format_args!(
                                "challenge invite={} local proof failed in {:?}: {local_error}",
                                proof_challenge.invite_id,
                                local_started.elapsed()
                            ));
                            // A bridge introducer may hold the invite but not be proof-capable.
                            // Relay to a cluster state authority that can produce an issuer-signed proof.
                            let runtime = require_service_runtime(
                                proof_runtime.as_ref(),
                                "cluster join challenge",
                            )?;
                            let forward_started = std::time::Instant::now();
                            match forward_cluster_invite_challenge_to_cluster_state_authority_async_on_runtime(
                                &proof_paths,
                                &proof_challenge,
                                runtime,
                            )
                            .await
                            {
                                Ok(proof) => {
                                    crate::cluster_join_trace(format_args!(
                                        "challenge invite={} forward proof ok in {:?}",
                                        proof_challenge.invite_id,
                                        forward_started.elapsed()
                                    ));
                                    Ok(proof)
                                }
                                Err(forward_error) => {
                                    crate::cluster_join_trace(format_args!(
                                        "challenge invite={} forward proof failed in {:?}: {forward_error}",
                                        proof_challenge.invite_id,
                                        forward_started.elapsed()
                                    ));
                                    tracing::warn!(
                                        "sinomo daemon join challenge for invite {} failed (local: {local_error}; forward-to-authority: {forward_error})",
                                        proof_challenge.invite_id
                                    );
                                    Err(daemon_error(local_error))
                                }
                            }
                        }
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    crate::cluster_join_trace(format_args!(
                        "challenge invite={} timed out after {:?}",
                        challenge.invite_id, CLUSTER_JOIN_CHALLENGE_BUDGET
                    ));
                    tracing::warn!(
                        "sinomo daemon join challenge for invite {} timed out after {}s",
                        challenge.invite_id,
                        CLUSTER_JOIN_CHALLENGE_BUDGET.as_secs()
                    );
                    Err(ServiceError::Service(
                        "cluster join challenge timed out".to_string(),
                    ))
                })?;
                ctx.respond(ObjectRpcResponse::encode_body(
                    CLUSTER_JOIN_CHALLENGE_ROUTE,
                    &proof,
                )?)?;
                return Ok(ServiceDisposition::Continue);
            }
            let cluster_paths = cluster_paths_from_daemon_paths(&paths).map_err(daemon_error)?;
            if request.route == CLUSTER_JOIN_REGISTER_ROUTE {
                let payload: sinomo_core::cluster::ClusterJoinRegistrationRequest =
                    request.decode_body().map_err(ServiceError::from)?;
                let register_started = std::time::Instant::now();
                let invite_id = payload.invite.invite_id.clone();
                crate::cluster_join_trace(format_args!(
                    "register invite={} received with {} member(s)",
                    invite_id,
                    payload.members.len()
                ));
                let lock_started = std::time::Instant::now();
                let _registration_guard = CLUSTER_JOIN_REGISTRATION_LOCK
                    .get_or_init(|| tokio::sync::Mutex::new(()))
                    .lock()
                    .await;
                crate::cluster_join_trace(format_args!(
                    "register invite={} acquired registration lock in {:?}",
                    invite_id,
                    lock_started.elapsed()
                ));
                let service_runtime =
                    require_service_runtime(runtime.as_ref(), "cluster join registration")?;
                let response = tokio::time::timeout(
                    CLUSTER_JOIN_REGISTRATION_BUDGET,
                    sinomo_core::cluster::handle_cluster_join_registration_request_async_with_host(
                        service_runtime,
                        &cluster_paths,
                        payload,
                        time::OffsetDateTime::now_utc(),
                        &mut DaemonClusterJoinRegistrationHost::new_on_runtime(
                            &paths,
                            service_runtime,
                        ),
                    ),
                )
                .await
                .map_err(|_| {
                    crate::cluster_join_trace(format_args!(
                        "register invite={} timed out after {:?}",
                        invite_id, CLUSTER_JOIN_REGISTRATION_BUDGET
                    ));
                    tracing::warn!(
                        "sinomo daemon join registration timed out after {}s",
                        CLUSTER_JOIN_REGISTRATION_BUDGET.as_secs()
                    );
                    ServiceError::Service("cluster join registration timed out".to_string())
                })?
                .map_err(ServiceError::from)?;
                crate::cluster_join_trace(format_args!(
                    "register invite={} completed accepted={} in {:?}",
                    invite_id,
                    response.accepted,
                    register_started.elapsed()
                ));
                if response.accepted {
                    match remember_accepted_join_response_state_async(
                        service_runtime,
                        &cluster_paths,
                        response.state.clone(),
                    )
                    .await
                    {
                        Ok((before_members, after_members)) => {
                            crate::cluster_join_trace(format_args!(
                                "register invite={} remembered accepted state locally members {} -> {}",
                                invite_id, before_members, after_members
                            ));
                            crate::control::request_daemon_cluster_maintenance_refresh();
                        }
                        Err(error) => {
                            tracing::warn!(
                                "sinomo daemon join registration accepted invite {} but failed to refresh local state: {}",
                                invite_id,
                                error
                            );
                        }
                    }
                }
                ctx.respond(ObjectRpcResponse::encode_body(
                    CLUSTER_JOIN_REGISTER_ROUTE,
                    &response,
                )?)?;
                return Ok(ServiceDisposition::Continue);
            }
            let service_runtime =
                require_service_runtime(runtime.as_ref(), "cluster membership publish")?;
            let mut publish_host = DaemonClusterMemberMutationHost::new_on_runtime(service_runtime);
            let response = match request.route.as_str() {
                CLUSTER_INVITE_PUBLISH_ROUTE => {
                    let payload = request.decode_body().map_err(ServiceError::from)?;
                    let (signer_node_id, signer_public_key) =
                        service_signer_cluster_identity(&ctx.signer, CLUSTER_INVITE_PUBLISH_ROUTE)?;
                    let response =
                        sinomo_core::cluster::handle_cluster_invite_publish_request_async_with_host(
                            service_runtime,
                            &cluster_paths,
                            payload,
                            &signer_node_id,
                            &signer_public_key,
                            time::OffsetDateTime::now_utc(),
                            &mut publish_host,
                        )
                        .await
                        .map_err(ServiceError::from)?;
                    ObjectRpcResponse::encode_body(CLUSTER_INVITE_PUBLISH_ROUTE, &response)?
                }
                CLUSTER_MEMBER_PUBLISH_ROUTE => {
                    let payload = request.decode_body().map_err(ServiceError::from)?;
                    let (signer_node_id, _) =
                        service_signer_cluster_identity(&ctx.signer, CLUSTER_MEMBER_PUBLISH_ROUTE)?;
                    let response =
                        sinomo_core::cluster::handle_cluster_member_publish_request_async_with_host(
                            service_runtime,
                            &cluster_paths,
                            payload,
                            &signer_node_id,
                            &mut publish_host,
                        )
                        .await
                        .map_err(ServiceError::from)?;
                    ObjectRpcResponse::encode_body(CLUSTER_MEMBER_PUBLISH_ROUTE, &response)?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Daemon-foundational service: daemon-mediated cluster control
/// (`/cluster/control/*`). Each operation executes inside the daemon process,
/// reusing the daemon's cluster transports and hosts, so the CLI and app never
/// drive cluster mutations directly. Handlers are blocking drivers bridged via
/// `spawn_blocking`; every mutation commits through the existing quorum/txn
/// paths.
struct ClusterControlService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl Service for ClusterControlService {
    fn name(&self) -> &'static str {
        "cluster-control"
    }

    fn routes(&self) -> &'static [&'static str] {
        sinomo_core::cluster::CLUSTER_CONTROL_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        service_routes_from_cluster_control_policies(
            sinomo_core::cluster::CLUSTER_CONTROL_RPC_ROUTE_POLICIES,
        )
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let route = request.route.clone();
            let runtime = require_service_runtime(runtime.as_ref(), "cluster control")?.clone();
            let response = run_blocking_service_task(route.clone(), "cluster control", move || {
                handle_daemon_cluster_control_request(&paths, &request, &runtime)
            })
            .await?;
            ctx.respond(response)?;
            Ok(cluster_control_service_disposition(&route))
        })
    }
}

/// Cluster identity is captured by foundational daemon services and network
/// tasks at process startup. Once an operation creates or replaces that
/// identity, continuing to serve would expose stale authorization, routing,
/// and KV state. Drain after the response so the shadow supervisor can start
/// a process bound to the newly committed identity.
fn cluster_control_service_disposition(route: &str) -> ServiceDisposition {
    if cluster_control_service_restart_required(route) {
        ServiceDisposition::Stop
    } else {
        ServiceDisposition::Continue
    }
}

fn cluster_control_service_restart_required(route: &str) -> bool {
    matches!(
        route,
        sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE
            | sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE
            | sinomo_core::cluster::CLUSTER_CONTROL_LEAVE_ROUTE
    )
}

/// Mark a completed identity-changing response so a clean main-daemon exit is
/// classified as an intentional supervisor restart. Call this only after the
/// response has been delivered; otherwise a failed response could leave a
/// stale PID-scoped restart marker behind.
pub(crate) fn mark_cluster_identity_restart_after_response(
    route: &str,
    stop: bool,
    paths: Option<&DaemonPaths>,
) {
    if stop
        && cluster_control_service_restart_required(route)
        && let Some(paths) = paths
        && let Err(error) = crate::rollout::request_supervised_main_restart(paths)
    {
        tracing::error!(%error, "could not mark cluster identity restart for the shadow supervisor");
    }
}

/// Dispatches one `/cluster/control/*` request to the daemon-side operation
/// and encodes the typed response.
fn handle_daemon_cluster_control_request(
    paths: &DaemonPaths,
    request: &ObjectRpcRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResponse, ServiceError> {
    let payload = match request.route.as_str() {
        sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE => {
            let payload =
                sinomo_core::cluster::decode_cluster_control_init_rpc_request_body(&request.body)?;
            let response = handle_daemon_cluster_control_init_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            sinomo_core::cluster::cluster_control_init_response_rpc_payload(&response)?
        }
        sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE => {
            let payload =
                sinomo_core::cluster::decode_cluster_control_join_rpc_request_body(&request.body)?;
            let response = handle_daemon_cluster_control_join_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            sinomo_core::cluster::cluster_control_join_response_rpc_payload(&response)?
        }
        sinomo_core::cluster::CLUSTER_CONTROL_INVITE_ROUTE => {
            let payload = sinomo_core::cluster::decode_cluster_control_invite_rpc_request_body(
                &request.body,
            )?;
            let response = handle_daemon_cluster_control_invite_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            sinomo_core::cluster::cluster_control_invite_response_rpc_payload(&response)?
        }
        sinomo_core::cluster::CLUSTER_CONTROL_RENAME_ROUTE => {
            let payload = sinomo_core::cluster::decode_cluster_control_rename_rpc_request_body(
                &request.body,
            )?;
            let response = handle_daemon_cluster_control_rename_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            sinomo_core::cluster::cluster_control_rename_response_rpc_payload(&response)?
        }
        sinomo_core::cluster::CLUSTER_CONTROL_ROLE_ROUTE => {
            let payload =
                sinomo_core::cluster::decode_cluster_control_role_rpc_request_body(&request.body)?;
            let response = handle_daemon_cluster_control_role_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            sinomo_core::cluster::cluster_control_role_response_rpc_payload(&response)?
        }
        sinomo_core::cluster::CLUSTER_CONTROL_REMOVE_ROUTE => {
            let payload = sinomo_core::cluster::decode_cluster_control_remove_rpc_request_body(
                &request.body,
            )?;
            let response = handle_daemon_cluster_control_remove_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            sinomo_core::cluster::cluster_control_remove_response_rpc_payload(&response)?
        }
        sinomo_core::cluster::CLUSTER_CONTROL_LEAVE_ROUTE => {
            let payload =
                sinomo_core::cluster::decode_cluster_control_leave_rpc_request_body(&request.body)?;
            let response = handle_daemon_cluster_control_leave_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            sinomo_core::cluster::cluster_control_leave_response_rpc_payload(&response)?
        }
        other => return Err(ServiceError::UnknownRoute(other.to_string())),
    };
    Ok(object_rpc_response_from_cluster_control_payload(payload))
}

/// Daemon-foundational service: runtime network-module control
/// (`/network/modules` list + `/network/modules/set-enabled`), stage C of
/// docs/plans/r0-r1-crate-boundary-refactor.md. Toggles go through the
/// process-wide `NetworkModuleRegistry` (disable stops the module but never
/// unloads it) and persist the flag in the module loader state store so the
/// choice survives restarts.
struct NetworkModulesService {
    paths: DaemonPaths,
}

impl Service for NetworkModulesService {
    fn name(&self) -> &'static str {
        "network-modules"
    }

    fn routes(&self) -> &'static [&'static str] {
        sinomo_core::network::NETWORK_MODULES_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        service_routes_from_network_module_policies(
            sinomo_core::network::NETWORK_MODULES_RPC_ROUTE_POLICIES,
        )
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        Box::pin(async move {
            let route = request.route.clone();
            // Module start/stop hooks block (dylib work on its own runtime);
            // keep them off the async workers like the other daemon services.
            let response = run_blocking_service_task(route.clone(), "network modules", move || {
                handle_daemon_network_modules_request(&paths, &request)
            })
            .await?;
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn service_routes_from_network_module_policies(
    policies: &'static [sinomo_core::network::NetworkModuleRoutePolicy],
) -> Vec<ServiceRoute> {
    policies
        .iter()
        .map(|policy| {
            ServiceRoute::from_route_metadata(
                policy.route,
                policy.category,
                policy.auth.as_scope(),
                policy.required_capabilities.iter().copied(),
            )
        })
        .collect()
}

/// Dispatches one `/network/modules*` request against the daemon's loaded
/// network modules.
fn handle_daemon_network_modules_request(
    paths: &DaemonPaths,
    request: &ObjectRpcRequest,
) -> Result<ObjectRpcResponse, ServiceError> {
    let modules = crate::registry::daemon_network_modules().ok_or_else(|| {
        ServiceError::Service("daemon network module registry is not initialized".to_string())
    })?;
    match request.route.as_str() {
        sinomo_core::network::NETWORK_MODULES_LIST_ROUTE => {
            let _: () = request.decode_body()?;
            let response = sinomo_core::network::NetworkModuleListResponse {
                modules: modules.registry.snapshot(),
            };
            Ok(ObjectRpcResponse::encode_body(
                sinomo_core::network::NETWORK_MODULES_LIST_ROUTE,
                &response,
            )?)
        }
        sinomo_core::network::NETWORK_MODULES_SET_ENABLED_ROUTE => {
            let payload: sinomo_core::network::NetworkModuleSetEnabledRequest =
                request.decode_body()?;
            if payload.enabled {
                // A module registered pre-cluster has no stored context; give
                // it the current one so enabling actually starts the listener.
                match crate::cluster::daemon_network_module_context_now(&paths) {
                    Ok(Some(context)) => {
                        modules.registry.provide_context(&payload.id, &context);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(
                            "network module {} context refresh skipped: {error}",
                            payload.id
                        );
                    }
                }
            }
            let status = modules
                .registry
                .set_enabled(&payload.id, payload.enabled)
                .map_err(ServiceError::Network)?;
            if !payload.enabled && status.descriptor.listener {
                tracing::warn!(
                    "network module {} disabled: the node serves no {} listener until the module \
                     is re-enabled",
                    payload.id,
                    payload.id
                );
            }
            // Persist the flag so the toggle survives daemon restarts; a
            // persistence failure is surfaced but the runtime state already
            // changed, matching loader-state semantics elsewhere.
            if let Some(module_id) = modules.loader_module_ids.get(&payload.id) {
                let data_dir_root = daemon_data_dir_path(paths).map_err(|error| {
                    ServiceError::Service(format!(
                        "network module enabled flag not persisted: {error}"
                    ))
                })?;
                crate::registry::persist_network_module_enabled_state(
                    &data_dir_root,
                    module_id,
                    payload.enabled,
                    format!("set-enabled route toggled {}", payload.id),
                )
                .map_err(|error| {
                    ServiceError::Service(format!(
                        "network module enabled flag not persisted: {error}"
                    ))
                })?;
            }
            let response = sinomo_core::network::NetworkModuleSetEnabledResponse { module: status };
            Ok(ObjectRpcResponse::encode_body(
                sinomo_core::network::NETWORK_MODULES_SET_ENABLED_ROUTE,
                &response,
            )?)
        }
        other => Err(ServiceError::UnknownRoute(other.to_string())),
    }
}

/// Daemon-foundational service: rollout binary staging + source build. An accepted
/// rollout on the main daemon must restart the process, reported via
/// `ServiceDisposition::Stop` (the adapter then drains and stops). A handler error is
/// reported as a `failed` response rather than propagated, matching the prior dispatch.
struct RolloutBinaryService {
    paths: DaemonPaths,
}

impl RolloutBinaryService {
    fn finish(
        paths: &DaemonPaths,
        route: &'static str,
        result: Result<ClusterRolloutBinaryResponse, DaemonError>,
    ) -> Result<(ObjectRpcResponse, ServiceDisposition), ServiceError> {
        let response = result.unwrap_or_else(|error| ClusterRolloutBinaryResponse {
            status: "failed".to_string(),
            detail: error.to_string(),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        });
        let disposition = if accepted_rollout_should_stop_current_daemon(paths, &response) {
            ServiceDisposition::Stop
        } else {
            ServiceDisposition::Continue
        };
        Ok((
            ObjectRpcResponse::encode_body(route, &response)?,
            disposition,
        ))
    }
}

async fn decode_rollout_build_session_request(
    request: &ObjectRpcRequest,
    body: &mut sinomo_core::network::ObjectRpcIncomingBody,
) -> Result<sinomo_rollout::ClusterRolloutBuildRequest, ServiceError> {
    let mut payload: sinomo_rollout::ClusterRolloutBuildRequest = request.decode_body()?;
    if body.is_empty() {
        return Ok(payload);
    }
    if payload.source_bundle.is_some() {
        return Err(ServiceError::Service(
            "rollout build source bundle must be carried either in the request head or the session body, not both"
                .to_string(),
        ));
    }
    let mut source_bundle = Vec::new();
    body.read_to_end(&mut source_bundle)
        .await
        .map_err(|source| {
            ServiceError::Service(format!(
                "failed to receive rollout build source bundle body: {source}"
            ))
        })?;
    payload.source_bundle = Some(source_bundle);
    Ok(payload)
}

impl Service for RolloutBinaryService {
    fn name(&self) -> &'static str {
        "rollout-binary"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[
            ROLLOUT_BINARY_ROUTE,
            ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
            ROLLOUT_BUILD_ROUTE,
        ]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| {
                let service_route = rollout_binary_service_route(route);
                match route {
                    ROLLOUT_BINARY_ROUTE => {
                        service_route.with_request_body_mode(ServiceBodyMode::Required)
                    }
                    ROLLOUT_BUILD_ROUTE => {
                        service_route.with_request_body_mode(ServiceBodyMode::Optional)
                    }
                    _ => service_route,
                }
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        Box::pin(async move {
            let route = request.route.clone();
            let (response, disposition) = match route.as_str() {
                ROLLOUT_BINARY_ROUTE => {
                    let payload = request.decode_body()?;
                    let mut body = ctx.take_request_body();
                    let result =
                        handle_daemon_rollout_binary_request(&paths, payload, &mut body).await;
                    Self::finish(&paths, ROLLOUT_BINARY_ROUTE, result)?
                }
                ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE => {
                    let payload = request.decode_body()?;
                    let rollout_paths = paths.clone();
                    run_blocking_service_task(
                        route.clone(),
                        "rollout native dylib module",
                        move || {
                            Self::finish(
                                &rollout_paths,
                                ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
                                handle_daemon_rollout_native_dylib_module_request(
                                    &rollout_paths,
                                    payload,
                                ),
                            )
                        },
                    )
                    .await?
                }
                ROLLOUT_BUILD_ROUTE => {
                    let mut body = ctx.take_request_body();
                    let payload = decode_rollout_build_session_request(&request, &mut body).await?;
                    let rollout_paths = paths.clone();
                    run_blocking_service_task(route.clone(), "rollout build", move || {
                        Self::finish(
                            &rollout_paths,
                            ROLLOUT_BUILD_ROUTE,
                            handle_daemon_rollout_build_request(&rollout_paths, payload),
                        )
                    })
                    .await?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(disposition)
        })
    }
}

struct DaemonDfsBrowserService {
    runtime: Option<tokio::runtime::Handle>,
    event_hub: Arc<DaemonEventHub>,
}

struct DaemonDfsBrowserBackend {
    data_dir: SinomoDataDir,
    service: Mutex<sinomo_dfs::MountedSifsService>,
    peer_targets_refreshed_at: Mutex<Option<Instant>>,
}

#[derive(Clone, Debug)]
struct DaemonDfsBrowserControlPlaneKvClient {
    cluster_paths: ClusterPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl DaemonDfsBrowserControlPlaneKvClient {
    fn new(cluster_paths: &ClusterPaths, runtime: tokio::runtime::Handle) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
            runtime: Some(runtime),
        }
    }

    fn read_only(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
            runtime: None,
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
}

impl sinomo_dfs::DfsControlPlaneKvClient for DaemonDfsBrowserControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<sinomo_core::kv::KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            self.kv_error("read-only DFS browser KV client cannot submit transactions")
        })?;
        let response = crate::cluster::handle_cluster_kv_txn_request_with_cluster_paths_on_runtime(
            &self.cluster_paths,
            txn,
            true,
            runtime,
        )
        .map_err(|source| self.kv_error(source))?;
        Ok(response)
    }
}

static DAEMON_DFS_BROWSER_BACKENDS: OnceLock<
    Mutex<HashMap<PathBuf, Arc<DaemonDfsBrowserBackend>>>,
> = OnceLock::new();

// Browser reads run on blocking workers because the DFS POSIX surface is
// synchronous. A remote content fetch can consume the full 30-second POSIX
// deadline while holding the shared service lock. Do not let the other browser
// workers park behind that request: ordinary short local operations still get
// a small handoff window, while degraded remote reads fail fast and let their
// callers retry without building an in-process queue. This is an admission
// bound only; it does not impose an end-to-end timeout on an admitted DFS read.
const DAEMON_DFS_BROWSER_READ_LOCK_ADMISSION_TIMEOUT: Duration = Duration::from_millis(100);

impl DaemonDfsBrowserBackend {
    fn open(
        data_dir: SinomoDataDir,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, ServiceError> {
        let cluster_paths = ClusterPaths::new(&data_dir);
        let control_plane = cluster_paths.kv_paths().snapshot.exists().then(|| {
            Box::new(DaemonDfsBrowserControlPlaneKvClient::new(
                &cluster_paths,
                runtime.clone(),
            )) as Box<dyn sinomo_dfs::DfsControlPlaneKvClient>
        });
        let opened = sinomo_dfs::open_mounted_sifs_service_with_peer_transports(
            data_dir.clone(),
            daemon_dfs_blocking_peer_transport_on_runtime(&data_dir, runtime),
            daemon_dfs_async_peer_transport(&data_dir, runtime),
            control_plane,
        )
        .map_err(dfs_browser_service_error)?;
        Ok(Self {
            data_dir,
            service: Mutex::new(opened.service),
            peer_targets_refreshed_at: Mutex::new(Some(Instant::now())),
        })
    }

    fn for_data_dir(
        data_dir: Arc<SinomoDataDir>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Arc<Self>, ServiceError> {
        let root = data_dir.root();
        let cache = DAEMON_DFS_BROWSER_BACKENDS.get_or_init(|| Mutex::new(HashMap::new()));
        {
            let guard = cache.lock().map_err(|source| {
                ServiceError::Service(format!("DFS browser backend cache lock failed: {source}"))
            })?;
            if let Some(backend) = guard.get(&root) {
                return Ok(Arc::clone(backend));
            }
        }

        let backend = Arc::new(Self::open((*data_dir).clone(), runtime)?);
        let mut guard = cache.lock().map_err(|source| {
            ServiceError::Service(format!("DFS browser backend cache lock failed: {source}"))
        })?;
        Ok(Arc::clone(
            guard.entry(root).or_insert_with(|| Arc::clone(&backend)),
        ))
    }

    fn lock_service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, sinomo_dfs::MountedSifsService>, ServiceError> {
        self.service.lock().map_err(|source| {
            ServiceError::Service(format!("DFS browser service lock failed: {source}"))
        })
    }

    fn lock_service_for_read(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, sinomo_dfs::MountedSifsService>, ServiceError> {
        let started = Instant::now();
        loop {
            match self.service.try_lock() {
                Ok(service) => return Ok(service),
                Err(std::sync::TryLockError::Poisoned(source)) => {
                    return Err(ServiceError::Service(format!(
                        "DFS browser service lock failed: {source}"
                    )));
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let Some(remaining) = DAEMON_DFS_BROWSER_READ_LOCK_ADMISSION_TIMEOUT
                        .checked_sub(started.elapsed())
                        .filter(|remaining| !remaining.is_zero())
                    else {
                        return Err(ServiceError::Service(format!(
                            "DFS browser service is busy after waiting {}ms; retry the read",
                            DAEMON_DFS_BROWSER_READ_LOCK_ADMISSION_TIMEOUT.as_millis()
                        )));
                    };
                    std::thread::sleep(Duration::from_millis(1).min(remaining));
                }
            }
        }
    }

    fn refresh_peer_targets(&self, service: &mut sinomo_dfs::MountedSifsService) {
        let now = Instant::now();
        if let Ok(mut refreshed_at) = self.peer_targets_refreshed_at.lock() {
            if refreshed_at
                .as_ref()
                .is_some_and(|last| now.duration_since(*last) < Duration::from_secs(1))
            {
                return;
            }
            if self.refresh_peer_targets_now(service) {
                *refreshed_at = Some(now);
            }
            return;
        }
        let _ = self.refresh_peer_targets_now(service);
    }

    fn refresh_peer_targets_now(&self, service: &mut sinomo_dfs::MountedSifsService) -> bool {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) else {
            return false;
        };
        service.refresh_peer_targets(sinomo_dfs::cluster_dfs_peer_node_ids(&state));
        true
    }

    fn node_id(&self) -> Result<String, ServiceError> {
        Ok(self.lock_service_for_read()?.node_id().to_string())
    }

    fn event_origin_node_id(&self) -> Result<sinomo_core::Id, ServiceError> {
        Ok(self.lock_service_for_read()?.node_id().clone())
    }

    fn health_summary(&self) -> Result<crate::DaemonDfsBrowserHealthSummary, ServiceError> {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&self.data_dir, &cluster_paths)
            .map_err(dfs_browser_service_error)?;
        Ok(daemon_dfs_browser_health_summary(status))
    }

    fn list(
        &self,
        request: crate::DaemonDfsBrowserListRequest,
    ) -> Result<crate::DaemonDfsBrowserListResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let mut entries: Vec<_> = match service.posix_list_dir_distributed(&path) {
            Ok(entries) => entries,
            Err(sinomo_dfs::DfsPosixError::NotFound { .. }) if request.missing_is_empty => {
                Vec::new()
            }
            Err(error) => return Err(dfs_browser_service_error(error)),
        }
        .into_iter()
        .map(|entry| crate::DaemonDfsBrowserEntry {
            name: entry.name,
            path: entry.path,
            kind: entry.kind,
            size: None,
        })
        .collect();
        sort_daemon_dfs_browser_entries(&mut entries, &request.sort_key, &request.sort_direction);
        let total_count = entries.len();
        let offset = request.offset.min(total_count);
        let limit = request.limit;
        let entries = match limit {
            Some(limit) => entries.into_iter().skip(offset).take(limit).collect(),
            None => entries.into_iter().skip(offset).collect(),
        };
        Ok(crate::DaemonDfsBrowserListResponse {
            node_id: service.node_id().to_string(),
            entries,
            total_count,
            offset,
            limit,
        })
    }

    fn stat(
        &self,
        request: crate::DaemonDfsBrowserStatRequest,
    ) -> Result<crate::DaemonDfsBrowserStatResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(dfs_browser_service_error)?;
        let health = daemon_dfs_browser_object_health(&service, &stat.object_id);
        let local_pins =
            daemon_dfs_browser_local_pins(&self.data_dir, &stat.file_system_id, &stat.path);
        let local_node_id = service.node_id().clone();
        drop(service);
        let metadata_shard = self
            .metadata_shard_for_path(&stat.path, None)
            .ok()
            .flatten()
            .map(|shard| daemon_dfs_browser_metadata_shard(shard, &local_node_id));
        Ok(daemon_dfs_browser_stat_response(
            stat,
            local_pins,
            health,
            metadata_shard,
        ))
    }

    fn metadata_shard_for_path(
        &self,
        path: &str,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> Result<Option<sinomo_dfs::DfsMetadataShardConfig>, ServiceError> {
        let path = normalize_sifs_browser_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Ok(None);
        }
        let mut kv = if let Some(runtime) = runtime {
            DaemonDfsBrowserControlPlaneKvClient::new(&cluster_paths, runtime.clone())
        } else {
            DaemonDfsBrowserControlPlaneKvClient::read_only(&cluster_paths)
        };
        let store = sinomo_dfs::DfsControlPlaneStore::new(&mut kv);
        store
            .select_metadata_shard_for_path_strict_default_sifs_split(
                &sinomo_dfs::sifs_file_system_id(),
                &path,
            )
            .map_err(dfs_browser_service_error)
    }

    fn read_file(
        &self,
        request: crate::DaemonDfsBrowserReadFileRequest,
    ) -> Result<crate::DaemonDfsBrowserReadFileResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let bytes = service
            .posix_read_file_distributed(&path)
            .map_err(dfs_browser_service_error)?;
        Ok(crate::DaemonDfsBrowserReadFileResponse { bytes })
    }

    fn read_object_version(
        &self,
        request: crate::DaemonDfsBrowserReadObjectVersionRequest,
    ) -> Result<crate::DaemonDfsBrowserReadObjectVersionResponse, ServiceError> {
        let object_id = sinomo_dfs::DfsObjectId::new(&request.object_id)
            .map_err(|source| ServiceError::Service(source.to_string()))?;
        let version = sinomo_dfs::DfsVersion::new(&request.version)
            .map_err(|source| ServiceError::Service(source.to_string()))?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let bytes = service
            .read_object_version_content(
                &object_id,
                &version,
                &request.content_hash,
                request.size_bytes,
            )
            .map_err(dfs_browser_service_error)?;
        Ok(crate::DaemonDfsBrowserReadObjectVersionResponse { bytes })
    }

    fn read_file_prefix(
        &self,
        request: crate::DaemonDfsBrowserReadFilePrefixRequest,
    ) -> Result<crate::DaemonDfsBrowserReadFilePrefixResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let max_bytes = usize::try_from(request.max_bytes).unwrap_or(usize::MAX);
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(dfs_browser_service_error)?;
        let truncated = stat
            .size_bytes
            .map(|size| size > request.max_bytes)
            .unwrap_or(false);
        let bytes = service
            .posix_read_file_range(&path, 0, max_bytes)
            .map_err(dfs_browser_service_error)?;
        Ok(crate::DaemonDfsBrowserReadFilePrefixResponse { bytes, truncated })
    }

    fn read_files(
        &self,
        request: crate::DaemonDfsBrowserReadFilesRequest,
    ) -> Result<crate::DaemonDfsBrowserReadFilesResponse, ServiceError> {
        let paths = request
            .paths
            .into_iter()
            .map(|path| normalize_sifs_browser_path(&path).map(|normalized| (path, normalized)))
            .collect::<Result<Vec<_>, _>>()?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let files = paths
            .into_iter()
            .map(
                |(requested_path, path)| match service.posix_read_file_distributed(&path) {
                    Ok(bytes) => crate::DaemonDfsBrowserFileRead {
                        path: requested_path,
                        bytes: Some(bytes),
                        error: None,
                    },
                    Err(error) => crate::DaemonDfsBrowserFileRead {
                        path: requested_path,
                        bytes: None,
                        error: Some(error.to_string()),
                    },
                },
            )
            .collect();
        Ok(crate::DaemonDfsBrowserReadFilesResponse { files })
    }

    fn write_file(
        &self,
        request: crate::DaemonDfsBrowserWriteFileRequest,
    ) -> Result<crate::DaemonDfsBrowserWriteFileResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service()?;
        if let Some(parent) = parent_sifs_browser_path(&path) {
            service
                .posix_mkdir_all(&parent)
                .map_err(dfs_browser_service_error)?;
        }
        let replica_candidates =
            sinomo_dfs::discover_mounted_sifs_content_replica_candidates(&self.data_dir)
                .map_err(dfs_browser_service_error)?;
        let options = sifs_browser_write_options(&replica_candidates);
        service
            .posix_write_file(&path, &request.bytes, options)
            .map_err(dfs_browser_service_error)?;
        self.flush_after_mutation(&mut service)?;
        Ok(crate::DaemonDfsBrowserWriteFileResponse)
    }

    fn replace_file_if_version(
        &self,
        request: crate::DaemonDfsBrowserReplaceFileIfVersionRequest,
    ) -> Result<crate::DaemonDfsBrowserReplaceFileIfVersionResponse, ServiceError> {
        use crate::DaemonDfsBrowserReplaceFileIfVersionOutcome as Outcome;

        let path = match normalize_sifs_browser_path(&request.path) {
            Ok(path) => path,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: Outcome::Rejected {
                        detail: error.to_string(),
                    },
                });
            }
        };
        let object_id = match sinomo_dfs::DfsObjectId::new(&request.expected_object_id) {
            Ok(object_id) => object_id,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: Outcome::Rejected {
                        detail: format!("invalid expected SIFS object id: {error}"),
                    },
                });
            }
        };
        let version = match sinomo_dfs::DfsVersion::new(&request.expected_version) {
            Ok(version) => version,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: Outcome::Rejected {
                        detail: format!("invalid expected SIFS version: {error}"),
                    },
                });
            }
        };
        let mut service = self.lock_service()?;
        let replica_candidates =
            sinomo_dfs::discover_mounted_sifs_content_replica_candidates(&self.data_dir)
                .map_err(dfs_browser_service_error)?;
        let options = sifs_browser_write_options(&replica_candidates);
        let stat = match service.posix_replace_file_if_version(
            &path,
            &object_id,
            &version,
            &request.bytes,
            options,
        ) {
            Ok(stat) => stat,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: daemon_dfs_browser_conditional_write_error(error),
                });
            }
        };
        let maintenance_warning = self.flush_after_mutation(&mut service).err().map(|error| {
            format!(
                "the file version committed, but follow-up DFS maintenance needs retry: {error}"
            )
        });
        Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
            outcome: Outcome::Committed {
                receipt: daemon_dfs_browser_write_receipt(&stat),
                maintenance_warning,
            },
        })
    }

    fn create_dir_all(
        &self,
        request: crate::DaemonDfsBrowserCreateDirAllRequest,
    ) -> Result<crate::DaemonDfsBrowserCreateDirAllResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service()?;
        service
            .posix_mkdir_all(&path)
            .map_err(dfs_browser_service_error)?;
        self.flush_after_mutation(&mut service)?;
        Ok(crate::DaemonDfsBrowserCreateDirAllResponse)
    }

    fn rename(
        &self,
        request: crate::DaemonDfsBrowserRenameRequest,
    ) -> Result<crate::DaemonDfsBrowserRenameResponse, ServiceError> {
        let from = normalize_sifs_browser_path(&request.from)?;
        let to = normalize_sifs_browser_path(&request.to)?;
        let mut service = self.lock_service()?;
        service
            .posix_rename(&from, &to)
            .map_err(dfs_browser_service_error)?;
        self.flush_after_mutation(&mut service)?;
        Ok(crate::DaemonDfsBrowserRenameResponse)
    }

    fn delete(
        &self,
        request: crate::DaemonDfsBrowserDeleteRequest,
    ) -> Result<crate::DaemonDfsBrowserDeleteResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service()?;
        if request.directory {
            service
                .posix_rmdir(&path)
                .map_err(dfs_browser_service_error)?;
        } else {
            service
                .posix_unlink(&path)
                .map_err(dfs_browser_service_error)?;
        }
        self.flush_after_mutation(&mut service)?;
        Ok(crate::DaemonDfsBrowserDeleteResponse)
    }

    fn pin_path_mode(
        &self,
        path: &str,
        mode: sinomo_dfs::DfsPinMode,
    ) -> Result<String, ServiceError> {
        let path = normalize_sifs_browser_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_peer_targets(&mut service);
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(dfs_browser_service_error)?;
        let node_id = service.node_id().clone();
        drop(service);
        sinomo_dfs::add_local_dfs_pin(
            sinomo_dfs::default_dfs_pin_root(&self.data_dir),
            sinomo_dfs::DfsPinAddRequest {
                file_system_id: stat.file_system_id,
                path: stat.path,
                object_id: Some(stat.object_id),
                pinned_by_node_id: node_id.clone(),
                mode,
                created_at: time::OffsetDateTime::now_utc(),
            },
        )
        .map_err(dfs_browser_service_error)?;
        Ok(node_id.to_string())
    }

    fn unpin_path_mode(
        &self,
        path: &str,
        mode: sinomo_dfs::DfsPinMode,
    ) -> Result<bool, ServiceError> {
        let path = normalize_sifs_browser_path(path)?;
        let mut service = self.lock_service()?;
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(dfs_browser_service_error)?;
        let node_id = service.node_id().clone();
        drop(service);
        sinomo_dfs::remove_local_dfs_pin(
            sinomo_dfs::default_dfs_pin_root(&self.data_dir),
            &stat.file_system_id,
            &stat.path,
            Some(&node_id),
            Some(&mode),
        )
        .map_err(dfs_browser_service_error)
    }

    fn pin_metadata(
        &self,
        request: crate::DaemonDfsBrowserPinMetadataRequest,
    ) -> Result<crate::DaemonDfsBrowserPinMetadataResponse, ServiceError> {
        let node_id = self.pin_path_mode(&request.path, sinomo_dfs::DfsPinMode::Metadata)?;
        Ok(crate::DaemonDfsBrowserPinMetadataResponse { node_id })
    }

    fn unpin_metadata(
        &self,
        request: crate::DaemonDfsBrowserUnpinMetadataRequest,
    ) -> Result<crate::DaemonDfsBrowserUnpinMetadataResponse, ServiceError> {
        let removed = self.unpin_path_mode(&request.path, sinomo_dfs::DfsPinMode::Metadata)?;
        Ok(crate::DaemonDfsBrowserUnpinMetadataResponse { removed })
    }

    fn pin_content(
        &self,
        request: crate::DaemonDfsBrowserPinContentRequest,
    ) -> Result<crate::DaemonDfsBrowserPinContentResponse, ServiceError> {
        let node_id = self.pin_path_mode(&request.path, sinomo_dfs::DfsPinMode::Content)?;
        Ok(crate::DaemonDfsBrowserPinContentResponse { node_id })
    }

    fn unpin_content(
        &self,
        request: crate::DaemonDfsBrowserUnpinContentRequest,
    ) -> Result<crate::DaemonDfsBrowserUnpinContentResponse, ServiceError> {
        let removed = self.unpin_path_mode(&request.path, sinomo_dfs::DfsPinMode::Content)?;
        Ok(crate::DaemonDfsBrowserUnpinContentResponse { removed })
    }

    fn transfer_metadata_primary(
        &self,
        request: crate::DaemonDfsBrowserTransferMetadataPrimaryRequest,
        runtime: &tokio::runtime::Handle,
    ) -> Result<crate::DaemonDfsBrowserTransferMetadataPrimaryResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err(ServiceError::Service(
                "SIFS metadata primary transfer requires a cluster".to_string(),
            ));
        }
        let state = sinomo_core::cluster::read_cluster_state(&cluster_paths)
            .map_err(|error| ServiceError::Service(error.to_string()))?;
        let local_node_id = state.local_node_id;
        let mut kv = DaemonDfsBrowserControlPlaneKvClient::new(&cluster_paths, runtime.clone());
        let mut store = sinomo_dfs::DfsControlPlaneStore::new(&mut kv);
        let previous = store
            .select_metadata_shard_for_path_strict_default_sifs_split(
                &sinomo_dfs::sifs_file_system_id(),
                &path,
            )
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service(format!("no SIFS metadata shard controls {path}"))
            })?;
        if let Some(expected_epoch) = request.expected_epoch {
            if previous.epoch != expected_epoch {
                return Err(ServiceError::Service(format!(
                    "SIFS metadata shard {} epoch is {}, expected {}",
                    previous.shard_id, previous.epoch, expected_epoch
                )));
            }
        }
        if previous.primary_node_id == local_node_id {
            return Ok(crate::DaemonDfsBrowserTransferMetadataPrimaryResponse {
                file_system_id: previous.file_system_id.to_string(),
                shard_id: previous.shard_id.clone(),
                previous_primary_node_id: previous.primary_node_id.to_string(),
                new_primary_node_id: previous.primary_node_id.to_string(),
                previous_epoch: previous.epoch,
                new_epoch: previous.epoch,
                changed: false,
                metadata_shard: daemon_dfs_browser_metadata_shard(previous, &local_node_id),
            });
        }
        if !previous
            .replica_node_ids
            .iter()
            .any(|node_id| node_id == &local_node_id)
        {
            return Err(ServiceError::Service(format!(
                "local node {} is not a metadata replica for SIFS shard {}",
                local_node_id, previous.shard_id
            )));
        }
        let expected_epoch = previous.epoch;
        let updated = store
            .transfer_metadata_shard_primary(
                &previous.file_system_id,
                &previous.shard_id,
                expected_epoch,
                local_node_id.clone(),
            )
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service(format!(
                    "SIFS metadata shard {} changed while transferring primary; refresh placement and retry",
                    previous.shard_id
                ))
            })?;
        if let Ok(mut service) = self.lock_service() {
            service.invalidate_sifs_metadata_shard_cache();
        }
        Ok(crate::DaemonDfsBrowserTransferMetadataPrimaryResponse {
            file_system_id: previous.file_system_id.to_string(),
            shard_id: previous.shard_id,
            previous_primary_node_id: previous.primary_node_id.to_string(),
            new_primary_node_id: updated.primary_node_id.to_string(),
            previous_epoch: expected_epoch,
            new_epoch: updated.epoch,
            changed: true,
            metadata_shard: daemon_dfs_browser_metadata_shard(updated, &local_node_id),
        })
    }

    fn flush_after_mutation(
        &self,
        service: &mut sinomo_dfs::MountedSifsService,
    ) -> Result<(), ServiceError> {
        service
            .flush_deferred_content_durability()
            .map_err(dfs_browser_service_error)?;
        service
            .flush_detached_transport_sends()
            .map_err(dfs_browser_service_error)?;
        service
            .process_incoming()
            .map_err(dfs_browser_service_error)?;
        Ok(())
    }
}

impl Service for DaemonDfsBrowserService {
    fn name(&self) -> &'static str {
        "dfs-browser"
    }

    fn routes(&self) -> &'static [&'static str] {
        DAEMON_DFS_BROWSER_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| {
                ServiceRoute::new(route, DAEMON_RPC_CATEGORY)
                    .with_auth_scope("local-command")
                    .require_capability("daemon.dfs.browser")
                    .with_concurrency_limit(4)
                    .with_timeout(Duration::from_secs(60))
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let runtime = self.runtime.clone();
        let event_hub = Arc::clone(&self.event_hub);
        Box::pin(async move {
            let runtime = require_service_runtime(runtime.as_ref(), "DFS browser")?.clone();
            let route = request.route.clone();
            let request_traceparent = request.traceparent().map(ToString::to_string);
            let data_dir = Arc::clone(&ctx.data_dir);
            let response = match route.as_str() {
                DAEMON_DFS_BROWSER_STATUS_ROUTE => {
                    let payload: crate::DaemonDfsBrowserStatusRequest = request.decode_body()?;
                    let backend =
                        run_blocking_service_task(route.clone(), "DFS browser status", move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)
                        })
                        .await?;
                    let (health, health_error) = if payload.include_health {
                        match backend.health_summary() {
                            Ok(health) => (Some(health), None),
                            Err(error) => (None, Some(error.to_string())),
                        }
                    } else {
                        (None, None)
                    };
                    ObjectRpcResponse::encode_body(
                        DAEMON_DFS_BROWSER_STATUS_ROUTE,
                        &crate::DaemonDfsBrowserStatusResponse {
                            node_id: backend.node_id()?,
                            health,
                            health_error,
                        },
                    )?
                }
                DAEMON_DFS_BROWSER_LIST_ROUTE => {
                    let payload = request.decode_body()?;
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser list", move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?.list(payload)
                        })
                        .await?;
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_LIST_ROUTE, &result)?
                }
                DAEMON_DFS_BROWSER_STAT_ROUTE => {
                    let payload = request.decode_body()?;
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser stat", move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?.stat(payload)
                        })
                        .await?;
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_STAT_ROUTE, &result)?
                }
                DAEMON_DFS_BROWSER_READ_FILE_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser read file",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .read_file(payload)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_READ_FILE_ROUTE, &result)?
                }
                DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser read object version",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .read_object_version(payload)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(
                        DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
                        &result,
                    )?
                }
                DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser read file prefix",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .read_file_prefix(payload)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(
                        DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
                        &result,
                    )?
                }
                DAEMON_DFS_BROWSER_READ_FILES_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser read files",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .read_files(payload)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_READ_FILES_ROUTE, &result)?
                }
                DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE => {
                    let payload: crate::DaemonDfsBrowserWriteFileRequest = request.decode_body()?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser write file",
                        move || {
                            sinomo_dfs::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DaemonDfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                )?;
                                let result = backend.write_file(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        },
                    )
                    .await?;
                    schedule_daemon_sifs_metadata_publish(
                        publish_data_dir.as_ref(),
                        Some(&runtime),
                        request_traceparent.as_deref(),
                    );
                    event_hub.publish_dfs_namespace_change(
                        result.1.clone(),
                        "write-file",
                        vec![changed_path],
                    );
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE, &result.0)?
                }
                DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE => {
                    let payload: crate::DaemonDfsBrowserReplaceFileIfVersionRequest =
                        request.decode_body()?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser conditional replace file",
                        move || {
                            sinomo_dfs::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DaemonDfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                )?;
                                let result = backend.replace_file_if_version(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        },
                    )
                    .await?;
                    if result.0.outcome.is_committed() {
                        schedule_daemon_sifs_metadata_publish(
                            publish_data_dir.as_ref(),
                            Some(&runtime),
                            request_traceparent.as_deref(),
                        );
                        event_hub.publish_dfs_namespace_change(
                            result.1.clone(),
                            "replace-file-if-version",
                            vec![changed_path],
                        );
                    }
                    ObjectRpcResponse::encode_body(
                        DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
                        &result.0,
                    )?
                }
                DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE => {
                    let payload: crate::DaemonDfsBrowserCreateDirAllRequest =
                        request.decode_body()?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser create dir all",
                        move || {
                            sinomo_dfs::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DaemonDfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                )?;
                                let result = backend.create_dir_all(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        },
                    )
                    .await?;
                    schedule_daemon_sifs_metadata_publish(
                        publish_data_dir.as_ref(),
                        Some(&runtime),
                        request_traceparent.as_deref(),
                    );
                    event_hub.publish_dfs_namespace_change(
                        result.1.clone(),
                        "create-dir-all",
                        vec![changed_path],
                    );
                    ObjectRpcResponse::encode_body(
                        DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
                        &result.0,
                    )?
                }
                DAEMON_DFS_BROWSER_RENAME_ROUTE => {
                    let payload: crate::DaemonDfsBrowserRenameRequest = request.decode_body()?;
                    let changed_paths = vec![payload.from.clone(), payload.to.clone()];
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser rename", move || {
                            sinomo_dfs::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DaemonDfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                )?;
                                let result = backend.rename(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        })
                        .await?;
                    schedule_daemon_sifs_metadata_publish(
                        publish_data_dir.as_ref(),
                        Some(&runtime),
                        request_traceparent.as_deref(),
                    );
                    event_hub.publish_dfs_namespace_change(
                        result.1.clone(),
                        "rename",
                        changed_paths,
                    );
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_RENAME_ROUTE, &result.0)?
                }
                DAEMON_DFS_BROWSER_DELETE_ROUTE => {
                    let payload: crate::DaemonDfsBrowserDeleteRequest = request.decode_body()?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser delete", move || {
                            sinomo_dfs::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DaemonDfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                )?;
                                let result = backend.delete(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        })
                        .await?;
                    schedule_daemon_sifs_metadata_publish(
                        publish_data_dir.as_ref(),
                        Some(&runtime),
                        request_traceparent.as_deref(),
                    );
                    event_hub.publish_dfs_namespace_change(
                        result.1.clone(),
                        "delete",
                        vec![changed_path],
                    );
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_DELETE_ROUTE, &result.0)?
                }
                DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser pin metadata",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .pin_metadata(payload)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE, &result)?
                }
                DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser unpin metadata",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .unpin_metadata(payload)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(
                        DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE,
                        &result,
                    )?
                }
                DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser pin content",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .pin_content(payload)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE, &result)?
                }
                DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser unpin content",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .unpin_content(payload)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE, &result)?
                }
                DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE => {
                    let payload = request.decode_body()?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser metadata primary transfer",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .transfer_metadata_primary(payload, &runtime)
                        },
                    )
                    .await?;
                    ObjectRpcResponse::encode_body(
                        DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
                        &result,
                    )?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn daemon_dfs_browser_stat_response(
    stat: sinomo_dfs::DfsStat,
    local_pins: Vec<crate::DaemonDfsBrowserPinRecord>,
    health: Option<crate::DaemonDfsBrowserObjectHealth>,
    metadata_shard: Option<crate::DaemonDfsBrowserMetadataShard>,
) -> crate::DaemonDfsBrowserStatResponse {
    crate::DaemonDfsBrowserStatResponse {
        file_system_id: stat.file_system_id.to_string(),
        object_id: stat.object_id.to_string(),
        path: stat.path,
        kind: stat.kind,
        version: stat.version.to_string(),
        content_hash: stat.content_hash,
        size_bytes: stat.size_bytes,
        link_count: stat.link_count,
        mode: stat.mode,
        uid: stat.uid,
        gid: stat.gid,
        accessed_at_unix_nanos: stat.accessed_at_unix_nanos,
        modified_at_unix_nanos: stat.modified_at_unix_nanos,
        created_at_unix_nanos: stat.created_at_unix_nanos,
        changed_at_unix_nanos: stat.changed_at_unix_nanos,
        policy: stat.policy,
        current_locations: stat
            .current_locations
            .into_iter()
            .map(|node_id| node_id.to_string())
            .collect(),
        local_pins,
        health,
        metadata_shard,
    }
}

fn sort_daemon_dfs_browser_entries(
    entries: &mut [crate::DaemonDfsBrowserEntry],
    sort_key: &crate::DaemonDfsBrowserSortKey,
    sort_direction: &crate::DaemonDfsBrowserSortDirection,
) {
    entries.sort_by(|left, right| {
        let directory_order = daemon_dfs_browser_kind_rank(&left.kind)
            .cmp(&daemon_dfs_browser_kind_rank(&right.kind));
        if directory_order != std::cmp::Ordering::Equal {
            return directory_order;
        }
        let ordering = match sort_key {
            crate::DaemonDfsBrowserSortKey::Name => compare_daemon_dfs_browser_names(left, right),
            crate::DaemonDfsBrowserSortKey::Kind => daemon_dfs_browser_kind_rank(&left.kind)
                .cmp(&daemon_dfs_browser_kind_rank(&right.kind))
                .then_with(|| compare_daemon_dfs_browser_names(left, right)),
            crate::DaemonDfsBrowserSortKey::Size => left
                .size
                .unwrap_or(0)
                .cmp(&right.size.unwrap_or(0))
                .then_with(|| compare_daemon_dfs_browser_names(left, right)),
        };
        match sort_direction {
            crate::DaemonDfsBrowserSortDirection::Ascending => ordering,
            crate::DaemonDfsBrowserSortDirection::Descending => ordering.reverse(),
        }
    });
}

fn daemon_dfs_browser_kind_rank(kind: &sinomo_dfs::DfsObjectKind) -> u8 {
    match kind {
        sinomo_dfs::DfsObjectKind::Directory | sinomo_dfs::DfsObjectKind::Mount => 0,
        sinomo_dfs::DfsObjectKind::Symlink | sinomo_dfs::DfsObjectKind::Hardlink => 1,
        sinomo_dfs::DfsObjectKind::File | sinomo_dfs::DfsObjectKind::Artifact => 2,
        sinomo_dfs::DfsObjectKind::Tombstone => 3,
    }
}

fn compare_daemon_dfs_browser_names(
    left: &crate::DaemonDfsBrowserEntry,
    right: &crate::DaemonDfsBrowserEntry,
) -> std::cmp::Ordering {
    left.name
        .to_ascii_lowercase()
        .cmp(&right.name.to_ascii_lowercase())
        .then_with(|| left.name.cmp(&right.name))
}

fn daemon_dfs_browser_metadata_shard(
    shard: sinomo_dfs::DfsMetadataShardConfig,
    local_node_id: &sinomo_core::node::NodeId,
) -> crate::DaemonDfsBrowserMetadataShard {
    let primary_node_id = shard.primary_node_id.to_string();
    let local_node_id = local_node_id.to_string();
    let replica_node_ids = shard
        .replica_node_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let local_is_primary = primary_node_id == local_node_id;
    let local_is_replica = replica_node_ids
        .iter()
        .any(|node_id| node_id == &local_node_id);
    crate::DaemonDfsBrowserMetadataShard {
        shard_id: shard.shard_id,
        primary_node_id,
        replica_node_ids,
        epoch: shard.epoch,
        local_node_id,
        local_is_primary,
        local_is_replica,
    }
}

fn daemon_dfs_browser_health_summary(
    status: sinomo_dfs::DfsOperatorStatus,
) -> crate::DaemonDfsBrowserHealthSummary {
    crate::DaemonDfsBrowserHealthSummary {
        file_system_count: status.file_system_count,
        object_count: status.object_count,
        version_count: status.version_count,
        pending_write_count: status.pending_write_count,
        open_conflict_count: status.open_conflict_count,
        pending_repair_count: status.pending_repair_count,
        pending_namespace_job_count: status.pending_namespace_job_count,
        failed_namespace_job_count: status.failed_namespace_job_count,
        metadata_shard_count: status.metadata_shards.len(),
        repair_attempt_count: status
            .repair_attempt_count
            .max(status.repair_attempts.len()),
    }
}

fn daemon_dfs_browser_local_pins(
    data_dir: &SinomoDataDir,
    file_system_id: &sinomo_dfs::DfsFileSystemId,
    path: &str,
) -> Vec<crate::DaemonDfsBrowserPinRecord> {
    let normalized_path = normalize_sifs_browser_path(path).unwrap_or_else(|_| path.to_string());
    let Ok(pins) = sinomo_dfs::list_local_dfs_pins(
        sinomo_dfs::default_dfs_pin_root(data_dir),
        Some(file_system_id),
    ) else {
        return Vec::new();
    };
    pins.into_iter()
        .filter(|pin| {
            normalize_sifs_browser_path(&pin.path).unwrap_or_else(|_| pin.path.clone())
                == normalized_path
        })
        .map(|pin| crate::DaemonDfsBrowserPinRecord {
            pinned_by_node_id: pin.pinned_by_node_id.to_string(),
            mode: pin.mode,
            created_at_unix_nanos: u64::try_from(pin.created_at.unix_timestamp_nanos()).ok(),
        })
        .collect()
}

fn daemon_dfs_browser_object_health(
    service: &sinomo_dfs::MountedSifsService,
    object_id: &sinomo_dfs::DfsObjectId,
) -> Option<crate::DaemonDfsBrowserObjectHealth> {
    let backend = service.metadata_backend()?;
    let view = sinomo_dfs::DfsOperatorInspector::new(backend)
        .object(object_id)
        .ok()
        .flatten()?;
    let missing_replica_nodes = view
        .repairs
        .iter()
        .map(|repair| repair.missing_replica_node_id.to_string())
        .collect();
    Some(crate::DaemonDfsBrowserObjectHealth {
        agent_state: view.agent_state,
        version_count: view.versions.len(),
        receipt_count: view.receipts.len(),
        intent_count: view.intents.len(),
        open_conflict_count: view.conflicts.len(),
        pending_repair_count: view.repairs.len(),
        missing_replica_nodes,
    })
}

fn schedule_daemon_sifs_metadata_publish(
    data_dir: &SinomoDataDir,
    runtime: Option<&tokio::runtime::Handle>,
    traceparent: Option<&str>,
) {
    crate::dfs::notify_daemon_dfs_maintenance_work();
    let Some(runtime) = runtime else {
        tracing::warn!(
            "sinomo daemon DFS browser metadata publish has no runtime; maintenance will retry"
        );
        return;
    };
    let daemon_paths = crate::DaemonPaths::new(data_dir);
    match crate::dfs::schedule_daemon_dfs_metadata_publish_on_runtime(
        &daemon_paths,
        runtime,
        traceparent.map(ToString::to_string),
    ) {
        Ok(start) => tracing::trace!(
            target: "sinomo.dfs.perf",
            traceparent = %traceparent.unwrap_or_default(),
            ?start,
            "dfs-browser-metadata-publish-scheduled"
        ),
        Err(error) => tracing::warn!(
            traceparent = %traceparent.unwrap_or_default(),
            "sinomo daemon DFS browser metadata publish could not be scheduled; maintenance will retry: {error}"
        ),
    }
}

fn sifs_browser_write_options(
    replica_candidates: &[sinomo_core::node::NodeId],
) -> sinomo_dfs::DfsWriteOptions {
    let mut options = sinomo_dfs::DfsWriteOptions::replicated_eventual(replica_candidates.to_vec());
    options.foreground_replica_repair = false;
    options
}

fn dfs_browser_service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}

fn daemon_dfs_browser_write_receipt(
    stat: &sinomo_dfs::DfsStat,
) -> crate::DaemonDfsBrowserWriteReceipt {
    crate::DaemonDfsBrowserWriteReceipt {
        object_id: stat.object_id.to_string(),
        version: stat.version.to_string(),
        content_hash: stat.content_hash.clone(),
        size_bytes: stat.size_bytes,
    }
}

fn daemon_dfs_browser_conditional_write_error(
    error: sinomo_dfs::DfsPosixError,
) -> crate::DaemonDfsBrowserReplaceFileIfVersionOutcome {
    use crate::DaemonDfsBrowserReplaceFileIfVersionOutcome as Outcome;

    match error {
        sinomo_dfs::DfsPosixError::Conflict { path, reason } => Outcome::Conflict {
            detail: format!("write conflicted at {path}: {reason}"),
        },
        sinomo_dfs::DfsPosixError::NotFound { path } => Outcome::NotFound {
            detail: format!("not found: {path}"),
        },
        error => Outcome::Rejected {
            detail: error.to_string(),
        },
    }
}

fn normalize_sifs_browser_path(path: &str) -> Result<String, ServiceError> {
    let path = path.trim();
    let path = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if path.bytes().any(|byte| byte == 0) {
        return Err(ServiceError::Service(format!("invalid DFS path: {path}")));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn parent_sifs_browser_path(path: &str) -> Option<String> {
    let path = normalize_sifs_browser_path(path).ok()?;
    if path == "/" {
        return None;
    }
    let (parent, _) = path.rsplit_once('/')?;
    Some(if parent.is_empty() {
        "/".to_string()
    } else {
        parent.to_string()
    })
}

/// Maps a daemon error from a wrapped sandbox handler into the host-agnostic backend
/// error (the message is preserved; the seam re-maps it to `DaemonError` on the way out).
fn daemon_sandbox_error(error: DaemonError) -> SandboxBackendError {
    SandboxBackendError::Other(error.to_string())
}

/// Clustered sandbox backend: serves the sandbox routes against the daemon's SIFS-over-DFS
/// store by delegating to the existing daemon handlers (which evaluate cluster policy, emit
/// events, and require cluster state).
struct DaemonSandboxBackend {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl SandboxBackend for DaemonSandboxBackend {
    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError> {
        handle_daemon_sandbox_create_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError> {
        handle_daemon_sandbox_status_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError> {
        handle_daemon_sandbox_update_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError> {
        handle_daemon_sandbox_checkpoint_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError> {
        handle_daemon_sandbox_resume_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError> {
        handle_daemon_sandbox_reboot_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError> {
        handle_daemon_sandbox_destroy_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError> {
        handle_daemon_sandbox_list_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError> {
        handle_daemon_sandbox_read_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError> {
        handle_daemon_sandbox_write_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn http_fetch(
        &mut self,
        request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError> {
        handle_daemon_sandbox_http_fetch(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn http_fetch_to_file(
        &mut self,
        request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
        handle_daemon_sandbox_http_fetch_to_file_on_runtime(
            &self.paths,
            request,
            self.runtime.as_ref(),
        )
        .map_err(daemon_sandbox_error)
    }

    fn import_busybox(
        &mut self,
        request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
        handle_daemon_sandbox_import_busybox_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn toolchain_add_command(
        &mut self,
        request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
        handle_daemon_sandbox_toolchain_add_command_on_runtime(
            &self.paths,
            request,
            self.runtime.as_ref(),
        )
        .map_err(daemon_sandbox_error)
    }

    fn toolchain_add_artifact(
        &mut self,
        request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
        handle_daemon_sandbox_toolchain_add_artifact_on_runtime(
            &self.paths,
            request,
            self.runtime.as_ref(),
        )
        .map_err(daemon_sandbox_error)
    }

    fn import_toolchain_package(
        &mut self,
        request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        handle_daemon_sandbox_toolchain_import_package_on_runtime(
            &self.paths,
            request,
            self.runtime.as_ref(),
        )
        .map_err(daemon_sandbox_error)
    }

    fn import_toolchain_package_from_path(
        &mut self,
        request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        handle_daemon_sandbox_toolchain_import_package_from_path_on_runtime(
            &self.paths,
            request,
            self.runtime.as_ref(),
        )
        .map_err(daemon_sandbox_error)
    }

    fn run_toolchain(
        &mut self,
        request: SandboxRunToolchainRequest,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        handle_daemon_sandbox_run_toolchain_on_runtime(&self.paths, request, self.runtime.as_ref())
            .map_err(daemon_sandbox_error)
    }

    fn run_toolchain_with_cancellation(
        &mut self,
        request: SandboxRunToolchainRequest,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        handle_daemon_sandbox_run_toolchain_with_cancellation_on_runtime(
            &self.paths,
            request,
            self.runtime.as_ref(),
            cancellation_token,
        )
        .map_err(daemon_sandbox_error)
    }

    fn run_process_plan(
        &mut self,
        request: SandboxRunProcessPlanRequest,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        handle_daemon_sandbox_run_process_plan_on_runtime(
            &self.paths,
            request,
            self.runtime.as_ref(),
        )
        .map_err(daemon_sandbox_error)
    }

    fn run_process_plan_with_cancellation(
        &mut self,
        request: SandboxRunProcessPlanRequest,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        handle_daemon_sandbox_run_process_plan_with_cancellation_on_runtime(
            &self.paths,
            request,
            self.runtime.as_ref(),
            cancellation_token,
        )
        .map_err(daemon_sandbox_error)
    }
}

/// Selects the sandbox backend by context: the daemon's clustered SIFS-over-DFS store when
/// a cluster is initialized, else the host-agnostic local filesystem store — so sandbox
/// works on this node even without a cluster.
struct DaemonSandboxBackendFactory {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl SandboxBackendFactory for DaemonSandboxBackendFactory {
    fn open(&self, ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError> {
        if ctx.cluster.is_some() {
            Ok(Box::new(DaemonSandboxBackend {
                paths: self.paths.clone(),
                runtime: self.runtime.clone(),
            }))
        } else {
            let backend =
                LocalSandboxBackend::new(ctx.data_dir.root().join("sandbox").join("runtime"))
                    .with_container_config_from_env()
                    .map_err(|source| ServiceError::Service(source.to_string()))?;
            Ok(Box::new(backend))
        }
    }
}

/// Build the registry of services the daemon serves through the seam, bound to this
/// daemon's `paths` so foundational services act for the right process. Request-specific
/// identity is supplied through [`ServiceContext::signer`] at dispatch time.
fn daemon_service_registry(
    paths: &DaemonPaths,
    service_runtime: Option<&tokio::runtime::Handle>,
    service_hooks: &[crate::DaemonServiceRegistryHook],
) -> ServiceRegistry {
    let event_hub = Arc::new(DaemonEventHub::new());
    let mut builder: ServiceRegistryBuilder = register_dfs_services(ServiceRegistry::builder());
    if observe_module_should_register(paths) {
        builder = sinomo_observe::register_services(builder);
    }
    let config_registry = daemon_config_registry(paths, service_hooks);
    let builder = register_config_services(builder, config_registry);
    let builder = register_rollout_services(builder)
        .register(DaemonControlService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        })
        .register(DaemonEventService::new(
            service_runtime.cloned(),
            Arc::clone(&event_hub),
        ))
        .register(ClusterKvService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        })
        .register(ClusterHealthService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        })
        .register(ClusterRelayService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        })
        .register(ClusterKvConsensusService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        })
        .register(ClusterMembershipService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        })
        .register(ClusterControlService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        })
        .register(NetworkModulesService {
            paths: paths.clone(),
        })
        .register(RolloutBinaryService {
            paths: paths.clone(),
        })
        .register(DaemonDfsBrowserService {
            runtime: service_runtime.cloned(),
            event_hub,
        });
    // Main and shadow share the data-dir build root. Only the main daemon may
    // own its durable queue, worker, and cache; the shadow keeps rollout
    // recovery services but must neither serve nor advertise build routes.
    let builder = if crate::daemon_paths_are_shadow(paths) {
        builder
    } else {
        builder.register(crate::build_service::DaemonBuildService::new(paths))
    };
    let builder = if let Some(runtime) = service_runtime {
        sinomo_dfs::register_data_plane_services(
            builder,
            Arc::new(DaemonDfsDataPlaneHost::new_on_runtime(
                paths.clone(),
                runtime,
            )),
        )
    } else {
        builder
    };
    let mut builder = sinomo_sandbox::register_services_with_factory(
        builder,
        Arc::new(DaemonSandboxBackendFactory {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        }),
    );
    for hook in service_hooks {
        builder = hook.register(builder);
    }
    builder.build()
}

#[derive(Clone)]
pub(crate) struct DaemonServiceRegistryHost {
    paths: DaemonPaths,
    router: ObjectRpcRouter,
    route_policy: ObjectRpcRoutePolicyLayer,
    /// Live service-registry snapshot store; the daemon maintenance loop
    /// publishes its active snapshot's instances into cluster KV.
    service_registry: ServiceRegistrySnapshotStore,
}

static DAEMON_SERVICE_REGISTRY_HOSTS: OnceLock<Mutex<HashMap<PathBuf, DaemonServiceRegistryHost>>> =
    OnceLock::new();

impl DaemonServiceRegistryHost {
    #[cfg_attr(not(test), allow(dead_code))]
    fn from_registry_for_paths(
        registry: ServiceRegistry,
        paths: &DaemonPaths,
    ) -> Result<Self, DaemonError> {
        Self::from_registry_with_internal_modules(
            registry,
            paths.clone(),
            None,
            observe_module_should_register(paths),
        )
    }

    fn from_registry_for_paths_on_runtime(
        registry: ServiceRegistry,
        paths: &DaemonPaths,
        service_runtime: tokio::runtime::Handle,
    ) -> Result<Self, DaemonError> {
        Self::from_registry_with_internal_modules(
            registry,
            paths.clone(),
            Some(service_runtime),
            observe_module_should_register(paths),
        )
    }

    fn from_registry_with_internal_modules(
        registry: ServiceRegistry,
        paths: DaemonPaths,
        service_runtime: Option<tokio::runtime::Handle>,
        register_observe_module: bool,
    ) -> Result<Self, DaemonError> {
        let service_registry = ServiceRegistrySnapshotStore::new(registry);
        let service_snapshot = service_registry.active_snapshot();
        let data_dir_root =
            daemon_data_dir_path(&paths).map_err(|source| DaemonError::Network {
                address: "daemon module registry".to_string(),
                detail: source.to_string(),
            })?;
        let module_snapshots =
            build_daemon_internal_module_snapshot_store(&data_dir_root, register_observe_module)
                .map_err(|source| DaemonError::Network {
                    address: "daemon module registry".to_string(),
                    detail: source.to_string(),
                })?;
        let route_registry = daemon_service_object_rpc_route_registry(
            service_snapshot,
            &module_snapshots,
            paths.clone(),
            service_runtime,
        )
        .map_err(|source| DaemonError::Network {
            address: "daemon service route registry".to_string(),
            detail: source.to_string(),
        })?;
        let router = ObjectRpcRouter::from_route_registry(route_registry).map_err(|source| {
            DaemonError::Network {
                address: "daemon service route router".to_string(),
                detail: source.to_string(),
            }
        })?;
        let route_policy = ObjectRpcRoutePolicyLayer::new(router.catalog()).map_err(|source| {
            DaemonError::Network {
                address: "daemon service route policy".to_string(),
                detail: source.to_string(),
            }
        })?;
        Ok(Self {
            paths,
            router,
            route_policy,
            service_registry,
        })
    }

    pub(crate) async fn new_on_runtime(
        paths: DaemonPaths,
        service_runtime: tokio::runtime::Handle,
    ) -> Result<Self, DaemonError> {
        let cache_key = paths.root.clone();
        if let Some(host) = DAEMON_SERVICE_REGISTRY_HOSTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|source| DaemonError::Network {
                address: "daemon service registry cache".to_string(),
                detail: source.to_string(),
            })?
            .get(&cache_key)
            .cloned()
        {
            return Ok(host);
        }
        let registry_paths = paths.clone();
        let registry = daemon_service_registry_on_blocking_pool(
            registry_paths,
            service_runtime.clone(),
            Vec::new(),
        )
        .await?;
        let host = Self::from_registry_for_paths_on_runtime(registry, &paths, service_runtime)?;
        let mut cache = DAEMON_SERVICE_REGISTRY_HOSTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|source| DaemonError::Network {
                address: "daemon service registry cache".to_string(),
                detail: source.to_string(),
            })?;
        Ok(cache.entry(cache_key).or_insert_with(|| host).clone())
    }

    pub(crate) async fn new_on_runtime_with_services(
        paths: DaemonPaths,
        service_runtime: tokio::runtime::Handle,
        service_hooks: Vec<crate::DaemonServiceRegistryHook>,
    ) -> Result<Self, DaemonError> {
        if service_hooks.is_empty() {
            return Self::new_on_runtime(paths, service_runtime).await;
        }
        let registry_paths = paths.clone();
        let registry = daemon_service_registry_on_blocking_pool(
            registry_paths,
            service_runtime.clone(),
            service_hooks,
        )
        .await?;
        Self::from_registry_for_paths_on_runtime(registry, &paths, service_runtime)
    }

    fn active_router(&self) -> ObjectRpcRouter {
        self.router.clone()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn active_route_snapshot(&self) -> Arc<ObjectRpcRouteRegistry> {
        self.router.route_registry_snapshot()
    }

    pub(crate) fn object_rpc_route_descriptor(&self, route: &str) -> Option<ObjectRouteDescriptor> {
        self.router.object_rpc_route_descriptor(route)
    }

    fn route_policy_layer(&self) -> ObjectRpcRoutePolicyLayer {
        self.route_policy.clone()
    }

    /// The live service-registry snapshot store behind this host's routes.
    pub(crate) fn service_registry_snapshot_store(&self) -> ServiceRegistrySnapshotStore {
        self.service_registry.clone()
    }
}

async fn daemon_service_registry_on_blocking_pool(
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    service_hooks: Vec<crate::DaemonServiceRegistryHook>,
) -> Result<ServiceRegistry, DaemonError> {
    tokio::task::spawn_blocking(move || {
        daemon_service_registry(&paths, Some(&service_runtime), &service_hooks)
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "daemon service registry".to_string(),
        detail: format!("daemon service registry task failed: {source}"),
    })
}

fn observe_module_should_register(paths: &DaemonPaths) -> bool {
    daemon_data_dir_path(paths)
        .ok()
        .and_then(|data_dir| sinomo_observe::observe_module_enabled(&data_dir).ok())
        .unwrap_or(true)
}

fn daemon_config_registry(
    paths: &DaemonPaths,
    service_hooks: &[crate::DaemonServiceRegistryHook],
) -> ConfigRegistry {
    let mut builder = register_daemon_config_definitions(ConfigRegistry::builder())
        .expect("daemon config definitions must be valid");
    for hook in service_hooks {
        builder = hook
            .register_config_definitions(builder)
            .expect("daemon config hook definitions must be valid");
    }
    builder = register_daemon_dynamic_module_config_definitions(paths, builder);
    builder.build()
}

fn register_daemon_config_definitions(
    builder: ConfigRegistryBuilder,
) -> Result<ConfigRegistryBuilder, String> {
    let builder =
        sinomo_module::register_config_definitions(builder).map_err(|error| error.to_string())?;
    let mut builder =
        sinomo_sandbox::register_config_definitions(builder).map_err(|error| error.to_string())?;
    for registration in [
        sinomo_dfs::dfs_module_registration(),
        sinomo_rollout::rollout_module_registration(),
    ] {
        builder = registration
            .surfaces
            .register_config_definitions(builder)
            .map_err(|error| error.to_string())?;
    }
    Ok(builder)
}

fn register_daemon_dynamic_module_config_definitions(
    paths: &DaemonPaths,
    mut builder: ConfigRegistryBuilder,
) -> ConfigRegistryBuilder {
    let data_dir_root = match daemon_data_dir_path(paths) {
        Ok(path) => path,
        Err(error) => {
            tracing::warn!("daemon dynamic module settings unavailable: {error}");
            return builder;
        }
    };
    let modules = match daemon_internal_module_registry(
        &data_dir_root,
        observe_module_should_register(paths),
    ) {
        Ok(modules) => modules,
        Err(error) => {
            // Active native-dylib descriptors are external state. The host's
            // module-registry startup path will report the hard failure; the
            // config catalog must not panic before that diagnostic is returned.
            tracing::warn!("daemon dynamic module settings unavailable: {error}");
            return builder;
        }
    };
    let dfs_module_id = sinomo_dfs::dfs_module_id();
    let rollout_module_id = sinomo_rollout::rollout_module_id();
    for registration in modules.registrations() {
        if registration.module_id() == &dfs_module_id
            || registration.module_id() == &rollout_module_id
        {
            continue;
        }
        builder = register_module_surface_config_definitions(
            builder,
            registration.module_id(),
            &registration.surfaces,
        );
    }
    builder
}

fn register_module_surface_config_definitions(
    mut builder: ConfigRegistryBuilder,
    module_id: &sinomo_module::ModuleId,
    surfaces: &sinomo_module::ModuleSurfaces,
) -> ConfigRegistryBuilder {
    let definitions = match surfaces.config_definitions() {
        Ok(definitions) => definitions,
        Err(error) => {
            tracing::warn!("daemon module {module_id} settings skipped: {error}");
            return builder;
        }
    };
    for definition in definitions {
        match builder.clone().register(definition) {
            Ok(next) => builder = next,
            Err(error) => tracing::warn!(
                "daemon module {module_id} settings skipped after static catalog registration: {error}"
            ),
        }
    }
    builder
}

/// Captures the exactly-once service response for the network session adapter.
struct BufferSink {
    response: Arc<Mutex<Option<ObjectRpcSessionResponse>>>,
}

impl ResponseSink for BufferSink {
    fn send(self: Box<Self>, response: ObjectRpcSessionResponse) -> Result<(), ServiceError> {
        let mut slot = self
            .response
            .lock()
            .map_err(|_| ServiceError::Service("daemon response sink poisoned".to_string()))?;
        if slot.is_some() {
            return Err(ServiceError::Service(
                "daemon response sink received more than one response".to_string(),
            ));
        }
        *slot = Some(response);
        Ok(())
    }
}

pub(crate) fn is_sandbox_stream_route(request: &ObjectRpcRequest) -> bool {
    matches!(
        request.route.as_str(),
        SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE | SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE
    )
}

fn service_error_to_network_error(error: ServiceError) -> NetworkError {
    match error {
        ServiceError::Network(source) => source,
        ServiceError::Kv(source) => NetworkError::Serialization {
            detail: source.to_string(),
        },
        ServiceError::NotClustered(route) => NetworkError::ClusterReadOnly {
            detail: format!("service route {route} requires an initialized cluster"),
        },
        ServiceError::UnknownRoute(route) => NetworkError::UnknownRoute { route },
        ServiceError::Service(detail) => NetworkError::Serialization { detail },
    }
}

fn daemon_signer_authorizes_route_scope(authority: &DaemonSignerAuthority, scope: &str) -> bool {
    match scope {
        "authenticated" => true,
        "local-command" => authority.can_run_local_command(),
        "local-or-cluster-member" | "cluster-member" => matches!(
            authority,
            DaemonSignerAuthority::Local
                | DaemonSignerAuthority::Voter
                | DaemonSignerAuthority::Member
        ),
        "local-or-cluster-voter" => matches!(
            authority,
            DaemonSignerAuthority::Local | DaemonSignerAuthority::Voter
        ),
        "admin" => authority.can_run_admin_command(),
        _ => false,
    }
}

fn daemon_route_local_permissions_are_granted(
    paths: &DaemonPaths,
    descriptor: &ObjectRouteDescriptor,
) -> bool {
    if descriptor.required_local_permissions.is_empty() {
        return true;
    }
    let data_dir = match daemon_data_dir_path(paths) {
        Ok(path) => path,
        Err(error) => {
            tracing::warn!(
                target: "sinomo.daemon.service_auth",
                route = %descriptor.route,
                error = %error,
                "daemon route local permissions denied because the data directory is unavailable"
            );
            return false;
        }
    };
    let store = sinomo_core::node::LocalNodePermissionStore::new(&data_dir);
    for name in &descriptor.required_local_permissions {
        let permission = match sinomo_core::node::LocalNodePermission::try_from(name.as_str()) {
            Ok(permission) => permission,
            Err(error) => {
                tracing::warn!(
                    target: "sinomo.daemon.service_auth",
                    route = %descriptor.route,
                    permission = %name,
                    error = %error,
                    "daemon route declares an unknown node-local permission"
                );
                return false;
            }
        };
        match store.try_permission_granted(permission) {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    target: "sinomo.daemon.service_auth",
                    route = %descriptor.route,
                    permission = %name,
                    "daemon route denied by node-local permission"
                );
                return false;
            }
            Err(error) => {
                tracing::warn!(
                    target: "sinomo.daemon.service_auth",
                    route = %descriptor.route,
                    permission = %name,
                    error = %error,
                    "daemon route node-local permission state is unavailable"
                );
                return false;
            }
        }
    }
    true
}

fn daemon_nonlocal_config_permission_is_granted(
    paths: &DaemonPaths,
    signer_authority: &DaemonSignerAuthority,
    route: &str,
) -> bool {
    if matches!(signer_authority, DaemonSignerAuthority::Local)
        || !matches!(
            route,
            CONFIG_CATALOG_ROUTE | CONFIG_SEARCH_ROUTE | CONFIG_GET_ROUTE | CONFIG_SET_ROUTE
        )
    {
        return true;
    }
    let data_dir = match daemon_data_dir_path(paths) {
        Ok(path) => path,
        Err(error) => {
            tracing::warn!(
                target: "sinomo.daemon.service_auth",
                route,
                error = %error,
                "remote config route denied because the local permission store is unavailable"
            );
            return false;
        }
    };
    let permission = sinomo_core::node::LocalNodePermission::RemoteManagement;
    match sinomo_core::node::LocalNodePermissionStore::new(data_dir)
        .try_permission_granted(permission)
    {
        Ok(true) => true,
        Ok(false) => {
            tracing::warn!(
                target: "sinomo.daemon.service_auth",
                route,
                permission = permission.as_str(),
                "remote config route denied by node-local permission"
            );
            false
        }
        Err(error) => {
            tracing::warn!(
                target: "sinomo.daemon.service_auth",
                route,
                permission = permission.as_str(),
                error = %error,
                "remote config route node-local permission state is unavailable"
            );
            false
        }
    }
}

fn daemon_authenticated_command_authorizes_route_descriptor(
    paths: &DaemonPaths,
    authenticated: &AuthenticatedDaemonCommand,
    descriptor: &ObjectRouteDescriptor,
) -> bool {
    descriptor
        .auth_scope
        .as_deref()
        .is_none_or(|scope| daemon_signer_authorizes_route_scope(&authenticated.signer, scope))
        && daemon_route_local_permissions_are_granted(paths, descriptor)
}

/// Authenticates a request against the live route catalog. Catalog policy is
/// authoritative for dynamic routes; legacy routes without policy metadata
/// retain the static authorization table in `auth.rs`.
pub(crate) fn authenticate_daemon_control_request_via_registry(
    paths: &DaemonPaths,
    request: &DaemonControlRequest,
    service_host: &DaemonServiceRegistryHost,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    let Some(descriptor) = service_host.object_rpc_route_descriptor(&request.route) else {
        return crate::auth::authenticate_daemon_control_request(paths, request);
    };
    if descriptor.auth_scope.is_none() {
        let authenticated = crate::auth::authenticate_daemon_control_request(paths, request)?;
        return daemon_route_local_permissions_are_granted(paths, &descriptor)
            .then_some(authenticated)
            .ok_or(DaemonError::AuthDenied);
    }
    let authenticated =
        crate::auth::authenticate_daemon_control_request_without_route_authorization(
            paths, request,
        )?;
    if daemon_authenticated_command_authorizes_route_descriptor(paths, &authenticated, &descriptor)
    {
        Ok(authenticated)
    } else {
        Err(DaemonError::AuthDenied)
    }
}

fn daemon_auth_public_key_prefix(public_key: &str) -> &str {
    public_key
        .get(..public_key.len().min(8))
        .unwrap_or(public_key)
}

fn warn_daemon_service_auth_denied(
    route: &str,
    auth_scope: &str,
    signer: &DaemonSignerAuthority,
    signer_node_id: Option<&str>,
    signer_public_key: Option<&str>,
) {
    tracing::warn!(
        target: "sinomo.daemon.service_auth",
        route,
        auth_scope,
        signer = ?signer,
        signer_node_id = signer_node_id.unwrap_or("unknown"),
        signer_public_key_prefix = signer_public_key
            .map(daemon_auth_public_key_prefix)
            .unwrap_or("unknown"),
        "daemon service object RPC authorization denied"
    );
}

fn module_error_to_network_error(error: impl std::fmt::Display) -> NetworkError {
    NetworkError::Serialization {
        detail: error.to_string(),
    }
}

#[derive(Debug)]
struct DaemonInternalModuleSnapshot {
    snapshot: Arc<sinomo_module::ModuleRegistrySnapshot>,
    routes: Vec<ObjectRouteDescriptor>,
    native_dylib_routes: BTreeMap<String, DaemonNativeDylibRouteTarget>,
}

#[derive(Clone, Debug)]
struct DaemonNativeDylibRouteTarget {
    module_id: sinomo_module::ModuleId,
    operation: String,
}

#[derive(Clone, Debug, Default)]
struct DaemonOperationCancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl DaemonOperationCancellationToken {
    fn new() -> Self {
        Self::default()
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

struct NativeDylibModuleInvocationCallbacks {
    cancellation_token: DaemonOperationCancellationToken,
}

impl sinomo_module::NativeModuleHostCallbacks for NativeDylibModuleInvocationCallbacks {
    fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }
}

fn daemon_internal_module_registry(
    data_dir_root: &std::path::Path,
    register_observe_module: bool,
) -> Result<sinomo_module::ModuleRegistry, NetworkError> {
    let mut builder = sinomo_module::ModuleRegistry::builder()
        .register(sinomo_dfs::dfs_module_registration())
        .map_err(module_error_to_network_error)?
        .register(crate::registry::network_module_registration())
        .map_err(module_error_to_network_error)?
        .register(sinomo_rollout::rollout_module_registration())
        .map_err(module_error_to_network_error)?;
    if register_observe_module {
        builder = builder
            .register(sinomo_observe::observe_module_registration())
            .map_err(module_error_to_network_error)?;
    }
    let native_dylib_states =
        match sinomo_module::read_native_dylib_module_loader_states(data_dir_root) {
            Ok(states) => states,
            Err(error) => {
                tracing::warn!("daemon native-dylib module loader states unreadable: {error}");
                Vec::new()
            }
        };
    for state in native_dylib_states {
        if !sinomo_rollout::native_dylib_loader_state_is_host_compatible(&state) {
            tracing::warn!(
                "daemon native-dylib module {} skipped because its build platform or native ABI does not match this host",
                state.module_id
            );
            continue;
        }
        let registration = match sinomo_module::native_dylib_module_registration_from_loader_state(
            data_dir_root,
            &state,
        ) {
            Ok(registration) => registration,
            Err(error) => {
                tracing::warn!(
                    "daemon native-dylib module {} registration skipped: {error}",
                    state.module_id
                );
                continue;
            }
        };
        builder = builder
            .register(registration)
            .map_err(module_error_to_network_error)?;
    }
    builder.try_build().map_err(module_error_to_network_error)
}

fn build_daemon_internal_module_snapshot_store(
    data_dir_root: &std::path::Path,
    register_observe_module: bool,
) -> Result<sinomo_module::ModuleRegistrySnapshotStore, NetworkError> {
    sinomo_module::ModuleRegistrySnapshotStore::new(daemon_internal_module_registry(
        data_dir_root,
        register_observe_module,
    )?)
    .map_err(module_error_to_network_error)
}

fn daemon_internal_module_snapshot(
    module_snapshots: &sinomo_module::ModuleRegistrySnapshotStore,
) -> Result<DaemonInternalModuleSnapshot, NetworkError> {
    let snapshot = module_snapshots
        .active_snapshot()
        .map_err(module_error_to_network_error)?;
    daemon_internal_module_snapshot_from_snapshot(snapshot)
}

fn daemon_internal_module_snapshot_from_snapshot(
    snapshot: Arc<sinomo_module::ModuleRegistrySnapshot>,
) -> Result<DaemonInternalModuleSnapshot, NetworkError> {
    let (routes, native_dylib_routes) = daemon_module_snapshot_route_descriptors(&snapshot)?;
    Ok(DaemonInternalModuleSnapshot {
        snapshot,
        routes,
        native_dylib_routes,
    })
}

fn module_object_rpc_route_descriptor_to_daemon_route(
    descriptor: &sinomo_module::ModuleObjectRpcRouteDescriptor,
) -> ObjectRouteDescriptor {
    let mut route = ObjectRouteDescriptor::new(
        descriptor.route.clone(),
        descriptor.category.clone(),
        format!(
            "module object RPC operation {}",
            descriptor.operation.as_str()
        ),
        "sinomo_core::network::ObjectRpcRequest",
        "sinomo_core::network::ObjectRpcResponse",
    )
    .with_auth_scope(descriptor.auth_scope.clone())
    .observable("daemon.service_registry", "true")
    .observable("daemon.module_route", "true")
    .observable("module.operation", descriptor.operation.as_str());
    for capability in &descriptor.required_capabilities {
        route = route.require_capability(capability.as_str());
    }
    for permission in &descriptor.required_local_permissions {
        route = route.require_local_permission(permission);
    }
    route
}

fn daemon_module_snapshot_route_descriptors(
    snapshot: &sinomo_module::ModuleRegistrySnapshot,
) -> Result<
    (
        Vec<ObjectRouteDescriptor>,
        BTreeMap<String, DaemonNativeDylibRouteTarget>,
    ),
    NetworkError,
> {
    let mut seen_routes = BTreeSet::new();
    let mut descriptors = Vec::new();
    let mut native_dylib_routes = BTreeMap::new();
    for registration in snapshot.registry().registrations() {
        for descriptor in registration
            .surfaces
            .object_rpc_route_descriptors()
            .map_err(module_error_to_network_error)?
        {
            let operation = descriptor.operation.as_str().to_string();
            let descriptor = module_object_rpc_route_descriptor_to_daemon_route(&descriptor);
            if !seen_routes.insert(descriptor.route.clone()) {
                return Err(NetworkError::Serialization {
                    detail: format!("duplicate module object rpc route {}", descriptor.route),
                });
            }
            if matches!(
                &registration.backend,
                sinomo_module::ModuleBackendRegistration::NativeDylib { .. }
            ) {
                native_dylib_routes.insert(
                    descriptor.route.clone(),
                    DaemonNativeDylibRouteTarget {
                        module_id: registration.manifest.module_id.clone(),
                        operation: operation.clone(),
                    },
                );
            }
            descriptors.push(descriptor);
        }
    }
    Ok((descriptors, native_dylib_routes))
}

fn service_route_descriptor(route: &ServiceRoute) -> ObjectRouteDescriptor {
    let mut descriptor = ObjectRouteDescriptor::new(
        route.route.clone(),
        route.category.clone(),
        format!("daemon service route {}", route.route),
        "sinomo_core::network::ObjectRpcRequest",
        "sinomo_core::network::ObjectRpcResponse",
    )
    .observable("daemon.service_registry", "true");
    if let Some(auth_scope) = route.auth_scope.as_deref() {
        descriptor = descriptor.with_auth_scope(auth_scope);
    }
    for capability in &route.required_capabilities {
        descriptor = descriptor.require_capability(capability.as_str());
    }
    for permission in &route.required_local_permissions {
        descriptor = descriptor.require_local_permission(permission);
    }
    descriptor = apply_service_route_execution_policy(descriptor, route);
    descriptor
}

fn apply_service_route_execution_policy(
    mut descriptor: ObjectRouteDescriptor,
    route: &ServiceRoute,
) -> ObjectRouteDescriptor {
    descriptor.concurrency_limit = route.concurrency_limit;
    descriptor.timeout_ms = route.timeout_ms;
    descriptor.request_body_mode = route.request_body_mode;
    descriptor.response_body_mode = route.response_body_mode;
    descriptor
}

fn mark_service_snapshot_route(
    descriptor: ObjectRouteDescriptor,
    snapshot: &ServiceRegistrySnapshot,
) -> ObjectRouteDescriptor {
    descriptor.observable(
        "daemon.service_registry.snapshot_version",
        snapshot.version().to_string(),
    )
}

fn validate_module_service_route_contract(
    descriptor: &ObjectRouteDescriptor,
    route: &ServiceRoute,
) -> Result<(), NetworkError> {
    if descriptor.category != route.category {
        return Err(NetworkError::Serialization {
            detail: format!(
                "module route {} category {} does not match service category {}",
                descriptor.route, descriptor.category, route.category
            ),
        });
    }
    if descriptor.auth_scope != route.auth_scope {
        return Err(NetworkError::Serialization {
            detail: format!(
                "module route {} auth scope {:?} does not match service auth scope {:?}",
                descriptor.route, descriptor.auth_scope, route.auth_scope
            ),
        });
    }
    if descriptor.required_capabilities != route.required_capabilities {
        return Err(NetworkError::Serialization {
            detail: format!(
                "module route {} capabilities {:?} do not match service capabilities {:?}",
                descriptor.route, descriptor.required_capabilities, route.required_capabilities
            ),
        });
    }
    if descriptor.required_local_permissions != route.required_local_permissions {
        return Err(NetworkError::Serialization {
            detail: format!(
                "module route {} local permissions {:?} do not match service local permissions {:?}",
                descriptor.route,
                descriptor.required_local_permissions,
                route.required_local_permissions
            ),
        });
    }
    Ok(())
}

async fn dispatch_service_registry_request(
    registry: Arc<ServiceRegistry>,
    data_dir: Arc<SinomoDataDir>,
    cluster: Option<Arc<dyn ClusterAccess>>,
    signer: ServiceSigner,
    request: ObjectRpcSessionRequest,
) -> Result<ObjectRpcSessionResponse, NetworkError> {
    let route = request.request.route.clone();
    let response = Arc::new(Mutex::new(None));
    let ctx = ServiceContext::new(
        data_dir,
        cluster,
        signer,
        request.body,
        Box::new(BufferSink {
            response: Arc::clone(&response),
        }),
    );
    let service_disposition = registry
        .dispatch_direct(ctx, request.request)
        .await
        .map_err(service_error_to_network_error)?;

    let mut response = response.lock().map_err(|_| NetworkError::Io {
        operation: format!("take service response for {route}"),
        detail: "service response sink lock poisoned".to_string(),
    })?;
    let mut response = response.take().ok_or_else(|| NetworkError::Serialization {
        detail: format!("service route {route} sent no response"),
    })?;
    response.response.metadata.insert(
        DAEMON_SERVICE_DISPOSITION_METADATA_KEY.to_string(),
        match service_disposition {
            ServiceDisposition::Continue => "continue",
            ServiceDisposition::Stop => "stop",
        }
        .to_string(),
    );
    Ok(response)
}

fn register_native_dylib_operation_cancellation(
    active_operations: &Arc<Mutex<BTreeMap<String, DaemonOperationCancellationToken>>>,
    operation_id: Option<&str>,
) -> Result<DaemonOperationCancellationToken, NetworkError> {
    let token = DaemonOperationCancellationToken::new();
    let Some(operation_id) = operation_id else {
        return Ok(token);
    };
    let mut active = active_operations.lock().map_err(|_| NetworkError::Io {
        operation: "register native dylib module cancellation".to_string(),
        detail: "native dylib operation cancellation registry lock poisoned".to_string(),
    })?;
    if active.contains_key(operation_id) {
        return Err(NetworkError::Serialization {
            detail: format!("native dylib module operation `{operation_id}` is already active"),
        });
    }
    active.insert(operation_id.to_string(), token.clone());
    Ok(token)
}

fn unregister_native_dylib_operation_cancellation(
    active_operations: &Arc<Mutex<BTreeMap<String, DaemonOperationCancellationToken>>>,
    operation_id: Option<&str>,
) {
    let Some(operation_id) = operation_id else {
        return;
    };
    if let Ok(mut active) = active_operations.lock() {
        active.remove(operation_id);
    }
}

fn dispatch_native_dylib_module_cancel_request(
    active_operations: &Arc<Mutex<BTreeMap<String, DaemonOperationCancellationToken>>>,
    request: ObjectRpcRequest,
) -> Result<ObjectRpcResponse, NetworkError> {
    let payload: ObjectRpcCancelRequest = request.decode_body()?;
    let operation_id = payload.operation_id;
    let metadata_operation_id = request.cancels_operation_id().map(str::to_string);
    if metadata_operation_id
        .as_deref()
        .is_some_and(|metadata| metadata != operation_id)
    {
        return ObjectRpcResponse::encode_body(
            MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
            &ObjectRpcCancelResponse {
                operation_id,
                cancelled: false,
                detail: "cancel metadata operation id does not match request body".to_string(),
            },
        );
    }
    let token = active_operations
        .lock()
        .map_err(|_| NetworkError::Io {
            operation: "cancel native dylib module operation".to_string(),
            detail: "native dylib operation cancellation registry lock poisoned".to_string(),
        })?
        .get(&operation_id)
        .cloned();
    let (cancelled, detail) = if let Some(token) = token {
        token.cancel();
        (true, payload.reason)
    } else {
        (
            false,
            "native dylib module operation is not active".to_string(),
        )
    };
    ObjectRpcResponse::encode_body(
        MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
        &ObjectRpcCancelResponse {
            operation_id,
            cancelled,
            detail,
        },
    )
}

fn inject_native_dylib_host_authority(
    data_dir_root: &std::path::Path,
    target: &DaemonNativeDylibRouteTarget,
    request: &mut ObjectRpcRequest,
) -> Result<(), NetworkError> {
    if target.operation != sinomo_remote_management::REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION {
        return Ok(());
    }
    let mut observation: sinomo_remote_management::RemoteManagementSystemObserveRequest =
        request.decode_body()?;
    // Never trust a caller-provided resource-policy view. The receiving daemon
    // owns the exact data directory whose route permission was just authorized
    // and overwrites the field immediately before invoking native code. These
    // declarations do not establish that a resource exists; a future resource
    // broker must still call the store's fail-closed
    // `resource_operation_exposed` guard at final operation dispatch.
    observation.host_authorized_resource_exposures.clear();
    if observation.include_resource_exposures {
        let store = sinomo_core::node::LocalNodePermissionStore::new(data_dir_root);
        match store.load() {
            Ok(permissions) => {
                observation.host_authorized_resource_exposures = permissions
                    .resource_exposures()
                    .iter()
                    .take(sinomo_remote_management::MAX_OBSERVED_RESOURCE_EXPOSURES)
                    .map(|(resource_id, exposure)| {
                        sinomo_remote_management::RemoteManagementResourceExposurePolicy {
                            resource_id: resource_id.clone(),
                            allowed_operations: exposure.operations().iter().cloned().collect(),
                        }
                    })
                    .collect();
            }
            Err(error) => {
                tracing::warn!(
                    target: "sinomo.daemon.service_auth",
                    route = %request.route,
                    error = %error,
                    "remote resource-exposure policy observations omitted because local exposure state is unavailable"
                );
            }
        }
    }
    request.body =
        sinomo_module_api::encode_sobj(&observation).map_err(module_error_to_network_error)?;
    Ok(())
}

async fn dispatch_native_dylib_module_request(
    paths: DaemonPaths,
    target: DaemonNativeDylibRouteTarget,
    active_operations: Arc<Mutex<BTreeMap<String, DaemonOperationCancellationToken>>>,
    request: ObjectRpcRequest,
) -> Result<ObjectRpcResponse, NetworkError> {
    let route_for_error = request.route.clone();
    let operation_id = request.operation_id().map(str::to_string);
    tokio::task::spawn_blocking(move || {
        let data_dir_root = daemon_data_dir_path(&paths).map_err(daemon_error_to_network_error)?;
        let mut request = request;
        inject_native_dylib_host_authority(&data_dir_root, &target, &mut request)?;
        let state =
            sinomo_module::read_native_dylib_module_loader_state(&data_dir_root, &target.module_id)
                .map_err(module_error_to_network_error)?
                .ok_or_else(|| NetworkError::Serialization {
                    detail: format!(
                        "native dylib module {} is not active for route {}",
                        target.module_id, request.route
                    ),
                })?;
        let library_path = sinomo_module::native_module_cache_path(
            &data_dir_root,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        let module = unsafe {
            sinomo_module::NativeModule::load_from_content_addressed_path(
                &library_path,
                &state.content_hash,
            )
        }
        .map_err(module_error_to_network_error)?;
        let descriptor = module.descriptor();
        let descriptor_module_id =
            sinomo_module::parse_native_dylib_module_id(&descriptor.module_id)
                .map_err(module_error_to_network_error)?;
        if descriptor_module_id != target.module_id.clone() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "native dylib route {} resolved module {}, but descriptor reports {}",
                    request.route, target.module_id, descriptor.module_id
                ),
            });
        }
        if !descriptor
            .supported_operations
            .iter()
            .any(|operation| operation == &target.operation)
        {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "native dylib module {} does not support operation {}",
                    target.module_id, target.operation
                ),
            });
        }
        let cancellation_token = register_native_dylib_operation_cancellation(
            &active_operations,
            operation_id.as_deref(),
        )?;
        let callbacks: Arc<dyn sinomo_module::NativeModuleHostCallbacks> =
            Arc::new(NativeDylibModuleInvocationCallbacks {
                cancellation_token: cancellation_token.clone(),
            });
        let native_response = module
            .invoke_with_callbacks(
                &sinomo_module_api::NativeModuleRequest::Invoke(
                    sinomo_module_api::NativeModuleInvocationRequest {
                        operation: target.operation.clone(),
                        payload_sobj: request.body,
                    },
                ),
                Some(callbacks),
            )
            .map_err(module_error_to_network_error);
        unregister_native_dylib_operation_cancellation(&active_operations, operation_id.as_deref());
        let native_response = native_response?;
        match native_response {
            sinomo_module_api::NativeModuleResponse::Invocation(response) => {
                if response.operation != target.operation {
                    return Err(NetworkError::Serialization {
                        detail: format!(
                            "native dylib route {} expected operation {}, got {}",
                            request.route, target.operation, response.operation
                        ),
                    });
                }
                Ok(ObjectRpcResponse::from_encoded_body(
                    request.route,
                    response.payload_sobj,
                ))
            }
            sinomo_module_api::NativeModuleResponse::Error(error) => {
                Err(NetworkError::Serialization {
                    detail: format!(
                        "native dylib module {} returned error {}: {}",
                        target.module_id, error.code, error.detail
                    ),
                })
            }
            sinomo_module_api::NativeModuleResponse::Capabilities(_) => {
                Err(NetworkError::Serialization {
                    detail: format!(
                        "native dylib module {} returned capabilities for route invocation",
                        target.module_id
                    ),
                })
            }
        }
    })
    .await
    .map_err(|source| NetworkError::Io {
        operation: format!("dispatch native dylib module route {route_for_error}"),
        detail: format!("native dylib module route task failed: {source}"),
    })?
}

fn insert_daemon_route_descriptor(
    catalog: &mut ServiceCatalog,
    descriptor: ObjectRouteDescriptor,
) -> Result<(), NetworkError> {
    if catalog.routes.contains_key(&descriptor.route) {
        return Err(NetworkError::Serialization {
            detail: format!("duplicate daemon object rpc route {}", descriptor.route),
        });
    }
    catalog.register_route(descriptor);
    Ok(())
}

fn append_daemon_module_snapshot_routes(
    catalog: &mut ServiceCatalog,
    module_snapshot: &DaemonInternalModuleSnapshot,
    service_snapshot: Arc<ServiceRegistrySnapshot>,
    service_routes: Arc<BTreeMap<String, ServiceRoute>>,
) -> Result<BTreeSet<String>, NetworkError> {
    let mut installed_routes = BTreeSet::new();
    for descriptor in module_snapshot.routes.iter().cloned() {
        let is_native_dylib_route = module_snapshot
            .native_dylib_routes
            .contains_key(&descriptor.route);
        let service_route = service_routes.get(&descriptor.route);
        if is_native_dylib_route && service_route.is_some() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "native dylib module route {} collides with a daemon service route",
                    descriptor.route
                ),
            });
        }
        let route = descriptor.route.clone();
        let descriptor = if let Some(service_route) = service_route {
            validate_module_service_route_contract(&descriptor, service_route)?;
            apply_service_route_execution_policy(descriptor, service_route)
        } else if is_native_dylib_route {
            apply_native_dylib_route_execution_policy(descriptor)
        } else {
            continue;
        };
        let descriptor = mark_service_snapshot_route(
            descriptor.observable(
                "module.registry.snapshot_version",
                module_snapshot.snapshot.version().to_string(),
            ),
            &service_snapshot,
        );
        insert_daemon_route_descriptor(catalog, descriptor)?;
        installed_routes.insert(route);
    }
    Ok(installed_routes)
}

fn apply_native_dylib_route_execution_policy(
    mut descriptor: ObjectRouteDescriptor,
) -> ObjectRouteDescriptor {
    descriptor.concurrency_limit = Some(4);
    descriptor.timeout_ms = Some(310_000);
    descriptor.observable("daemon.native_dylib_module_route", "true")
}

fn native_dylib_cancel_route_descriptor() -> ObjectRouteDescriptor {
    ObjectRouteDescriptor::new(
        MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
        MODULE_RPC_CATEGORY,
        "cancel an active native-dylib module object-RPC operation by id",
        "sinomo_core::network::ObjectRpcCancelRequest",
        "sinomo_core::network::ObjectRpcCancelResponse",
    )
    .with_auth_scope("local-or-cluster-member")
    .require_capability("module.native-dylib.cancel")
    .observable("daemon.native_dylib_module_cancel_route", "true")
}

const DAEMON_SERVICE_SIGNER_AUTHORITY_METADATA_KEY: &str = "daemon.service.signer.authority";
const DAEMON_SERVICE_SIGNER_NODE_ID_METADATA_KEY: &str = "daemon.service.signer.node_id";
const DAEMON_SERVICE_SIGNER_PUBLIC_KEY_METADATA_KEY: &str = "daemon.service.signer.public_key";
const DAEMON_SERVICE_DISPOSITION_METADATA_KEY: &str = "daemon.service.disposition";

fn daemon_service_object_rpc_route_registry(
    service_snapshot: Arc<ServiceRegistrySnapshot>,
    module_snapshots: &sinomo_module::ModuleRegistrySnapshotStore,
    paths: DaemonPaths,
    service_runtime: Option<tokio::runtime::Handle>,
) -> Result<Arc<ObjectRpcRouteRegistry>, NetworkError> {
    let module_snapshot = daemon_internal_module_snapshot(module_snapshots)?;
    daemon_service_object_rpc_route_registry_from_module_snapshot(
        service_snapshot,
        module_snapshot,
        paths,
        service_runtime,
    )
}

fn daemon_service_object_rpc_route_descriptors_from_module_snapshot(
    service_snapshot: Arc<ServiceRegistrySnapshot>,
    module_snapshot: DaemonInternalModuleSnapshot,
) -> Result<Vec<ObjectRouteDescriptor>, NetworkError> {
    let registry = service_snapshot.registry_arc();
    let route_metadata = registry.route_categories();
    let service_routes = Arc::new(
        route_metadata
            .iter()
            .map(|route| (route.route.clone(), route.clone()))
            .collect::<BTreeMap<_, _>>(),
    );
    let mut catalog = ServiceCatalog::default();
    let module_routes = append_daemon_module_snapshot_routes(
        &mut catalog,
        &module_snapshot,
        Arc::clone(&service_snapshot),
        Arc::clone(&service_routes),
    )?;
    for route in route_metadata {
        if module_routes.contains(&route.route) {
            continue;
        }
        let descriptor =
            mark_service_snapshot_route(service_route_descriptor(&route), &service_snapshot);
        insert_daemon_route_descriptor(&mut catalog, descriptor)?;
    }
    let cancel_descriptor =
        mark_service_snapshot_route(native_dylib_cancel_route_descriptor(), &service_snapshot);
    insert_daemon_route_descriptor(&mut catalog, cancel_descriptor)?;
    Ok(catalog.routes.into_values().collect())
}

fn daemon_service_object_rpc_route_registry_from_module_snapshot(
    service_snapshot: Arc<ServiceRegistrySnapshot>,
    module_snapshot: DaemonInternalModuleSnapshot,
    paths: DaemonPaths,
    service_runtime: Option<tokio::runtime::Handle>,
) -> Result<Arc<ObjectRpcRouteRegistry>, NetworkError> {
    let service_registry = service_snapshot.registry_arc();
    let native_dylib_routes = Arc::new(module_snapshot.native_dylib_routes.clone());
    let active_native_dylib_operations = Arc::new(Mutex::new(BTreeMap::<
        String,
        DaemonOperationCancellationToken,
    >::new()));
    let descriptors = daemon_service_object_rpc_route_descriptors_from_module_snapshot(
        service_snapshot,
        module_snapshot,
    )?;
    let mut builder = ObjectRpcRouteRegistry::builder();
    for descriptor in descriptors {
        let registry = Arc::clone(&service_registry);
        let native_target = native_dylib_routes.get(&descriptor.route).cloned();
        let is_native_cancel_route = descriptor.route == MODULE_NATIVE_DYLIB_CANCEL_ROUTE;
        let handler_paths = paths.clone();
        let handler_runtime = service_runtime.clone();
        let active_native_dylib_operations = Arc::clone(&active_native_dylib_operations);
        builder = builder.try_register_session_route(descriptor, move |request| {
            let registry = Arc::clone(&registry);
            let paths = handler_paths.clone();
            let runtime = handler_runtime.clone();
            let native_target = native_target.clone();
            let active_native_dylib_operations = Arc::clone(&active_native_dylib_operations);
            async move {
                if is_native_cancel_route {
                    if !request.body.is_empty() {
                        return Err(NetworkError::Serialization {
                            detail: "native module cancellation does not accept a request body"
                                .to_string(),
                        });
                    }
                    return dispatch_native_dylib_module_cancel_request(
                        &active_native_dylib_operations,
                        request.request,
                    )
                    .map(ObjectRpcSessionResponse::buffered);
                }
                if let Some(native_target) = native_target {
                    if !request.body.is_empty() {
                        return Err(NetworkError::Serialization {
                            detail: "native module invocation does not accept a request body"
                                .to_string(),
                        });
                    }
                    return dispatch_native_dylib_module_request(
                        paths,
                        native_target,
                        active_native_dylib_operations,
                        request.request,
                    )
                    .await
                    .map(ObjectRpcSessionResponse::buffered);
                }
                let route = request.request.route.clone();
                let category = request.request.category.clone();
                let traceparent = request
                    .request
                    .traceparent()
                    .unwrap_or_default()
                    .to_string();
                let started_at = Instant::now();
                let signer = daemon_service_signer_from_request_metadata(&request.request)?;
                let (data_dir, cluster) = tokio::task::spawn_blocking(move || {
                    let data_dir = Arc::new(SinomoDataDir::new(
                        daemon_data_dir_path(&paths).map_err(daemon_error_to_network_error)?,
                    ));
                    let cluster: Option<Arc<dyn ClusterAccess>> =
                        if ClusterPaths::new(data_dir.as_ref()).state.exists() {
                            Some(Arc::new(DaemonClusterAccess::new(
                                paths.clone(),
                                data_dir.as_ref(),
                                runtime.as_ref(),
                            )))
                        } else {
                            None
                        };
                    Ok::<_, NetworkError>((data_dir, cluster))
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: format!(
                        "build daemon service context for {}",
                        request.request.route
                    ),
                    detail: format!("daemon service context task failed: {source}"),
                })??;
                let context_us = daemon_service_rpc_elapsed_us(started_at);
                metrics::histogram!(
                    "sinomo.daemon.service.context.us",
                    "route" => route.clone(),
                    "category" => category.clone()
                )
                .record(context_us as f64);
                tracing::trace!(
                    target: "sinomo.daemon.service_rpc",
                    %traceparent,
                    route = %route,
                    category = %category,
                    context_us,
                    "daemon service context built"
                );
                let dispatch_started_at = Instant::now();
                let result =
                    dispatch_service_registry_request(registry, data_dir, cluster, signer, request)
                        .await;
                let outcome = if result.is_ok() { "ok" } else { "error" };
                let dispatch_us = daemon_service_rpc_elapsed_us(dispatch_started_at);
                let total_us = daemon_service_rpc_elapsed_us(started_at);
                metrics::counter!(
                    "sinomo.daemon.service.requests",
                    "route" => route.clone(),
                    "category" => category.clone(),
                    "outcome" => outcome
                )
                .increment(1);
                metrics::histogram!(
                    "sinomo.daemon.service.dispatch.us",
                    "route" => route.clone(),
                    "category" => category.clone(),
                    "outcome" => outcome
                )
                .record(dispatch_us as f64);
                metrics::histogram!(
                    "sinomo.daemon.service.total.us",
                    "route" => route.clone(),
                    "category" => category.clone(),
                    "outcome" => outcome
                )
                .record(total_us as f64);
                tracing::debug!(
                    target: "sinomo.daemon.service_rpc",
                    %traceparent,
                    route = %route,
                    category = %category,
                    dispatch_us,
                    total_us,
                    outcome,
                    "daemon service dispatched object rpc"
                );
                result
            }
        })?;
    }
    builder.try_build()
}

fn daemon_service_rpc_elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

fn daemon_error_to_network_error(error: DaemonError) -> NetworkError {
    NetworkError::Serialization {
        detail: error.to_string(),
    }
}

fn daemon_signer_authority_metadata_value(authority: &DaemonSignerAuthority) -> &'static str {
    match authority {
        DaemonSignerAuthority::Local => "local",
        DaemonSignerAuthority::Voter => "voter",
        DaemonSignerAuthority::Member => "member",
        DaemonSignerAuthority::JoinCandidate => "join-candidate",
        DaemonSignerAuthority::StatusProbe => "status-probe",
    }
}

fn annotate_daemon_service_request(
    request: &mut ObjectRpcRequest,
    authenticated: &AuthenticatedDaemonCommand,
) {
    request.metadata.insert(
        DAEMON_SERVICE_SIGNER_AUTHORITY_METADATA_KEY.to_string(),
        daemon_signer_authority_metadata_value(&authenticated.signer).to_string(),
    );
    request.metadata.insert(
        DAEMON_SERVICE_SIGNER_NODE_ID_METADATA_KEY.to_string(),
        authenticated.signer_node_id.clone(),
    );
    request.metadata.insert(
        DAEMON_SERVICE_SIGNER_PUBLIC_KEY_METADATA_KEY.to_string(),
        authenticated.signer_public_key.clone(),
    );
}

fn daemon_service_signer_from_request_metadata(
    request: &ObjectRpcRequest,
) -> Result<ServiceSigner, NetworkError> {
    let authority = request
        .metadata
        .get(DAEMON_SERVICE_SIGNER_AUTHORITY_METADATA_KEY)
        .ok_or_else(|| NetworkError::Serialization {
            detail: format!(
                "daemon service request {} is missing authenticated signer authority",
                request.route
            ),
        })?;
    let node_id = request
        .metadata
        .get(DAEMON_SERVICE_SIGNER_NODE_ID_METADATA_KEY)
        .cloned()
        .unwrap_or_default();
    let public_key = request
        .metadata
        .get(DAEMON_SERVICE_SIGNER_PUBLIC_KEY_METADATA_KEY)
        .cloned()
        .unwrap_or_default();
    if authority == "local" {
        Ok(ServiceSigner::LocalNode {
            node_id,
            public_key,
        })
    } else {
        Ok(ServiceSigner::Remote {
            node_id,
            public_key,
        })
    }
}

fn daemon_service_object_rpc_authorization_layer(
    signer_authority: DaemonSignerAuthority,
    paths: DaemonPaths,
) -> ObjectRpcAuthorizationLayer {
    ObjectRpcAuthorizationLayer::new(move |descriptor, _context| {
        let signer_authority = signer_authority.clone();
        let paths = paths.clone();
        async move {
            if let Some(auth_scope) = descriptor.auth_scope.as_deref()
                && !daemon_signer_authorizes_route_scope(&signer_authority, auth_scope)
            {
                warn_daemon_service_auth_denied(
                    &descriptor.route,
                    auth_scope,
                    &signer_authority,
                    None,
                    None,
                );
                return Err(NetworkError::AuthDenied);
            }
            if !daemon_nonlocal_config_permission_is_granted(
                &paths,
                &signer_authority,
                &descriptor.route,
            ) {
                return Err(NetworkError::AuthDenied);
            }
            if !daemon_route_local_permissions_are_granted(&paths, &descriptor) {
                return Err(NetworkError::AuthDenied);
            }
            Ok(())
        }
    })
}

pub(crate) async fn try_dispatch_pre_auth_via_registry_async(
    request: &DaemonControlRequest,
    stream: &mut AsyncDaemonControlStream,
    stream_address: &str,
    service_host: &DaemonServiceRegistryHost,
) -> Result<bool, DaemonError> {
    match dispatch_pre_auth_via_registry_to_response_async(request, service_host).await {
        Ok(Some(response)) => {
            write_async_daemon_rpc_response_frame(stream, stream_address, response).await?;
            Ok(true)
        }
        Ok(None) => Ok(false),
        Err(error) => {
            tracing::warn!("sinomo daemon pre-auth join challenge ignored: {error}");
            let _ = stream.shutdown_write(stream_address).await;
            Ok(true)
        }
    }
}

pub(crate) async fn dispatch_pre_auth_via_registry_to_response_async(
    request: &DaemonControlRequest,
    service_host: &DaemonServiceRegistryHost,
) -> Result<Option<ObjectRpcResponse>, DaemonError> {
    if request.route != CLUSTER_JOIN_CHALLENGE_ROUTE
        || request.category != CLUSTER_MEMBERSHIP_RPC_CATEGORY
    {
        return Ok(None);
    }
    if request.auth.is_some()
        && crate::auth::authenticate_daemon_control_request_without_nonce(
            &service_host.paths,
            request,
        )
        .is_ok()
    {
        return Ok(None);
    }

    let authenticated = AuthenticatedDaemonCommand {
        route: request.route.clone(),
        signer: DaemonSignerAuthority::JoinCandidate,
        signer_node_id: String::new(),
        signer_public_key: String::new(),
    };
    if service_host
        .object_rpc_route_descriptor(CLUSTER_JOIN_CHALLENGE_ROUTE)
        .is_none()
    {
        return Ok(None);
    }
    let (response, _stop) = dispatch_rpc_via_registry_to_response_async(
        service_host,
        &authenticated,
        request.clone(),
        None,
        None,
    )
    .await?;
    Ok(Some(response))
}

pub(crate) async fn try_dispatch_via_registry_async(
    authenticated: &AuthenticatedDaemonCommand,
    rpc_request: &Option<ObjectRpcRequest>,
    stream: &mut AsyncDaemonControlStream,
    stream_address: &str,
    service_host: &DaemonServiceRegistryHost,
    paths: &DaemonPaths,
    replay_key: Option<Vec<u8>>,
) -> Option<Result<bool, DaemonError>> {
    let request = rpc_request.as_ref()?;
    if is_sandbox_stream_route(request) {
        return None;
    }
    if service_host
        .object_rpc_route_descriptor(&request.route)
        .is_none()
    {
        return None;
    }
    tracing::trace!(
        target: "sinomo.daemon.service_rpc",
        traceparent = %request.traceparent().unwrap_or_default(),
        route = %request.route,
        category = %request.category,
        stream_address = %stream_address,
        signer_node_id = %authenticated.signer_node_id,
        body_bytes = request.body.len(),
        dfs_data_plane = sinomo_dfs::dfs_route_is_message_data_plane(&request.route),
        "daemon service rpc dispatch accepted"
    );
    Some(
        dispatch_via_registry_async(
            service_host,
            authenticated,
            request.clone(),
            stream,
            stream_address,
            Some(paths),
            replay_key,
        )
        .await,
    )
}

async fn dispatch_via_registry_async(
    service_host: &DaemonServiceRegistryHost,
    authenticated: &AuthenticatedDaemonCommand,
    request: ObjectRpcRequest,
    stream: &mut AsyncDaemonControlStream,
    stream_address: &str,
    paths: Option<&DaemonPaths>,
    replay_key: Option<Vec<u8>>,
) -> Result<bool, DaemonError> {
    let route = request.route.clone();
    let (response, stop) = dispatch_rpc_via_registry_to_response_async(
        service_host,
        authenticated,
        request,
        paths,
        replay_key,
    )
    .await?;
    write_async_daemon_rpc_response_frame(stream, stream_address, response).await?;
    mark_cluster_identity_restart_after_response(&route, stop, paths);
    if stop {
        stream
            .shutdown_write(stream_address)
            .await
            .map_err(DaemonError::from)?;
        tokio::time::sleep(ACCEPTED_ROLLOUT_RESPONSE_DRAIN_GRACE).await;
    }
    Ok(stop)
}

pub(crate) async fn dispatch_rpc_via_registry_to_response_async(
    service_host: &DaemonServiceRegistryHost,
    authenticated: &AuthenticatedDaemonCommand,
    request: ObjectRpcRequest,
    paths: Option<&DaemonPaths>,
    replay_key: Option<Vec<u8>>,
) -> Result<(ObjectRpcResponse, bool), DaemonError> {
    let (mut response, stop) = dispatch_rpc_via_registry_to_session_response_async(
        service_host,
        authenticated,
        ObjectRpcSessionRequest::buffered(request),
        paths,
        replay_key,
    )
    .await?;
    if !response.body.is_empty() {
        response.response.body = response
            .body
            .collect_to_vec(sinomo_core::network::OBJECT_RPC_UNARY_ADAPTER_MAX_BODY_BYTES)
            .await
            .map_err(DaemonError::from)?;
    }
    Ok((response.response, stop))
}

pub(crate) async fn dispatch_rpc_via_registry_to_session_response_async(
    service_host: &DaemonServiceRegistryHost,
    authenticated: &AuthenticatedDaemonCommand,
    mut request: ObjectRpcSessionRequest,
    paths: Option<&DaemonPaths>,
    replay_key: Option<Vec<u8>>,
) -> Result<(ObjectRpcSessionResponse, bool), DaemonError> {
    let traceparent = request
        .request
        .traceparent()
        .unwrap_or_default()
        .to_string();
    let route = request.request.route.clone();
    let category = request.request.category.clone();
    let head_body_bytes = request.request.body.len();
    let has_body = !request.body.is_empty();
    let dispatch_started = Instant::now();
    tracing::trace!(
        target: "sinomo.daemon.service_rpc",
        traceparent = %traceparent,
        route = %route,
        category = %category,
        signer_node_id = %authenticated.signer_node_id,
        head_body_bytes,
        has_body,
        dfs_data_plane = sinomo_dfs::dfs_route_is_message_data_plane(&route),
        "daemon service rpc dispatch start"
    );
    annotate_daemon_service_request(&mut request.request, authenticated);
    let router = service_host
        .route_policy_layer()
        .layer(service_host.active_router());
    let mut router = daemon_service_object_rpc_authorization_layer(
        authenticated.signer.clone(),
        service_host.paths.clone(),
    )
    .layer(router);
    let mut response =
        match sinomo_core::network::call_object_rpc_session_service_async(&mut router, request)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if let (Some(paths), Some(key)) = (paths, replay_key.as_ref()) {
                    crate::control::abandon_daemon_dfs_message_replay(paths, key);
                }
                return Err(error.into());
            }
        };
    if let Some(paths) = paths {
        mark_daemon_rpc_response_responder(paths, &mut response.response);
    }
    let stop = response
        .response
        .metadata
        .remove(DAEMON_SERVICE_DISPOSITION_METADATA_KEY)
        .is_some_and(|value| value == "stop");
    if let (Some(paths), Some(key)) = (paths, replay_key.as_ref()) {
        if response.body.is_empty() {
            crate::control::complete_daemon_dfs_message_replay(paths, key, &response.response);
        } else {
            // The legacy head-only cache cannot safely replay an attached body.
            crate::control::abandon_daemon_dfs_message_replay(paths, key);
        }
    }
    tracing::trace!(
        target: "sinomo.daemon.service_rpc",
        traceparent = %traceparent,
        route = %route,
        category = %category,
        response_head_body_bytes = response.response.body.len(),
        response_has_body = !response.body.is_empty(),
        elapsed_ms = dispatch_started.elapsed().as_millis(),
        stop,
        "daemon service rpc dispatch complete"
    );
    Ok((response, stop))
}

async fn write_async_daemon_rpc_response_frame(
    stream: &mut AsyncDaemonControlStream,
    stream_address: &str,
    response: ObjectRpcResponse,
) -> Result<(), DaemonError> {
    stream
        .write_response(
            stream_address,
            &response,
            sinomo_core::network::ObjectFrameIoOptions::default(),
        )
        .await
        .map_err(DaemonError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::{InProcessKvStore, KvEntry, KvRevision, KvVersion};
    use std::time::Duration;

    #[test]
    fn module_route_local_permissions_propagate_to_daemon_catalog_descriptor() {
        let descriptor = sinomo_module::ModuleObjectRpcRoute::new(
            "/remote/status",
            "remote.management",
            "remote.management.status.read",
            sinomo_module::ModuleRouteAuth::Admin,
        )
        .require_local_permissions(["remote-management", "device:display"])
        .descriptor()
        .expect("module route descriptor");

        let daemon = module_object_rpc_route_descriptor_to_daemon_route(&descriptor);

        assert_eq!(
            daemon.required_local_permissions,
            vec!["remote-management", "device:display"]
        );
    }

    #[test]
    fn conditional_write_maps_expected_posix_failures_to_typed_outcomes() {
        assert!(matches!(
            daemon_dfs_browser_conditional_write_error(sinomo_dfs::DfsPosixError::Conflict {
                path: "/notes.txt".to_string(),
                reason: "stale base".to_string(),
            }),
            crate::DaemonDfsBrowserReplaceFileIfVersionOutcome::Conflict { .. }
        ));
        assert!(matches!(
            daemon_dfs_browser_conditional_write_error(sinomo_dfs::DfsPosixError::NotFound {
                path: "/notes.txt".to_string(),
            }),
            crate::DaemonDfsBrowserReplaceFileIfVersionOutcome::NotFound { .. }
        ));
        assert!(matches!(
            daemon_dfs_browser_conditional_write_error(sinomo_dfs::DfsPosixError::IsDirectory {
                path: "/notes.txt".to_string(),
            }),
            crate::DaemonDfsBrowserReplaceFileIfVersionOutcome::Rejected { .. }
        ));
    }

    #[test]
    fn rollout_build_session_body_rehydrates_source_bundle() {
        let payload = sinomo_rollout::ClusterRolloutBuildRequest {
            source: "test-source".to_string(),
            remote_repo: None,
            source_bundle: None,
            source_head: Some("abc123".to_string()),
            force_source_overwrite: false,
            host: "127.0.0.1".to_string(),
            port: 21_353,
            shadow_port: 21_354,
            requested_at: "2026-07-12T00:00:00Z".to_string(),
        };
        let request = ObjectRpcRequest::encode_body(
            ROLLOUT_BUILD_ROUTE,
            sinomo_rollout::ROLLOUT_RPC_CATEGORY,
            &payload,
        )
        .expect("request head");
        let expected_bundle = vec![9_u8; 48 * 1024];
        let mut body = sinomo_core::network::ObjectRpcIncomingBody::bytes(expected_bundle.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let decoded = runtime
            .block_on(decode_rollout_build_session_request(&request, &mut body))
            .expect("session request");

        assert_eq!(decoded.source_bundle, Some(expected_bundle));
        assert_eq!(decoded.source_head, payload.source_head);
    }

    #[test]
    fn dfs_browser_write_targets_only_discovered_content_servers() {
        let first = sinomo_core::node::NodeId::from_legacy_named(
            "node_dfs_browser_discovered_content_first",
        )
        .expect("first content server");
        let second = sinomo_core::node::NodeId::from_legacy_named(
            "node_dfs_browser_discovered_content_second",
        )
        .expect("second content server");

        let options = sifs_browser_write_options(&[first.clone(), second.clone()]);

        assert_eq!(options.replica_candidates, vec![first, second]);
        assert!(!options.foreground_replica_repair);
    }

    #[test]
    fn dfs_browser_reads_do_not_queue_behind_busy_service() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-browser-read-contention");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let backend = Arc::new(
            DaemonDfsBrowserBackend::open(SinomoDataDir::new(&root), runtime.handle())
                .expect("open DFS browser backend"),
        );
        let held_service = backend.lock_service().expect("hold DFS browser service");
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        let list_backend = Arc::clone(&backend);
        let list_tx = result_tx.clone();
        let list_worker = std::thread::spawn(move || {
            let started = Instant::now();
            let result = list_backend
                .list(crate::DaemonDfsBrowserListRequest {
                    path: "/".to_string(),
                    shallow: true,
                    missing_is_empty: true,
                    sort_key: crate::DaemonDfsBrowserSortKey::default(),
                    sort_direction: crate::DaemonDfsBrowserSortDirection::default(),
                    offset: 0,
                    limit: None,
                })
                .map(|_| ())
                .map_err(|error| error.to_string());
            list_tx
                .send(("list", started.elapsed(), result))
                .expect("report list result");
        });

        let read_backend = Arc::clone(&backend);
        let read_worker = std::thread::spawn(move || {
            let started = Instant::now();
            let result = read_backend
                .read_files(crate::DaemonDfsBrowserReadFilesRequest {
                    paths: vec!["/missing.json".to_string()],
                })
                .map(|_| ())
                .map_err(|error| error.to_string());
            result_tx
                .send(("read-files", started.elapsed(), result))
                .expect("report read-files result");
        });

        for _ in 0..2 {
            let (operation, elapsed, result) = result_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("contending browser read must not wait for the held service lock");
            assert!(
                elapsed < Duration::from_secs(1),
                "{operation} remained queued for {elapsed:?}"
            );
            let error = result.expect_err("contending browser read should report busy");
            assert!(
                error.contains("DFS browser service is busy") && error.contains("retry the read"),
                "unexpected {operation} error: {error}"
            );
        }

        drop(held_service);
        list_worker.join().expect("join list worker");
        read_worker.join().expect("join read-files worker");
        assert!(
            backend.node_id().is_ok(),
            "a busy result must not poison or retain the service lock"
        );
    }

    struct TestStopService;

    impl Service for TestStopService {
        fn name(&self) -> &'static str {
            "test-stop"
        }

        fn routes(&self) -> &'static [&'static str] {
            &["/test/stop"]
        }

        fn route_categories(&self) -> Vec<ServiceRoute> {
            vec![
                ServiceRoute::new("/test/stop", "test-stop")
                    .with_auth_scope("admin")
                    .require_capability("test.stop")
                    .require_local_permission("remote-management")
                    .with_concurrency_limit(1)
                    .with_timeout(Duration::from_millis(500)),
            ]
        }

        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async move {
                let _: () = request.decode_body()?;
                ctx.respond(ObjectRpcResponse::encode_body(
                    request.route,
                    &"stopping".to_string(),
                )?)?;
                Ok(ServiceDisposition::Stop)
            })
        }
    }

    struct TestConfigRouteService;

    impl Service for TestConfigRouteService {
        fn name(&self) -> &'static str {
            "test-config-route"
        }

        fn routes(&self) -> &'static [&'static str] {
            &[CONFIG_GET_ROUTE]
        }

        fn route_categories(&self) -> Vec<ServiceRoute> {
            vec![ServiceRoute::new(CONFIG_GET_ROUTE, "config")]
        }

        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async move {
                let _: () = request.decode_body()?;
                ctx.respond(ObjectRpcResponse::encode_body(
                    request.route,
                    &"config-read".to_string(),
                )?)?;
                Ok(ServiceDisposition::Continue)
            })
        }
    }

    struct TestPermissionOnlyService;

    impl Service for TestPermissionOnlyService {
        fn name(&self) -> &'static str {
            "test-permission-only"
        }

        fn routes(&self) -> &'static [&'static str] {
            &["/test/permission-only"]
        }

        fn route_categories(&self) -> Vec<ServiceRoute> {
            vec![
                ServiceRoute::new("/test/permission-only", "test-permission-only")
                    .require_local_permission("remote-management"),
            ]
        }

        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async move {
                let _: () = request.decode_body()?;
                ctx.respond(ObjectRpcResponse::encode_body(request.route, &())?)?;
                Ok(ServiceDisposition::Continue)
            })
        }
    }

    struct TestConcurrencyLimitService;

    impl Service for TestConcurrencyLimitService {
        fn name(&self) -> &'static str {
            "test-concurrency"
        }

        fn routes(&self) -> &'static [&'static str] {
            &["/test/concurrency"]
        }

        fn route_categories(&self) -> Vec<ServiceRoute> {
            vec![
                ServiceRoute::new("/test/concurrency", "test-concurrency")
                    .with_concurrency_limit(8),
            ]
        }

        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async move {
                let _: () = request.decode_body()?;
                ctx.respond(ObjectRpcResponse::encode_body(
                    request.route,
                    &"limited".to_string(),
                )?)?;
                Ok(ServiceDisposition::Continue)
            })
        }
    }

    struct TestJoinChallengeService;

    impl Service for TestJoinChallengeService {
        fn name(&self) -> &'static str {
            "test-join-challenge"
        }

        fn routes(&self) -> &'static [&'static str] {
            &[CLUSTER_JOIN_CHALLENGE_ROUTE]
        }

        fn route_categories(&self) -> Vec<ServiceRoute> {
            vec![ServiceRoute::new(
                CLUSTER_JOIN_CHALLENGE_ROUTE,
                CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            )]
        }

        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async move {
                let _: () = request.decode_body()?;
                let signer_authority = request
                    .metadata
                    .get(DAEMON_SERVICE_SIGNER_AUTHORITY_METADATA_KEY)
                    .cloned()
                    .unwrap_or_default();
                let signer_node_id = request
                    .metadata
                    .get(DAEMON_SERVICE_SIGNER_NODE_ID_METADATA_KEY)
                    .cloned()
                    .unwrap_or_default();
                ctx.respond(ObjectRpcResponse::encode_body(
                    request.route,
                    &(signer_authority, signer_node_id),
                )?)?;
                Ok(ServiceDisposition::Continue)
            })
        }
    }

    fn test_service_host(
        root: &std::path::Path,
        registry: ServiceRegistry,
    ) -> DaemonServiceRegistryHost {
        let data_dir = SinomoDataDir::new(root);
        let paths = DaemonPaths::new(&data_dir);
        DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service registry host")
    }

    fn test_authenticated_command(signer: DaemonSignerAuthority) -> AuthenticatedDaemonCommand {
        AuthenticatedDaemonCommand {
            route: "/test".to_string(),
            signer,
            signer_node_id: "node_test".to_string(),
            signer_public_key: "public-key".to_string(),
        }
    }

    fn annotate_test_request(request: &mut ObjectRpcRequest, signer: DaemonSignerAuthority) {
        annotate_daemon_service_request(request, &test_authenticated_command(signer));
    }

    #[test]
    fn daemon_service_registry_host_dispatches_registry_service_and_records_stop() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-service-router-adapter");
        let registry = ServiceRegistry::builder().register(TestStopService).build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry.clone());
        let mut router = host.route_policy_layer().layer(host.active_router());
        let catalog = host.active_route_snapshot().catalog().clone();
        assert!(catalog.routes.contains_key("/test/stop"));
        let descriptor = catalog
            .routes
            .get("/test/stop")
            .expect("service route descriptor");
        assert_eq!(descriptor.auth_scope.as_deref(), Some("admin"));
        assert_eq!(
            descriptor.required_capabilities,
            vec!["test.stop".to_string()]
        );
        assert_eq!(
            descriptor.required_local_permissions,
            vec!["remote-management".to_string()]
        );
        assert_eq!(
            descriptor
                .observability
                .get("daemon.service_registry.snapshot_version"),
            Some(&"1".to_string())
        );
        assert_eq!(descriptor.concurrency_limit, Some(1));
        assert_eq!(descriptor.timeout_ms, Some(500));
        assert_eq!(
            registry
                .route_categories()
                .first()
                .expect("service route metadata")
                .concurrency_limit,
            Some(1)
        );

        let response = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request = ObjectRpcRequest::encode_body("/test/stop", "test-stop", &())
                        .expect("request");
                    annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                    request
                },
            ))
            .expect("response");

        assert_eq!(
            response.decode_body::<String>().expect("decode"),
            "stopping"
        );
        assert_eq!(
            response
                .metadata
                .get(DAEMON_SERVICE_DISPOSITION_METADATA_KEY)
                .map(String::as_str),
            Some("stop")
        );
    }

    #[test]
    fn daemon_service_registry_host_dispatches_limited_registry_service() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-service-router-concurrency");
        let registry = ServiceRegistry::builder()
            .register(TestConcurrencyLimitService)
            .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry.clone());
        let mut router = host.route_policy_layer().layer(host.active_router());
        let catalog = host.active_route_snapshot().catalog().clone();
        let descriptor = catalog
            .routes
            .get("/test/concurrency")
            .expect("concurrency service route descriptor");
        assert_eq!(descriptor.concurrency_limit, Some(8));
        assert_eq!(
            registry
                .route_categories()
                .first()
                .expect("service route metadata")
                .concurrency_limit,
            Some(8)
        );

        let response = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request =
                        ObjectRpcRequest::encode_body("/test/concurrency", "test-concurrency", &())
                            .expect("request");
                    annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                    request
                },
            ))
            .expect("response");

        assert_eq!(response.decode_body::<String>().expect("decode"), "limited");
        assert_eq!(
            response
                .metadata
                .get(DAEMON_SERVICE_DISPOSITION_METADATA_KEY)
                .map(String::as_str),
            Some("continue")
        );
    }

    #[test]
    fn daemon_maintenance_refresh_rpc_notifies_dfs_maintenance() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-maintenance-refresh-dfs");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = ServiceRegistry::builder()
            .register(DaemonControlService {
                paths,
                runtime: Some(runtime.handle().clone()),
            })
            .build();
        let host = test_service_host(&root, registry);
        let mut router = host.route_policy_layer().layer(host.active_router());

        runtime.block_on(async {
            let dfs_notify = crate::dfs::daemon_dfs_maintenance_notify();
            while tokio::time::timeout(Duration::from_millis(1), dfs_notify.notified())
                .await
                .is_ok()
            {}
            let notified = dfs_notify.notified();
            tokio::pin!(notified);
            let response = sinomo_core::network::call_object_rpc_service_async(&mut router, {
                let mut request = ObjectRpcRequest::encode_body(
                    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
                    DAEMON_RPC_CATEGORY,
                    &crate::DaemonClusterMaintenanceRefreshRequest,
                )
                .expect("request");
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            })
            .await
            .expect("maintenance refresh response");
            response.error_detail().expect("maintenance refresh ok");
            tokio::time::timeout(Duration::from_secs(1), &mut notified)
                .await
                .expect("DFS maintenance notification");
        });
    }

    #[test]
    fn daemon_local_connectivity_reload_rpc_dispatches() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-connectivity-reload");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = ServiceRegistry::builder()
            .register(DaemonControlService {
                paths,
                runtime: Some(runtime.handle().clone()),
            })
            .build();
        let host = test_service_host(&root, registry);
        let mut router = host.route_policy_layer().layer(host.active_router());

        let response = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request = ObjectRpcRequest::encode_body(
                        DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
                        DAEMON_RPC_CATEGORY,
                        &crate::DaemonLocalConnectivityReloadRequest,
                    )
                    .expect("request");
                    annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                    request
                },
            ))
            .expect("connectivity reload response");

        response.error_detail().expect("connectivity reload ok");
        response
            .decode_body::<crate::DaemonLocalConnectivityReloadResponse>()
            .expect("decode connectivity reload response");
    }

    #[test]
    fn daemon_service_registry_host_installs_native_dylib_cancel_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-native-dylib-cancel-route");
        let registry = ServiceRegistry::builder().build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);
        let catalog = host.active_route_snapshot().catalog().clone();
        let descriptor = catalog
            .routes
            .get(MODULE_NATIVE_DYLIB_CANCEL_ROUTE)
            .expect("native dylib cancel route descriptor");
        assert_eq!(descriptor.category, MODULE_RPC_CATEGORY);
        assert_eq!(
            descriptor.auth_scope.as_deref(),
            Some("local-or-cluster-member")
        );
        assert_eq!(
            descriptor.required_capabilities,
            vec!["module.native-dylib.cancel".to_string()]
        );
        assert_eq!(
            descriptor
                .observability
                .get("daemon.native_dylib_module_cancel_route")
                .map(String::as_str),
            Some("true")
        );

        let mut router = host.route_policy_layer().layer(host.active_router());
        let response = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                ObjectRpcRequest::encode_body(
                    MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
                    MODULE_RPC_CATEGORY,
                    &ObjectRpcCancelRequest::new("agent_module_cancel_route_probe", "route probe"),
                )
                .expect("request")
                .with_cancels_operation_id("agent_module_cancel_route_probe"),
            ))
            .expect("native dylib cancel route response");
        let cancel_response: ObjectRpcCancelResponse = response.decode_body().expect("decode");
        assert_eq!(
            cancel_response.operation_id,
            "agent_module_cancel_route_probe"
        );
        assert!(!cancel_response.cancelled);
        assert!(
            cancel_response
                .detail
                .contains("native dylib module operation is not active")
        );
    }

    #[test]
    fn daemon_service_registry_skips_broken_active_native_dylib_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-broken-native-dylib-state");
        let module_id =
            sinomo_module::ModuleId::from_legacy_named("module.dfs.cloud-files").expect("module");
        let content_hash =
            "blake3:0000000000000000000000000000000000000000000000000000000000000000";
        let state = sinomo_module::NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: content_hash.to_string(),
            cache_key: "module/dfs/cloud-files".to_string(),
            library_name: "sinomo_dfs_cloud_files.dll".to_string(),
            platform_triple: "x86_64-pc-windows-msvc".to_string(),
            native_abi_version: 1,
            interface_version: sinomo_module::SINOMO_MODULE_V1_INTERFACE_VERSION.to_string(),
            activated_at: sinomo_module::module_loader_applied_at_now(),
            source: "test".to_string(),
        };
        sinomo_module::write_native_dylib_module_loader_state(&root, &state)
            .expect("write native dylib state");
        let cache_path = sinomo_module::native_module_cache_path(
            &root,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        std::fs::create_dir_all(cache_path.parent().expect("cache path parent"))
            .expect("cache dir");
        std::fs::write(&cache_path, b"wrong native module bytes").expect("cache bytes");

        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = daemon_service_registry(&paths, None, &[]);
        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service host");
        let catalog = host.active_route_snapshot().catalog().clone();
        assert!(
            catalog.routes.contains_key(DAEMON_STATUS_ROUTE),
            "built-in daemon routes should still be available"
        );
        assert!(
            catalog
                .routes
                .contains_key(MODULE_NATIVE_DYLIB_CANCEL_ROUTE),
            "module control routes should still be available"
        );
    }

    #[test]
    fn pre_auth_join_challenge_dispatches_as_join_candidate() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-pre-auth-join-challenge");
        let registry = ServiceRegistry::builder()
            .register(TestJoinChallengeService)
            .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);
        let request = ObjectRpcRequest::encode_body(
            CLUSTER_JOIN_CHALLENGE_ROUTE,
            CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &(),
        )
        .expect("join challenge rpc");

        let response = runtime
            .block_on(dispatch_pre_auth_via_registry_to_response_async(
                &request, &host,
            ))
            .expect("pre-auth dispatch")
            .expect("pre-auth response");
        let (signer_authority, signer_node_id): (String, String) =
            response.decode_body().expect("decode response");

        assert_eq!(signer_authority, "join-candidate");
        assert_eq!(signer_node_id, "");
    }

    #[test]
    fn non_member_signed_join_challenge_dispatches_as_join_candidate() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-signed-candidate-join-challenge");
        let registry = ServiceRegistry::builder()
            .register(TestJoinChallengeService)
            .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);
        let request = ObjectRpcRequest::encode_body(
            CLUSTER_JOIN_CHALLENGE_ROUTE,
            CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &(),
        )
        .expect("join challenge rpc")
        .with_auth(b"signed-member-request".to_vec());

        let response = runtime
            .block_on(dispatch_pre_auth_via_registry_to_response_async(
                &request, &host,
            ))
            .expect("pre-auth dispatch")
            .expect("pre-auth response");
        let (signer_authority, signer_node_id): (String, String) =
            response.decode_body().expect("decode response");

        assert_eq!(signer_authority, "join-candidate");
        assert_eq!(signer_node_id, "");
    }

    #[test]
    fn local_signed_join_challenge_skips_pre_auth_dispatch() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-local-signed-join-challenge");
        let registry = ServiceRegistry::builder()
            .register(TestJoinChallengeService)
            .build();
        sinomo_core::node::load_or_create_main_node_identity(root.path()).expect("main identity");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);
        let data_dir = SinomoDataDir::new(root.path());
        let request = ObjectRpcRequest::encode_body(
            CLUSTER_JOIN_CHALLENGE_ROUTE,
            CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &(),
        )
        .expect("join challenge rpc");
        let auth =
            crate::auth::sign_daemon_rpc_request_from_data_dir(&data_dir, &request).expect("auth");
        let request = request.with_auth(auth);

        let response = runtime
            .block_on(dispatch_pre_auth_via_registry_to_response_async(
                &request, &host,
            ))
            .expect("pre-auth dispatch");

        assert!(response.is_none());
    }

    #[test]
    fn daemon_service_registry_host_reports_live_service_route_readiness() {
        struct HoldingService {
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }

        impl Service for HoldingService {
            fn name(&self) -> &'static str {
                "holding"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/holding"]
            }

            fn route_categories(&self) -> Vec<ServiceRoute> {
                vec![ServiceRoute::new("/test/holding", "holding").with_concurrency_limit(1)]
            }

            fn handle(
                &self,
                mut ctx: ServiceContext,
                request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                let entered = Arc::clone(&self.entered);
                let release = Arc::clone(&self.release);
                Box::pin(async move {
                    let _: () = request.decode_body()?;
                    entered.notify_one();
                    release.notified().await;
                    ctx.respond(ObjectRpcResponse::encode_body(
                        request.route,
                        &"released".to_string(),
                    )?)?;
                    Ok(ServiceDisposition::Continue)
                })
            }
        }

        let root = sinomo_testdir::TestDir::new("sinomo-daemon-service-router-readiness");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let registry = ServiceRegistry::builder()
            .register(HoldingService {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            })
            .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);
        let router = host.route_policy_layer().layer(host.active_router());

        let initial = router
            .object_rpc_route_readiness("/test/holding")
            .expect("initial service route readiness");
        assert_eq!(initial.concurrency_limit, Some(1));
        assert_eq!(initial.available_concurrency, Some(1));
        assert!(initial.is_ready());

        runtime.block_on(async {
            let mut in_flight_router = router.clone();
            let in_flight = tokio::spawn(async move {
                let mut request = ObjectRpcRequest::encode_body("/test/holding", "holding", &())
                    .expect("request");
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                sinomo_core::network::call_object_rpc_service_async(&mut in_flight_router, request)
                    .await
            });
            entered.notified().await;

            let saturated = router
                .object_rpc_route_readiness("/test/holding")
                .expect("saturated service route readiness");
            assert_eq!(saturated.available_concurrency, Some(0));
            assert!(saturated.is_saturated());

            release.notify_one();
            let response = in_flight
                .await
                .expect("join holding service")
                .expect("holding service response");
            assert_eq!(
                response.decode_body::<String>().expect("decode"),
                "released"
            );
        });

        let recovered = router
            .object_rpc_route_readiness("/test/holding")
            .expect("recovered service route readiness");
        assert_eq!(recovered.available_concurrency, Some(1));
        assert!(recovered.is_ready());
    }

    #[test]
    fn daemon_service_registry_host_denies_route_scope_before_handler() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-service-router-auth");
        let registry = ServiceRegistry::builder().register(TestStopService).build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);
        let router = host.route_policy_layer().layer(host.active_router());
        let mut router = daemon_service_object_rpc_authorization_layer(
            DaemonSignerAuthority::Member,
            host.paths.clone(),
        )
        .layer(router);

        let error = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request = ObjectRpcRequest::encode_body("/test/stop", "test-stop", &())
                        .expect("request");
                    annotate_test_request(&mut request, DaemonSignerAuthority::Member);
                    request
                },
            ))
            .expect_err("member must not run admin route");

        assert_eq!(error, NetworkError::AuthDenied);
    }

    #[test]
    fn catalog_authentication_requires_node_local_permission_for_dynamic_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dynamic-route-local-permission");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = ServiceRegistry::builder().register(TestStopService).build();
        let host = test_service_host(&root, registry);
        let unsigned =
            ObjectRpcRequest::encode_body("/test/stop", "test-stop", &()).expect("dynamic request");

        let mut denied = unsigned.clone();
        denied.auth = Some(
            crate::auth::sign_daemon_rpc_request_from_data_dir(&data_dir, &denied)
                .expect("sign denied request"),
        );
        assert!(matches!(
            authenticate_daemon_control_request_via_registry(&paths, &denied, &host),
            Err(DaemonError::AuthDenied)
        ));

        let mut permissions = sinomo_core::node::LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        sinomo_core::node::LocalNodePermissionStore::new(data_dir.root())
            .save(&permissions)
            .expect("enable remote management locally");
        let mut allowed = unsigned;
        allowed.auth = Some(
            crate::auth::sign_daemon_rpc_request_from_data_dir(&data_dir, &allowed)
                .expect("sign allowed request"),
        );
        let authenticated =
            authenticate_daemon_control_request_via_registry(&paths, &allowed, &host)
                .expect("catalog auth succeeds after local grant");
        assert_eq!(authenticated.signer, DaemonSignerAuthority::Local);
    }

    #[test]
    fn local_permission_metadata_never_replaces_route_signer_authority() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-permission-only-route-auth");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = ServiceRegistry::builder()
            .register(TestPermissionOnlyService)
            .build();
        let host = test_service_host(&root, registry);
        let mut permissions = sinomo_core::node::LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        sinomo_core::node::LocalNodePermissionStore::new(data_dir.root())
            .save(&permissions)
            .expect("enable remote management locally");
        let mut request =
            ObjectRpcRequest::encode_body("/test/permission-only", "test-permission-only", &())
                .expect("request");
        request.auth = Some(
            crate::auth::sign_daemon_rpc_request_from_data_dir(&data_dir, &request)
                .expect("sign request"),
        );

        assert!(matches!(
            authenticate_daemon_control_request_via_registry(&paths, &request, &host),
            Err(DaemonError::AuthDenied)
        ));
    }

    #[test]
    fn daemon_authorization_layer_rechecks_node_local_permission_at_dispatch() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-route-permission-recheck");
        let data_dir = SinomoDataDir::new(&root);
        let registry = ServiceRegistry::builder().register(TestStopService).build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);

        let call = |host: &DaemonServiceRegistryHost| {
            let router = host.route_policy_layer().layer(host.active_router());
            let mut router = daemon_service_object_rpc_authorization_layer(
                DaemonSignerAuthority::Local,
                host.paths.clone(),
            )
            .layer(router);
            runtime.block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request = ObjectRpcRequest::encode_body("/test/stop", "test-stop", &())
                        .expect("request");
                    annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                    request
                },
            ))
        };

        assert_eq!(
            call(&host).expect_err("missing permission must deny dispatch"),
            NetworkError::AuthDenied
        );
        let mut permissions = sinomo_core::node::LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        sinomo_core::node::LocalNodePermissionStore::new(data_dir.root())
            .save(&permissions)
            .expect("enable remote management locally");
        let response = call(&host).expect("enabled permission allows dispatch");
        assert_eq!(
            response.decode_body::<String>().expect("decode response"),
            "stopping"
        );
    }

    #[test]
    fn daemon_authorization_layer_rechecks_remote_config_permission_after_revocation() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-config-permission-recheck");
        let data_dir = SinomoDataDir::new(&root);
        let registry = ServiceRegistry::builder()
            .register(TestConfigRouteService)
            .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);

        let call = |authority: DaemonSignerAuthority| {
            let router = host.route_policy_layer().layer(host.active_router());
            let mut router = daemon_service_object_rpc_authorization_layer(
                authority.clone(),
                host.paths.clone(),
            )
            .layer(router);
            runtime.block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request =
                        ObjectRpcRequest::encode_body(CONFIG_GET_ROUTE, "config", &())
                            .expect("request");
                    annotate_test_request(&mut request, authority);
                    request
                },
            ))
        };

        let local = call(DaemonSignerAuthority::Local)
            .expect("local config remains available without remote-management permission");
        assert_eq!(
            local.decode_body::<String>().expect("local response"),
            "config-read"
        );
        assert_eq!(
            call(DaemonSignerAuthority::Voter)
                .expect_err("remote config must be denied before a local grant"),
            NetworkError::AuthDenied
        );

        let store = sinomo_core::node::LocalNodePermissionStore::new(data_dir.root());
        let mut permissions = sinomo_core::node::LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        store.save(&permissions).expect("enable remote management");
        let remote = call(DaemonSignerAuthority::Voter)
            .expect("remote config is available while locally granted");
        assert_eq!(
            remote.decode_body::<String>().expect("remote response"),
            "config-read"
        );

        permissions.set_remote_management_enabled(false);
        store.save(&permissions).expect("revoke remote management");
        assert_eq!(
            call(DaemonSignerAuthority::Voter)
                .expect_err("final dispatch must observe local revocation"),
            NetworkError::AuthDenied
        );
    }

    #[test]
    fn native_remote_observation_separates_authoritative_policy_from_device_inventory() {
        let authoritative = sinomo_testdir::TestDir::new("sinomo-daemon-remote-resource-authority");
        let unrelated = sinomo_testdir::TestDir::new("sinomo-daemon-remote-resource-unrelated");
        let mut permissions = sinomo_core::node::LocalNodePermissions::new();
        permissions
            .expose_resource_operation("camera.front", "observe")
            .expect("expose authoritative camera");
        sinomo_core::node::LocalNodePermissionStore::new(&authoritative)
            .save(&permissions)
            .expect("save authoritative exposures");
        let mut unrelated_permissions = sinomo_core::node::LocalNodePermissions::new();
        unrelated_permissions
            .expose_resource_operation("microphone.unrelated", "capture")
            .expect("expose unrelated microphone");
        sinomo_core::node::LocalNodePermissionStore::new(&unrelated)
            .save(&unrelated_permissions)
            .expect("save unrelated exposures");

        let caller_policy = sinomo_remote_management::RemoteManagementResourceExposurePolicy {
            resource_id: "caller.spoofed".to_string(),
            allowed_operations: vec!["everything".to_string()],
        };
        let payload = sinomo_remote_management::RemoteManagementSystemObserveRequest::new()
            .including_devices(true)
            .including_resource_exposures(true)
            .with_host_authorized_resource_exposures([caller_policy]);
        let mut request = ObjectRpcRequest::encode_body(
            sinomo_remote_management::REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
            sinomo_remote_management::REMOTE_MANAGEMENT_RPC_CATEGORY,
            &payload,
        )
        .expect("observation request");
        let target = DaemonNativeDylibRouteTarget {
            module_id: sinomo_module::ModuleId::from_legacy_named("remote.management")
                .expect("module id"),
            operation: sinomo_remote_management::REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION
                .to_string(),
        };

        inject_native_dylib_host_authority(&authoritative, &target, &mut request)
            .expect("inject host authority");
        let injected: sinomo_remote_management::RemoteManagementSystemObserveRequest =
            request.decode_body().expect("decode injected request");
        assert_eq!(injected.host_authorized_resource_exposures.len(), 1);
        assert_eq!(
            injected.host_authorized_resource_exposures[0].resource_id,
            "camera.front"
        );
        assert_eq!(
            injected.host_authorized_resource_exposures[0].allowed_operations,
            vec!["observe".to_string()]
        );
    }

    #[test]
    fn cluster_kv_snapshot_service_dispatches_async_snapshot_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-cluster-kv-snapshot-service");
        let data_dir = Arc::new(SinomoDataDir::new(&root));
        let paths = DaemonPaths::new(data_dir.as_ref());
        let cluster_paths = ClusterPaths::new(data_dir.as_ref());
        cluster_paths.ensure().expect("cluster paths");
        let node_id =
            sinomo_core::node::NodeId::from_legacy_named("node_service_snapshot").expect("node id");
        let cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_service_snapshot")
                .expect("cluster id"),
            "service snapshot test",
            node_id,
            "service snapshot node",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        let mut store = InProcessKvStore::new();
        store
            .put("/service/snapshot", b"async".to_vec(), None)
            .expect("put snapshot entry");
        sinomo_core::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &store.snapshot().expect("snapshot"),
        )
        .expect("write cluster kv snapshot");

        let registry = ServiceRegistry::builder()
            .register(ClusterKvService {
                paths: paths.clone(),
                runtime: None,
            })
            .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service host");
        let mut router = host.route_policy_layer().layer(host.active_router());

        let response = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request = ObjectRpcRequest::encode_body(
                        CLUSTER_KV_SNAPSHOT_ROUTE,
                        sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
                        &(),
                    )
                    .expect("request");
                    annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                    request
                },
            ))
            .expect("response");

        let snapshot = response.decode_body::<KvSnapshot>().expect("snapshot");
        let entry = snapshot.get("/service/snapshot").expect("snapshot entry");
        assert_eq!(entry.value, b"async".to_vec());
        assert_eq!(
            response
                .metadata
                .get(DAEMON_SERVICE_DISPOSITION_METADATA_KEY)
                .map(String::as_str),
            Some("continue")
        );
    }

    #[test]
    fn cluster_control_service_initializes_and_renames_cluster() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-cluster-control-service");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        let paths = DaemonPaths::new(&data_dir);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        let registry = ServiceRegistry::builder()
            .register(ClusterControlService {
                paths: paths.clone(),
                runtime: Some(runtime.handle().clone()),
            })
            .build();
        let init_route = registry
            .route_categories()
            .into_iter()
            .find(|route| route.route == sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE)
            .expect("cluster control init route metadata");
        assert_eq!(
            init_route.category,
            sinomo_core::cluster::CLUSTER_CONTROL_RPC_CATEGORY
        );
        assert_eq!(
            init_route.auth_scope.as_deref(),
            Some("local-or-cluster-voter")
        );
        assert_eq!(
            init_route.required_capabilities,
            vec![sinomo_core::cluster::CLUSTER_CONTROL_ADMIN_CAPABILITY.to_string()]
        );

        let host = DaemonServiceRegistryHost::from_registry_for_paths_on_runtime(
            registry,
            &paths,
            runtime.handle().clone(),
        )
        .expect("service host");
        let mut router = host.route_policy_layer().layer(host.active_router());

        let init_payload = sinomo_core::cluster::cluster_control_init_rpc_payload(
            &sinomo_core::cluster::ClusterControlInitRequest {
                cluster_name: "cluster-control-service-test".to_string(),
                node_display_name: Some("cluster-control-node".to_string()),
            },
        )
        .expect("init payload");
        let response = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request = ObjectRpcRequest::from_encoded_body(
                        init_payload.route,
                        init_payload.category,
                        init_payload.body,
                    );
                    annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                    request
                },
            ))
            .expect("init response");
        let init =
            sinomo_core::cluster::decode_cluster_control_init_rpc_response_body(&response.body)
                .expect("init response body");
        assert_eq!(
            response
                .metadata
                .get(DAEMON_SERVICE_DISPOSITION_METADATA_KEY)
                .map(String::as_str),
            Some("stop"),
            "identity-changing cluster control must drain the boot-time daemon runtime"
        );

        let cluster_paths = ClusterPaths::new(&data_dir);
        assert!(cluster_paths.state.exists());
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("initialized cluster state");
        assert_eq!(state.cluster_id, init.cluster_id);
        assert_eq!(state.local_node_id, init.node_id);
        assert_eq!(state.cluster_name, "cluster-control-service-test");
        assert_eq!(state.local_node_display_name, "cluster-control-node");

        let rename_payload = sinomo_core::cluster::cluster_control_rename_rpc_payload(
            &sinomo_core::cluster::ClusterControlRenameRequest {
                node_id: None,
                display_name: "cluster-control-node-renamed".to_string(),
            },
        )
        .expect("rename payload");
        let response = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request = ObjectRpcRequest::from_encoded_body(
                        rename_payload.route,
                        rename_payload.category,
                        rename_payload.body,
                    );
                    annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                    request
                },
            ))
            .expect("rename response");
        let rename =
            sinomo_core::cluster::decode_cluster_control_rename_rpc_response_body(&response.body)
                .expect("rename response body");
        assert_eq!(
            response
                .metadata
                .get(DAEMON_SERVICE_DISPOSITION_METADATA_KEY)
                .map(String::as_str),
            Some("continue")
        );
        assert_eq!(rename.node_id, init.node_id);
        assert!(rename.changed);
        assert_eq!(rename.name, "cluster-control-node-renamed");
        assert_eq!(rename.previous_name, "cluster-control-node");

        let renamed_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("renamed cluster state");
        let renamed_member = renamed_state
            .members
            .iter()
            .find(|member| member.node_id == init.node_id)
            .expect("renamed cluster member");
        assert_eq!(renamed_member.display_name, "cluster-control-node-renamed");
    }

    #[test]
    fn cluster_identity_changes_stop_the_boot_time_daemon_runtime() {
        for route in [
            sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_LEAVE_ROUTE,
        ] {
            assert_eq!(
                cluster_control_service_disposition(route),
                ServiceDisposition::Stop,
                "{route} replaces the cluster identity captured at daemon startup"
            );
        }
        for route in [
            sinomo_core::cluster::CLUSTER_CONTROL_INVITE_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_RENAME_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_ROLE_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_REMOVE_ROUTE,
        ] {
            assert_eq!(
                cluster_control_service_disposition(route),
                ServiceDisposition::Continue,
                "{route} does not replace the local daemon cluster identity"
            );
        }
    }

    #[test]
    fn completed_cluster_identity_response_marks_clean_exit_for_supervisor_restart() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-cluster-restart-marker");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let marker = root.join("rollout").join("supervised-main-restart.request");

        mark_cluster_identity_restart_after_response(
            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
            false,
            Some(&paths),
        );
        assert!(
            !marker.exists(),
            "a response that did not stop must not mark a restart"
        );

        mark_cluster_identity_restart_after_response(
            sinomo_core::cluster::CLUSTER_CONTROL_RENAME_ROUTE,
            true,
            Some(&paths),
        );
        assert!(
            !marker.exists(),
            "a non-identity stop must not mark a restart"
        );

        mark_cluster_identity_restart_after_response(
            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
            true,
            Some(&paths),
        );
        assert_eq!(
            std::fs::read_to_string(marker).expect("supervisor restart marker"),
            format!("{}\n", std::process::id())
        );
    }

    #[test]
    fn cluster_control_service_denies_non_admin_signers() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-cluster-control-auth");
        let registry = ServiceRegistry::builder()
            .register(ClusterControlService {
                paths: DaemonPaths::new(&SinomoDataDir::new(&root)),
                runtime: None,
            })
            .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);
        let router = host.route_policy_layer().layer(host.active_router());
        let mut router = daemon_service_object_rpc_authorization_layer(
            DaemonSignerAuthority::Member,
            host.paths.clone(),
        )
        .layer(router);

        let payload = sinomo_core::cluster::cluster_control_leave_rpc_payload(
            &sinomo_core::cluster::ClusterControlLeaveRequest::default(),
        )
        .expect("leave payload");
        let error = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                {
                    let mut request = ObjectRpcRequest::from_encoded_body(
                        payload.route,
                        payload.category,
                        payload.body,
                    );
                    annotate_test_request(&mut request, DaemonSignerAuthority::Member);
                    request
                },
            ))
            .expect_err("member signer must not run cluster control mutations");
        assert_eq!(error, NetworkError::AuthDenied);
    }

    #[test]
    fn observe_service_registration_tracks_native_rust_enablement() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-observe-module-toggle");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);

        let registry = daemon_service_registry(&paths, None, &[]);
        assert!(registry.owns(sinomo_observe::OBSERVE_EVENTS_ROUTE));

        sinomo_observe::set_observe_module_enabled(&root, false, "test", "disable observe")
            .expect("disable observe");
        let registry = daemon_service_registry(&paths, None, &[]);
        assert!(!registry.owns(sinomo_observe::OBSERVE_EVENTS_ROUTE));

        sinomo_observe::set_observe_module_enabled(&root, true, "test", "enable observe")
            .expect("enable observe");
        let registry = daemon_service_registry(&paths, None, &[]);
        assert!(registry.owns(sinomo_observe::OBSERVE_EVENTS_ROUTE));
    }

    #[test]
    fn daemon_service_registry_accepts_application_service_hooks() {
        fn register_test_stop_service(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
            builder.register(TestStopService)
        }

        let root = sinomo_testdir::TestDir::new("sinomo-daemon-service-hooks");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);

        let hook = crate::DaemonServiceRegistryHook::new(register_test_stop_service);
        let registry = daemon_service_registry(&paths, None, &[hook]);
        assert!(registry.owns("/test/stop"));
        assert_eq!(registry.category_for_route("/test/stop"), Some("test-stop"));

        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service host");
        let descriptor = host
            .active_route_snapshot()
            .catalog()
            .routes
            .get("/test/stop")
            .expect("hooked service route")
            .clone();
        assert_eq!(descriptor.category, "test-stop");
        assert_eq!(
            descriptor
                .observability
                .get("daemon.service_registry")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn daemon_config_registry_accepts_application_config_hooks() {
        let hook = crate::DaemonServiceRegistryHook::new(|builder| builder)
            .with_config_definitions(|builder| {
                let definition = sinomo_config::ConfigDefinition::new(
                    "test.config",
                    "test",
                    sinomo_config::ConfigValueType::String {
                        choices: Vec::new(),
                    },
                    "test hook config",
                    sinomo_config::ConfigClusterMode::ClusterOverrideLocal,
                )
                .map_err(|error| error.to_string())?
                .with_default(&"hooked")
                .map_err(|error| error.to_string())?;
                builder
                    .register(definition)
                    .map_err(|error| error.to_string())
            });

        let root = sinomo_testdir::TestDir::new("sinomo-daemon-config-hook");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = daemon_config_registry(&paths, &[hook]);
        let definition = registry
            .get("test.config")
            .expect("hooked config definition");

        assert_eq!(definition.owner, "test");
        assert_eq!(definition.description, "test hook config");
    }

    #[test]
    fn daemon_config_registry_composes_settings_from_module_surfaces() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-module-config-surfaces");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = daemon_config_registry(&paths, &[]);

        let dfs = registry
            .get(sinomo_dfs::DFS_EXPOSE_CONFIG_KEY)
            .expect("DFS module settings");
        assert_eq!(
            dfs.module_id.as_deref(),
            Some(sinomo_dfs::dfs_module_id().as_str())
        );
        assert!(dfs.schema.is_some());

        let rollout = registry
            .get(sinomo_rollout::ROLLOUT_TELEMETRY_GATE_CONFIG_KEY)
            .expect("rollout module settings");
        assert_eq!(
            rollout.module_id.as_deref(),
            Some(sinomo_rollout::rollout_module_id().as_str())
        );
        assert!(rollout.schema.is_some());

        let sandbox = registry
            .get(sinomo_sandbox::SANDBOX_POLICY_CONFIG_KEY)
            .expect("sandbox settings");
        assert!(sandbox.schema.is_some());
    }

    #[test]
    fn static_config_catalog_wins_over_dynamic_module_key_collisions() {
        let static_definition = sinomo_config::ConfigDefinition::new(
            "test.config-collision",
            "application-hook",
            sinomo_config::ConfigValueType::String {
                choices: Vec::new(),
            },
            "application-owned config",
            sinomo_config::ConfigClusterMode::LocalOnly,
        )
        .expect("static definition");
        let builder = sinomo_config::ConfigRegistry::builder()
            .register(static_definition)
            .expect("static definition registration");

        let module_id = sinomo_module::ModuleId::from_legacy_named("module.dynamic-collision")
            .expect("module id");
        let dynamic_definition = sinomo_config::ConfigDefinition::new(
            "test.config-collision",
            "dynamic-module",
            sinomo_config::ConfigValueType::Object {
                schema_name: "DynamicCollisionConfig".to_string(),
            },
            "dynamic module config",
            sinomo_config::ConfigClusterMode::LocalOnly,
        )
        .expect("dynamic definition")
        .with_schema(sinomo_config::ConfigSchema::object(
            "DynamicCollisionConfig",
            "Dynamic module collision test settings.",
            vec![sinomo_config::ConfigSchemaField::new(
                "enabled",
                "Enabled",
                "Whether the test behavior is enabled.",
                sinomo_config::ConfigSchemaValue::bool(),
            )],
        ))
        .expect("dynamic schema")
        .with_module_id(module_id.to_string())
        .expect("dynamic ownership");
        let surfaces = sinomo_module::ModuleSurfaces::default()
            .with_config_definition(dynamic_definition)
            .expect("dynamic settings surface");

        let registry =
            register_module_surface_config_definitions(builder, &module_id, &surfaces).build();
        assert_eq!(
            registry
                .get("test.config-collision")
                .expect("static definition remains")
                .owner,
            "application-hook"
        );
    }

    #[test]
    fn daemon_service_registry_host_uses_observe_enablement_for_route_snapshot() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-observe-module-snapshot-toggle");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);

        let enabled_registry = daemon_service_registry(&paths, None, &[]);
        let enabled_host =
            DaemonServiceRegistryHost::from_registry_for_paths(enabled_registry, &paths)
                .expect("enabled service host");
        assert!(
            enabled_host
                .active_route_snapshot()
                .catalog()
                .routes
                .get(sinomo_observe::OBSERVE_EVENTS_ROUTE)
                .is_some_and(|descriptor| descriptor
                    .observability
                    .contains_key("daemon.module_route")),
            "enabled observe route should be installed as a module-owned daemon route"
        );

        sinomo_observe::set_observe_module_enabled(&root, false, "test", "disable observe")
            .expect("disable observe");
        let disabled_registry = daemon_service_registry(&paths, None, &[]);
        let disabled_host =
            DaemonServiceRegistryHost::from_registry_for_paths(disabled_registry, &paths)
                .expect("disabled service host");
        assert!(
            !disabled_host
                .active_route_snapshot()
                .catalog()
                .routes
                .contains_key(sinomo_observe::OBSERVE_EVENTS_ROUTE),
            "disabled observe service should not leave an object RPC route behind"
        );
    }

    #[test]
    fn daemon_service_registry_host_installs_module_snapshot_routes() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-module-snapshot-routes");
        let data_dir = Arc::new(SinomoDataDir::new(&root));
        let paths = DaemonPaths::new(data_dir.as_ref());
        let registry = daemon_service_registry(&paths, None, &[]);

        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service host");
        let catalog = host.active_route_snapshot().catalog().clone();
        let dfs_status = catalog
            .routes
            .get(sinomo_dfs::DFS_STATUS_ROUTE)
            .expect("dfs status module route");
        assert_eq!(dfs_status.category, sinomo_dfs::DFS_RPC_CATEGORY);
        assert_eq!(
            dfs_status.auth_scope.as_deref(),
            Some(sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope())
        );
        assert_eq!(
            dfs_status.required_capabilities,
            vec!["dfs.status.read".to_string()]
        );
        assert_eq!(
            dfs_status.observability.get("daemon.service_registry"),
            Some(&"true".to_string())
        );
        assert_eq!(
            dfs_status.observability.get("daemon.module_route"),
            Some(&"true".to_string())
        );
        assert_eq!(
            dfs_status.observability.get("module.operation"),
            Some(&"dfs.status.read".to_string())
        );
        assert!(
            dfs_status
                .observability
                .get("module.registry.snapshot_version")
                .is_some()
        );
        assert_eq!(
            dfs_status
                .observability
                .get("daemon.service_registry.snapshot_version"),
            Some(&"1".to_string())
        );

        let daemon_status = catalog
            .routes
            .get(DAEMON_STATUS_ROUTE)
            .expect("daemon status route");
        assert_eq!(
            daemon_status.observability.get("daemon.service_registry"),
            Some(&"true".to_string())
        );
        assert_eq!(
            daemon_status
                .observability
                .get("daemon.service_registry.snapshot_version"),
            Some(&"1".to_string())
        );
        assert!(
            !daemon_status
                .observability
                .contains_key("daemon.module_route")
        );
        let connectivity_reload = catalog
            .routes
            .get(DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE)
            .expect("local connectivity reload route");
        assert_eq!(
            connectivity_reload.auth_scope.as_deref(),
            Some("local-command")
        );
    }

    #[test]
    fn dfs_status_service_dispatch_allows_cluster_member_signer() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-status-member-dispatch");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = daemon_service_registry(&paths, None, &[]);
        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service host");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut authenticated = test_authenticated_command(DaemonSignerAuthority::Member);
        authenticated.route = sinomo_dfs::DFS_STATUS_ROUTE.to_string();
        let request = sinomo_dfs::dfs_status_rpc_request().expect("dfs status request");

        let (response, stop) = runtime
            .block_on(dispatch_rpc_via_registry_to_response_async(
                &host,
                &authenticated,
                request,
                None,
                None,
            ))
            .expect("dfs status dispatch");

        assert!(!stop);
        let status =
            sinomo_dfs::decode_dfs_status_rpc_response(&response).expect("dfs status response");
        assert_eq!(status.file_system_count, 0);
    }

    #[test]
    fn daemon_service_registry_host_installs_active_native_dylib_routes_from_descriptor() {
        let Ok(library_path) = std::env::var("SINOMO_DAEMON_TEST_NATIVE_DYLIB") else {
            return;
        };
        let library_path = resolve_test_dylib_path(std::path::PathBuf::from(library_path));
        let module = unsafe { sinomo_module::NativeModule::load_verified(&library_path) }
            .expect("load test native dylib");
        let descriptor = module.descriptor().clone();
        let route = descriptor
            .surfaces
            .object_rpc_routes
            .first()
            .expect("test native dylib route")
            .clone();
        drop(module);

        let root = sinomo_testdir::TestDir::new("sinomo-daemon-active-native-dylib-routes");
        let library_name = library_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("test dylib file name")
            .to_string();
        sinomo_module::install_native_dylib_module_from_file(
            &root,
            &library_path,
            sinomo_module::NativeDylibModuleInstallRequest {
                expected_module_id: None,
                artifact_id: None,
                expected_interface_version: None,
                cache_key: Some(format!("test/{}", descriptor.module_id)),
                library_name: Some(library_name),
                source: "test".to_string(),
            },
        )
        .expect("install test native dylib");

        let host = test_service_host(&root, ServiceRegistry::builder().build());
        let catalog = host.active_route_snapshot().catalog().clone();
        let route_descriptor = catalog
            .routes
            .get(&route.route)
            .expect("native dylib route descriptor");
        assert_eq!(route_descriptor.category, route.category);
        assert_eq!(
            route_descriptor
                .observability
                .get("daemon.native_dylib_module_route")
                .map(String::as_str),
            Some("true")
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut router = host.route_policy_layer().layer(host.active_router());
        let response = runtime
            .block_on(sinomo_core::network::call_object_rpc_service_async(
                &mut router,
                ObjectRpcRequest::encode_body(&route.route, &route.category, &()).expect("request"),
            ))
            .expect("native dylib route response");
        assert_eq!(response.route, route.route);
        assert!(!response.body.is_empty());
    }

    fn resolve_test_dylib_path(path: std::path::PathBuf) -> std::path::PathBuf {
        if path.is_absolute() {
            return path;
        }
        let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root")
            .to_path_buf();
        workspace_root.join(path)
    }

    #[test]
    fn cluster_kv_snapshot_response_rejects_oversized_body_as_rpc_error() {
        let mut store = InProcessKvStore::new();
        store
            .put("/snapshot/small", b"ok".to_vec(), None)
            .expect("small put");
        let snapshot = store.snapshot().expect("snapshot");

        let response =
            encode_cluster_kv_snapshot_response_limited(&snapshot, usize::MAX).expect("encode ok");
        let decoded: KvSnapshot = response.decode_body().expect("decode snapshot");
        assert_eq!(decoded.entries.len(), 1);

        let oversized = KvSnapshot {
            revision: KvRevision(1),
            compacted_revision: KvRevision::ZERO,
            entries: vec![KvEntry {
                key: "/snapshot/large".to_string(),
                value: vec![7_u8; 64],
                create_revision: KvRevision(1),
                modify_revision: KvRevision(1),
                version: KvVersion(1),
                lease_id: None,
            }],
            leases: Vec::new(),
            events: Vec::new(),
        };
        let response =
            encode_cluster_kv_snapshot_response_limited(&oversized, 1).expect("encode rpc error");
        let error = response
            .decode_body::<KvSnapshot>()
            .expect_err("oversized body");
        assert!(
            error
                .to_string()
                .contains("cluster kv snapshot response is"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn cluster_registry_hosts_cluster_routes() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-cluster-kv-registry-routes");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let registry = daemon_service_registry(&paths, Some(runtime.handle()), &[]);

        for route in CLUSTER_KV_RPC_ROUTES
            .iter()
            .copied()
            .chain(CLUSTER_RAFT_RPC_ROUTES.iter().copied())
            .chain(sinomo_dfs::DFS_DATA_PLANE_ROUTES.iter().copied())
            .chain([
                sinomo_core::network::CLUSTER_HEALTH_ROUTE,
                sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
                sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE,
                CLUSTER_RELAY_ROUTE,
                CLUSTER_JOIN_CHALLENGE_ROUTE,
                CLUSTER_JOIN_REGISTER_ROUTE,
                CLUSTER_INVITE_PUBLISH_ROUTE,
                CLUSTER_MEMBER_PUBLISH_ROUTE,
            ])
        {
            assert!(registry.owns(route), "registry should own {route}");
        }
        assert_eq!(
            registry.category_for_route(CLUSTER_KV_TXN_ROUTE),
            Some(sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY)
        );
        assert_eq!(
            registry.category_for_route(CLUSTER_RAFT_APPEND_ROUTE),
            Some(sinomo_core::kv::CLUSTER_RAFT_RPC_CATEGORY)
        );
        assert_eq!(
            registry.category_for_route(sinomo_core::network::CLUSTER_HEALTH_ROUTE),
            Some(sinomo_core::network::CLUSTER_HEALTH_RPC_CATEGORY)
        );
        assert_eq!(
            registry.category_for_route(sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE),
            Some(sinomo_core::network::CLUSTER_NETWORK_RPC_CATEGORY)
        );
        assert_eq!(
            registry.category_for_route(sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE),
            Some(sinomo_core::network::CLUSTER_NETWORK_RPC_CATEGORY)
        );
        assert_eq!(
            registry.category_for_route(CLUSTER_RELAY_ROUTE),
            Some(sinomo_core::network::CLUSTER_ROUTING_RPC_CATEGORY)
        );
        assert_eq!(
            registry.category_for_route(CLUSTER_JOIN_REGISTER_ROUTE),
            Some(CLUSTER_MEMBERSHIP_RPC_CATEGORY)
        );
        assert!(!registry.owns("/dfs/message"));
        assert_eq!(registry.category_for_route("/dfs/message"), None);
        assert_eq!(
            registry.category_for_route(sinomo_dfs::DFS_DATA_READ_ROUTE),
            Some(sinomo_dfs::DFS_RPC_CATEGORY)
        );
        assert_eq!(
            registry.category_for_route(sinomo_dfs::DFS_DIRECT_READ_ROUTE),
            Some(sinomo_dfs::DFS_RPC_CATEGORY)
        );
        assert!(!registry.owns("/dfs/protocol/frame"));
        assert_eq!(registry.category_for_route("/dfs/protocol/frame"), None);
        let route_metadata = registry.route_categories();
        let object_rpc_forward = route_metadata
            .iter()
            .find(|route| route.route == crate::DAEMON_OBJECT_RPC_FORWARD_ROUTE)
            .expect("daemon object RPC forward route metadata");
        assert_eq!(object_rpc_forward.category, crate::DAEMON_RPC_CATEGORY);
        assert_eq!(
            object_rpc_forward.auth_scope.as_deref(),
            Some("local-command")
        );
        assert_eq!(object_rpc_forward.request_body_mode, ServiceBodyMode::None);
        assert_eq!(object_rpc_forward.response_body_mode, ServiceBodyMode::None);
        let dfs_object_rpc_forward = route_metadata
            .iter()
            .find(|route| route.route == crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE)
            .expect("daemon DFS object RPC forward route metadata");
        assert_eq!(dfs_object_rpc_forward.category, crate::DAEMON_RPC_CATEGORY);
        assert_eq!(
            dfs_object_rpc_forward.auth_scope.as_deref(),
            Some("local-command")
        );
        assert_eq!(
            dfs_object_rpc_forward.request_body_mode,
            ServiceBodyMode::Optional
        );
        assert_eq!(
            dfs_object_rpc_forward.response_body_mode,
            ServiceBodyMode::Optional
        );
        let event_subscription = route_metadata
            .iter()
            .find(|route| route.route == crate::DAEMON_EVENT_SUBSCRIBE_ROUTE)
            .expect("daemon event subscription route metadata");
        assert_eq!(event_subscription.category, crate::DAEMON_RPC_CATEGORY);
        assert_eq!(
            event_subscription.auth_scope.as_deref(),
            Some("local-command")
        );
        assert_eq!(
            event_subscription.required_capabilities,
            vec!["daemon.events.subscribe".to_string()]
        );
        assert_eq!(
            event_subscription.response_body_mode,
            ServiceBodyMode::Required
        );
        let snapshot = route_metadata
            .iter()
            .find(|route| route.route == CLUSTER_KV_SNAPSHOT_ROUTE)
            .expect("cluster kv snapshot route metadata");
        assert_eq!(
            snapshot.auth_scope.as_deref(),
            Some(sinomo_core::kv::ClusterKvRouteAuth::Authenticated.as_scope())
        );
        assert_eq!(
            snapshot.required_capabilities,
            vec![sinomo_core::kv::CLUSTER_KV_READ_CAPABILITY.to_string()]
        );
        let txn = route_metadata
            .iter()
            .find(|route| route.route == CLUSTER_KV_TXN_ROUTE)
            .expect("cluster kv txn route metadata");
        assert_eq!(
            txn.auth_scope.as_deref(),
            Some(sinomo_core::kv::ClusterKvRouteAuth::LocalOrClusterMember.as_scope())
        );
        assert_eq!(
            txn.required_capabilities,
            vec![sinomo_core::kv::CLUSTER_KV_WRITE_CAPABILITY.to_string()]
        );
        let raft_append = route_metadata
            .iter()
            .find(|route| route.route == CLUSTER_RAFT_APPEND_ROUTE)
            .expect("cluster raft append route metadata");
        assert_eq!(
            raft_append.auth_scope.as_deref(),
            Some(sinomo_core::kv::ClusterKvRouteAuth::LocalOrRaftVoter.as_scope())
        );
        assert_eq!(
            raft_append.required_capabilities,
            vec![sinomo_core::kv::CLUSTER_RAFT_APPEND_CAPABILITY.to_string()]
        );
        let health = route_metadata
            .iter()
            .find(|route| route.route == sinomo_core::network::CLUSTER_HEALTH_ROUTE)
            .expect("cluster health route metadata");
        let network_module_routes = network_module_surfaces()
            .object_rpc_route_descriptors()
            .expect("network module surfaces")
            .into_iter()
            .map(|descriptor| (descriptor.route.clone(), descriptor))
            .collect::<std::collections::BTreeMap<_, _>>();
        let health_module_route = network_module_routes
            .get(sinomo_core::network::CLUSTER_HEALTH_ROUTE)
            .expect("cluster health module route");
        assert_eq!(health.category, health_module_route.category);
        assert_eq!(
            health.auth_scope.as_deref(),
            Some(health_module_route.auth_scope.as_str())
        );
        assert_eq!(
            health.required_capabilities,
            health_module_route
                .required_capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect::<Vec<_>>()
        );
        let ping = route_metadata
            .iter()
            .find(|route| route.route == sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE)
            .expect("cluster network ping route metadata");
        let ping_module_route = network_module_routes
            .get(sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE)
            .expect("cluster network ping module route");
        assert_eq!(ping.category, ping_module_route.category);
        assert_eq!(
            ping.auth_scope.as_deref(),
            Some(ping_module_route.auth_scope.as_str())
        );
        assert_eq!(
            ping.required_capabilities,
            ping_module_route
                .required_capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect::<Vec<_>>()
        );
        let dfs_data_read = route_metadata
            .iter()
            .find(|route| route.route == sinomo_dfs::DFS_DATA_READ_ROUTE)
            .expect("dfs data read route metadata");
        assert_eq!(
            dfs_data_read.auth_scope.as_deref(),
            Some(sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope())
        );
        assert_eq!(
            dfs_data_read.required_capabilities,
            vec!["dfs.object.transfer".to_string()]
        );
        let dfs_read = route_metadata
            .iter()
            .find(|route| route.route == sinomo_dfs::DFS_DATA_READ_ROUTE)
            .expect("dfs typed read route metadata");
        assert_eq!(
            dfs_read.auth_scope.as_deref(),
            Some(sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope())
        );
        assert_eq!(
            dfs_read.required_capabilities,
            vec!["dfs.object.transfer".to_string()]
        );
        let dfs_direct_read = route_metadata
            .iter()
            .find(|route| route.route == sinomo_dfs::DFS_DIRECT_READ_ROUTE)
            .expect("dfs direct read route metadata");
        assert_eq!(
            dfs_direct_read.auth_scope.as_deref(),
            Some(sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope())
        );
        assert_eq!(
            dfs_direct_read.required_capabilities,
            vec!["dfs.object.transfer".to_string()]
        );
        for route in crate::DAEMON_DFS_BROWSER_ROUTES {
            let dfs_browser = route_metadata
                .iter()
                .find(|metadata| metadata.route == *route)
                .unwrap_or_else(|| panic!("daemon DFS browser service route metadata for {route}"));
            assert_eq!(dfs_browser.category, DAEMON_RPC_CATEGORY);
            assert_eq!(
                dfs_browser.auth_scope.as_deref(),
                Some(sinomo_module::ModuleRouteAuth::LocalCommand.as_scope())
            );
            assert_eq!(
                dfs_browser.required_capabilities,
                vec!["daemon.dfs.browser".to_string()]
            );
        }
        let rollout_module_routes = rollout_module_surfaces()
            .object_rpc_route_descriptors()
            .expect("rollout module surfaces")
            .into_iter()
            .map(|descriptor| (descriptor.route.clone(), descriptor))
            .collect::<std::collections::BTreeMap<_, _>>();
        for route in [
            ROLLOUT_BINARY_ROUTE,
            ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
            ROLLOUT_BUILD_ROUTE,
        ] {
            let service_route = route_metadata
                .iter()
                .find(|metadata| metadata.route == route)
                .unwrap_or_else(|| panic!("rollout service route metadata for {route}"));
            let module_route = rollout_module_routes
                .get(route)
                .unwrap_or_else(|| panic!("rollout module route metadata for {route}"));
            assert_eq!(service_route.category, module_route.category);
            assert_eq!(
                service_route.auth_scope.as_deref(),
                Some(module_route.auth_scope.as_str())
            );
            assert_eq!(
                service_route.required_capabilities,
                module_route
                    .required_capabilities
                    .iter()
                    .map(|capability| capability.as_str().to_string())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                service_route.request_body_mode,
                match route {
                    ROLLOUT_BINARY_ROUTE => ServiceBodyMode::Required,
                    ROLLOUT_BUILD_ROUTE => ServiceBodyMode::Optional,
                    _ => ServiceBodyMode::None,
                }
            );
        }
        let build_module_routes = sinomo_build::build_module_surfaces()
            .object_rpc_route_descriptors()
            .expect("build module surfaces")
            .into_iter()
            .map(|descriptor| (descriptor.route.clone(), descriptor))
            .collect::<std::collections::BTreeMap<_, _>>();
        for route in sinomo_build::BUILD_OBJECT_RPC_ROUTES {
            let service_route = route_metadata
                .iter()
                .find(|metadata| metadata.route == *route)
                .unwrap_or_else(|| panic!("build service route metadata for {route}"));
            let module_route = build_module_routes
                .get(*route)
                .unwrap_or_else(|| panic!("build module route metadata for {route}"));
            assert_eq!(service_route.category, module_route.category);
            assert_eq!(
                service_route.auth_scope.as_deref(),
                Some(module_route.auth_scope.as_str())
            );
            assert_eq!(
                service_route.required_capabilities,
                module_route
                    .required_capabilities
                    .iter()
                    .map(|capability| capability.as_str().to_string())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                service_route.request_body_mode,
                if *route == sinomo_build::BUILD_SUBMIT_ROUTE {
                    ServiceBodyMode::Optional
                } else {
                    ServiceBodyMode::None
                }
            );
            assert_eq!(
                service_route.response_body_mode,
                if *route == sinomo_build::BUILD_ARTIFACT_ROUTE {
                    ServiceBodyMode::Required
                } else {
                    ServiceBodyMode::None
                }
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn daemon_dfs_forward_rebinds_stream_bodies_across_two_authenticated_hops() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-session-forward-bodies");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main identity");
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().expect("daemon paths");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");

        let target_listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("target listener");
        let target_address = target_listener
            .local_addr_string()
            .expect("target listener address");
        let local_node =
            sinomo_core::node::NodeId::from_legacy_named("node_daemon_dfs_session_forward_local")
                .expect("local node");
        let target_node =
            sinomo_core::node::NodeId::from_legacy_named("node_daemon_dfs_session_forward_target")
                .expect("target node");
        let target_data_dir = SinomoDataDir::new(root.join("target-node"));
        target_data_dir
            .ensure_root_layout()
            .expect("target data directory");
        let target_identity =
            sinomo_core::node::load_or_create_main_node_identity(&target_data_dir.root())
                .expect("target identity");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_session_forward")
                .expect("cluster id");
        let now = time::OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon DFS session forward",
            local_node.clone(),
            "local",
            identity.public_key,
            now,
        );
        let mut target_member = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id,
            target_node.clone(),
            "target",
            target_identity.public_key,
            now,
        );
        target_member
            .endpoints
            .push(sinomo_core::ClusterEndpointRecord {
                endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                    "endpoint_daemon_dfs_session_forward_target",
                )
                .expect("endpoint id"),
                node_id: target_node.clone(),
                transport: sinomo_core::ClusterTransportKind::LanTcp,
                address: target_address,
                source: sinomo_core::ClusterEndpointSource::Static,
                privacy: sinomo_core::ClusterEndpointPrivacy::Lan,
                priority: 0,
                last_observed_latency_ms: None,
            });
        state.members.push(target_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        // Case zero exercises bodies in both directions. Case one proves that
        // a head-only request still receives a streamed response.
        let request_bodies = [vec![0x31_u8; 2 * 1024 * 1024 + 113], Vec::new()];
        let response_bodies = [
            vec![0x72_u8; 3 * 1024 * 1024 + 211],
            vec![0x54_u8; 1024 * 1024 + 79],
        ];
        let target_expected_requests = request_bodies.clone();
        let target_responses = response_bodies.clone();
        let target_server = tokio::spawn(async move {
            for (index, (expected_request, response_body)) in target_expected_requests
                .into_iter()
                .zip(target_responses)
                .enumerate()
            {
                let stream = target_listener.accept().await.expect("target accept");
                let (mut incoming, responder) =
                    sinomo_core::network::accept_object_rpc_server_session(
                        Box::new(stream.into_inner()),
                        target_data_dir.clone(),
                        sinomo_core::network::NetworkCallPolicy::default(),
                        |_request, _node_id, _public_key| Ok(()),
                    )
                    .await
                    .expect("target session");
                assert_eq!(
                    incoming.request.route,
                    format!("/dfs/test/session-forward/{index}")
                );
                let mut actual_request = Vec::new();
                incoming
                    .body
                    .read_to_end(&mut actual_request)
                    .await
                    .expect("target request body");
                assert_eq!(actual_request, expected_request);
                responder
                    .respond(ObjectRpcSessionResponse {
                        response: ObjectRpcResponse::encode_body(
                            incoming.request.route,
                            &format!("response-{index}"),
                        )
                        .expect("target response head"),
                        body: ObjectRpcBodySource::bytes(response_body),
                    })
                    .await
                    .expect("target response");
            }
        });

        let outer_listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("outer listener");
        let outer_address = outer_listener
            .local_addr_string()
            .expect("outer listener address");
        let outer_paths = paths.clone();
        let outer_data_dir = data_dir.clone();
        let outer_runtime = tokio::runtime::Handle::current();
        let outer_server = tokio::spawn(async move {
            for _ in 0..2 {
                let stream = outer_listener.accept().await.expect("outer accept");
                let (incoming, responder) = sinomo_core::network::accept_object_rpc_server_session(
                    Box::new(stream.into_inner()),
                    outer_data_dir.clone(),
                    sinomo_core::network::NetworkCallPolicy::default(),
                    |_request, _node_id, _public_key| Ok(()),
                )
                .await
                .expect("outer session");
                assert_eq!(
                    incoming.request.route,
                    crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
                );
                let payload: crate::DaemonObjectRpcForwardRequest =
                    incoming.request.decode_body().expect("forward payload");
                let (response, body) = forward_object_rpc_session_from_daemon_control(
                    &outer_paths,
                    payload,
                    incoming.body,
                    &outer_runtime,
                )
                .await
                .expect("forward session");
                responder
                    .respond(ObjectRpcSessionResponse {
                        response: ObjectRpcResponse::encode_body(
                            crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
                            &response,
                        )
                        .expect("forward response head"),
                        body,
                    })
                    .await
                    .expect("forward response");
            }
        });

        let runtime = tokio::runtime::Handle::current();
        let mut host =
            crate::client::DaemonSignedObjectRpcAsyncHost::new(&runtime, false).with_paths(&paths);
        let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
        for (index, (request_body, expected_response)) in
            request_bodies.into_iter().zip(response_bodies).enumerate()
        {
            let inner_request = ObjectRpcRequest::encode_body(
                format!("/dfs/test/session-forward/{index}"),
                sinomo_dfs::DFS_RPC_CATEGORY,
                &format!("request-{index}"),
            )
            .expect("inner request head");
            let payload = crate::DaemonObjectRpcForwardRequest {
                target_node_id: target_node.clone(),
                request: inner_request,
                connect_timeout_millis: 5_000,
                overall_timeout_millis: None,
                observe_connectivity: false,
            };
            let outer_head = ObjectRpcRequest::encode_body(
                crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
                crate::DAEMON_RPC_CATEGORY,
                &payload,
            )
            .expect("outer request head");
            let request_body = if request_body.is_empty() {
                ObjectRpcBodySource::empty()
            } else {
                ObjectRpcBodySource::bytes(request_body)
            };
            let result = client
                .call_loopback_session(
                    &data_dir,
                    &outer_address,
                    ObjectRpcSessionCall::new(outer_head, request_body),
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await
                .expect("outer forwarded session");
            let envelope: crate::DaemonObjectRpcForwardResponse = result
                .response
                .decode_body()
                .expect("forward response envelope");
            let crate::DaemonObjectRpcForwardResponse::Success { response } = envelope else {
                panic!("forward returned a network error: {envelope:?}");
            };
            assert_eq!(
                response.decode_body::<String>().expect("inner response"),
                format!("response-{index}")
            );
            assert_eq!(
                result
                    .body
                    .collect_to_vec(8 * 1024 * 1024)
                    .await
                    .expect("forwarded response body"),
                expected_response
            );
        }

        outer_server.await.expect("outer server");
        target_server.await.expect("target server");
    }

    #[test]
    fn only_main_registry_hosts_build_routes_while_shadow_keeps_rollout_routes() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-main-shadow-build-routes");
        let data_dir = SinomoDataDir::new(&root);
        let main_paths = DaemonPaths::new(&data_dir);
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let main_registry = daemon_service_registry(&main_paths, Some(runtime.handle()), &[]);
        let shadow_registry = daemon_service_registry(&shadow_paths, Some(runtime.handle()), &[]);

        for route in sinomo_build::BUILD_OBJECT_RPC_ROUTES {
            assert!(
                main_registry.owns(route),
                "main registry should own {route}"
            );
            assert!(
                !shadow_registry.owns(route),
                "shadow registry must not own {route}"
            );
        }
        for route in [
            sinomo_rollout::ROLLOUT_DIAGNOSTICS_ROUTE,
            sinomo_rollout::ROLLOUT_BINARY_ROUTE,
            sinomo_rollout::ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
            sinomo_rollout::ROLLOUT_BUILD_ROUTE,
        ] {
            assert!(
                shadow_registry.owns(route),
                "shadow registry should retain rollout recovery route {route}"
            );
        }

        let main_host =
            DaemonServiceRegistryHost::from_registry_for_paths(main_registry, &main_paths)
                .expect("main registry host");
        let shadow_host =
            DaemonServiceRegistryHost::from_registry_for_paths(shadow_registry, &shadow_paths)
                .expect("shadow registry host");
        for route in sinomo_build::BUILD_OBJECT_RPC_ROUTES {
            assert!(
                main_host.object_rpc_route_descriptor(route).is_some(),
                "main host should advertise {route}"
            );
            assert!(
                shadow_host.object_rpc_route_descriptor(route).is_none(),
                "shadow host must not advertise {route}"
            );
        }
    }
}
