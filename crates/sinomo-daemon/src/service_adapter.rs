//! Bridges the daemon's control loop to the async, host-agnostic service seam
//! (`sinomo-service`).
//!
//! Routes owned by the [`ServiceRegistry`] are served here; everything else falls
//! through to the daemon's own dispatch. Sandbox live output is produced by the
//! daemon and carried as the standard stream-native object-RPC body;
//! the synchronous command runners remain isolated behind a bounded producer.
//!
//! The registry composes host-agnostic services from their owner crates with
//! narrow daemon host adapters. Those adapters hold process state (including
//! [`DaemonPaths`], which encode main versus shadow ownership) and bridge owner
//! extension points to cluster/control effects. Consuming services receive a
//! real [`DaemonClusterAccess`] via [`ServiceContext::cluster`] when clustered.

use sinomo_core::cluster::{ClusterKvPathsExt, RemoteClusterControlAuthorityFence};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
#[cfg(test)]
use tokio::io::AsyncReadExt as _;

#[cfg(test)]
use crate::DAEMON_STATUS_ROUTE;
use crate::auth::{AuthenticatedDaemonCommand, DaemonSignerAuthority};
use crate::cluster::{
    DaemonClusterJoinRegistrationHost, DaemonClusterMemberMutationHost, DaemonClusterStatusProbe,
    cluster_paths_from_daemon_paths,
    forward_cluster_invite_challenge_to_cluster_state_authority_async_on_runtime,
    handle_daemon_cluster_control_init_on_runtime,
    handle_daemon_cluster_control_invite_with_authority_on_runtime,
    handle_daemon_cluster_control_join_on_runtime, handle_daemon_cluster_control_leave_on_runtime,
    handle_daemon_cluster_control_remove_with_authority_on_runtime,
    handle_daemon_cluster_control_rename_with_authority_on_runtime,
    handle_daemon_cluster_control_role_with_authority_on_runtime,
    handle_daemon_cluster_kv_authoritative_entries_request_async_on_runtime,
    handle_daemon_cluster_kv_authoritative_entry_request_async_on_runtime,
    handle_daemon_cluster_kv_authoritative_range_prefix_request_async_on_runtime,
    handle_daemon_cluster_kv_authoritative_snapshot_request_async_on_runtime,
    handle_daemon_cluster_kv_request_outcome_request_async_on_runtime,
    handle_daemon_cluster_kv_txn_rpc_request_compact_async_on_runtime,
};
use crate::control::{daemon_data_dir_path, daemon_status_response_async};
use crate::error::DaemonError;
use crate::host_api::DaemonEventHub;
#[cfg(test)]
use crate::host_api::enforce_daemon_object_rpc_forward_route_affinity;
use crate::paths::DaemonPaths;
#[cfg(test)]
use crate::registry::network_module_surfaces;
#[cfg(test)]
use crate::{
    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE, DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE, DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE, DAEMON_OBJECT_RPC_FORWARD_ROUTE, DAEMON_RPC_CATEGORY,
};
use crate::{DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE, DAEMON_SHUTDOWN_ROUTE};
#[cfg(test)]
use sinomo_config::CONFIG_GET_ROUTE;
use sinomo_config::{
    ConfigRegistry, ConfigRegistryBuilder, register_services as register_config_services,
};
#[cfg(test)]
use sinomo_core::cluster::{
    fence_remote_cluster_kv_txn_request_with_entries, remote_cluster_kv_authorization_keys,
    validate_remote_cluster_control_authority,
};
use sinomo_core::kv::KvEntry;
#[cfg(test)]
use sinomo_core::kv::{
    CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE, CLUSTER_KV_RPC_ROUTES, CLUSTER_KV_TXN_ROUTE,
    CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_RPC_ROUTES, ClusterKvWatchCancelRequest,
    ClusterKvWatchDrainRequest,
};
#[cfg(test)]
use sinomo_core::kv::{KvOp, KvTxn};
use sinomo_core::local::SinomoDataDir;
#[cfg(test)]
use sinomo_core::network::ObjectRpcCancelRequest;
#[cfg(test)]
use sinomo_core::network::{
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE, CLUSTER_MEMBER_PUBLISH_ROUTE,
    CLUSTER_NETWORK_CONNECTIVITY_ROUTE, CLUSTER_NETWORK_PING_ROUTE, ObjectRpcCancelResponse,
};
use sinomo_core::network::{
    CLUSTER_JOIN_CHALLENGE_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY, ClusterPaths,
    DaemonControlRequest, NetworkError, ObjectRpcBodySource, ObjectRpcCall,
    ObjectRpcIncomingRequest, ObjectRpcOutgoingResponse, ObjectRpcRequest, ObjectRpcResponse,
    ObjectRpcResult,
};
use sinomo_core::service::{
    ClusterAccess, ResponseSink, ServiceContext, ServiceDisposition, ServiceError, ServiceRegistry,
    ServiceRegistrySnapshotStore, ServiceRoute, ServiceSigner,
};
#[cfg(test)]
use sinomo_core::service::{Service, ServiceBodyMode, ServiceFuture, ServiceRegistryBuilder};
#[cfg(test)]
use sinomo_module::inspect_native_module_staged_body as inspect_native_dylib_staged_body;
use sinomo_module::{
    ComponentActivityObserver, ComponentCatalog, ComponentContext, ComponentFactory,
    ComponentHostRole, ComponentObjectRpcTransport, ComponentReconciler,
    module_service_snapshot as daemon_internal_module_snapshot,
};
#[cfg(test)]
use sinomo_module::{
    MODULE_NATIVE_DYLIB_CANCEL_ROUTE, MODULE_RPC_CATEGORY,
    NativeModuleOperationCancellationRegistry,
    NativeModuleOperationCancellationToken as DaemonOperationCancellationToken,
    NativeModuleSessionStaging as NativeDylibSessionStaging,
    stage_native_module_request_body as stage_native_dylib_request_body,
    validate_native_module_request_body_mode as validate_native_dylib_request_body_mode,
    validate_native_module_response_body as validate_native_dylib_response_body,
    validate_native_module_staged_request_body as validate_native_dylib_staged_request_body,
};
#[cfg(test)]
use sinomo_rollout::{
    ROLLOUT_BINARY_ROUTE, ROLLOUT_BUILD_ROUTE, ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
    rollout_module_surfaces,
};

static DAEMON_CLUSTER_NETWORK_PING_NODE_IDS: OnceLock<
    Mutex<HashMap<PathBuf, sinomo_core::node::NodeId>>,
> = OnceLock::new();

struct DaemonComponentActivityObserver {
    idle_stats: Arc<crate::IdleStatsAtomics>,
}

impl ComponentActivityObserver for DaemonComponentActivityObserver {
    fn increment(&self, counter: &'static str, amount: u64) {
        match counter {
            sinomo_module::COMPONENT_ACTIVITY_DISK_READ_DFS_METADATA => {
                for _ in 0..amount {
                    self.idle_stats.incr_disk_reads_dfs_metadata();
                }
            }
            sinomo_module::COMPONENT_ACTIVITY_DFS_METADATA_REPLICATION_SENT => {
                for _ in 0..amount {
                    self.idle_stats.incr_dfs_metadata_replications_sent();
                }
            }
            _ => tracing::trace!(counter, amount, "unmapped component activity counter"),
        }
    }
}

fn daemon_error(error: DaemonError) -> ServiceError {
    match error {
        // Preserve control-plane error identity across the service registry and
        // Object RPC response boundary. Cluster forwarding relies on an
        // authenticated non-leader rejection remaining `ClusterReadOnly` so it
        // can clear a stale durable leader hint and campaign. Flattening this
        // into `ServiceError::Service` turns it into a generic parse error and
        // can leave every voter permanently suppressing elections after the
        // recorded leader restarts.
        DaemonError::ClusterReadOnly { detail } => {
            ServiceError::Network(NetworkError::ClusterReadOnly { detail })
        }
        DaemonError::AuthDenied => ServiceError::Network(NetworkError::AuthDenied),
        DaemonError::AuthExpired => ServiceError::Network(NetworkError::AuthExpired),
        DaemonError::ClusterOrphan => ServiceError::Network(NetworkError::ClusterOrphan),
        other => ServiceError::Service(other.to_string()),
    }
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

pub(crate) async fn forward_object_rpc_call_from_daemon_control(
    paths: &crate::DaemonPaths,
    payload: crate::DaemonObjectRpcForwardRequest,
    request_body: sinomo_core::network::ObjectRpcIncomingBody,
    runtime: &tokio::runtime::Handle,
) -> Result<(crate::DaemonObjectRpcForwardResponse, ObjectRpcBodySource), ServiceError> {
    let options = crate::host_api::daemon_object_rpc_forward_options(&payload);
    let forward_transport_timeouts =
        crate::host_api::daemon_object_rpc_forward_transport_timeouts(&payload);

    // The proxy transfers the body allocation directly into the next
    // transport. Reliable transports preserve ordering and length, so this
    // boundary does not hash or sign the body again.
    let call = if payload.streamed_value_body {
        ObjectRpcCall::from_forwarded_streamed_value(payload.request, request_body)?
    } else {
        ObjectRpcCall::from_forwarded_body(payload.request, request_body)
    };
    let forward_started = std::time::Instant::now();
    let target_is_local_main = if payload.destination_process_affinity
        == crate::DaemonObjectRpcDestinationProcessAffinity::MainDaemon
    {
        let data_dir = SinomoDataDir::new(
            daemon_data_dir_path(paths)
                .map_err(daemon_error_to_network_error)
                .map_err(ServiceError::Network)?,
        );
        let cluster_paths = ClusterPaths::new(&data_dir);
        payload.target_node_id
            == sinomo_core::cluster::read_cluster_state_cached_shared(&cluster_paths)
                .map_err(ServiceError::Network)?
                .local_node_id
    } else {
        false
    };
    let result = if target_is_local_main {
        forward_object_rpc_call_to_local_main_daemon(
            paths,
            &payload.target_node_id,
            forward_transport_timeouts,
            call,
            options,
            runtime,
        )
        .await
    } else {
        crate::client::send_object_rpc_call_to_target_async_with_daemon_paths_and_network_error(
            paths,
            &payload.target_node_id,
            call,
            options,
            runtime,
            payload.observe_connectivity,
        )
        .await
    };
    match result {
        Ok(result) => {
            let streamed_value_body = result.has_streamed_value();
            match result.into_forwarded_response() {
                Ok(ObjectRpcOutgoingResponse { response, body }) => Ok((
                    crate::DaemonObjectRpcForwardResponse::Success {
                        response,
                        streamed_value_body,
                        forward_elapsed_micros: forward_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX))
                            as u64,
                    },
                    body,
                )),
                Err(error) => Ok((
                    daemon_object_rpc_forward_network_error(error),
                    ObjectRpcBodySource::empty(),
                )),
            }
        }
        Err(error) => Ok((
            daemon_object_rpc_forward_network_error(error),
            ObjectRpcBodySource::empty(),
        )),
    }
}

async fn forward_object_rpc_call_to_local_main_daemon(
    paths: &crate::DaemonPaths,
    target_node_id: &sinomo_core::node::NodeId,
    transport_timeouts: sinomo_core::network::DaemonRpcTimeouts,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, NetworkError> {
    let data_dir_path = daemon_data_dir_path(paths).map_err(daemon_error_to_network_error)?;
    let main_paths = crate::DaemonPaths::new(&SinomoDataDir::new(data_dir_path));
    let main_status = match sinomo_api::probe_daemon_status_with_timeouts_async(
        &main_paths,
        transport_timeouts,
        runtime,
    )
    .await
    {
        crate::DaemonProbeStatus::Running(status) => status,
        crate::DaemonProbeStatus::Locked { .. } => {
            return Err(NetworkError::delivery_failed(
                "resolve local main daemon for process-affine object RPC",
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                "the main daemon owns its lock but did not answer the status probe",
            ));
        }
        crate::DaemonProbeStatus::NotRunning => {
            return Err(NetworkError::delivery_failed(
                "resolve local main daemon for process-affine object RPC",
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                "the main daemon is not running",
            ));
        }
    };
    let Some(cluster) = main_status.cluster.as_ref() else {
        return Err(NetworkError::delivery_failed(
            "resolve local main daemon for process-affine object RPC",
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            "the main daemon status is not clustered",
        ));
    };
    if &cluster.node_id != target_node_id
        || cluster.process != sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE
    {
        return Err(NetworkError::delivery_failed(
            "resolve local main daemon for process-affine object RPC",
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            format!(
                "the main daemon status reported node {} process {}, expected node {} process {}",
                cluster.node_id,
                cluster.process,
                target_node_id,
                sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE,
            ),
        ));
    }
    crate::client::send_object_rpc_call_to_local_daemon_address_with_daemon_paths_and_network_error(
        paths,
        &main_status.address,
        call,
        options,
        runtime,
    )
    .await
}

/// Real cluster-KV access for services that consume it. Callers choose either an
/// explicitly stale-capable local read or an authoritative read that establishes
/// a quorum barrier. Commits use the daemon's quorum-replicated transaction path,
/// forwarding to the leader when needed.
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

    fn runtime(&self) -> Result<&tokio::runtime::Handle, ServiceError> {
        require_service_runtime(self.runtime.as_ref(), "daemon cluster access")
    }
}

impl ClusterAccess for DaemonClusterAccess {
    fn kv_get_local(
        &self,
        key: &str,
    ) -> Result<sinomo_core::kv::KvLocalRead<Option<KvEntry>>, ServiceError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)?;
        sinomo_core::kv::read_cluster_kv_local_entry(
            &self.cluster_paths.kv_paths(),
            &state.local_node_id,
            key,
        )
        .map_err(ServiceError::from)
    }

    fn kv_range_prefix_local(
        &self,
        prefix: &str,
    ) -> Result<sinomo_core::kv::KvLocalRead<Vec<KvEntry>>, ServiceError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)?;
        sinomo_core::kv::read_cluster_kv_local_prefix(
            &self.cluster_paths.kv_paths(),
            &state.local_node_id,
            prefix,
        )
        .map_err(ServiceError::from)
    }

    fn kv_get_authoritative_async<'a>(
        &'a self,
        key: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Option<KvEntry>>,
                        ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            handle_daemon_cluster_kv_authoritative_entry_request_async_on_runtime(
                &self.paths,
                key.to_string(),
                true,
                runtime,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn kv_get_authoritative_exact_set_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>,
                        ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            handle_daemon_cluster_kv_authoritative_entries_request_async_on_runtime(
                &self.paths,
                keys,
                true,
                runtime,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn kv_range_prefix_authoritative_async<'a>(
        &'a self,
        prefix: String,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
                        ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            crate::cluster::read_cluster_kv_authoritative_range_prefix_with_cluster_paths_async_on_runtime(
                &self.cluster_paths,
                prefix,
                true,
                runtime,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn kv_range_prefix_complete_authoritative_async<'a>(
        &'a self,
        prefix: String,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
                        ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            crate::cluster::handle_daemon_cluster_kv_authoritative_complete_range_prefix_request_async_on_runtime(
                &self.paths, prefix, true, self.runtime()?,
            ).await.map_err(daemon_error)
        })
    }

    fn kv_request_outcome_authoritative_async<'a>(
        &'a self,
        request_id: sinomo_core::kv::KvRequestId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<sinomo_core::kv::ClusterKvRequestOutcomeRead, ServiceError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            let payload = sinomo_core::kv::cluster_kv_request_outcome_rpc_payload(
                &sinomo_core::kv::ClusterKvRequestOutcomeRequest { request_id },
            )?;
            crate::cluster::handle_daemon_cluster_kv_request_outcome_request_async_on_runtime(
                &self.paths,
                &payload.body,
                true,
                runtime,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn kv_commit_async<'a>(
        &'a self,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<sinomo_core::kv::ClusterKvTxnCommitResult, ServiceError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            crate::cluster::handle_daemon_cluster_kv_request_async_on_runtime_with_timeouts(
                &self.paths,
                request,
                runtime,
                self.control_plane_timeouts,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn kv_commit_compact_async<'a>(
        &'a self,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<sinomo_core::kv::ClusterKvTxnCompactCommitResult, ServiceError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            crate::cluster::handle_daemon_cluster_kv_request_compact_async_on_runtime_with_timeouts(
                &self.paths,
                request,
                runtime,
                self.control_plane_timeouts,
            )
            .await
            .map_err(daemon_error)
        })
    }
}

#[derive(Clone)]
struct DaemonComponentObjectRpcTransport {
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
}

impl ComponentObjectRpcTransport for DaemonComponentObjectRpcTransport {
    fn availability(
        &self,
        target_node_id: &sinomo_core::node::NodeId,
    ) -> sinomo_core::network::RoutedAvailability {
        crate::client::object_rpc_target_availability_with_daemon_paths(
            &self.paths,
            target_node_id,
            &self.runtime,
        )
    }

    fn call(
        &self,
        target_node_id: &sinomo_core::node::NodeId,
        call: ObjectRpcCall,
        options: sinomo_core::network::ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, NetworkError> {
        crate::client::send_object_rpc_call_to_target_on_runtime_with_daemon_paths_and_network_error(
            &self.paths,
            target_node_id,
            call,
            options,
            &self.runtime,
            observe_connectivity,
        )
    }

    fn call_endpoint(
        &self,
        endpoint: &sinomo_core::ClusterEndpointRecord,
        call: ObjectRpcCall,
        options: sinomo_core::network::ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, NetworkError> {
        crate::client::send_object_rpc_call_to_exact_endpoint_on_runtime(
            &self.paths,
            endpoint,
            call,
            options,
            &self.runtime,
            observe_connectivity,
        )
    }

    fn call_async<'a>(
        &'a self,
        target_node_id: &'a sinomo_core::node::NodeId,
        call: ObjectRpcCall,
        options: sinomo_core::network::ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ObjectRpcResult, NetworkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            crate::client::send_object_rpc_call_to_target_async_with_daemon_paths_and_network_error(
                &self.paths,
                target_node_id,
                call,
                options,
                &self.runtime,
                observe_connectivity,
            )
            .await
        })
    }
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
    tokio::task::spawn_blocking(sinomo_core::service::observed_blocking_task(
        "daemon.service",
        task,
    ))
    .await
    .map_err(|source| {
        ServiceError::Service(format!("{label} route {route} task failed: {source}"))
    })?
}

#[cfg(test)]
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

/// Process-specific status and shutdown effects for the API-owned lifecycle
/// service adapter.
struct DaemonLifecycleServiceHost {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
    status_contributors: Vec<sinomo_module::ComponentStatusContributor>,
}

impl crate::host_api::DaemonLifecycleHost for DaemonLifecycleServiceHost {
    fn is_shadow(&self) -> bool {
        crate::daemon_paths_are_shadow(&self.paths)
    }

    fn status(
        &self,
    ) -> crate::host_api::DaemonLifecycleFuture<'_, Result<sinomo_api::DaemonStatus, String>> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let contributors = self.status_contributors.clone();
        Box::pin(async move {
            let mut status = daemon_status_response_async(&paths, runtime.as_ref())
                .await
                .map_err(|error| error.to_string())?;
            let data_dir = paths.root.parent().unwrap_or(paths.root.as_path());
            for contribute in contributors {
                match contribute(data_dir) {
                    Ok(Some(projection)) => {
                        if let Err(error) = status
                            .apply_component_status_projection(projection.key, projection.value)
                        {
                            tracing::warn!(%error, "component daemon-status projection was invalid");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%error, "component daemon-status contribution failed");
                    }
                }
            }
            Ok(status)
        })
    }

    fn shutdown_requested(&self, kind: crate::host_api::DaemonShutdownKind) {
        let (event_kind, message, route) = match kind {
            crate::host_api::DaemonShutdownKind::Local => (
                "daemon.lifecycle.shutdown_requested",
                "daemon shutdown requested",
                DAEMON_SHUTDOWN_ROUTE,
            ),
            crate::host_api::DaemonShutdownKind::RemoteShadow => (
                "daemon.lifecycle.remote_shadow_shutdown_requested",
                "remote shadow shutdown requested by an authenticated cluster voter",
                DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE,
            ),
        };
        crate::append_operational_event_best_effort(
            &self.paths,
            crate::OperationalSeverity::Info,
            "daemon-control",
            event_kind,
            message,
            None,
            serde_json::json!({ "route": route }),
        );
    }
}

/// Process effects behind the daemon operations service owned by `sinomo-api`.
struct DaemonOperationsServiceHost {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

fn publish_component_host_change(paths: &DaemonPaths, kind: &str, reason: &str) {
    let event = sinomo_module::ComponentEvent {
        publisher: sinomo_module::ComponentEventPublisher {
            component_id: "sinomo.component-host".to_string(),
            component_version: env!("CARGO_PKG_VERSION").to_string(),
            artifact_id: None,
            artifact_generation: 0,
        },
        schema_id: sinomo_module::COMPONENT_HOST_EVENT_SCHEMA_ID.to_string(),
        schema_version: sinomo_module::COMPONENT_HOST_EVENT_SCHEMA_VERSION.to_string(),
        envelope: sinomo_core::EventEnvelope::new(
            sinomo_core::Id::generate(),
            time::OffsetDateTime::now_utc(),
            sinomo_core::Id::derived(
                "sinomo.component-host.local",
                paths.root.display().to_string().as_bytes(),
            ),
            sinomo_core::Source::new("component/sinomo.component-host")
                .expect("static component-host source"),
            sinomo_core::EventKind::new(kind).expect("static component-host event kind"),
            sinomo_core::Salience::new(0.2).expect("static component-host salience"),
            sinomo_core::Confidence::new(1.0).expect("static component-host confidence"),
            sinomo_core::RoutingMetadata::new(
                sinomo_core::RoutingScope::Local,
                sinomo_core::LatencyClass::Reactive,
                sinomo_core::PrivacyClass::Private,
            ),
        )
        .with_payload(serde_json::json!({ "reason": reason })),
    };
    if let Err(error) = publish_component_event_for_paths(
        paths,
        event,
        sinomo_module::ComponentEventDurability::Transient,
    ) {
        tracing::warn!(%error, kind, "component host change publication failed");
    }
}

impl crate::host_api::DaemonOperationsHost for DaemonOperationsServiceHost {
    fn is_shadow(&self) -> bool {
        crate::daemon_paths_are_shadow(&self.paths)
    }

    fn forward_object_rpc(
        &self,
        payload: crate::DaemonObjectRpcForwardRequest,
        request_body: sinomo_core::network::ObjectRpcIncomingBody,
    ) -> crate::host_api::DaemonOperationsFuture<
        '_,
        Result<
            (
                crate::DaemonObjectRpcForwardResponse,
                sinomo_core::network::ObjectRpcBodySource,
            ),
            ServiceError,
        >,
    > {
        Box::pin(async move {
            let runtime =
                require_service_runtime(self.runtime.as_ref(), "daemon object RPC call forward")?;
            forward_object_rpc_call_from_daemon_control(&self.paths, payload, request_body, runtime)
                .await
        })
    }

    fn cluster_maintenance_refresh_requested(&self, traceparent: Option<String>) {
        crate::control::request_daemon_cluster_maintenance(
            crate::control::DaemonClusterMaintenanceReason::ManualBroad,
        );
        let _ = traceparent;
        publish_component_host_change(
            &self.paths,
            sinomo_module::COMPONENT_HOST_CLUSTER_CONTROL_CHANGED_EVENT_KIND,
            "manual cluster maintenance refresh",
        );
        tracing::info!("sinomo daemon cluster maintenance refresh requested by RPC");
    }

    fn reload_local_connectivity(&self) -> Result<(), ServiceError> {
        let cluster_paths = cluster_paths_from_daemon_paths(&self.paths).map_err(daemon_error)?;
        sinomo_core::network::invalidate_local_cluster_connectivity_cache(&cluster_paths)?;
        // A repaired route can unblock component-owned durable transfers even
        // when no new domain mutation occurs.
        publish_component_host_change(
            &self.paths,
            sinomo_module::COMPONENT_HOST_CONNECTIVITY_CHANGED_EVENT_KIND,
            "local connectivity cache reloaded",
        );
        Ok(())
    }
}

fn daemon_operations_service_host(
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
) -> Arc<dyn crate::host_api::DaemonOperationsHost> {
    Arc::new(DaemonOperationsServiceHost { paths, runtime })
}

/// Process-specific authority and commit effects for the cluster-owned KV service.
struct DaemonClusterKvServiceHost {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl DaemonClusterKvServiceHost {
    fn runtime(&self) -> Result<&tokio::runtime::Handle, ServiceError> {
        require_service_runtime(self.runtime.as_ref(), "cluster kv")
    }
}

impl sinomo_core::cluster::ClusterKvServiceHost for DaemonClusterKvServiceHost {
    fn authoritative_snapshot<'a>(
        &'a self,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<sinomo_core::kv::KvSnapshot>,
    > {
        Box::pin(async move {
            let payload = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
            handle_daemon_cluster_kv_authoritative_snapshot_request_async_on_runtime(
                &self.paths,
                &payload.body,
                allow_forwarding,
                self.runtime()?,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn authoritative_entry<'a>(
        &'a self,
        key: String,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Option<KvEntry>>,
    > {
        Box::pin(async move {
            handle_daemon_cluster_kv_authoritative_entry_request_async_on_runtime(
                &self.paths,
                key,
                allow_forwarding,
                self.runtime()?,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn authoritative_entries<'a>(
        &'a self,
        keys: Vec<String>,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>,
    > {
        Box::pin(async move {
            handle_daemon_cluster_kv_authoritative_entries_request_async_on_runtime(
                &self.paths,
                keys,
                allow_forwarding,
                self.runtime()?,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn authoritative_range_prefix<'a>(
        &'a self,
        prefix: String,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
    > {
        Box::pin(async move {
            handle_daemon_cluster_kv_authoritative_range_prefix_request_async_on_runtime(
                &self.paths,
                prefix,
                allow_forwarding,
                self.runtime()?,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn authoritative_complete_range_prefix<'a>(
        &'a self,
        prefix: String,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
    > {
        Box::pin(async move {
            crate::cluster::handle_daemon_cluster_kv_authoritative_complete_range_prefix_request_async_on_runtime(
                &self.paths, prefix, allow_forwarding, self.runtime()?,
            ).await.map_err(daemon_error)
        })
    }

    fn request_outcome<'a>(
        &'a self,
        request: sinomo_core::kv::ClusterKvRequestOutcomeRequest,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::ClusterKvRequestOutcomeRead,
    > {
        Box::pin(async move {
            let payload = sinomo_core::kv::cluster_kv_request_outcome_rpc_payload(&request)?;
            handle_daemon_cluster_kv_request_outcome_request_async_on_runtime(
                &self.paths,
                &payload.body,
                allow_forwarding,
                self.runtime()?,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn commit_transaction<'a>(
        &'a self,
        request: sinomo_core::kv::KvTxnRequest,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<'a, sinomo_core::kv::ClusterKvTxnRpcResponse>
    {
        Box::pin(async move {
            handle_daemon_cluster_kv_txn_rpc_request_compact_async_on_runtime(
                &self.paths,
                request,
                allow_forwarding,
                self.runtime()?,
            )
            .await
            .map_err(daemon_error)
        })
    }
}

fn register_daemon_cluster_kv_service(
    builder: sinomo_core::service::ServiceRegistryBuilder,
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> sinomo_core::service::ServiceRegistryBuilder {
    sinomo_core::cluster::register_cluster_kv_service(
        builder,
        Arc::new(DaemonClusterKvServiceHost {
            paths: paths.clone(),
            runtime: runtime.cloned(),
        }),
    )
}

/// Process-specific effects for the cluster-owned network service adapter.
struct DaemonClusterNetworkServiceHost {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
    include_authority_health: bool,
}

impl sinomo_core::cluster::ClusterNetworkServiceHost for DaemonClusterNetworkServiceHost {
    fn include_authority_health(&self) -> bool {
        self.include_authority_health
    }

    fn handle_ping<'a>(
        &'a self,
        data_dir: &'a SinomoDataDir,
        request: ObjectRpcIncomingRequest,
    ) -> sinomo_core::cluster::ClusterNetworkServiceFuture<
        'a,
        Result<ObjectRpcOutgoingResponse, ServiceError>,
    > {
        Box::pin(handle_daemon_cluster_network_ping_request(
            &self.paths,
            data_dir,
            request,
        ))
    }

    fn authority_health<'a>(
        &'a self,
        data_dir: &'a SinomoDataDir,
        request: ObjectRpcRequest,
    ) -> sinomo_core::cluster::ClusterNetworkServiceFuture<
        'a,
        Result<Vec<sinomo_core::cluster::ClusterHealthRow>, ServiceError>,
    > {
        Box::pin(async move {
            let runtime = require_service_runtime(self.runtime.as_ref(), "cluster health")?;
            let cluster_paths = ClusterPaths::new(data_dir);
            let probe = DaemonClusterStatusProbe::new(&cluster_paths, runtime);
            sinomo_core::cluster::current_cluster_health_rows_for_decoded_request_async(
                runtime,
                &cluster_paths,
                &probe,
                &request,
                time::OffsetDateTime::now_utc(),
            )
            .await
            .map_err(ServiceError::from)
        })
    }
}

fn register_daemon_cluster_network_service(
    builder: sinomo_core::service::ServiceRegistryBuilder,
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
    include_authority_health: bool,
) -> sinomo_core::service::ServiceRegistryBuilder {
    sinomo_core::cluster::register_cluster_network_service(
        builder,
        Arc::new(DaemonClusterNetworkServiceHost {
            paths: paths.clone(),
            runtime: runtime.cloned(),
            include_authority_health,
        }),
    )
}

/// Handles both value-only and semantic-attachment forms of the daemon network ping.
///
/// Keeping this at the service boundary makes the typed SOBJ head and its optional body
/// one operation regardless of whether control dispatch takes the core-route fast path or
/// enters through the generic service registry.
pub(crate) async fn handle_daemon_cluster_network_ping_request(
    paths: &DaemonPaths,
    data_dir: &SinomoDataDir,
    request: ObjectRpcIncomingRequest,
) -> Result<ObjectRpcOutgoingResponse, ServiceError> {
    let timeline =
        sinomo_core::network::network_timeline_trace_context_for_request(&request.request);
    handle_daemon_cluster_network_ping_request_with_timeline(paths, data_dir, request, timeline)
        .await
}

pub(crate) async fn handle_daemon_cluster_network_ping_request_with_timeline(
    paths: &DaemonPaths,
    data_dir: &SinomoDataDir,
    request: ObjectRpcIncomingRequest,
    timeline: Option<sinomo_core::network::NetworkTimelineTraceContext>,
) -> Result<ObjectRpcOutgoingResponse, ServiceError> {
    let responder_node_id = cached_cluster_network_ping_node_id(paths, data_dir)?;
    sinomo_core::cluster::handle_cluster_network_ping_service_request_for_node(
        &responder_node_id,
        request,
        timeline,
    )
    .await
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

struct DaemonClusterConsensusServiceHost;

impl sinomo_core::cluster::ClusterConsensusServiceHost for DaemonClusterConsensusServiceHost {
    fn raft_append_received(&self) {
        if let Some(stats) = crate::process_idle_stats() {
            stats.incr_raft_appends_recv();
        }
    }
}

fn register_daemon_cluster_consensus_service(
    builder: sinomo_core::service::ServiceRegistryBuilder,
    runtime: Option<&tokio::runtime::Handle>,
) -> sinomo_core::service::ServiceRegistryBuilder {
    sinomo_core::cluster::register_cluster_consensus_service(
        builder,
        runtime.cloned(),
        Arc::new(DaemonClusterConsensusServiceHost),
    )
}

/// Process-specific transports and maintenance effects for the cluster-owned
/// membership service.
struct DaemonClusterMembershipServiceHost {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl DaemonClusterMembershipServiceHost {
    fn runtime(&self) -> Result<&tokio::runtime::Handle, ServiceError> {
        require_service_runtime(self.runtime.as_ref(), "cluster membership")
    }
}

impl sinomo_core::cluster::ClusterMembershipServiceHost for DaemonClusterMembershipServiceHost {
    fn local_join_challenge<'a>(
        &'a self,
        request: sinomo_core::cluster::ClusterInviteChallengeRequest,
        cluster: Arc<dyn sinomo_core::service::ClusterAccess>,
    ) -> sinomo_core::cluster::ClusterMembershipServiceFuture<
        'a,
        sinomo_core::cluster::ClusterInviteChallengeProof,
    > {
        let paths = self.paths.clone();
        Box::pin(async move {
            crate::auth::create_cluster_invite_challenge_proof(&paths, &request, cluster.as_ref())
                .await
                .map_err(daemon_error)
        })
    }

    fn forward_join_challenge<'a>(
        &'a self,
        request: sinomo_core::cluster::ClusterInviteChallengeRequest,
    ) -> sinomo_core::cluster::ClusterMembershipServiceFuture<
        'a,
        sinomo_core::cluster::ClusterInviteChallengeProof,
    > {
        Box::pin(async move {
            forward_cluster_invite_challenge_to_cluster_state_authority_async_on_runtime(
                &self.paths,
                &request,
                self.runtime()?,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn register_join<'a>(
        &'a self,
        _data_dir: &'a SinomoDataDir,
        request: sinomo_core::cluster::ClusterJoinRegistrationRequest,
    ) -> sinomo_core::cluster::ClusterMembershipServiceFuture<
        'a,
        sinomo_core::cluster::ClusterJoinRegistrationResponse,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            let cluster_paths =
                cluster_paths_from_daemon_paths(&self.paths).map_err(daemon_error)?;
            sinomo_core::cluster::handle_cluster_join_registration_request_async_with_host(
                runtime,
                &cluster_paths,
                request,
                time::OffsetDateTime::now_utc(),
                &mut DaemonClusterJoinRegistrationHost::new_on_runtime(&self.paths, runtime),
            )
            .await
            .map_err(ServiceError::from)
        })
    }

    fn publish_invite<'a>(
        &'a self,
        _data_dir: &'a SinomoDataDir,
        request: sinomo_core::cluster::ClusterInvitePublishRequest,
        signer_node_id: String,
        signer_public_key: String,
    ) -> sinomo_core::cluster::ClusterMembershipServiceFuture<
        'a,
        sinomo_core::cluster::ClusterInvitePublishResponse,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            let cluster_paths =
                cluster_paths_from_daemon_paths(&self.paths).map_err(daemon_error)?;
            let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime);
            sinomo_core::cluster::handle_cluster_invite_publish_request_async_with_host(
                runtime,
                &cluster_paths,
                request,
                &signer_node_id,
                &signer_public_key,
                time::OffsetDateTime::now_utc(),
                &mut host,
            )
            .await
            .map_err(ServiceError::from)
        })
    }

    fn publish_members<'a>(
        &'a self,
        _data_dir: &'a SinomoDataDir,
        request: sinomo_core::cluster::ClusterMemberPublishRequest,
        signer_node_id: String,
    ) -> sinomo_core::cluster::ClusterMembershipServiceFuture<
        'a,
        sinomo_core::cluster::ClusterMemberPublishResponse,
    > {
        Box::pin(async move {
            let runtime = self.runtime()?;
            let cluster_paths =
                cluster_paths_from_daemon_paths(&self.paths).map_err(daemon_error)?;
            let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime);
            sinomo_core::cluster::handle_cluster_member_publish_request_async_with_host(
                runtime,
                &cluster_paths,
                request,
                &signer_node_id,
                &mut host,
            )
            .await
            .map_err(ServiceError::from)
        })
    }

    fn join_accepted(&self) {
        crate::control::request_daemon_cluster_maintenance(
            crate::control::DaemonClusterMaintenanceReason::JoinAccepted,
        );
    }

    fn trace_join(&self, detail: &str) {
        crate::cluster_join_trace(format_args!("{detail}"));
    }
}

fn register_daemon_cluster_membership_service(
    builder: sinomo_core::service::ServiceRegistryBuilder,
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> sinomo_core::service::ServiceRegistryBuilder {
    sinomo_core::cluster::register_cluster_membership_service(
        builder,
        Arc::new(DaemonClusterMembershipServiceHost {
            paths: paths.clone(),
            runtime: runtime.cloned(),
        }),
    )
}

/// Process runtime and mutation effects for the cluster-owned control service.
struct DaemonClusterControlServiceHost {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

impl DaemonClusterControlServiceHost {
    fn runtime(&self) -> Result<&tokio::runtime::Handle, ServiceError> {
        require_service_runtime(self.runtime.as_ref(), "cluster control")
    }
}

impl sinomo_core::cluster::ClusterControlServiceHost for DaemonClusterControlServiceHost {
    fn route_hint(
        &self,
        request: sinomo_core::cluster::ClusterRouteHintRequest,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterRouteHintResponse,
    > {
        let paths = self.paths.clone();
        Box::pin(async move {
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_ROUTE_HINT_ROUTE,
                "route hint",
                move || {
                    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(&paths)
                        .map_err(daemon_error)?;
                    let response =
                        sinomo_core::cluster::apply_cluster_route_hint(&cluster_paths, &request)?;
                    if response.changed {
                        // Probe and publish over the new route now; contact
                        // with the leader starts this node's catch-up.
                        crate::control::request_daemon_cluster_maintenance(
                            crate::control::DaemonClusterMaintenanceReason::ManualBroad,
                        );
                    }
                    Ok(response)
                },
            )
            .await
        })
    }
    fn state_authority(
        &self,
        request: sinomo_core::cluster::ClusterControlStateAuthorityRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterControlStateAuthorityResponse,
    > {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "cluster control state authority")?
                    .clone();
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_STATE_AUTHORITY_ROUTE,
                "cluster control state authority",
                move || {
                    crate::cluster::handle_daemon_cluster_control_state_authority_on_runtime(
                        &paths, request, &runtime, authority,
                    )
                    .map_err(daemon_error)
                },
            )
            .await
        })
    }
    fn authoritative_snapshot(
        &self,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::kv::KvAuthoritativeRead<sinomo_core::kv::KvSnapshot>,
    > {
        Box::pin(async move {
            let barrier = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
            handle_daemon_cluster_kv_authoritative_snapshot_request_async_on_runtime(
                &self.paths,
                &barrier.body,
                true,
                self.runtime()?,
            )
            .await
            .map_err(daemon_error)
        })
    }

    fn init(
        &self,
        request: sinomo_core::cluster::ClusterControlInitRequest,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterControlInitResponse,
    > {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "cluster control init")?.clone();
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE,
                "cluster control init",
                move || {
                    handle_daemon_cluster_control_init_on_runtime(&paths, request, &runtime)
                        .map_err(daemon_error)
                },
            )
            .await
        })
    }

    fn join(
        &self,
        request: sinomo_core::cluster::ClusterControlJoinRequest,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterControlJoinResponse,
    > {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "cluster control join")?.clone();
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
                "cluster control join",
                move || {
                    handle_daemon_cluster_control_join_on_runtime(&paths, request, &runtime)
                        .map_err(daemon_error)
                },
            )
            .await
        })
    }

    fn invite(
        &self,
        request: sinomo_core::cluster::ClusterControlInviteRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterControlInviteResponse,
    > {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "cluster control invite")?.clone();
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_INVITE_ROUTE,
                "cluster control invite",
                move || {
                    handle_daemon_cluster_control_invite_with_authority_on_runtime(
                        &paths, request, &runtime, authority,
                    )
                    .map_err(daemon_error)
                },
            )
            .await
        })
    }

    fn rename(
        &self,
        request: sinomo_core::cluster::ClusterControlRenameRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterControlRenameResponse,
    > {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "cluster control rename")?.clone();
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_RENAME_ROUTE,
                "cluster control rename",
                move || {
                    handle_daemon_cluster_control_rename_with_authority_on_runtime(
                        &paths, request, &runtime, authority,
                    )
                    .map_err(daemon_error)
                },
            )
            .await
        })
    }

    fn role(
        &self,
        request: sinomo_core::cluster::ClusterControlRoleRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterControlRoleResponse,
    > {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "cluster control role")?.clone();
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_ROLE_ROUTE,
                "cluster control role",
                move || {
                    handle_daemon_cluster_control_role_with_authority_on_runtime(
                        &paths, request, &runtime, authority,
                    )
                    .map_err(daemon_error)
                },
            )
            .await
        })
    }

    fn remove(
        &self,
        request: sinomo_core::cluster::ClusterControlRemoveRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterControlRemoveResponse,
    > {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "cluster control remove")?.clone();
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_REMOVE_ROUTE,
                "cluster control remove",
                move || {
                    handle_daemon_cluster_control_remove_with_authority_on_runtime(
                        &paths, request, &runtime, authority,
                    )
                    .map_err(daemon_error)
                },
            )
            .await
        })
    }

    fn leave(
        &self,
        request: sinomo_core::cluster::ClusterControlLeaveRequest,
    ) -> sinomo_core::cluster::ClusterControlServiceFuture<
        '_,
        sinomo_core::cluster::ClusterControlLeaveResponse,
    > {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "cluster control leave")?.clone();
            run_blocking_service_task(
                sinomo_core::cluster::CLUSTER_CONTROL_LEAVE_ROUTE,
                "cluster control leave",
                move || {
                    handle_daemon_cluster_control_leave_on_runtime(&paths, request, &runtime)
                        .map_err(daemon_error)
                },
            )
            .await
        })
    }
}

fn register_daemon_cluster_control_service(
    builder: sinomo_core::service::ServiceRegistryBuilder,
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> sinomo_core::service::ServiceRegistryBuilder {
    sinomo_core::cluster::register_cluster_control_service(
        builder,
        Arc::new(DaemonClusterControlServiceHost {
            paths: paths.clone(),
            runtime: runtime.cloned(),
        }),
    )
}

/// Mark a committed identity-changing operation after its response write has
/// been attempted so a clean main-daemon exit is classified as an intentional
/// supervisor restart. The mutation remains committed when the peer disconnects
/// during that write, so callers must mark the restart before propagating the
/// transport error.
pub(crate) fn mark_cluster_identity_restart_after_response(
    route: &str,
    stop: bool,
    paths: Option<&DaemonPaths>,
) {
    if stop
        && sinomo_core::cluster::cluster_control_route_requires_runtime_restart(route)
        && let Some(paths) = paths
        && let Err(error) = crate::request_supervised_main_restart(paths)
    {
        tracing::error!(%error, "could not mark cluster identity restart for the shadow supervisor");
    }
}

/// Process-owned network-module registry access. `sinomo-module` owns the RPC
/// routes and contracts; this adapter only supplies live registry mutation,
/// cluster-context refresh, and durable loader-state persistence.
struct DaemonNetworkModulesServiceHost {
    paths: DaemonPaths,
}

impl sinomo_module::NetworkModulesServiceHost for DaemonNetworkModulesServiceHost {
    fn list_modules(&self) -> Result<Vec<sinomo_core::network::NetworkModuleStatus>, String> {
        let modules = crate::registry::daemon_network_modules()
            .ok_or_else(|| "daemon network module registry is not initialized".to_string())?;
        Ok(modules.registry.snapshot())
    }

    fn set_module_enabled(
        &self,
        id: &str,
        enabled: bool,
    ) -> Result<sinomo_core::network::NetworkModuleStatus, String> {
        let modules = crate::registry::daemon_network_modules()
            .ok_or_else(|| "daemon network module registry is not initialized".to_string())?;
        if enabled {
            // A module registered pre-cluster has no stored context; give it
            // the current one so enabling actually starts the listener.
            match crate::cluster::daemon_network_module_context_now(&self.paths) {
                Ok(Some(context)) => {
                    modules.registry.provide_context(id, &context);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!("network module {id} context refresh skipped: {error}");
                }
            }
        }
        let status = modules
            .registry
            .set_enabled(id, enabled)
            .map_err(|error| error.to_string())?;
        if !enabled && status.descriptor.listener {
            tracing::warn!(
                "network module {id} disabled: the node serves no {id} listener until the module \
                 is re-enabled"
            );
        }
        // Persist the flag so the toggle survives daemon restarts. A
        // persistence failure is surfaced after the runtime state changes,
        // matching the loader-state semantics used by the previous service.
        if let Some(module_id) = modules.loader_module_ids.get(id) {
            let data_dir_root = daemon_data_dir_path(&self.paths)
                .map_err(|error| format!("network module enabled flag not persisted: {error}"))?;
            crate::registry::persist_network_module_enabled_state(
                &data_dir_root,
                module_id,
                enabled,
                format!("set-enabled route toggled {id}"),
            )
            .map_err(|error| format!("network module enabled flag not persisted: {error}"))?;
        }
        Ok(status)
    }
}

/// Build the registry of services the daemon serves through the seam, bound to this
/// daemon's `paths` so foundational services act for the right process. Request-specific
/// identity is supplied through [`ServiceContext::signer`] at dispatch time.
fn daemon_base_service_registry(
    paths: &DaemonPaths,
    service_runtime: Option<&tokio::runtime::Handle>,
    component_catalog: &ComponentCatalog,
    event_hub: Arc<DaemonEventHub>,
) -> (ServiceRegistry, Arc<DaemonEventHub>) {
    let is_shadow = crate::daemon_paths_are_shadow(paths);
    let data_dir_root = paths.root.parent().unwrap_or(paths.root.as_path());
    let mut builder = ServiceRegistry::builder();
    // Config definitions and arbitrary extension hooks may receive
    // `DaemonClusterAccess`; keep them with the main authority owner. The
    // shadow remains a bounded passive/recovery endpoint.
    if !is_shadow {
        let config_registry = daemon_config_registry(component_catalog);
        builder = register_config_services(builder, config_registry);
    }
    builder = crate::host_api::register_hosted_daemon_lifecycle_service(
        builder,
        Arc::new(DaemonLifecycleServiceHost {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
            status_contributors: component_catalog.status_contributors(),
        }),
    );
    builder = crate::host_api::register_hosted_daemon_operations_service(
        builder,
        daemon_operations_service_host(paths.clone(), service_runtime.cloned()),
    );
    builder = crate::host_api::register_daemon_event_service(
        builder,
        service_runtime.cloned(),
        Arc::clone(&event_hub),
    );
    builder = register_daemon_cluster_network_service(builder, paths, service_runtime, !is_shadow);
    builder = sinomo_module::register_native_rust_modules_service(
        builder,
        data_dir_root.to_path_buf(),
        component_catalog.native_rust_control_targets(),
    );
    builder = sinomo_module::register_network_modules_service(
        builder,
        Arc::new(DaemonNetworkModulesServiceHost {
            paths: paths.clone(),
        }),
    );
    if !is_shadow {
        builder = register_daemon_cluster_kv_service(builder, paths, service_runtime);
        builder = register_daemon_cluster_consensus_service(builder, service_runtime);
        builder = register_daemon_cluster_membership_service(builder, paths, service_runtime);
        builder = register_daemon_cluster_control_service(builder, paths, service_runtime);
    }
    (builder.build(), event_hub)
}

#[cfg(test)]
fn daemon_service_registry(
    paths: &DaemonPaths,
    service_runtime: Option<&tokio::runtime::Handle>,
    supplied_components: &[crate::DaemonComponentFactory],
) -> ServiceRegistry {
    let is_shadow = crate::daemon_paths_are_shadow(paths);
    let supplied_catalog = component_catalog_from_factories(supplied_components.to_vec())
        .expect("supplied daemon component catalog must be valid");
    let event_hub = Arc::new(DaemonEventHub::new());
    let component_catalog =
        daemon_component_catalog(paths, service_runtime, &supplied_catalog, &event_hub)
            .expect("daemon component catalog must be valid");
    let component_factories = component_catalog.factories();
    let (base_registry, _) =
        daemon_base_service_registry(paths, service_runtime, &component_catalog, event_hub);
    let data_dir_root = paths.root.parent().unwrap_or(paths.root.as_path());
    let data_dir = Arc::new(SinomoDataDir::new(data_dir_root));
    let mut component_context = ComponentContext::new(data_dir).with_host_role(if is_shadow {
        ComponentHostRole::Shadow
    } else {
        ComponentHostRole::Main
    });
    if let Some(runtime) = service_runtime {
        component_context = component_context.with_runtime(runtime.clone());
    }
    let mut reconciler = ComponentReconciler::new(component_context, base_registry);
    reconciler
        .reconcile(&component_factories)
        .expect("daemon component graph must be valid");
    reconciler
        .service_registry()
        .active_snapshot()
        .registry()
        .clone()
}

fn daemon_component_catalog(
    _paths: &DaemonPaths,
    _service_runtime: Option<&tokio::runtime::Handle>,
    supplied_catalog: &ComponentCatalog,
    _event_hub: &Arc<DaemonEventHub>,
) -> Result<ComponentCatalog, sinomo_module::ComponentCatalogError> {
    Ok(supplied_catalog.clone())
}

struct DaemonComponentEventPersistence {
    store: Mutex<sinomo_core::events::JsonlEventStore>,
}

impl sinomo_module::ComponentEventPersistence for DaemonComponentEventPersistence {
    fn append(&self, event: &sinomo_module::ComponentEvent) -> Result<(), String> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| "component event persistence lock poisoned".to_string())?;
        match sinomo_core::events::EventStore::append(&mut *store, event.envelope.clone()) {
            Ok(_) | Err(sinomo_core::events::EventStoreError::DuplicateEventId { .. }) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }
}

struct DaemonComponentEventTransport {
    runtime: tokio::runtime::Handle,
    cluster_paths: ClusterPaths,
    local_node_id: sinomo_core::node::NodeId,
}

impl sinomo_module::ComponentEventTransport for DaemonComponentEventTransport {
    fn publish<'a>(
        &'a self,
        event: Arc<sinomo_module::ComponentEvent>,
        durability: sinomo_module::ComponentEventDurability,
    ) -> sinomo_module::ComponentEventTransportFuture<'a> {
        Box::pin(async move {
            let state =
                sinomo_core::cluster::read_cluster_state_async(&self.runtime, &self.cluster_paths)
                    .await
                    .map_err(|error| error.to_string())?;
            let request = ObjectRpcRequest::encode_body(
                sinomo_module::COMPONENT_EVENT_INGRESS_ROUTE,
                sinomo_module::COMPONENT_EVENT_RPC_CATEGORY,
                &sinomo_module::ComponentEventIngressRequest {
                    event: event.as_ref().clone(),
                    durability,
                },
            )
            .map_err(|error| error.to_string())?;
            let mut pending = Vec::new();
            for member in state.members.into_iter().filter(|member| {
                member.node_id != self.local_node_id && member.state.is_authorized()
            }) {
                let target = member.node_id;
                let request = request.clone();
                let runtime = self.runtime.clone();
                pending.push((
                    target.clone(),
                    tokio::spawn(async move {
                        crate::client::send_object_rpc_to_target_async(
                            &target,
                            &request,
                            Duration::from_secs(5),
                            &runtime,
                            true,
                        )
                        .await
                    }),
                ));
            }
            let mut failures = Vec::new();
            for (target, task) in pending {
                match task.await {
                    Ok(Ok(response)) => {
                        let response: sinomo_module::ComponentEventIngressResponse =
                            response.decode_body().map_err(|error| error.to_string())?;
                        if !response.accepted
                            || response.event_id != event.envelope.event_id.as_str()
                        {
                            failures.push(format!("{target}: event acknowledgement mismatch"));
                        }
                    }
                    Ok(Err(error)) => failures.push(format!("{target}: {error}")),
                    Err(error) => failures.push(format!("{target}: task failed: {error}")),
                }
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(failures.join("; "))
            }
        })
    }
}

fn daemon_component_event_bus(
    paths: &DaemonPaths,
    data_dir: &SinomoDataDir,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_module::ComponentEventBus, DaemonError> {
    let event_path = data_dir
        .root()
        .join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH);
    if let Some(parent) = event_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| DaemonError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut bus = sinomo_module::ComponentEventBus::default().with_persistence(Arc::new(
        DaemonComponentEventPersistence {
            store: Mutex::new(sinomo_core::events::JsonlEventStore::new(event_path)),
        },
    ));
    let cluster_paths = ClusterPaths::new(data_dir);
    if cluster_paths.state.exists() {
        match sinomo_core::cluster::read_cluster_state(&cluster_paths) {
            Ok(state) => {
                bus = bus.with_origin_node_id(state.local_node_id.clone());
                if !crate::daemon_paths_are_shadow(paths)
                    && let Some(runtime) = runtime
                {
                    bus = bus.with_transport(Arc::new(DaemonComponentEventTransport {
                        runtime: runtime.clone(),
                        cluster_paths,
                        local_node_id: state.local_node_id,
                    }));
                }
            }
            Err(error) => tracing::warn!(
                %error,
                "component event cluster transport unavailable from invalid local identity"
            ),
        }
    }
    Ok(bus)
}

fn component_event_stream_bridge_factory(
    event_bus: sinomo_module::ComponentEventBus,
    event_hub: Arc<DaemonEventHub>,
) -> Result<Arc<dyn ComponentFactory>, DaemonError> {
    let factory = sinomo_module::StaticComponentFactory::new(
        sinomo_module::ComponentDescriptor::new(
            "sinomo.daemon.event-stream-bridge",
            env!("CARGO_PKG_VERSION"),
        ),
        move |context, scope| {
            if let Some(runtime) = context.runtime() {
                let mut receiver = event_bus.subscribe_host();
                let hub = Arc::clone(&event_hub);
                let mut shutdown = scope.shutdown_receiver();
                scope.own_task(
                    "daemon event stream compatibility bridge",
                    runtime.spawn(async move {
                        loop {
                            tokio::select! {
                            event = receiver.recv() => match event {
                                Ok(event) if event.schema_id != "sinomo.daemon-event-legacy" => {
                                    hub.publish(event.envelope.clone());
                                }
                                Ok(_) => {}
                                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                        tracing::warn!(skipped, "daemon event stream bridge lagged");
                                    }
                                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                                },
                                changed = shutdown.changed() => {
                                    if changed.is_err() || *shutdown.borrow() {
                                        break;
                                    }
                                }
                            }
                        }
                    }),
                );
            }
            Ok(())
        },
    )
    .map_err(|error| DaemonError::Network {
        address: "daemon event stream compatibility bridge".to_string(),
        detail: error.to_string(),
    })?;
    Ok(Arc::new(factory))
}

struct DaemonComponentEventMirror {
    event_bus: sinomo_module::ComponentEventBus,
}

impl crate::host_api::DaemonEventMirror for DaemonComponentEventMirror {
    fn publish(&self, event: sinomo_core::EventEnvelope) {
        if event.source.as_str().starts_with("component/") {
            return;
        }
        let source = event.source.as_str().to_string();
        let component_event = sinomo_module::ComponentEvent {
            publisher: sinomo_module::ComponentEventPublisher {
                component_id: source,
                component_version: env!("CARGO_PKG_VERSION").to_string(),
                artifact_id: None,
                artifact_generation: 0,
            },
            schema_id: "sinomo.daemon-event-legacy".to_string(),
            schema_version: "1.0.0".to_string(),
            envelope: event,
        };
        if let Err(error) = self.event_bus.publish_host_local(
            component_event,
            sinomo_module::ComponentEventDurability::Transient,
        ) {
            tracing::warn!(%error, "legacy daemon event mirror failed");
        }
    }
}

#[derive(Clone)]
pub struct DaemonServiceRegistryHost {
    paths: DaemonPaths,
    service_data_dir: Arc<SinomoDataDir>,
    service_cluster: Option<Arc<dyn ClusterAccess>>,
    /// Live service-registry snapshot store observed by the catalog publisher
    /// component and request dispatch.
    service_registry: ServiceRegistrySnapshotStore,
    component_reconciler: Arc<Mutex<ComponentReconciler>>,
    module_snapshots: sinomo_module::ModuleRegistrySnapshotStore,
    module_runtime: sinomo_module::ModuleRuntimeHost,
    component_catalog: Arc<Mutex<ComponentCatalog>>,
    host_registry: ServiceRegistry,
    component_event_bus: sinomo_module::ComponentEventBus,
    event_hub: Arc<DaemonEventHub>,
    module_transition: Arc<tokio::sync::Mutex<()>>,
}

struct DaemonServiceAssembly {
    base_registry: ServiceRegistry,
    component_catalog: ComponentCatalog,
    event_hub: Arc<DaemonEventHub>,
}

static DAEMON_SERVICE_REGISTRY_HOSTS: OnceLock<Mutex<HashMap<PathBuf, DaemonServiceRegistryHost>>> =
    OnceLock::new();
static DAEMON_COMPONENT_EVENT_BUSES: OnceLock<
    Mutex<HashMap<PathBuf, sinomo_module::ComponentEventBus>>,
> = OnceLock::new();
static DAEMON_STATUS_CONTRIBUTORS: OnceLock<
    Mutex<HashMap<PathBuf, Vec<sinomo_module::ComponentStatusContributor>>>,
> = OnceLock::new();
static DAEMON_MIGRATION_CONTRIBUTORS: OnceLock<
    Mutex<HashMap<PathBuf, Vec<sinomo_module::ComponentMigrationContributor>>>,
> = OnceLock::new();

pub(crate) fn component_migration_contributions(
    paths: &DaemonPaths,
    source_cluster_id: &sinomo_core::node::ClusterId,
) -> Result<sinomo_module::ComponentMigrationContribution, String> {
    let contributors = DAEMON_MIGRATION_CONTRIBUTORS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "daemon migration contributor map lock poisoned".to_string())?
        .get(&paths.root)
        .cloned()
        .unwrap_or_default();
    let data_dir = paths.root.parent().unwrap_or(paths.root.as_path());
    let mut combined = sinomo_module::ComponentMigrationContribution::default();
    for contribute in contributors {
        let mut contribution = contribute(data_dir, source_cluster_id)?;
        combined.moves.append(&mut contribution.moves);
        combined
            .evidence_refs
            .append(&mut contribution.evidence_refs);
    }
    Ok(combined)
}

pub(crate) fn apply_component_status_contributors(
    paths: &DaemonPaths,
    status: &mut sinomo_api::DaemonStatus,
) -> Result<(), String> {
    let contributors = DAEMON_STATUS_CONTRIBUTORS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "daemon status contributor map lock poisoned".to_string())?
        .get(&paths.root)
        .cloned()
        .unwrap_or_default();
    let data_dir = paths.root.parent().unwrap_or(paths.root.as_path());
    for contribute in contributors {
        if let Some(projection) = contribute(data_dir)? {
            status.apply_component_status_projection(projection.key, projection.value)?;
        }
    }
    Ok(())
}

pub(crate) fn publish_component_event_for_paths(
    paths: &DaemonPaths,
    event: sinomo_module::ComponentEvent,
    durability: sinomo_module::ComponentEventDurability,
) -> Result<bool, String> {
    let bus = DAEMON_COMPONENT_EVENT_BUSES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "daemon component event bus map lock poisoned".to_string())?
        .get(&paths.root)
        .cloned();
    let Some(bus) = bus else {
        return Ok(false);
    };
    bus.publish_host_local(event, durability)
        .map(|_| true)
        .map_err(|error| error.to_string())
}

impl DaemonServiceRegistryHost {
    #[cfg(test)]
    fn from_registry_for_paths(
        registry: ServiceRegistry,
        paths: &DaemonPaths,
    ) -> Result<Self, DaemonError> {
        let event_hub = Arc::new(DaemonEventHub::new());
        let catalog =
            ComponentCatalog::builder()
                .build()
                .map_err(|error| DaemonError::Network {
                    address: "daemon component catalog".to_string(),
                    detail: error.to_string(),
                })?;
        Self::from_registry_with_internal_modules(
            registry,
            paths.clone(),
            None,
            catalog,
            event_hub,
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_registry_for_test(
        registry: ServiceRegistry,
        paths: &DaemonPaths,
    ) -> Result<Self, DaemonError> {
        Self::from_registry_for_paths(registry, paths)
    }

    fn from_registry_for_paths_on_runtime(
        assembly: DaemonServiceAssembly,
        paths: &DaemonPaths,
        service_runtime: tokio::runtime::Handle,
    ) -> Result<Self, DaemonError> {
        Self::from_registry_with_internal_modules(
            assembly.base_registry,
            paths.clone(),
            Some(service_runtime),
            assembly.component_catalog,
            assembly.event_hub,
            None,
        )
    }

    fn from_registry_with_internal_modules(
        registry: ServiceRegistry,
        paths: DaemonPaths,
        service_runtime: Option<tokio::runtime::Handle>,
        component_catalog: ComponentCatalog,
        event_hub: Arc<DaemonEventHub>,
        node_resources: Option<sinomo_device::NodeResourceMonitor>,
    ) -> Result<Self, DaemonError> {
        let registry = register_daemon_module_hot_unload_service(registry, paths.clone());
        let host_registry = registry.clone();
        let data_dir_root =
            daemon_data_dir_path(&paths).map_err(|source| DaemonError::Network {
                address: "daemon module registry".to_string(),
                detail: source.to_string(),
            })?;
        let module_snapshots =
            sinomo_module::ModuleRegistrySnapshotStore::new(component_catalog.modules().clone())
                .map_err(|source| DaemonError::Network {
                    address: "daemon module registry".to_string(),
                    detail: source.to_string(),
                })?;
        let module_snapshot =
            daemon_internal_module_snapshot(&module_snapshots).map_err(|source| {
                DaemonError::Network {
                    address: "daemon service route registry".to_string(),
                    detail: source.to_string(),
                }
            })?;
        let materialized = sinomo_module::materialize_module_service_snapshot_with_extensions(
            registry,
            module_snapshot,
            data_dir_root.clone(),
            Arc::new(component_catalog.clone()),
            sinomo_module::NativeModuleServiceMaterialization::new("daemon"),
        )
        .map_err(|source| DaemonError::Network {
            address: "daemon service route registry".to_string(),
            detail: source.to_string(),
        })?;
        let module_runtime = materialized.runtime.clone();
        let registry = materialized.registry;
        let service_data_dir = Arc::new(SinomoDataDir::new(data_dir_root));
        let component_event_bus = daemon_component_event_bus(
            &paths,
            service_data_dir.as_ref(),
            service_runtime.as_ref(),
        )?;
        event_hub.set_mirror(Arc::new(DaemonComponentEventMirror {
            event_bus: component_event_bus.clone(),
        }));
        let registry = sinomo_module::register_component_event_ingress_service(
            registry.to_builder(),
            component_event_bus.clone(),
        )
        .build();
        let service_cluster: Option<Arc<dyn ClusterAccess>> =
            if !crate::daemon_paths_are_shadow(&paths)
                && ClusterPaths::new(service_data_dir.as_ref()).state.exists()
            {
                Some(Arc::new(DaemonClusterAccess::new(
                    paths.clone(),
                    service_data_dir.as_ref(),
                    service_runtime.as_ref(),
                )))
            } else {
                None
            };
        let service_registry = ServiceRegistrySnapshotStore::new(registry.clone());
        let active_runtime_generation = module_runtime.generations.active_snapshot();
        module_runtime
            .generations
            .publish(
                active_runtime_generation.module_registry.clone(),
                materialized.extension_registry.clone(),
                service_registry.active_snapshot().version(),
            )
            .map_err(|source| DaemonError::Network {
                address: "daemon module runtime generation".to_string(),
                detail: source.to_string(),
            })?;
        let mut component_context = ComponentContext::new(Arc::clone(&service_data_dir))
            .with_event_bus(component_event_bus.clone())
            .with_module_runtime(module_runtime.clone())
            .with_host_role(if crate::daemon_paths_are_shadow(&paths) {
                ComponentHostRole::Shadow
            } else {
                ComponentHostRole::Main
            });
        if let Some(runtime) = service_runtime.as_ref() {
            component_context = component_context.with_runtime(runtime.clone());
        }
        if let Some(node_resources) = node_resources {
            component_context = component_context.with_node_resources(node_resources);
        }
        if let Some(idle_stats) = crate::process_idle_stats() {
            component_context = component_context
                .with_activity_observer(Arc::new(DaemonComponentActivityObserver { idle_stats }));
        }
        if let Some(cluster) = service_cluster.as_ref() {
            component_context = component_context.with_cluster_access(Arc::clone(cluster));
            if let Some(runtime) = service_runtime.as_ref() {
                component_context = component_context.with_object_rpc_transport(Arc::new(
                    DaemonComponentObjectRpcTransport {
                        paths: paths.clone(),
                        runtime: runtime.clone(),
                    },
                ));
            }
        }
        let mut component_reconciler = ComponentReconciler::with_service_registry(
            component_context,
            registry,
            service_registry.clone(),
        );
        let mut component_factories = component_catalog.factories();
        component_factories.push(component_event_stream_bridge_factory(
            component_event_bus.clone(),
            Arc::clone(&event_hub),
        )?);
        component_reconciler
            .reconcile(&component_factories)
            .map_err(|source| DaemonError::Network {
                address: "daemon component registry".to_string(),
                detail: source.to_string(),
            })?;
        component_reconciler
            .prepare_host_on_runtime(service_runtime.as_ref())
            .map_err(|source| DaemonError::Network {
                address: "daemon component startup".to_string(),
                detail: source.to_string(),
            })?;
        let active_runtime_generation = module_runtime.generations.active_snapshot();
        module_runtime
            .generations
            .publish(
                active_runtime_generation.module_registry.clone(),
                active_runtime_generation.extension_registry.clone(),
                service_registry.active_snapshot().version(),
            )
            .map_err(|source| DaemonError::Network {
                address: "daemon component runtime generation".to_string(),
                detail: source.to_string(),
            })?;
        DAEMON_COMPONENT_EVENT_BUSES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon component event bus map".to_string(),
                detail: "event bus map lock poisoned".to_string(),
            })?
            .insert(paths.root.clone(), component_event_bus.clone());
        DAEMON_STATUS_CONTRIBUTORS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon status contributor map".to_string(),
                detail: "status contributor map lock poisoned".to_string(),
            })?
            .insert(paths.root.clone(), component_catalog.status_contributors());
        DAEMON_MIGRATION_CONTRIBUTORS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon migration contributor map".to_string(),
                detail: "migration contributor map lock poisoned".to_string(),
            })?
            .insert(
                paths.root.clone(),
                component_catalog.migration_contributors(),
            );
        Ok(Self {
            paths,
            service_data_dir,
            service_cluster,
            service_registry,
            component_reconciler: Arc::new(Mutex::new(component_reconciler)),
            module_snapshots,
            module_runtime,
            component_catalog: Arc::new(Mutex::new(component_catalog)),
            host_registry,
            component_event_bus,
            event_hub,
            module_transition: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub async fn new_on_runtime(
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
        let assembly = daemon_service_registry_on_blocking_pool(
            registry_paths,
            service_runtime.clone(),
            ComponentCatalog::default(),
        )
        .await?;
        let host = Self::from_registry_for_paths_on_runtime(assembly, &paths, service_runtime)?;
        let mut cache = DAEMON_SERVICE_REGISTRY_HOSTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|source| DaemonError::Network {
                address: "daemon service registry cache".to_string(),
                detail: source.to_string(),
            })?;
        Ok(cache.entry(cache_key).or_insert_with(|| host).clone())
    }

    #[cfg(test)]
    pub(crate) async fn new_on_runtime_with_components(
        paths: DaemonPaths,
        service_runtime: tokio::runtime::Handle,
        component_factories: Vec<crate::DaemonComponentFactory>,
    ) -> Result<Self, DaemonError> {
        if component_factories.is_empty() {
            return Self::new_on_runtime(paths, service_runtime).await;
        }
        let component_catalog = component_catalog_from_factories(component_factories)?;
        Self::new_on_runtime_with_catalog(paths, service_runtime, component_catalog).await
    }

    pub async fn new_on_runtime_with_catalog(
        paths: DaemonPaths,
        service_runtime: tokio::runtime::Handle,
        component_catalog: ComponentCatalog,
    ) -> Result<Self, DaemonError> {
        Self::new_on_runtime_with_catalog_and_resources(
            paths,
            service_runtime,
            component_catalog,
            sinomo_device::NodeResourceMonitor::default(),
        )
        .await
    }

    pub async fn new_on_runtime_with_catalog_and_resources(
        paths: DaemonPaths,
        service_runtime: tokio::runtime::Handle,
        component_catalog: ComponentCatalog,
        node_resources: sinomo_device::NodeResourceMonitor,
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
        let assembly = daemon_service_registry_on_blocking_pool(
            registry_paths,
            service_runtime.clone(),
            component_catalog,
        )
        .await?;
        let host = Self::from_registry_with_internal_modules(
            assembly.base_registry,
            paths.clone(),
            Some(service_runtime),
            assembly.component_catalog,
            assembly.event_hub,
            Some(node_resources),
        )?;
        let mut cache = DAEMON_SERVICE_REGISTRY_HOSTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|source| DaemonError::Network {
                address: "daemon service registry cache".to_string(),
                detail: source.to_string(),
            })?;
        Ok(cache.entry(cache_key).or_insert_with(|| host).clone())
    }

    pub(crate) fn object_rpc_route_descriptor(&self, route: &str) -> Option<ServiceRoute> {
        self.service_registry
            .active_snapshot()
            .registry()
            .route(route)
    }

    pub(crate) fn notify_component_host_start(&self) {
        if let Ok(reconciler) = self.component_reconciler.lock() {
            reconciler.notify_host_start();
        }
    }

    pub(crate) fn notify_component_host_ready(&self) {
        if let Ok(reconciler) = self.component_reconciler.lock() {
            reconciler.notify_host_ready();
        }
    }

    pub async fn hot_unload_module(
        &self,
        module_id: &sinomo_module::ModuleId,
        drain_timeout: Duration,
        reason: &str,
    ) -> Result<sinomo_module::ModuleHotUnloadReport, DaemonError> {
        let _transition = self.module_transition.lock().await;
        let transition_started = Instant::now();
        let runtime_before = self.module_runtime.generations.active_snapshot();
        let current_catalog = self
            .component_catalog
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon module hot unload".to_string(),
                detail: "component catalog lock poisoned".to_string(),
            })?
            .clone();
        let _registration = current_catalog
            .modules()
            .get(module_id)
            .cloned()
            .ok_or_else(|| DaemonError::Network {
                address: "daemon module hot unload".to_string(),
                detail: format!("module {module_id} is not active"),
            })?;
        let active_before = self
            .component_reconciler
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon module hot unload plan".to_string(),
                detail: "component reconciler lock poisoned".to_string(),
            })?
            .active_component_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut next_catalog = current_catalog
            .without_module(module_id)
            .map_err(|source| DaemonError::Network {
                address: "daemon module hot unload catalog".to_string(),
                detail: source.to_string(),
            })?;
        let mut cascade_module_ids = Vec::new();
        loop {
            let planned = self
                .component_reconciler
                .lock()
                .map_err(|_| DaemonError::Network {
                    address: "daemon module hot unload plan".to_string(),
                    detail: "component reconciler lock poisoned".to_string(),
                })?
                .planned_active_component_ids(&next_catalog.factories())
                .map_err(|source| DaemonError::Network {
                    address: "daemon module hot unload plan".to_string(),
                    detail: source.to_string(),
                })?
                .into_iter()
                .collect::<BTreeSet<_>>();
            let newly_inactive = next_catalog
                .modules()
                .registrations()
                .filter_map(|candidate| {
                    let component =
                        sinomo_module::component_descriptor_from_registration(candidate).ok()?;
                    (active_before.contains(&component.component_id)
                        && !planned.contains(&component.component_id))
                    .then(|| candidate.module_id().clone())
                })
                .collect::<Vec<_>>();
            if newly_inactive.is_empty() {
                break;
            }
            for cascade_module_id in newly_inactive {
                next_catalog =
                    next_catalog
                        .without_module(&cascade_module_id)
                        .map_err(|source| DaemonError::Network {
                            address: "daemon module hot unload cascade".to_string(),
                            detail: source.to_string(),
                        })?;
                cascade_module_ids.push(cascade_module_id);
            }
        }
        let mut draining_module_ids = vec![module_id.clone()];
        draining_module_ids.extend(cascade_module_ids.iter().cloned());
        let mut invocations_at_drain_start = 0_u64;
        for draining_module_id in &draining_module_ids {
            invocations_at_drain_start = invocations_at_drain_start.saturating_add(
                self.module_runtime
                    .invocations
                    .begin_drain(draining_module_id.as_str())
                    .map_err(|source| DaemonError::Network {
                        address: "daemon module hot unload drain".to_string(),
                        detail: source.to_string(),
                    })?,
            );
        }

        let next_module_snapshot = self
            .module_snapshots
            .replace_registry(next_catalog.modules().clone())
            .map_err(|source| DaemonError::Network {
                address: "daemon module hot unload registry".to_string(),
                detail: source.to_string(),
            })?;
        let module_service_snapshot =
            sinomo_module::module_service_snapshot_from_snapshot(next_module_snapshot.clone())
                .map_err(|source| DaemonError::Network {
                    address: "daemon module hot unload routes".to_string(),
                    detail: source.to_string(),
                })?;
        let materialized = sinomo_module::materialize_module_service_snapshot_for_runtime(
            self.host_registry.clone(),
            module_service_snapshot,
            self.service_data_dir.root(),
            Arc::new(next_catalog.clone()),
            sinomo_module::NativeModuleServiceMaterialization::new("daemon"),
            self.module_runtime.clone(),
        )
        .map_err(|source| DaemonError::Network {
            address: "daemon module hot unload materialization".to_string(),
            detail: source.to_string(),
        })?;
        let next_base_registry = sinomo_module::register_component_event_ingress_service(
            materialized.registry.to_builder(),
            self.component_event_bus.clone(),
        )
        .build();
        let mut factories = next_catalog.factories();
        factories.push(component_event_stream_bridge_factory(
            self.component_event_bus.clone(),
            Arc::clone(&self.event_hub),
        )?);
        let reconcile = self
            .component_reconciler
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon module hot unload components".to_string(),
                detail: "component reconciler lock poisoned".to_string(),
            })?
            .reconcile_staged_with_base_registry(next_base_registry, &factories)
            .map_err(|source| DaemonError::Network {
                address: "daemon module hot unload components".to_string(),
                detail: source.to_string(),
            })?;
        let runtime_after = self
            .module_runtime
            .generations
            .publish(
                next_module_snapshot,
                materialized.extension_registry,
                reconcile.service_registry_version,
            )
            .map_err(|source| DaemonError::Network {
                address: "daemon module hot unload generation".to_string(),
                detail: source.to_string(),
            })?;
        *self
            .component_catalog
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon module hot unload".to_string(),
                detail: "component catalog lock poisoned".to_string(),
            })? = next_catalog.clone();
        DAEMON_STATUS_CONTRIBUTORS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon module hot unload status contributors".to_string(),
                detail: "status contributor map lock poisoned".to_string(),
            })?
            .insert(self.paths.root.clone(), next_catalog.status_contributors());
        DAEMON_MIGRATION_CONTRIBUTORS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "daemon module hot unload migration contributors".to_string(),
                detail: "migration contributor map lock poisoned".to_string(),
            })?
            .insert(
                self.paths.root.clone(),
                next_catalog.migration_contributors(),
            );

        let mut network_module_reports = Vec::new();
        if let Some(network_modules) = sinomo_module::process_network_modules() {
            for draining_module_id in &draining_module_ids {
                let remaining = drain_timeout
                    .checked_sub(transition_started.elapsed())
                    .unwrap_or(Duration::ZERO);
                let network_modules = Arc::clone(&network_modules);
                let draining_module_id = draining_module_id.clone();
                let reports = tokio::task::spawn_blocking(move || {
                    network_modules.unload_loader_module(&draining_module_id, remaining)
                })
                .await
                .map_err(|source| DaemonError::Network {
                    address: "daemon network module hot unload".to_string(),
                    detail: format!("network module retirement task failed: {source}"),
                })?
                .map_err(|source| DaemonError::Network {
                    address: "daemon network module hot unload".to_string(),
                    detail: source.to_string(),
                })?;
                network_module_reports.extend(reports);
            }
        }
        let network_modules_retired = network_module_reports.len() as u64;
        let network_module_retirement_complete = network_module_reports
            .iter()
            .all(|report| report.stop_complete && report.object_released);

        let native_operations_cancelled = draining_module_ids
            .iter()
            .map(|draining_module_id| {
                self.module_runtime
                    .native_operations
                    .cancel_module(draining_module_id.as_str()) as u64
            })
            .sum();
        let mut drained = true;
        let mut remaining_invocations = 0_u64;
        for draining_module_id in &draining_module_ids {
            let remaining_timeout = drain_timeout
                .checked_sub(transition_started.elapsed())
                .unwrap_or(Duration::ZERO);
            let outcome = self
                .module_runtime
                .invocations
                .wait_for_drain(draining_module_id.as_str(), remaining_timeout)
                .await
                .map_err(|source| DaemonError::Network {
                    address: "daemon module hot unload drain".to_string(),
                    detail: source.to_string(),
                })?;
            drained &= outcome.drained;
            remaining_invocations =
                remaining_invocations.saturating_add(outcome.remaining_invocations);
        }
        if !drained {
            return Ok(sinomo_module::ModuleHotUnloadReport {
                module_id: module_id.to_string(),
                cascade_modules: cascade_module_ids.iter().map(ToString::to_string).collect(),
                runtime_generation_before: runtime_before.generation,
                runtime_generation_after: runtime_after.generation,
                module_registry_generation: runtime_after.module_registry.version(),
                service_registry_generation: runtime_after.service_registry_generation,
                invocations_at_drain_start,
                native_operations_cancelled,
                network_modules_retired,
                network_module_retirement_complete,
                drained: false,
                remaining_invocations,
                logical_unload_complete: true,
                component_teardown_complete: false,
                physical_unload_attempted: false,
                physical_unload_succeeded: false,
                restart_required: true,
                detail: format!(
                    "module routes and contributions were withdrawn, but invocation drain timed out; component state remains quarantined; reason={reason}"
                ),
            });
        }

        let mut persistence_details = Vec::new();
        for draining_module_id in &draining_module_ids {
            let Some(draining_registration) = current_catalog.modules().get(draining_module_id)
            else {
                continue;
            };
            if !matches!(
                draining_registration.backend,
                sinomo_module::ModuleBackendRegistration::NativeDylib { .. }
            ) {
                persistence_details.push(persist_non_dylib_hot_unload_state(
                    self.service_data_dir.as_ref(),
                    draining_registration,
                    reason,
                )?);
            }
        }

        let deadline = tokio::time::Instant::from_std(transition_started + drain_timeout);
        let reconciler = Arc::clone(&self.component_reconciler);
        let stopped = reconcile.stopped.clone();
        let runtime = tokio::runtime::Handle::current();
        // Bridge only component retirement, never replay an effect. The same
        // reconciler retains scopes/child handles through cancellation or timeout;
        // physical module release below occurs only after this real join returns.
        tokio::task::spawn_blocking(move || {
            let mut reconciler = reconciler.lock().map_err(|_| DaemonError::Network {
                address: "daemon module hot unload teardown".into(),
                detail: "component reconciler lock poisoned".into(),
            })?;
            runtime
                .block_on(reconciler.finish_retiring_and_wait(&stopped, deadline))
                .map_err(|error| DaemonError::Network {
                    address: "daemon module hot unload teardown".into(),
                    detail: error.to_string(),
                })
        })
        .await
        .map_err(|error| DaemonError::Network {
            address: "daemon module hot unload teardown".into(),
            detail: format!("component retirement join stopped: {error}"),
        })??;
        for draining_module_id in &draining_module_ids {
            self.module_runtime
                .invocations
                .complete_unload(draining_module_id.as_str())
                .map_err(|source| DaemonError::Network {
                    address: "daemon module hot unload completion".to_string(),
                    detail: source.to_string(),
                })?;
        }

        let mut physical_unload_attempted = false;
        let mut physical_unload_succeeded = network_module_retirement_complete;
        let mut physical_details = persistence_details;
        physical_details.extend(
            network_module_reports
                .iter()
                .map(|report| format!("network {}: {}", report.module_id, report.detail)),
        );
        for draining_module_id in &draining_module_ids {
            let Some(draining_registration) = current_catalog.modules().get(draining_module_id)
            else {
                continue;
            };
            if matches!(
                draining_registration.backend,
                sinomo_module::ModuleBackendRegistration::NativeDylib { .. }
            ) {
                let (attempted, succeeded, detail) = hot_unload_native_dylib_runtime(
                    self.service_data_dir.as_ref(),
                    draining_module_id,
                    reason,
                )?;
                physical_unload_attempted |= attempted;
                physical_unload_succeeded &= succeeded;
                physical_details.push(format!("{draining_module_id}: {detail}"));
            }
        }
        let physical_detail = if physical_details.is_empty() {
            "modules have no dynamically mapped runtime artifacts".to_string()
        } else {
            physical_details.join("; ")
        };
        let restart_required = !physical_unload_succeeded;
        Ok(sinomo_module::ModuleHotUnloadReport {
            module_id: module_id.to_string(),
            cascade_modules: cascade_module_ids.iter().map(ToString::to_string).collect(),
            runtime_generation_before: runtime_before.generation,
            runtime_generation_after: runtime_after.generation,
            module_registry_generation: runtime_after.module_registry.version(),
            service_registry_generation: runtime_after.service_registry_generation,
            invocations_at_drain_start,
            native_operations_cancelled,
            network_modules_retired,
            network_module_retirement_complete,
            drained: true,
            remaining_invocations: 0,
            logical_unload_complete: true,
            component_teardown_complete: true,
            physical_unload_attempted,
            physical_unload_succeeded,
            restart_required,
            detail: format!("{physical_detail}; reason={reason}"),
        })
    }

    pub(crate) fn shutdown_components(
        &self,
        runtime: &tokio::runtime::Handle,
        timeout: Duration,
    ) -> Result<(), DaemonError> {
        let mut reconciler =
            self.component_reconciler
                .try_lock()
                .map_err(|error| DaemonError::Network {
                    address: "daemon component shutdown".into(),
                    detail: format!(
                        "component reconciler is unavailable for bounded shutdown: {error}"
                    ),
                })?;
        let shutdown = reconciler.shutdown_and_wait(timeout);
        let result = if let Ok(current) = tokio::runtime::Handle::try_current() {
            if !matches!(
                current.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                return Err(DaemonError::Network {
                    address: "daemon component shutdown".into(),
                    detail:
                        "component shutdown cannot synchronously bridge a current-thread runtime"
                            .into(),
                });
            }
            tokio::task::block_in_place(|| runtime.block_on(shutdown))
        } else {
            runtime.block_on(shutdown)
        };
        result.map_err(|error| DaemonError::Network {
            address: "daemon component shutdown".into(),
            detail: error.to_string(),
        })
    }
}

fn register_daemon_module_hot_unload_service(
    registry: ServiceRegistry,
    paths: DaemonPaths,
) -> ServiceRegistry {
    let route = ServiceRoute::from_route_metadata(
        sinomo_module::MODULE_HOT_UNLOAD_ROUTE,
        sinomo_module::MODULE_RPC_CATEGORY,
        "local-or-cluster-member",
        [sinomo_core::node::MODULE_HOT_UNLOAD_ADMIN_CAPABILITY],
    )
    .require_local_permission("remote-management")
    .observable("module.hot-unload", "admin");
    registry
        .to_builder()
        .register_dynamic("module-hot-unload", route, move |mut context, request| {
            let paths = paths.clone();
            async move {
                let payload: sinomo_module::ModuleHotUnloadRequest =
                    context.decode_request_body(&request).await?;
                payload.validate().map_err(ServiceError::Service)?;
                let module_id = sinomo_module::ModuleId::new(payload.module_id.clone())
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                let host = DAEMON_SERVICE_REGISTRY_HOSTS
                    .get_or_init(|| Mutex::new(HashMap::new()))
                    .lock()
                    .map_err(|_| {
                        ServiceError::Service(
                            "daemon service registry cache lock poisoned".to_string(),
                        )
                    })?
                    .get(&paths.root)
                    .cloned()
                    .ok_or_else(|| {
                        ServiceError::Service(
                            "daemon module hot-unload host is unavailable".to_string(),
                        )
                    })?;
                let report = host
                    .hot_unload_module(
                        &module_id,
                        Duration::from_millis(payload.drain_timeout_ms),
                        &payload.reason,
                    )
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                context.respond_value(sinomo_module::MODULE_HOT_UNLOAD_ROUTE, report)?;
                Ok(ServiceDisposition::Continue)
            }
        })
        .build()
}

fn persist_non_dylib_hot_unload_state(
    data_dir: &SinomoDataDir,
    registration: &sinomo_module::ModuleRegistration,
    reason: &str,
) -> Result<String, DaemonError> {
    let module_id = registration.module_id();
    let detail = match &registration.backend {
        sinomo_module::ModuleBackendRegistration::NativeRust { .. } => {
            sinomo_module::set_native_rust_module_enabled(
                &data_dir.root(),
                module_id,
                false,
                "daemon-hot-unload",
                reason,
            )
            .map_err(|source| DaemonError::Network {
                address: "daemon native Rust module hot unload state".to_string(),
                detail: source.to_string(),
            })?;
            "persisted disabled native-Rust runtime state".to_string()
        }
        sinomo_module::ModuleBackendRegistration::Declarative { .. } => {
            if let Some(mut state) =
                sinomo_module::read_declarative_module_loader_state(&data_dir.root(), module_id)
                    .map_err(|source| DaemonError::Network {
                        address: "daemon declarative module hot unload state".to_string(),
                        detail: source.to_string(),
                    })?
            {
                state.active = false;
                state.source = format!("daemon-hot-unload: {reason}");
                sinomo_module::write_declarative_module_loader_state(&data_dir.root(), &state)
                    .map_err(|source| DaemonError::Network {
                        address: "daemon declarative module hot unload state".to_string(),
                        detail: source.to_string(),
                    })?;
                "persisted inactive declarative loader state".to_string()
            } else {
                "declarative loader state was already absent".to_string()
            }
        }
        sinomo_module::ModuleBackendRegistration::SandboxProcess { .. }
        | sinomo_module::ModuleBackendRegistration::WasmComponent { .. } => {
            "backend has no durable applied-state adapter; logical unload is process-scoped"
                .to_string()
        }
        sinomo_module::ModuleBackendRegistration::NativeDylib { .. } => {
            unreachable!("native dylib persistence is handled with physical unload")
        }
    };
    let record = sinomo_module::ModuleUnloadRecord {
        module_id: module_id.clone(),
        backend: registration.manifest.kind.clone(),
        artifact_id: None,
        content_hash: None,
        cache_key: None,
        library_name: None,
        state_removed: true,
        runtime_unload_attempted: false,
        runtime_unload_succeeded: true,
        runtime_unload_detail: format!("{detail}; reason={reason}"),
        unloaded_at: sinomo_module::module_loader_applied_at_now(),
        source: "daemon-hot-unload".to_string(),
    };
    sinomo_module::write_module_unload_record(&data_dir.root(), &record).map_err(|source| {
        DaemonError::Network {
            address: "daemon module hot unload record".to_string(),
            detail: source.to_string(),
        }
    })?;
    Ok(format!("{module_id}: {detail}"))
}

fn hot_unload_native_dylib_runtime(
    data_dir: &SinomoDataDir,
    module_id: &sinomo_module::ModuleId,
    reason: &str,
) -> Result<(bool, bool, String), DaemonError> {
    let previous =
        sinomo_module::remove_native_dylib_module_loader_state(&data_dir.root(), module_id)
            .map_err(|source| DaemonError::Network {
                address: "daemon native module hot unload state".to_string(),
                detail: source.to_string(),
            })?;
    let Some(state) = previous else {
        return Ok((
            false,
            true,
            "native module loader state was already absent after logical unload".to_string(),
        ));
    };
    let path = sinomo_module::native_module_cache_path(
        data_dir.root(),
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    let (attempted, succeeded, detail) = if path.exists() {
        match unsafe {
            sinomo_module::NativeModule::load_from_content_addressed_path(
                &path,
                &state.content_hash,
            )
        } {
            Ok(module) => {
                let report = module.unload();
                (
                    true,
                    report.shutdown_ok && report.library_handle_released,
                    format!(
                        "{}; library_handle_released={}",
                        report.detail, report.library_handle_released
                    ),
                )
            }
            Err(error) => (
                true,
                false,
                format!("load cached native module for shutdown: {error}"),
            ),
        }
    } else {
        (
            false,
            false,
            format!(
                "cached native module artifact {} is missing; logical unload completed but physical unmapping could not be verified",
                path.display()
            ),
        )
    };
    let record_detail = format!("{detail}; reason={reason}");
    let record = sinomo_module::ModuleUnloadRecord {
        module_id: module_id.clone(),
        backend: sinomo_core::ModuleKind::NativeDylib,
        artifact_id: state.artifact_id,
        content_hash: Some(state.content_hash),
        cache_key: Some(state.cache_key),
        library_name: Some(state.library_name),
        state_removed: true,
        runtime_unload_attempted: attempted,
        runtime_unload_succeeded: succeeded,
        runtime_unload_detail: record_detail,
        unloaded_at: sinomo_module::module_loader_applied_at_now(),
        source: "daemon-hot-unload".to_string(),
    };
    sinomo_module::write_module_unload_record(&data_dir.root(), &record).map_err(|source| {
        DaemonError::Network {
            address: "daemon native module hot unload record".to_string(),
            detail: source.to_string(),
        }
    })?;
    Ok((attempted, succeeded, detail))
}

async fn daemon_service_registry_on_blocking_pool(
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    supplied_catalog: ComponentCatalog,
) -> Result<DaemonServiceAssembly, DaemonError> {
    tokio::task::spawn_blocking(move || {
        let event_hub = Arc::new(DaemonEventHub::new());
        let component_catalog = daemon_component_catalog(
            &paths,
            Some(&service_runtime),
            &supplied_catalog,
            &event_hub,
        )
        .map_err(|error| DaemonError::Network {
            address: "daemon component catalog".to_string(),
            detail: error.to_string(),
        })?;
        let (base_registry, event_hub) = daemon_base_service_registry(
            &paths,
            Some(&service_runtime),
            &component_catalog,
            event_hub,
        );
        Ok(DaemonServiceAssembly {
            base_registry,
            component_catalog,
            event_hub,
        })
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "daemon service registry".to_string(),
        detail: format!("daemon service registry task failed: {source}"),
    })?
}

pub(crate) fn component_catalog_from_factories(
    factories: Vec<crate::DaemonComponentFactory>,
) -> Result<ComponentCatalog, DaemonError> {
    let mut builder = ComponentCatalog::builder();
    for factory in factories {
        builder = builder
            .register_factory(factory)
            .map_err(|error| DaemonError::Network {
                address: "daemon supplied component catalog".to_string(),
                detail: error.to_string(),
            })?;
    }
    builder.build().map_err(|error| DaemonError::Network {
        address: "daemon supplied component catalog".to_string(),
        detail: error.to_string(),
    })
}

fn daemon_config_registry(component_catalog: &ComponentCatalog) -> ConfigRegistry {
    let mut builder = register_daemon_config_definitions(ConfigRegistry::builder())
        .expect("daemon config definitions must be valid");
    builder = component_catalog
        .register_config_definitions(builder)
        .expect("daemon component config definitions must be valid");
    builder.build()
}

fn register_daemon_config_definitions(
    builder: ConfigRegistryBuilder,
) -> Result<ConfigRegistryBuilder, String> {
    sinomo_module::register_config_definitions(builder).map_err(|error| error.to_string())
}

/// Captures the exactly-once service response for the network session adapter.
struct BufferSink {
    response: Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>,
}

impl ResponseSink for BufferSink {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
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

fn daemon_route_local_permissions_are_granted(
    paths: &DaemonPaths,
    descriptor: &ServiceRoute,
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

/// Authenticates a request against the live route catalog. Catalog policy is
/// authoritative for dynamic routes; routes without policy metadata use the
/// static authorization table in `auth.rs`.
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
    let authenticated = crate::auth::authenticate_daemon_control_request_for_service(
        paths,
        request,
        descriptor.exact_member_identity,
    )?;
    if authenticated.authorizes_service_route_with(&descriptor, |descriptor| {
        daemon_route_local_permissions_are_granted(paths, descriptor)
    }) {
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

#[cfg(test)]
fn module_object_rpc_route_descriptor_to_daemon_route(
    descriptor: &sinomo_module::ModuleObjectRpcRouteDescriptor,
) -> ServiceRoute {
    sinomo_module::module_object_rpc_route_descriptor_to_service(descriptor)
        .observable("daemon.service_registry", "true")
        .observable("daemon.module_route", "true")
}

async fn dispatch_service_registry_request(
    registry: Arc<ServiceRegistry>,
    data_dir: Arc<SinomoDataDir>,
    cluster: Option<Arc<dyn ClusterAccess>>,
    signer: ServiceSigner,
    request: ObjectRpcIncomingRequest,
) -> Result<ObjectRpcOutgoingResponse, NetworkError> {
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
        .dispatch(ctx, request.request)
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

fn daemon_error_to_network_error(error: DaemonError) -> NetworkError {
    NetworkError::Serialization {
        detail: error.to_string(),
    }
}

fn annotate_daemon_service_request(
    request: &mut ObjectRpcRequest,
    authenticated: &AuthenticatedDaemonCommand,
) {
    request.metadata.insert(
        DAEMON_SERVICE_SIGNER_AUTHORITY_METADATA_KEY.to_string(),
        authenticated.signer.metadata_value().to_string(),
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

const DAEMON_SERVICE_SIGNER_AUTHORITY_METADATA_KEY: &str = "daemon.service.signer.authority";
const DAEMON_SERVICE_SIGNER_NODE_ID_METADATA_KEY: &str = "daemon.service.signer.node_id";
const DAEMON_SERVICE_SIGNER_PUBLIC_KEY_METADATA_KEY: &str = "daemon.service.signer.public_key";
const DAEMON_SERVICE_DISPOSITION_METADATA_KEY: &str = "daemon.service.disposition";

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

pub(crate) async fn try_dispatch_pre_auth_via_registry_async(
    request: &DaemonControlRequest,
    stream: &mut tokio::net::TcpStream,
    service_host: &DaemonServiceRegistryHost,
) -> Result<bool, DaemonError> {
    match dispatch_pre_auth_via_registry_to_response_async(request, service_host).await {
        Ok(Some(response)) => {
            sinomo_core::network::write_object_frame_async(
                stream,
                &response,
                sinomo_core::network::ObjectFrameIoOptions::default(),
            )
            .await
            .map_err(DaemonError::from)?;
            Ok(true)
        }
        Ok(None) => Ok(false),
        Err(error) => {
            tracing::warn!("sinomo daemon pre-auth join challenge ignored: {error}");
            let _ = tokio::io::AsyncWriteExt::shutdown(stream).await;
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
    )
    .await?;
    Ok(Some(response))
}

pub(crate) async fn dispatch_rpc_via_registry_to_response_async(
    service_host: &DaemonServiceRegistryHost,
    authenticated: &AuthenticatedDaemonCommand,
    request: ObjectRpcRequest,
    paths: Option<&DaemonPaths>,
) -> Result<(ObjectRpcResponse, bool), DaemonError> {
    let (response, stop) = dispatch_rpc_via_registry_with_body_async(
        service_host,
        authenticated,
        ObjectRpcIncomingRequest::value(request),
        paths,
    )
    .await?;
    let response = response
        .into_materialized_value_response(usize::MAX)
        .await?;
    Ok((response, stop))
}

/// A known-behind member that reaches this daemon has a usable route back.
/// Its authenticated contact starts that peer's targeted Raft catch-up; the
/// catch-up replicates only while this process leads.
fn observe_cluster_peer_contact(paths: &DaemonPaths, authenticated: &AuthenticatedDaemonCommand) {
    let Some((cluster_paths, node_id)) = cluster_peer_contact_owes_catch_up(paths, authenticated)
    else {
        return;
    };
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let Ok(state) = sinomo_core::cluster::read_cluster_state(&cluster_paths) else {
        return;
    };
    crate::cluster::spawn_cluster_raft_member_catch_up(
        &runtime,
        cluster_paths,
        state,
        vec![node_id],
    );
}

fn cluster_peer_contact_owes_catch_up(
    paths: &DaemonPaths,
    authenticated: &AuthenticatedDaemonCommand,
) -> Option<(ClusterPaths, sinomo_core::node::NodeId)> {
    if !matches!(
        authenticated.signer,
        DaemonSignerAuthority::Voter | DaemonSignerAuthority::Member
    ) {
        return None;
    }
    let node_id = authenticated.signer_node_id.parse().ok()?;
    let cluster_paths = cluster_paths_from_daemon_paths(paths).ok()?;
    sinomo_core::cluster::cluster_raft_peer_is_behind(&cluster_paths, &node_id)
        .then_some((cluster_paths, node_id))
}

pub(crate) async fn dispatch_rpc_via_registry_with_body_async(
    service_host: &DaemonServiceRegistryHost,
    authenticated: &AuthenticatedDaemonCommand,
    mut request: ObjectRpcIncomingRequest,
    paths: Option<&DaemonPaths>,
) -> Result<(ObjectRpcOutgoingResponse, bool), DaemonError> {
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
        "daemon service rpc dispatch start"
    );
    annotate_daemon_service_request(&mut request.request, authenticated);
    let descriptor = service_host
        .object_rpc_route_descriptor(&route)
        .ok_or_else(|| NetworkError::UnknownRoute {
            route: route.clone(),
        })?;
    if let Some(auth_scope) = descriptor.auth_scope.as_deref()
        && !authenticated.signer.authorizes_route_scope(auth_scope)
    {
        warn_daemon_service_auth_denied(
            &route,
            auth_scope,
            &authenticated.signer,
            Some(&authenticated.signer_node_id),
            Some(&authenticated.signer_public_key),
        );
        return Err(DaemonError::AuthDenied);
    }
    if !daemon_route_local_permissions_are_granted(&service_host.paths, &descriptor) {
        return Err(DaemonError::AuthDenied);
    }
    observe_cluster_peer_contact(&service_host.paths, authenticated);
    let registry = service_host
        .service_registry
        .active_snapshot()
        .registry_arc();
    let signer = daemon_service_signer_from_request_metadata(&request.request)?;
    let mut response = dispatch_service_registry_request(
        registry,
        Arc::clone(&service_host.service_data_dir),
        service_host.service_cluster.clone(),
        signer,
        request,
    )
    .await
    .map_err(DaemonError::from)?;
    if let Some(paths) = paths {
        mark_daemon_rpc_response_responder(paths, &mut response.response);
    }
    let stop = response
        .response
        .metadata
        .remove(DAEMON_SERVICE_DISPOSITION_METADATA_KEY)
        .is_some_and(|value| value == "stop");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct TestRolloutHost;

    impl sinomo_rollout::RolloutServiceHost for TestRolloutHost {
        fn activate_binary<'a>(
            &'a self,
            _manifest: sinomo_rollout::ClusterRolloutBinaryManifest,
        ) -> sinomo_rollout::RolloutHostFuture<
            'a,
            Result<sinomo_rollout::ClusterRolloutBinaryResponse, String>,
        > {
            Box::pin(async { Err("rollout activation is not exercised".to_string()) })
        }

        fn activate_native_dylib(
            &self,
            _request: sinomo_rollout::ClusterRolloutNativeDylibModuleRequest,
        ) -> Result<sinomo_rollout::ClusterRolloutBinaryResponse, String> {
            Err("native rollout activation is not exercised".to_string())
        }

        fn build(
            &self,
            _request: sinomo_rollout::ClusterRolloutBuildRequest,
        ) -> Result<sinomo_rollout::ClusterRolloutBinaryResponse, String> {
            Err("rollout build is not exercised".to_string())
        }

        fn finalize_shadow(&self) -> Result<String, String> {
            Err("rollout finalization is not exercised".to_string())
        }

        fn clear_status(&self) -> Result<String, String> {
            Err("rollout status clearing is not exercised".to_string())
        }

        fn should_stop_after(
            &self,
            _response: &sinomo_rollout::ClusterRolloutBinaryResponse,
        ) -> bool {
            false
        }
    }

    fn test_rollout_component_factory() -> crate::DaemonComponentFactory {
        Arc::new(
            sinomo_rollout::component_factory(Arc::new(TestRolloutHost))
                .expect("rollout test component factory"),
        )
    }

    #[test]
    fn daemon_error_preserves_cluster_control_plane_identity() {
        assert!(matches!(
            daemon_error(DaemonError::ClusterReadOnly {
                detail: "forwarded authoritative cluster KV read reached a non-leader voter"
                    .to_string(),
            }),
            ServiceError::Network(NetworkError::ClusterReadOnly { detail })
                if detail.contains("non-leader voter")
        ));
        assert!(matches!(
            daemon_error(DaemonError::AuthDenied),
            ServiceError::Network(NetworkError::AuthDenied)
        ));
    }

    fn remote_cluster_kv_auth_fixture() -> (
        sinomo_core::kv::InProcessKvStore,
        ServiceSigner,
        sinomo_core::ClusterMemberRecord,
        sinomo_core::node::ClusterCapabilityPolicyRecord,
    ) {
        let cluster_id =
            sinomo_core::Id::from_legacy_named("cluster_remote_kv_auth").expect("cluster id");
        let node_id =
            sinomo_core::node::NodeId::from_legacy_named("node_remote_kv_auth").expect("node id");
        let mut member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id,
            node_id.clone(),
            "remote",
            "remote-public-key",
            time::OffsetDateTime::UNIX_EPOCH,
        );
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        let policy = sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member);
        let signer = ServiceSigner::Remote {
            node_id: node_id.to_string(),
            public_key: member.node_public_key.clone(),
        };
        let mut store = sinomo_core::kv::InProcessKvStore::new();
        store
            .put(
                sinomo_core::node::cluster_member_key(&node_id),
                sinomo_core::kv::encode_kv_value(&member).expect("encode member"),
                None,
            )
            .expect("seed member");
        store
            .put(
                sinomo_core::node::cluster_capability_policy_key(&node_id),
                sinomo_core::kv::encode_kv_value(&policy).expect("encode policy"),
                None,
            )
            .expect("seed policy");
        (store, signer, member, policy)
    }

    fn remote_cluster_kv_test_request() -> sinomo_core::kv::KvTxnRequest {
        sinomo_core::kv::KvTxnRequest::generated(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key: "/test/remote-authorized".to_string(),
                value: b"committed".to_vec(),
                lease_id: None,
            }],
            failure: Vec::new(),
        })
    }

    fn remote_cluster_kv_auth_entries<'a>(
        snapshot: &'a sinomo_core::kv::KvSnapshot,
        signer: &ServiceSigner,
    ) -> (Option<&'a KvEntry>, Option<&'a KvEntry>) {
        let (member_key, policy_key) =
            remote_cluster_kv_authorization_keys(signer).expect("authorization keys");
        (snapshot.get(&member_key), snapshot.get(&policy_key))
    }

    fn assert_remote_cluster_kv_auth_denied(error: ServiceError) {
        assert!(
            matches!(error, ServiceError::Network(NetworkError::AuthDenied)),
            "unexpected authorization error: {error}"
        );
    }

    #[test]
    fn remote_cluster_kv_txn_valid_origin_is_fenced_and_commits() {
        let (mut store, signer, member, _) = remote_cluster_kv_auth_fixture();
        let snapshot = store.snapshot().expect("authority snapshot");
        let (member_entry, policy_entry) = remote_cluster_kv_auth_entries(&snapshot, &signer);
        let request = fence_remote_cluster_kv_txn_request_with_entries(
            member_entry,
            policy_entry,
            &signer,
            remote_cluster_kv_test_request(),
        )
        .expect("authorized request");

        assert_eq!(request.txn.compares.len(), 4);
        assert!(request.txn.compares.iter().any(|compare| {
            matches!(compare, sinomo_core::kv::KvCompare::ModifyRevisionEquals { key, .. }
                if key == &sinomo_core::node::cluster_member_key(&member.node_id))
        }));
        assert!(request.txn.compares.iter().any(|compare| {
            matches!(compare, sinomo_core::kv::KvCompare::ValueEquals { key, .. }
                if key == &sinomo_core::node::cluster_member_key(&member.node_id))
        }));
        assert!(request.txn.compares.iter().any(|compare| {
            matches!(compare, sinomo_core::kv::KvCompare::ModifyRevisionEquals { key, .. }
                if key == &sinomo_core::node::cluster_capability_policy_key(&member.node_id))
        }));
        assert!(request.txn.compares.iter().any(|compare| {
            matches!(compare, sinomo_core::kv::KvCompare::ValueEquals { key, .. }
                if key == &sinomo_core::node::cluster_capability_policy_key(&member.node_id))
        }));
        let response = store.txn(request.txn).expect("commit fenced transaction");
        assert!(response.succeeded);
        assert_eq!(
            store
                .get("/test/remote-authorized")
                .expect("committed entry")
                .value,
            b"committed"
        );
    }

    #[test]
    fn remote_cluster_kv_txn_rejects_revoked_member_and_stale_key() {
        let (mut store, signer, mut member, _) = remote_cluster_kv_auth_fixture();
        member.state = sinomo_core::ClusterMemberState::Removed;
        store
            .put(
                sinomo_core::node::cluster_member_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&member).expect("encode removed member"),
                None,
            )
            .expect("store removed member");
        let snapshot = store.snapshot().expect("removed snapshot");
        let (member_entry, policy_entry) = remote_cluster_kv_auth_entries(&snapshot, &signer);
        let error = fence_remote_cluster_kv_txn_request_with_entries(
            member_entry,
            policy_entry,
            &signer,
            remote_cluster_kv_test_request(),
        )
        .expect_err("removed signer must be denied");
        assert_remote_cluster_kv_auth_denied(error);

        let (mut store, signer, mut member, _) = remote_cluster_kv_auth_fixture();
        member.node_public_key = "replacement-public-key".to_string();
        store
            .put(
                sinomo_core::node::cluster_member_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&member).expect("encode replaced key"),
                None,
            )
            .expect("store replaced key");
        let snapshot = store.snapshot().expect("rekeyed snapshot");
        let (member_entry, policy_entry) = remote_cluster_kv_auth_entries(&snapshot, &signer);
        let error = fence_remote_cluster_kv_txn_request_with_entries(
            member_entry,
            policy_entry,
            &signer,
            remote_cluster_kv_test_request(),
        )
        .expect_err("stale signer key must be denied");
        assert_remote_cluster_kv_auth_denied(error);
    }

    #[test]
    fn remote_cluster_kv_txn_rejects_policy_without_write_capability() {
        let (mut store, signer, member, mut policy) = remote_cluster_kv_auth_fixture();
        policy.capabilities.retain(|capability| {
            capability.as_str() != sinomo_core::kv::CLUSTER_KV_WRITE_CAPABILITY
        });
        policy.allowed_capabilities.retain(|capability| {
            capability.as_str() != sinomo_core::kv::CLUSTER_KV_WRITE_CAPABILITY
        });
        store
            .put(
                sinomo_core::node::cluster_capability_policy_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&policy).expect("encode restricted policy"),
                None,
            )
            .expect("store restricted policy");
        let snapshot = store.snapshot().expect("restricted snapshot");
        let (member_entry, policy_entry) = remote_cluster_kv_auth_entries(&snapshot, &signer);
        let error = fence_remote_cluster_kv_txn_request_with_entries(
            member_entry,
            policy_entry,
            &signer,
            remote_cluster_kv_test_request(),
        )
        .expect_err("missing write capability must be denied");
        assert_remote_cluster_kv_auth_denied(error);
    }

    #[test]
    fn remote_cluster_kv_txn_policy_revocation_race_fails_cas_without_mutation() {
        let (mut store, signer, member, mut policy) = remote_cluster_kv_auth_fixture();
        let snapshot = store.snapshot().expect("authority snapshot");
        let (member_entry, policy_entry) = remote_cluster_kv_auth_entries(&snapshot, &signer);
        let request = fence_remote_cluster_kv_txn_request_with_entries(
            member_entry,
            policy_entry,
            &signer,
            remote_cluster_kv_test_request(),
        )
        .expect("authorized request");

        policy.allowed_capabilities.retain(|capability| {
            capability.as_str() != sinomo_core::kv::CLUSTER_KV_WRITE_CAPABILITY
        });
        store
            .put(
                sinomo_core::node::cluster_capability_policy_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&policy).expect("encode revoked policy"),
                None,
            )
            .expect("commit revocation before requester transaction");

        let response = store.txn(request.txn).expect("evaluate fenced transaction");
        assert!(
            !response.succeeded,
            "revocation must fail the authority CAS"
        );
        assert!(store.get("/test/remote-authorized").is_none());
    }

    #[test]
    fn remote_cluster_kv_txn_policy_aba_race_fails_revision_guard() {
        let (mut store, signer, member, policy) = remote_cluster_kv_auth_fixture();
        let snapshot = store.snapshot().expect("authority snapshot");
        let (member_entry, policy_entry) = remote_cluster_kv_auth_entries(&snapshot, &signer);
        let request = fence_remote_cluster_kv_txn_request_with_entries(
            member_entry,
            policy_entry,
            &signer,
            remote_cluster_kv_test_request(),
        )
        .expect("authorized request");

        let policy_key = sinomo_core::node::cluster_capability_policy_key(&member.node_id);
        let mut revoked = policy.clone();
        revoked.allowed_capabilities.retain(|capability| {
            capability.as_str() != sinomo_core::kv::CLUSTER_KV_WRITE_CAPABILITY
        });
        store
            .put(
                policy_key.clone(),
                sinomo_core::kv::encode_kv_value(&revoked).expect("encode revoked policy"),
                None,
            )
            .expect("commit policy revocation");
        store
            .put(
                policy_key,
                sinomo_core::kv::encode_kv_value(&policy).expect("encode restored policy"),
                None,
            )
            .expect("commit policy restoration");

        let response = store.txn(request.txn).expect("evaluate fenced transaction");
        assert!(
            !response.succeeded,
            "an authorization revoke-and-restore cycle must invalidate an older request"
        );
        assert!(store.get("/test/remote-authorized").is_none());
    }

    #[test]
    fn remote_cluster_kv_txn_rejects_failure_branch_mutations() {
        let (store, signer, _, _) = remote_cluster_kv_auth_fixture();
        let mut request = remote_cluster_kv_test_request();
        request.txn.failure.push(KvOp::Put {
            key: "/test/remote-failure".to_string(),
            value: b"must-not-run".to_vec(),
            lease_id: None,
        });
        let snapshot = store.snapshot().expect("authority snapshot");
        let (member_entry, policy_entry) = remote_cluster_kv_auth_entries(&snapshot, &signer);
        let error = fence_remote_cluster_kv_txn_request_with_entries(
            member_entry,
            policy_entry,
            &signer,
            request,
        )
        .expect_err("failure branch must be rejected");
        assert!(
            error
                .to_string()
                .contains("authorization guards must fail closed")
        );
    }

    #[test]
    fn main_forward_routes_enforce_affinity_and_legacy_routes_reject_it() {
        let mut payload = crate::DaemonObjectRpcForwardRequest {
            target_node_id: sinomo_core::node::NodeId::from_legacy_named(
                "node_daemon_main_forward_route",
            )
            .expect("node id"),
            request: ObjectRpcRequest::encode_body("/test/main-forward-route", "test", &())
                .expect("request"),
            streamed_value_body: false,
            connect_timeout_millis: 5_000,
            idle_timeout_millis: None,
            overall_timeout_millis: None,
            observe_connectivity: false,
            destination_process_affinity: crate::DaemonObjectRpcDestinationProcessAffinity::Any,
        };

        enforce_daemon_object_rpc_forward_route_affinity(
            DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
            &mut payload,
        )
        .expect("main route must promote a legacy/default payload");
        assert_eq!(
            payload.destination_process_affinity,
            crate::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
        );
        assert!(matches!(
            enforce_daemon_object_rpc_forward_route_affinity(
                DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                &mut payload,
            ),
            Err(NetworkError::InvalidClusterCommand { .. })
        ));
    }

    #[tokio::test]
    async fn old_forward_registry_rejects_a_main_route_before_dispatch() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-old-forward-registry");
        let dispatches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registry = ServiceRegistry::builder()
            .register(LegacyForwardOnlyService {
                dispatches: Arc::clone(&dispatches),
            })
            .build();
        let host = test_service_host(&root, registry);
        let payload = crate::DaemonObjectRpcForwardRequest {
            target_node_id: sinomo_core::node::NodeId::from_legacy_named(
                "node_daemon_old_forward_registry",
            )
            .expect("node id"),
            request: ObjectRpcRequest::encode_body("/test/must-not-dispatch", "test", &())
                .expect("inner request"),
            streamed_value_body: false,
            connect_timeout_millis: 5_000,
            idle_timeout_millis: None,
            overall_timeout_millis: None,
            observe_connectivity: false,
            destination_process_affinity:
                crate::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
        };
        let request = ObjectRpcRequest::encode_body(
            DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
            DAEMON_RPC_CATEGORY,
            &payload,
        )
        .expect("main forward request");

        let error = call_value_service_host_async(&host, request)
            .await
            .expect_err("an old registry must not understand the main-only route");
        assert!(matches!(
            error,
            NetworkError::UnknownRoute { route }
                if route == DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE
        ));
        assert_eq!(
            dispatches.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "route rejection must happen before any legacy forward handler can dispatch the inner request"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn daemon_forward_delegates_a_local_main_target_to_the_main_listener() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-local-main-forward");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let main_paths = DaemonPaths::new(&data_dir);
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        main_paths.ensure().expect("main daemon paths");
        shadow_paths.ensure().expect("shadow daemon paths");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let local_node =
            sinomo_core::node::NodeId::from_legacy_named("node_daemon_local_main_forward")
                .expect("local node");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_local_main_forward")
                .expect("cluster id"),
            "daemon local main forward",
            local_node.clone(),
            "local",
            identity.public_key,
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("main listener");
        let address = listener.local_addr_string().expect("main listener address");
        let port = address
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .expect("main listener port");
        let cluster = crate::daemon_cluster_status(&state, crate::DaemonClusterProcess::Main)
            .expect("main cluster status");
        let main_status = crate::DaemonStatus::new(
            &crate::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port,
                port_fallback: false,
            },
            &main_paths,
            Some(cluster),
        );
        crate::write_daemon_status(&main_paths, &main_status).expect("main status");

        let expected_request_body = vec![0x41_u8; 512 * 1024 + 17];
        let response_body = vec![0x52_u8; 384 * 1024 + 29];
        let server_request_body = expected_request_body.clone();
        let server_response_body = response_body.clone();
        let server_status = main_status.clone();
        let server = tokio::spawn(async move {
            for expected_route in [DAEMON_STATUS_ROUTE, "/test/local-main-only"] {
                let mut stream = listener
                    .accept()
                    .await
                    .expect("accept main local carrier")
                    .into_inner();
                let carrier_context =
                    sinomo_core::network::read_local_object_rpc_carrier_prelude(&mut stream)
                        .await
                        .expect("main local carrier prelude");
                let mut acceptor = sinomo_core::network::accept_local_object_rpc_stream_carrier(
                    Box::new(stream),
                    &carrier_context,
                )
                .expect("main local carrier");
                let logical = acceptor
                    .accept()
                    .await
                    .expect("accept main logical stream")
                    .expect("main logical stream");
                let context = sinomo_core::network::RpcPeerContext::unauthenticated_local(
                    logical.session_id.clone(),
                    logical.channel_binding,
                );
                let status = server_status.clone();
                let request_body = server_request_body.clone();
                let response_body = server_response_body.clone();
                sinomo_core::network::serve_service_rpc_peer_stream(
                    logical,
                    &context,
                    sinomo_core::network::ObjectFrameIoOptions::default(),
                    move |incoming, _peer| async move {
                        assert_eq!(incoming.request.route, expected_route);
                        if expected_route == DAEMON_STATUS_ROUTE {
                            return ObjectRpcOutgoingResponse::encode_value(
                                DAEMON_STATUS_ROUTE,
                                status,
                            );
                        }
                        assert_eq!(
                            incoming
                                .body
                                .collect_to_vec(1024 * 1024)
                                .await
                                .expect("forwarded inner request body"),
                            request_body,
                        );
                        Ok(ObjectRpcOutgoingResponse::new(
                            ObjectRpcResponse::encode_body(expected_route, &"main-ok")
                                .expect("inner response"),
                            ObjectRpcBodySource::bytes(response_body),
                        ))
                    },
                )
                .await
                .expect("serve main local RPC");
            }
        });

        let payload = crate::DaemonObjectRpcForwardRequest {
            target_node_id: local_node,
            request: ObjectRpcRequest::encode_body("/test/local-main-only", "test", &"request")
                .expect("inner request"),
            streamed_value_body: false,
            connect_timeout_millis: 2_000,
            idle_timeout_millis: Some(2_000),
            overall_timeout_millis: Some(10_000),
            observe_connectivity: false,
            destination_process_affinity:
                crate::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
        };
        let (forwarded, body) = forward_object_rpc_call_from_daemon_control(
            &shadow_paths,
            payload,
            sinomo_core::network::ObjectRpcIncomingBody::bytes(expected_request_body),
            &tokio::runtime::Handle::current(),
        )
        .await
        .expect("local main delegation");
        let crate::DaemonObjectRpcForwardResponse::Success {
            response,
            streamed_value_body,
            ..
        } = forwarded
        else {
            panic!("local main delegation returned a network error");
        };
        assert!(!streamed_value_body);
        assert_eq!(
            response
                .decode_body::<String>()
                .expect("inner response value"),
            "main-ok"
        );
        assert_eq!(
            body.collect_to_vec(1024 * 1024)
                .await
                .expect("inner response attachment"),
            response_body,
        );
        server.await.expect("main listener task");
    }

    #[tokio::test]
    async fn module_observations_are_projected_through_the_local_daemon_route() {
        let root = sinomo_testdir::TestDir::new("daemon-module-observations");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().expect("daemon paths");
        let module_id =
            sinomo_module::ModuleId::from_legacy_named("module.fixture").expect("module ID");
        let state_path = sinomo_module::set_native_rust_module_enabled(
            &data_dir.root(),
            &module_id,
            false,
            "test",
            "operator disabled fixture",
        )
        .expect("persist module setting");
        let host =
            DaemonServiceRegistryHost::from_registry_for_paths(ServiceRegistry::default(), &paths)
                .expect("daemon service host");
        let descriptor = host
            .object_rpc_route_descriptor(sinomo_module::MODULE_OBSERVATIONS_ROUTE)
            .expect("observation route registered");
        assert_eq!(descriptor.auth_scope.as_deref(), Some("admin"));
        let request = ObjectRpcRequest::encode_body(
            sinomo_module::MODULE_OBSERVATIONS_ROUTE,
            sinomo_module::MODULE_UI_RPC_CATEGORY,
            &sinomo_module::ModuleObservationsRequest::default(),
        )
        .expect("request");
        let response: sinomo_module::ModuleObservationsResponse =
            call_value_service_host_async(&host, request.clone())
                .await
                .expect("module RPC")
                .decode_body()
                .expect("observation contract");
        let states = response.native_rust.expect("native Rust observation");
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].module_id, module_id);
        assert!(!states[0].enabled);
        assert!(
            response
                .native_dylib
                .expect("dylib observations")
                .is_empty()
        );
        assert!(
            response
                .inactive_cache
                .expect("cache observations")
                .is_empty()
        );

        std::fs::write(state_path, b"malformed module state").expect("corrupt fixture");
        let response: sinomo_module::ModuleObservationsResponse =
            call_value_service_host_async(&host, request)
                .await
                .expect("partial observation RPC")
                .decode_body()
                .expect("observation contract");
        assert!(response.native_rust.is_err());
        assert!(response.native_dylib.is_ok());
        assert!(response.declarative.is_ok());
    }

    async fn call_value_service_host_async(
        host: &DaemonServiceRegistryHost,
        request: sinomo_core::network::ObjectRpcRequest,
    ) -> Result<sinomo_core::network::ObjectRpcResponse, sinomo_core::network::NetworkError> {
        let response = dispatch_service_registry_request(
            host.service_registry.active_snapshot().registry_arc(),
            Arc::clone(&host.service_data_dir),
            host.service_cluster.clone(),
            ServiceSigner::Local,
            ObjectRpcIncomingRequest::value(request),
        )
        .await?;
        response.into_materialized_value_response(usize::MAX).await
    }

    async fn call_authorized_value_service_host_async(
        host: &DaemonServiceRegistryHost,
        signer: DaemonSignerAuthority,
        request: ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, DaemonError> {
        dispatch_rpc_via_registry_to_response_async(
            host,
            &test_authenticated_command(signer),
            request,
            None,
        )
        .await
        .map(|(response, _stop)| response)
    }

    #[test]
    fn module_route_local_permissions_propagate_to_daemon_catalog_descriptor() {
        let descriptor = sinomo_module::ModuleObjectRpcRoute::new(
            "/remote/status",
            "remote.management",
            "remote.management.status.read",
            sinomo_module::ModuleRouteAuth::Admin,
        )
        .with_request_body_mode(sinomo_module_api::NativeModuleBodyMode::Optional)
        .with_response_body_mode(sinomo_module_api::NativeModuleBodyMode::Required)
        .require_local_permissions(["remote-management", "device:display"])
        .descriptor()
        .expect("module route descriptor");

        let daemon = module_object_rpc_route_descriptor_to_daemon_route(&descriptor);

        assert_eq!(
            daemon.required_local_permissions,
            vec!["remote-management", "device:display"]
        );
        assert_eq!(daemon.request_body_mode, ServiceBodyMode::Optional);
        assert_eq!(daemon.response_body_mode, ServiceBodyMode::Required);
    }

    #[test]
    fn native_dylib_request_body_streams_to_private_staging_without_aggregate_cap() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-native-request-staging");
        let staging = NativeDylibSessionStaging::create(
            &root,
            ServiceBodyMode::Optional,
            ServiceBodyMode::None,
        )
        .expect("create staging");
        let staging_root = staging.root().to_path_buf();
        let request_path = staging.request_body_path().expect("request path");
        let payload = (0..(2 * 1024 * 1024 + 19))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let expected_digest = format!("blake3:{}", blake3::hash(&payload).to_hex());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let descriptor = runtime
            .block_on(stage_native_dylib_request_body(
                sinomo_core::network::ObjectRpcIncomingBody::bytes(payload.clone()),
                request_path,
                &DaemonOperationCancellationToken::new(),
            ))
            .expect("stage request body");

        assert_eq!(descriptor.length, payload.len() as u64);
        assert_eq!(descriptor.blake3, expected_digest);
        assert_eq!(
            std::fs::read(request_path).expect("read staged body"),
            payload
        );
        drop(staging);
        assert!(!staging_root.exists());
    }

    #[test]
    fn native_dylib_request_body_modes_reject_none_and_empty_required_bodies() {
        assert!(
            validate_native_dylib_request_body_mode("/native/value", ServiceBodyMode::None, false,)
                .is_ok()
        );
        assert!(
            validate_native_dylib_request_body_mode(
                "/native/optional",
                ServiceBodyMode::Optional,
                false,
            )
            .is_ok()
        );
        assert!(
            validate_native_dylib_request_body_mode(
                "/native/required",
                ServiceBodyMode::Required,
                true,
            )
            .is_ok()
        );
        assert!(
            validate_native_dylib_request_body_mode("/native/value", ServiceBodyMode::None, true,)
                .expect_err("none mode must reject an attachment")
                .to_string()
                .contains("does not accept")
        );
        assert!(
            validate_native_dylib_request_body_mode(
                "/native/required",
                ServiceBodyMode::Required,
                false,
            )
            .expect_err("required mode must reject an absent or zero-byte attachment")
            .to_string()
            .contains("non-empty")
        );

        // A streaming reader may advertise an unknown length and then reach
        // EOF without yielding bytes. The post-drain staged descriptor is the
        // authoritative check for Required mode.
        let empty_staged = sinomo_module_api::NativeModuleStagedBody::new(
            "/private/request.body",
            0,
            format!("blake3:{}", blake3::hash(&[]).to_hex()),
        );
        assert!(
            validate_native_dylib_staged_request_body(
                "/native/required",
                ServiceBodyMode::Required,
                Some(&empty_staged),
            )
            .expect_err("required mode must reject a drained zero-byte reader")
            .to_string()
            .contains("non-empty")
        );
        assert!(
            validate_native_dylib_staged_request_body(
                "/native/optional",
                ServiceBodyMode::Optional,
                Some(&empty_staged),
            )
            .is_ok()
        );
    }

    #[test]
    fn native_dylib_request_staging_observes_operation_cancellation() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-native-request-cancel");
        let staging = NativeDylibSessionStaging::create(
            &root,
            ServiceBodyMode::Optional,
            ServiceBodyMode::None,
        )
        .expect("create staging");
        let request_path = staging.request_body_path().expect("request path");
        let cancellation = DaemonOperationCancellationToken::new();
        cancellation.cancel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let error = runtime
            .block_on(stage_native_dylib_request_body(
                sinomo_core::network::ObjectRpcIncomingBody::bytes(vec![0x5a; 1024]),
                request_path,
                &cancellation,
            ))
            .expect_err("cancelled staging must fail");
        assert!(error.to_string().contains("cancelled"));
    }

    #[test]
    fn dropping_native_dylib_dispatch_registration_cancels_and_unregisters() {
        let active = NativeModuleOperationCancellationRegistry::default();
        let registration = active
            .register("module.fixture", Some("operation.drop".to_string()))
            .expect("register operation");
        let token = registration.token();
        assert!(!token.is_cancelled());
        assert!(active.is_active("operation.drop"));

        drop(registration);

        assert!(token.is_cancelled());
        assert_eq!(active.active_len(), 0);
    }

    #[test]
    fn native_dylib_final_permission_callback_observes_live_local_revocation() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-native-permission-callback");
        let store = sinomo_core::node::LocalNodePermissionStore::new(&root);
        let callbacks = sinomo_module::DefaultNativeModuleHostCallbacks::new(
            DaemonOperationCancellationToken::new(),
            root.to_path_buf(),
        );
        let granted = |permission| {
            sinomo_module::NativeModuleHostCallbacks::local_permission_granted(
                &callbacks, permission,
            )
        };

        assert!(!granted("remote-management"));
        assert!(!granted("unknown-permission"));
        let mut permissions = sinomo_core::node::LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        store.save(&permissions).expect("grant local permission");
        assert!(granted("remote-management"));

        permissions.set_remote_management_enabled(false);
        store.save(&permissions).expect("revoke local permission");
        assert!(!granted("remote-management"));
    }

    #[test]
    fn native_dylib_response_body_requires_exact_host_path_length_and_hash() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-native-response-staging");
        let staging = NativeDylibSessionStaging::create(
            &root,
            ServiceBodyMode::None,
            ServiceBodyMode::Optional,
        )
        .expect("create staging");
        let response_path = staging.response_body_path().expect("response path");
        std::fs::write(response_path, vec![0x5a; 512 * 1024 + 7]).expect("write response body");
        let descriptor = inspect_native_dylib_staged_body(response_path).expect("descriptor");

        let validated = validate_native_dylib_response_body(
            ServiceBodyMode::Optional,
            Some(response_path),
            Some(descriptor.clone()),
        )
        .expect("validate response")
        .expect("response path");
        assert_eq!(validated, response_path);

        let mut wrong_path = descriptor.clone();
        wrong_path.path.push_str(".other");
        assert!(
            validate_native_dylib_response_body(
                ServiceBodyMode::Optional,
                Some(response_path),
                Some(wrong_path),
            )
            .expect_err("foreign path must fail")
            .to_string()
            .contains("host-assigned path")
        );
        let mut wrong_length = descriptor;
        wrong_length.length += 1;
        assert!(
            validate_native_dylib_response_body(
                ServiceBodyMode::Optional,
                Some(response_path),
                Some(wrong_length),
            )
            .expect_err("wrong length must fail")
            .to_string()
            .contains("length mismatch")
        );

        std::fs::write(response_path, []).expect("truncate response body");
        let empty = inspect_native_dylib_staged_body(response_path).expect("empty descriptor");
        assert!(
            validate_native_dylib_response_body(
                ServiceBodyMode::Required,
                Some(response_path),
                Some(empty),
            )
            .expect_err("required response body must be non-empty")
            .to_string()
            .contains("non-empty")
        );
    }

    #[test]
    fn native_dylib_response_staging_cleanup_lives_with_body_source() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-native-response-lifetime");
        let staging = NativeDylibSessionStaging::create(
            &root,
            ServiceBodyMode::None,
            ServiceBodyMode::Required,
        )
        .expect("create staging");
        let staging_root = staging.root().to_path_buf();
        let response_path = staging
            .response_body_path()
            .map(std::path::Path::to_path_buf)
            .expect("response path");
        std::fs::write(&response_path, b"response attachment").expect("write response body");
        let descriptor = inspect_native_dylib_staged_body(&response_path).expect("descriptor");
        let validated = validate_native_dylib_response_body(
            ServiceBodyMode::Required,
            Some(&response_path),
            Some(descriptor),
        )
        .expect("validate response")
        .expect("response path");
        let source = ObjectRpcBodySource::file(validated)
            .expect("file body")
            .with_lifetime_guard(staging)
            .expect("attach cleanup guard");

        assert!(staging_root.exists());
        drop(source);
        assert!(!staging_root.exists());
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

    struct LegacyForwardOnlyService {
        dispatches: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Service for LegacyForwardOnlyService {
        fn name(&self) -> &'static str {
            "legacy-daemon-forward"
        }
        fn routes(&self) -> &'static [&'static str] {
            &[
                DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            ]
        }
        fn route_categories(&self) -> Vec<ServiceRoute> {
            service_route_categories(self.routes(), DAEMON_RPC_CATEGORY)
        }
        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            self.dispatches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                ctx.respond_value(request.route, ())?;
                Ok(ServiceDisposition::Continue)
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
            vec![ServiceRoute::new(CONFIG_GET_ROUTE, "config").with_auth_scope("admin")]
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

    struct TestServiceContextSnapshotService {
        observations: Arc<Mutex<Vec<(Arc<SinomoDataDir>, bool)>>>,
    }

    impl Service for TestServiceContextSnapshotService {
        fn name(&self) -> &'static str {
            "test-service-context-snapshot"
        }
        fn routes(&self) -> &'static [&'static str] {
            &["/test/service-context-snapshot"]
        }
        fn route_categories(&self) -> Vec<ServiceRoute> {
            vec![ServiceRoute::new(
                "/test/service-context-snapshot",
                "test-service-context-snapshot",
            )]
        }
        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            let observations = Arc::clone(&self.observations);
            Box::pin(async move {
                let _: () = request.decode_body()?;
                observations
                    .lock()
                    .expect("service context observations")
                    .push((Arc::clone(&ctx.data_dir), ctx.cluster.is_some()));
                ctx.respond(ObjectRpcResponse::encode_body(request.route, &())?)?;
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

    #[tokio::test]
    async fn cluster_network_ping_attachment_dispatches_through_service_registry() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-daemon-network-ping-service-registry-attachment");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().expect("daemon paths");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            sinomo_core::node::NodeId::from_legacy_named("network_ping_service_registry_local")
                .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "network_ping_service_registry_cluster",
            )
            .expect("cluster id"),
            "network ping service registry",
            local_node.clone(),
            "local",
            "local-public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let registry = register_daemon_cluster_network_service(
            ServiceRegistry::builder(),
            &paths,
            Some(&tokio::runtime::Handle::current()),
            true,
        )
        .build();
        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service registry host");
        let descriptor = host
            .object_rpc_route_descriptor(CLUSTER_NETWORK_PING_ROUTE)
            .expect("network ping descriptor");
        assert_eq!(descriptor.request_body_mode, ServiceBodyMode::Optional);
        assert_eq!(descriptor.response_body_mode, ServiceBodyMode::Optional);

        let request_head = sinomo_core::cluster::cluster_network_ping_attachment_rpc_request(
            "registry-attachment",
            4096,
            8192,
        )
        .expect("request head");
        let request = ObjectRpcIncomingRequest::new(
            request_head,
            sinomo_core::network::ObjectRpcIncomingBody::bytes(vec![0; 4096]),
        );
        let (response, stop) = dispatch_rpc_via_registry_with_body_async(
            &host,
            &test_authenticated_command(DaemonSignerAuthority::Local),
            request,
            Some(&paths),
        )
        .await
        .expect("registry attachment response");

        assert!(!stop);
        let response_head: sinomo_core::cluster::ClusterNetworkPingResponse =
            response.response.decode_body().expect("response head");
        assert_eq!(response_head.responder_node_id, local_node);
        assert_eq!(response_head.request_attachment_bytes, 4096);
        assert_eq!(response_head.response_attachment_bytes, 8192);
        assert_eq!(response.body.total_bytes(), Some(8192));
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
        let descriptor = host
            .object_rpc_route_descriptor("/test/stop")
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
            .block_on(call_value_service_host_async(&host, {
                let mut request =
                    ObjectRpcRequest::encode_body("/test/stop", "test-stop", &()).expect("request");
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
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
        let descriptor = host
            .object_rpc_route_descriptor("/test/concurrency")
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
            .block_on(call_value_service_host_async(&host, {
                let mut request =
                    ObjectRpcRequest::encode_body("/test/concurrency", "test-concurrency", &())
                        .expect("request");
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
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
    fn daemon_service_context_is_reused_until_identity_snapshot_rebuild() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-service-context-snapshot");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let observations = Arc::new(Mutex::new(Vec::new()));
        let registry = ServiceRegistry::builder()
            .register(TestServiceContextSnapshotService {
                observations: Arc::clone(&observations),
            })
            .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry.clone());

        let call = || {
            runtime
                .block_on(call_value_service_host_async(&host, {
                    let mut request = ObjectRpcRequest::encode_body(
                        "/test/service-context-snapshot",
                        "test-service-context-snapshot",
                        &(),
                    )
                    .expect("request");
                    annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                    request
                }))
                .expect("service context response")
                .error_detail()
                .expect("service context response is successful");
        };
        call();
        cluster_paths.ensure().expect("cluster paths");
        std::fs::write(&cluster_paths.state, b"identity-now-exists")
            .expect("write cluster identity marker");
        call();

        {
            let observations = observations.lock().expect("service context observations");
            assert_eq!(observations.len(), 2);
            assert!(
                Arc::ptr_eq(&observations[0].0, &observations[1].0),
                "one route registry must reuse its resident data-directory context"
            );
            assert_eq!(
                [observations[0].1, observations[1].1],
                [false, false],
                "cluster identity changes take effect only after rebuilding the daemon registry"
            );
        }

        let rebuilt_host = test_service_host(&root, registry);
        runtime
            .block_on(call_value_service_host_async(&rebuilt_host, {
                let mut request = ObjectRpcRequest::encode_body(
                    "/test/service-context-snapshot",
                    "test-service-context-snapshot",
                    &(),
                )
                .expect("rebuilt request");
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("rebuilt service context response")
            .error_detail()
            .expect("rebuilt service context response is successful");
        let observations = observations.lock().expect("service context observations");
        assert_eq!(observations.len(), 3);
        assert!(
            observations[2].1,
            "a rebuilt route registry observes the committed cluster identity"
        );
    }

    #[test]
    fn daemon_maintenance_refresh_rpc_dispatches_component_host_change() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-maintenance-refresh-dfs");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let data_dir = SinomoDataDir::new(&root);
        std::fs::create_dir_all(data_dir.root().join("cluster")).expect("cluster watch directory");
        let paths = DaemonPaths::new(&data_dir);
        let registry = ServiceRegistry::builder()
            .register(crate::host_api::HostedDaemonOperationsService::new(
                daemon_operations_service_host(paths.clone(), Some(runtime.handle().clone())),
            ))
            .build();
        let host = test_service_host(&root, registry);
        let mut request = ObjectRpcRequest::encode_body(
            DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
            DAEMON_RPC_CATEGORY,
            &crate::DaemonClusterMaintenanceRefreshRequest,
        )
        .expect("request");
        request.auth = Some(
            crate::auth::sign_daemon_rpc_request_from_data_dir(&data_dir, &request)
                .expect("sign maintenance refresh request"),
        );
        let authenticated =
            authenticate_daemon_control_request_via_registry(&paths, &request, &host)
                .expect("authenticate local maintenance refresh request");
        assert_eq!(authenticated.signer, DaemonSignerAuthority::Local);
        annotate_daemon_service_request(&mut request, &authenticated);

        let response = runtime
            .block_on(call_value_service_host_async(&host, request))
            .expect("maintenance refresh response");
        response.error_detail().expect("maintenance refresh ok");
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
            .register(crate::host_api::HostedDaemonOperationsService::new(
                daemon_operations_service_host(paths, Some(runtime.handle().clone())),
            ))
            .build();
        let host = test_service_host(&root, registry);
        let response = runtime
            .block_on(call_value_service_host_async(&host, {
                let mut request = ObjectRpcRequest::encode_body(
                    DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
                    DAEMON_RPC_CATEGORY,
                    &crate::DaemonLocalConnectivityReloadRequest,
                )
                .expect("request");
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("connectivity reload response");

        response.error_detail().expect("connectivity reload ok");
        response
            .decode_body::<crate::DaemonLocalConnectivityReloadResponse>()
            .expect("decode connectivity reload response");
    }

    #[test]
    fn daemon_network_chunk_metrics_rpc_samples_daemon_process() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-network-chunk-metrics");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = ServiceRegistry::builder()
            .register(crate::host_api::HostedDaemonOperationsService::new(
                daemon_operations_service_host(paths, Some(runtime.handle().clone())),
            ))
            .build();
        let host = test_service_host(&root, registry);

        let response = runtime
            .block_on(call_value_service_host_async(&host, {
                let mut request = ObjectRpcRequest::encode_body(
                    DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
                    DAEMON_RPC_CATEGORY,
                    &(),
                )
                .expect("request");
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("network chunk metrics response");

        response.error_detail().expect("network chunk metrics ok");
        let snapshot = response
            .decode_body::<crate::DaemonNetworkChunkMetricsResponse>()
            .expect("decode network chunk metrics response")
            .chunk_metrics;
        let after = sinomo_core::network::network_chunk_metrics();
        assert!(snapshot.allocations <= after.allocations);
        assert!(snapshot.payload_copy_bytes <= after.payload_copy_bytes);
        assert!(snapshot.peak_active_bytes <= after.peak_active_bytes);
        assert!(snapshot.peak_allocated_bytes <= after.peak_allocated_bytes);
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
        let descriptor = host
            .object_rpc_route_descriptor(MODULE_NATIVE_DYLIB_CANCEL_ROUTE)
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

        let response = runtime
            .block_on(call_value_service_host_async(
                &host,
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
            sinomo_module::ModuleId::from_legacy_named("module.external-fixture").expect("module");
        let content_hash =
            "blake3:0000000000000000000000000000000000000000000000000000000000000000";
        let state = sinomo_module::NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: content_hash.to_string(),
            replaces_content_hash: None,
            cache_key: "module/external-fixture".to_string(),
            library_name: "external_fixture.dll".to_string(),
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
        assert!(
            host.object_rpc_route_descriptor(DAEMON_STATUS_ROUTE)
                .is_some(),
            "built-in daemon routes should still be available"
        );
        assert!(
            host.object_rpc_route_descriptor(MODULE_NATIVE_DYLIB_CANCEL_ROUTE)
                .is_some(),
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
        std::fs::create_dir_all(root.path().join("cluster")).expect("create authority watch root");
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
        let active_registry = host.service_registry.active_snapshot().registry_arc();

        let initial = active_registry
            .route_readiness("/test/holding")
            .expect("initial service route readiness");
        assert_eq!(initial.concurrency_limit, Some(1));
        assert_eq!(initial.available_concurrency, Some(1));
        assert!(initial.is_ready());

        runtime.block_on(async {
            let in_flight_host = host.clone();
            let in_flight = tokio::spawn(async move {
                let mut request = ObjectRpcRequest::encode_body("/test/holding", "holding", &())
                    .expect("request");
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                call_value_service_host_async(&in_flight_host, request).await
            });
            entered.notified().await;

            let saturated = active_registry
                .route_readiness("/test/holding")
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

        let recovered = active_registry
            .route_readiness("/test/holding")
            .expect("recovered service route readiness");
        assert_eq!(recovered.available_concurrency, Some(1));
        assert!(recovered.is_ready());
    }

    #[test]
    fn only_contact_from_a_known_behind_member_owes_raft_catch_up() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-peer-contact-catch-up");
        let paths = DaemonPaths::with_root(root.path().join("daemon"));
        let cluster_paths = cluster_paths_from_daemon_paths(&paths).expect("cluster paths");
        let behind =
            sinomo_core::node::NodeId::from_legacy_named("node_contact_behind").expect("behind");
        let current =
            sinomo_core::node::NodeId::from_legacy_named("node_contact_current").expect("current");
        sinomo_core::cluster::record_cluster_raft_peer_append_results(
            &cluster_paths,
            std::slice::from_ref(&current),
            std::slice::from_ref(&behind),
        );
        let contact = |signer: DaemonSignerAuthority, node_id: &sinomo_core::node::NodeId| {
            AuthenticatedDaemonCommand {
                route: "/cluster/kv/txn".to_string(),
                signer,
                signer_node_id: node_id.to_string(),
                signer_public_key: "pk".to_string(),
            }
        };

        assert_eq!(
            cluster_peer_contact_owes_catch_up(
                &paths,
                &contact(DaemonSignerAuthority::Member, &behind)
            )
            .map(|(_, node_id)| node_id),
            Some(behind.clone())
        );
        assert!(
            cluster_peer_contact_owes_catch_up(
                &paths,
                &contact(DaemonSignerAuthority::Voter, &current)
            )
            .is_none()
        );
        assert!(
            cluster_peer_contact_owes_catch_up(
                &paths,
                &contact(DaemonSignerAuthority::Local, &behind)
            )
            .is_none(),
            "a local command is not contact from a cluster peer"
        );
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

        let error = runtime
            .block_on(call_authorized_value_service_host_async(
                &host,
                DaemonSignerAuthority::Member,
                ObjectRpcRequest::encode_body("/test/stop", "test-stop", &()).expect("request"),
            ))
            .expect_err("member must not run admin route");

        assert!(matches!(error, DaemonError::AuthDenied));
    }

    #[test]
    fn catalog_authentication_requires_node_local_permission_for_dynamic_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dynamic-route-local-permission");
        std::fs::create_dir_all(root.path().join("cluster")).expect("create authority watch root");
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
        std::fs::create_dir_all(root.path().join("cluster")).expect("create authority watch root");
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
            runtime.block_on(call_authorized_value_service_host_async(
                host,
                DaemonSignerAuthority::Local,
                ObjectRpcRequest::encode_body("/test/stop", "test-stop", &()).expect("request"),
            ))
        };

        assert!(matches!(
            call(&host).expect_err("missing permission must deny dispatch"),
            DaemonError::AuthDenied
        ));
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
    fn daemon_configuration_uses_cluster_authority_without_host_permission() {
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
            runtime.block_on(call_authorized_value_service_host_async(
                &host,
                authority,
                ObjectRpcRequest::encode_body(CONFIG_GET_ROUTE, "config", &()).expect("request"),
            ))
        };

        let local = call(DaemonSignerAuthority::Local)
            .expect("local config remains available without remote-management permission");
        assert_eq!(
            local.decode_body::<String>().expect("local response"),
            "config-read"
        );
        call(DaemonSignerAuthority::Voter)
            .expect("cluster config authority does not require host access");
        assert!(matches!(
            call(DaemonSignerAuthority::Member)
                .expect_err("ordinary membership is not config admin authority"),
            DaemonError::AuthDenied
        ));

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
        call(DaemonSignerAuthority::Voter)
            .expect("revoking host access does not revoke ordinary configuration authority");
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
        let mut cluster_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_service_snapshot")
                .expect("cluster id"),
            "service snapshot test",
            node_id,
            "service snapshot node",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        cluster_state.members[0].capabilities = vec![
            sinomo_core::base::CapabilityName::new(sinomo_core::node::DAEMON_MAIN_RUN_CAPABILITY)
                .expect("main daemon capability"),
        ];
        cluster_state.members[0].allowed_capabilities =
            cluster_state.members[0].capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
            &cluster_paths,
            &cluster_state,
        )
        .expect("seed local raft state");
        cluster_paths
            .kv_paths()
            .append_kv_command_and_publish(
                &cluster_state.local_node_id,
                sinomo_core::kv::KvCommand::Op(KvOp::Put {
                    key: "/service/snapshot".to_string(),
                    value: b"async".to_vec(),
                    lease_id: None,
                }),
                sinomo_core::kv::cluster_kv_key_can_defer_materialized_snapshot,
            )
            .expect("put local raft snapshot entry");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let registry = register_daemon_cluster_kv_service(
            ServiceRegistry::builder(),
            &paths,
            Some(runtime.handle()),
        )
        .build();
        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service host");

        let response = runtime
            .block_on(call_value_service_host_async(&host, {
                let payload = sinomo_core::kv::cluster_kv_local_snapshot_rpc_payload()
                    .expect("local snapshot request payload");
                let mut request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("response");

        let snapshot = sinomo_core::kv::decode_cluster_kv_local_snapshot_rpc_body(&response.body)
            .expect("local snapshot")
            .value;
        let entry = snapshot.get("/service/snapshot").expect("snapshot entry");
        assert_eq!(entry.value, b"async".to_vec());
        assert_eq!(
            response
                .metadata
                .get(DAEMON_SERVICE_DISPOSITION_METADATA_KEY)
                .map(String::as_str),
            Some("continue")
        );

        let range_response = runtime
            .block_on(call_value_service_host_async(&host, {
                let payload =
                    sinomo_core::kv::cluster_kv_authoritative_range_prefix_rpc_payload("/service/")
                        .expect("authoritative range payload");
                let mut request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("authoritative range response");
        let range = sinomo_core::kv::decode_cluster_kv_authoritative_range_prefix_rpc_body(
            &range_response.body,
        )
        .expect("authoritative range");
        assert_eq!(range.value.len(), 1);
        assert_eq!(range.value[0].key, "/service/snapshot");

        let authoritative_entry_response = runtime
            .block_on(call_value_service_host_async(&host, {
                let payload = sinomo_core::kv::cluster_kv_authoritative_entry_rpc_payload(
                    "/service/snapshot",
                )
                .expect("authoritative entry payload");
                let mut request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("authoritative entry response");
        let authoritative_entry = sinomo_core::kv::decode_cluster_kv_authoritative_entry_rpc_body(
            &authoritative_entry_response.body,
        )
        .expect("authoritative entry");
        assert_eq!(
            authoritative_entry
                .value
                .as_ref()
                .map(|entry| entry.key.as_str()),
            Some("/service/snapshot")
        );

        let exact_keys = vec![
            "/service/snapshot".to_string(),
            "/service/missing".to_string(),
            "/service/snapshot".to_string(),
        ];
        let authoritative_entries_response = runtime
            .block_on(call_value_service_host_async(&host, {
                let payload =
                    sinomo_core::kv::cluster_kv_authoritative_entries_rpc_payload(&exact_keys)
                        .expect("authoritative exact-set payload");
                let mut request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("authoritative exact-set response");
        let authoritative_entries =
            sinomo_core::kv::decode_cluster_kv_authoritative_entries_rpc_body(
                &authoritative_entries_response.body,
                &exact_keys,
            )
            .expect("authoritative exact-set read");
        assert_eq!(authoritative_entries.value.len(), exact_keys.len());
        assert_eq!(
            authoritative_entries.value[0]
                .as_ref()
                .map(|entry| (entry.key.as_str(), entry.value.as_slice())),
            Some(("/service/snapshot", b"async".as_slice()))
        );
        assert!(authoritative_entries.value[1].is_none());
        assert_eq!(
            authoritative_entries.value[2], authoritative_entries.value[0],
            "duplicate requested keys must retain duplicate ordered values"
        );

        let local_entry_response = runtime
            .block_on(call_value_service_host_async(&host, {
                let payload =
                    sinomo_core::kv::cluster_kv_local_entry_rpc_payload("/service/snapshot")
                        .expect("local entry payload");
                let mut request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("local entry response");
        let local_entry =
            sinomo_core::kv::decode_cluster_kv_local_entry_rpc_body(&local_entry_response.body)
                .expect("local entry");
        assert_eq!(
            local_entry.value.as_ref().map(|entry| entry.key.as_str()),
            Some("/service/snapshot")
        );

        let watch_response = runtime
            .block_on(call_value_service_host_async(&host, {
                let payload =
                    sinomo_core::kv::cluster_kv_authoritative_watch_start_rpc_payload("/service/")
                        .expect("authoritative watch payload");
                let mut request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("authoritative watch response");
        let watch = sinomo_core::kv::decode_cluster_kv_authoritative_watch_start_rpc_body(
            &watch_response.body,
        )
        .expect("authoritative watch");
        assert_eq!(watch.value.next_revision, watch.authority.revision.next());

        cluster_paths
            .kv_paths()
            .append_kv_command_and_publish(
                &cluster_state.local_node_id,
                sinomo_core::kv::KvCommand::Op(KvOp::Put {
                    key: "/service/after-barrier".to_string(),
                    value: b"observed".to_vec(),
                    lease_id: None,
                }),
                sinomo_core::kv::cluster_kv_key_can_defer_materialized_snapshot,
            )
            .expect("put after watch barrier");
        let drain_response = runtime
            .block_on(call_value_service_host_async(&host, {
                let payload = sinomo_core::kv::cluster_kv_local_watch_drain_rpc_payload(
                    &ClusterKvWatchDrainRequest {
                        watch_id: watch.value.watch_id,
                        wait_timeout_millis: 0,
                    },
                )
                .expect("watch drain payload");
                let mut request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("watch drain response");
        let drain =
            sinomo_core::kv::decode_cluster_kv_local_watch_drain_rpc_body(&drain_response.body)
                .expect("watch drain");
        assert!(
            drain
                .events
                .iter()
                .any(|event| event.key == "/service/after-barrier")
        );

        let cancel_response = runtime
            .block_on(call_value_service_host_async(&host, {
                let payload = sinomo_core::kv::cluster_kv_local_watch_cancel_rpc_payload(
                    &ClusterKvWatchCancelRequest {
                        watch_id: watch.value.watch_id,
                    },
                )
                .expect("watch cancel payload");
                let mut request = ObjectRpcRequest::from_encoded_body(
                    payload.route,
                    payload.category,
                    payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
            .expect("watch cancel response");
        let cancelled =
            sinomo_core::kv::decode_cluster_kv_local_watch_cancel_rpc_body(&cancel_response.body)
                .expect("watch cancel");
        assert_eq!(cancelled.watch_id, watch.value.watch_id);
        assert!(
            cancelled.cancelled,
            "watch shutdown must release its durable registration"
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

        let registry = register_daemon_cluster_control_service(
            ServiceRegistry::builder(),
            &paths,
            Some(runtime.handle()),
        )
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
        assert_eq!(init_route.auth_scope.as_deref(), Some("local-command"));
        assert_eq!(
            init_route.required_capabilities,
            vec![sinomo_core::cluster::CLUSTER_CONTROL_ADMIN_CAPABILITY.to_string()]
        );

        let event_hub = Arc::new(DaemonEventHub::new());
        let host = DaemonServiceRegistryHost::from_registry_for_paths_on_runtime(
            DaemonServiceAssembly {
                base_registry: registry,
                component_catalog: daemon_component_catalog(
                    &paths,
                    Some(runtime.handle()),
                    &ComponentCatalog::default(),
                    &event_hub,
                )
                .expect("component catalog"),
                event_hub,
            },
            &paths,
            runtime.handle().clone(),
        )
        .expect("service host");

        let init_payload = sinomo_core::cluster::cluster_control_init_rpc_payload(
            &sinomo_core::cluster::ClusterControlInitRequest {
                cluster_name: "cluster-control-service-test".to_string(),
                node_display_name: Some("cluster-control-node".to_string()),
                node_role: Some(sinomo_core::ClusterNodeRole::Mobile),
            },
        )
        .expect("init payload");
        let response = runtime
            .block_on(call_value_service_host_async(&host, {
                let mut request = ObjectRpcRequest::from_encoded_body(
                    init_payload.route,
                    init_payload.category,
                    init_payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
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
        assert_eq!(
            state.members[0].node_role,
            sinomo_core::ClusterNodeRole::Mobile
        );

        let rename_payload = sinomo_core::cluster::cluster_control_rename_rpc_payload(
            &sinomo_core::cluster::ClusterControlRenameRequest {
                node_id: None,
                display_name: "cluster-control-node-renamed".to_string(),
            },
        )
        .expect("rename payload");
        let response = runtime
            .block_on(call_value_service_host_async(&host, {
                let mut request = ObjectRpcRequest::from_encoded_body(
                    rename_payload.route,
                    rename_payload.category,
                    rename_payload.body,
                );
                annotate_test_request(&mut request, DaemonSignerAuthority::Local);
                request
            }))
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
        let paths = DaemonPaths::new(&SinomoDataDir::new(&root));
        let registry =
            register_daemon_cluster_control_service(ServiceRegistry::builder(), &paths, None)
                .build();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = test_service_host(&root, registry);

        let payload = sinomo_core::cluster::cluster_control_leave_rpc_payload(
            &sinomo_core::cluster::ClusterControlLeaveRequest::default(),
        )
        .expect("leave payload");
        let error = runtime
            .block_on(call_authorized_value_service_host_async(
                &host,
                DaemonSignerAuthority::Member,
                ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body),
            ))
            .expect_err("member signer must not run cluster control mutations");
        assert!(matches!(error, DaemonError::AuthDenied));
    }

    fn remote_cluster_control_auth_fixture() -> (
        sinomo_core::kv::InProcessKvStore,
        ServiceSigner,
        sinomo_core::ClusterMemberRecord,
        sinomo_core::node::ClusterCapabilityPolicyRecord,
        sinomo_core::kv::ClusterKvRaftConfigRecord,
    ) {
        let cluster_id =
            sinomo_core::Id::from_legacy_named("cluster_remote_control_auth").expect("cluster id");
        let node_id = sinomo_core::node::NodeId::from_legacy_named("node_remote_control_auth")
            .expect("node id");
        let mut member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            node_id.clone(),
            "remote voter",
            "remote-control-public-key",
            time::OffsetDateTime::UNIX_EPOCH,
        );
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        let policy = sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member);
        let config = sinomo_core::kv::ClusterKvRaftConfigRecord {
            cluster_id,
            term: 3,
            kv_revision: 1,
            voters: vec![node_id.clone()],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        let signer = ServiceSigner::Remote {
            node_id: node_id.to_string(),
            public_key: member.node_public_key.clone(),
        };
        let mut store = sinomo_core::kv::InProcessKvStore::new();
        store
            .put(
                sinomo_core::node::cluster_member_key(&node_id),
                sinomo_core::kv::encode_kv_value(&member).expect("encode member"),
                None,
            )
            .expect("seed member");
        store
            .put(
                sinomo_core::node::cluster_capability_policy_key(&node_id),
                sinomo_core::kv::encode_kv_value(&policy).expect("encode policy"),
                None,
            )
            .expect("seed policy");
        store
            .put(
                sinomo_core::kv::cluster_kv_raft_config_key(),
                sinomo_core::kv::encode_kv_value(&config).expect("encode config"),
                None,
            )
            .expect("seed config");
        (store, signer, member, policy, config)
    }

    #[test]
    fn cluster_control_remote_authority_requires_current_voter_identity_and_admin_policy() {
        let (mut store, signer, member, mut policy, mut config) =
            remote_cluster_control_auth_fixture();
        validate_remote_cluster_control_authority(
            &store.snapshot().expect("authority snapshot"),
            &signer,
        )
        .expect("current admin voter must be authorized");

        policy.allowed_capabilities.retain(|capability| {
            capability.as_str() != sinomo_core::cluster::CLUSTER_CONTROL_ADMIN_CAPABILITY
        });
        store
            .put(
                sinomo_core::node::cluster_capability_policy_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&policy).expect("encode revoked policy"),
                None,
            )
            .expect("revoke capability");
        assert_remote_cluster_kv_auth_denied(
            validate_remote_cluster_control_authority(
                &store.snapshot().expect("revoked snapshot"),
                &signer,
            )
            .expect_err("revoked admin capability must deny control"),
        );

        let policy = sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member);
        store
            .put(
                sinomo_core::node::cluster_capability_policy_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&policy).expect("restore policy"),
                None,
            )
            .expect("restore policy");
        config.voters.clear();
        config.non_voting.push(member.node_id.clone());
        store
            .put(
                sinomo_core::kv::cluster_kv_raft_config_key(),
                sinomo_core::kv::encode_kv_value(&config).expect("encode demoted config"),
                None,
            )
            .expect("demote voter");
        assert_remote_cluster_kv_auth_denied(
            validate_remote_cluster_control_authority(
                &store.snapshot().expect("demoted snapshot"),
                &signer,
            )
            .expect_err("demoted voter must deny control"),
        );
    }

    #[test]
    fn cluster_control_remote_authority_rejects_stale_signer_key() {
        let (mut store, signer, mut member, _, _) = remote_cluster_control_auth_fixture();
        member.node_public_key = "rotated-public-key".to_string();
        store
            .put(
                sinomo_core::node::cluster_member_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&member).expect("encode rotated member"),
                None,
            )
            .expect("rotate member key");
        assert_remote_cluster_kv_auth_denied(
            validate_remote_cluster_control_authority(
                &store.snapshot().expect("rotated snapshot"),
                &signer,
            )
            .expect_err("stale signer key must deny control"),
        );
    }

    fn assert_remote_cluster_control_race_is_fenced(
        mut store: sinomo_core::kv::InProcessKvStore,
        authority: RemoteClusterControlAuthorityFence,
        mutation_key: &str,
    ) {
        let request = authority
            .fence_request(sinomo_core::kv::KvTxnRequest::generated(KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: mutation_key.to_string(),
                    value: b"must-not-commit".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            }))
            .expect("append remote control authority guards");
        assert_eq!(
            request.txn.compares.len(),
            6,
            "three exact authority rows require value and revision guards"
        );
        let response = store.txn(request.txn).expect("evaluate fenced mutation");
        assert!(!response.succeeded, "authority race must fail the CAS");
        assert!(
            store.get(mutation_key).is_none(),
            "failed authority CAS must not execute the control mutation"
        );
    }

    #[test]
    fn cluster_control_member_revocation_after_authorization_blocks_mutation() {
        let (mut store, signer, mut member, _, _) = remote_cluster_control_auth_fixture();
        let authority = validate_remote_cluster_control_authority(
            &store.snapshot().expect("authorization snapshot"),
            &signer,
        )
        .expect("initial authorization");
        member.state = sinomo_core::ClusterMemberState::Removed;
        store
            .put(
                sinomo_core::node::cluster_member_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&member).expect("encode revoked member"),
                None,
            )
            .expect("commit revocation after authorization");

        assert_remote_cluster_control_race_is_fenced(
            store,
            authority,
            "/test/control-after-member-revocation",
        );
    }

    #[test]
    fn cluster_control_voter_demotion_after_authorization_blocks_mutation() {
        let (mut store, signer, member, _, mut config) = remote_cluster_control_auth_fixture();
        let authority = validate_remote_cluster_control_authority(
            &store.snapshot().expect("authorization snapshot"),
            &signer,
        )
        .expect("initial authorization");
        config.voters.clear();
        config.non_voting.push(member.node_id);
        store
            .put(
                sinomo_core::kv::cluster_kv_raft_config_key(),
                sinomo_core::kv::encode_kv_value(&config).expect("encode demoted config"),
                None,
            )
            .expect("commit demotion after authorization");

        assert_remote_cluster_control_race_is_fenced(
            store,
            authority,
            "/test/control-after-voter-demotion",
        );
    }

    #[test]
    fn cluster_control_policy_change_after_authorization_blocks_mutation() {
        let (mut store, signer, member, mut policy, _) = remote_cluster_control_auth_fixture();
        let authority = validate_remote_cluster_control_authority(
            &store.snapshot().expect("authorization snapshot"),
            &signer,
        )
        .expect("initial authorization");
        policy.allowed_capabilities.retain(|capability| {
            capability.as_str() != sinomo_core::cluster::CLUSTER_CONTROL_ADMIN_CAPABILITY
        });
        store
            .put(
                sinomo_core::node::cluster_capability_policy_key(&member.node_id),
                sinomo_core::kv::encode_kv_value(&policy).expect("encode changed policy"),
                None,
            )
            .expect("commit policy change after authorization");

        assert_remote_cluster_control_race_is_fenced(
            store,
            authority,
            "/test/control-after-policy-change",
        );
    }

    #[test]
    fn daemon_service_registry_uses_only_supplied_observe_component() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-observe-component-catalog");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);

        let registry = daemon_service_registry(&paths, None, &[]);
        assert!(!registry.owns(sinomo_observe::OBSERVE_EVENTS_ROUTE));

        let observe_factory: crate::DaemonComponentFactory =
            Arc::new(sinomo_observe::component_factory().expect("observe component factory"));
        let registry = daemon_service_registry(&paths, None, &[observe_factory]);
        assert!(registry.owns(sinomo_observe::OBSERVE_EVENTS_ROUTE));
    }

    #[test]
    fn daemon_service_registry_accepts_application_components() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-service-hooks");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);

        let factory: crate::DaemonComponentFactory = Arc::new(
            sinomo_module::StaticComponentFactory::new(
                sinomo_module::ComponentDescriptor::new("test.stop", "1.0.0"),
                |_, scope| {
                    scope.set_services(
                        ServiceRegistryBuilder::default()
                            .register(TestStopService)
                            .build(),
                    );
                    Ok(())
                },
            )
            .expect("test component"),
        );
        let registry = daemon_service_registry(&paths, None, &[factory]);
        assert!(registry.owns("/test/stop"));
        assert_eq!(registry.category_for_route("/test/stop"), Some("test-stop"));

        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service host");
        let descriptor = host
            .object_rpc_route_descriptor("/test/stop")
            .expect("hooked service route");
        assert_eq!(descriptor.category, "test-stop");
        assert_eq!(
            descriptor
                .observability
                .get("daemon.service_registry")
                .map(String::as_str),
            Some("true")
        );
    }

    #[tokio::test]
    async fn hot_unload_withdraws_routes_before_draining_and_then_tears_down_scope() {
        use std::sync::atomic::{AtomicBool, Ordering};

        const ROUTE: &str = "/fixture/hot-unload/wait";
        const CATEGORY: &str = "fixture.hot-unload";
        const OPERATION: &str = "fixture.hot-unload.wait";

        let root = sinomo_testdir::TestDir::new("daemon-module-hot-unload");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let paths = DaemonPaths::new(&data_dir);
        paths.ensure().expect("daemon paths");
        let module_id = sinomo_module::ModuleId::from_legacy_named("module.fixture.hot_unload")
            .expect("module id");
        let registration = sinomo_module::ModuleRegistration::native_rust(
            module_id.clone(),
            "1.0.0",
            "fixture",
            "fixture",
            "1.0.0",
            sinomo_core::TrustRing::R1TrustedRuntime,
            "module.fixture.hot_unload",
        )
        .with_surfaces(
            sinomo_module::ModuleSurfaces::default()
                .with_capability_export(
                    sinomo_core::CapabilityName::new(OPERATION).expect("capability"),
                )
                .with_object_rpc_route(sinomo_module::ModuleObjectRpcRoute::new(
                    ROUTE,
                    CATEGORY,
                    OPERATION,
                    sinomo_module::ModuleRouteAuth::LocalCommand,
                ))
                .with_ui_contribution(sinomo_module_api::UiExtensionContribution {
                    contribution_id: "module.fixture.hot-unload.preview".to_string(),
                    extension_point_id: sinomo_module_api::FILE_PREVIEW_EXTENSION_POINT_ID
                        .to_string(),
                    extension_point_version:
                        sinomo_module_api::FILE_PREVIEW_EXTENSION_POINT_VERSION.to_string(),
                    group: "preview".to_string(),
                    order: 0,
                    before: Vec::new(),
                    after: Vec::new(),
                    when: Vec::new(),
                    payload: sinomo_module_api::UiContributionPayload::Declarative {
                        schema_id: "fixture.preview/v1".to_string(),
                        payload_sobj: vec![1],
                    },
                }),
        );
        let descriptor = sinomo_module::component_descriptor_from_registration(&registration)
            .expect("component descriptor");
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let disposed = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn sinomo_module::ComponentFactory> = Arc::new(
            sinomo_module::StaticComponentFactory::new(descriptor, {
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                let disposed = Arc::clone(&disposed);
                move |_, scope| {
                    let service_started = Arc::clone(&started);
                    let service_release = Arc::clone(&release);
                    scope.set_services(
                        ServiceRegistry::builder()
                            .register_dynamic(
                                "fixture-hot-unload",
                                ServiceRoute::from_route_metadata(
                                    ROUTE,
                                    CATEGORY,
                                    "local-command",
                                    [OPERATION],
                                ),
                                move |mut context, request| {
                                    let started = Arc::clone(&service_started);
                                    let release = Arc::clone(&service_release);
                                    async move {
                                        let _: () = context.decode_request_body(&request).await?;
                                        started.notify_one();
                                        let permit = release.acquire().await.map_err(|error| {
                                            ServiceError::Service(error.to_string())
                                        })?;
                                        permit.forget();
                                        context.respond_value(request.route, ())?;
                                        Ok(ServiceDisposition::Continue)
                                    }
                                },
                            )
                            .build(),
                    );
                    scope.defer({
                        let disposed = Arc::clone(&disposed);
                        move || disposed.store(true, Ordering::Release)
                    });
                    Ok(())
                }
            })
            .expect("factory"),
        );
        let catalog = ComponentCatalog::builder()
            .register_module_factory(registration, factory)
            .expect("module factory")
            .build()
            .expect("catalog");
        let host = DaemonServiceRegistryHost::from_registry_with_internal_modules(
            ServiceRegistry::default(),
            paths,
            Some(tokio::runtime::Handle::current()),
            catalog,
            Arc::new(DaemonEventHub::new()),
            None,
        )
        .expect("service host");
        assert!(host.object_rpc_route_descriptor(ROUTE).is_some());
        let unload_route = host
            .object_rpc_route_descriptor(sinomo_module::MODULE_HOT_UNLOAD_ROUTE)
            .expect("hot unload route");
        assert!(
            unload_route
                .required_capabilities
                .iter()
                .any(|capability| capability
                    == sinomo_core::node::MODULE_HOT_UNLOAD_ADMIN_CAPABILITY)
        );
        assert_eq!(
            unload_route.required_local_permissions,
            ["remote-management"]
        );
        let old_registry = host.service_registry.active_snapshot().registry_arc();
        let ui_request = ObjectRpcRequest::encode_body(
            sinomo_module::MODULE_UI_CATALOG_ROUTE,
            sinomo_module::MODULE_UI_RPC_CATEGORY,
            &sinomo_module::UiExtensionCatalogRequest::default(),
        )
        .expect("UI catalog request");
        let ui_before: sinomo_module::UiExtensionCatalogResponse =
            call_value_service_host_async(&host, ui_request)
                .await
                .expect("UI catalog before unload")
                .decode_body()
                .expect("UI catalog response");
        assert_eq!(ui_before.entries.len(), 1);

        let request = ObjectRpcRequest::encode_body(ROUTE, CATEGORY, &()).expect("request");
        let call = {
            let host = host.clone();
            tokio::spawn(async move { call_value_service_host_async(&host, request).await })
        };
        started.notified().await;
        let unload = {
            let host = host.clone();
            let module_id = module_id.clone();
            tokio::spawn(async move {
                host.hot_unload_module(&module_id, Duration::from_secs(2), "test hot unload")
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.object_rpc_route_descriptor(ROUTE).is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("route withdrawal");
        assert!(!disposed.load(Ordering::Acquire));
        let stale_request =
            ObjectRpcRequest::encode_body(ROUTE, CATEGORY, &()).expect("stale request");
        let stale_error = dispatch_service_registry_request(
            old_registry,
            Arc::clone(&host.service_data_dir),
            host.service_cluster.clone(),
            ServiceSigner::Local,
            ObjectRpcIncomingRequest::value(stale_request),
        )
        .await
        .expect_err("stale registry invocation must be fenced");
        assert!(
            stale_error
                .to_string()
                .contains("cannot accept new invocations")
        );
        release.add_permits(1);
        call.await
            .expect("call join")
            .expect("in-flight call completes");
        let report = unload.await.expect("unload join").expect("hot unload");

        assert!(report.drained);
        assert!(report.logical_unload_complete);
        assert!(report.component_teardown_complete);
        assert!(!report.restart_required);
        assert!(disposed.load(Ordering::Acquire));
        let ui_request = ObjectRpcRequest::encode_body(
            sinomo_module::MODULE_UI_CATALOG_ROUTE,
            sinomo_module::MODULE_UI_RPC_CATEGORY,
            &sinomo_module::UiExtensionCatalogRequest::default(),
        )
        .expect("UI catalog request");
        let ui_after: sinomo_module::UiExtensionCatalogResponse =
            call_value_service_host_async(&host, ui_request)
                .await
                .expect("UI catalog after unload")
                .decode_body()
                .expect("UI catalog response");
        assert!(ui_after.registry_generation > ui_before.registry_generation);
        assert!(ui_after.entries.is_empty());
        let persisted = sinomo_module::read_native_rust_module_runtime_state(
            &host.service_data_dir.root(),
            &module_id,
        )
        .expect("native Rust runtime state")
        .expect("persisted disabled state");
        assert!(!persisted.enabled);
        let unload_record =
            sinomo_module::read_module_unload_record(&host.service_data_dir.root(), &module_id)
                .expect("unload record")
                .expect("persisted unload record");
        assert_eq!(unload_record.source, "daemon-hot-unload");
        assert_eq!(
            host.module_runtime
                .invocations
                .status(module_id.as_str())
                .expect("module lifecycle")
                .state,
            sinomo_module::ModuleRuntimeLifecycleState::Unloaded
        );
    }

    #[test]
    fn daemon_config_registry_accepts_application_components() {
        fn register_test_config(
            builder: sinomo_config::ConfigRegistryBuilder,
        ) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
            let definition = sinomo_config::ConfigDefinition::new(
                "test.config",
                "test",
                sinomo_config::ConfigValueType::String {
                    choices: Vec::new(),
                },
                "test component config",
                sinomo_config::ConfigClusterMode::ClusterOverrideLocal,
            )
            .map_err(|error| error.to_string())?
            .with_default(&"hooked")
            .map_err(|error| error.to_string())?;
            builder
                .register(definition)
                .map_err(|error| error.to_string())
        }
        let factory: Arc<dyn ComponentFactory> = Arc::new(
            sinomo_module::StaticComponentFactory::new(
                sinomo_module::ComponentDescriptor::new("test.config", "1.0.0"),
                |_, _| Ok(()),
            )
            .expect("test config component")
            .with_config_definitions(register_test_config),
        );

        let catalog = component_catalog_from_factories(vec![factory]).expect("component catalog");
        let registry = daemon_config_registry(&catalog);
        let definition = registry
            .get("test.config")
            .expect("hooked config definition");

        assert_eq!(definition.owner, "test");
        assert_eq!(definition.description, "test component config");
    }

    #[test]
    fn daemon_config_registry_composes_settings_from_module_surfaces() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-module-config-surfaces");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let supplied_catalog = ComponentCatalog::builder()
            .register_module(sinomo_dfs_nfs::nfs_module_registration())
            .expect("register product NFS module")
            .register_factory(Arc::new(
                sinomo_sandbox::component_factory().expect("sandbox component factory"),
            ))
            .expect("register product sandbox component")
            .register_factory(test_rollout_component_factory())
            .expect("register product rollout component")
            .build()
            .expect("product module catalog");
        let catalog = daemon_component_catalog(
            &paths,
            None,
            &supplied_catalog,
            &Arc::new(DaemonEventHub::new()),
        )
        .expect("component catalog");
        let registry = daemon_config_registry(&catalog);

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

        let nfs = registry
            .get(sinomo_dfs_nfs::NFS_MOUNT_CONFIG_KEY)
            .expect("NFS module settings");
        assert_eq!(
            nfs.module_id.as_deref(),
            Some(sinomo_dfs_nfs::nfs_module_id().as_str())
        );
        assert_eq!(
            nfs.module_label.as_deref(),
            Some(sinomo_dfs_nfs::NFS_MODULE_LABEL)
        );
        let nfs_status = registry
            .get(sinomo_dfs_nfs::NFS_MOUNT_STATUS_CONFIG_KEY)
            .expect("NFS module status");
        assert!(nfs_status.read_only);
        assert!(nfs_status.operational_error.is_some());
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

        let registration = sinomo_module::ModuleRegistration::native_rust(
            module_id.clone(),
            "1.0.0",
            "dynamic-module",
            "dynamic-module",
            "dynamic-module-v1",
            sinomo_core::TrustRing::R1TrustedRuntime,
            "dynamic-module",
        )
        .with_surfaces(surfaces);
        let catalog = ComponentCatalog::builder()
            .register_module(registration)
            .expect("register module")
            .build()
            .expect("component catalog");
        let registry = catalog
            .register_config_definitions(builder)
            .expect("compose config")
            .build();
        assert_eq!(
            registry
                .get("test.config-collision")
                .expect("static definition remains")
                .owner,
            "application-hook"
        );
    }

    #[test]
    fn identical_dynamic_module_settings_are_idempotent_with_the_static_catalog() {
        let module_id = sinomo_module::ModuleId::from_legacy_named("module.dynamic-identical")
            .expect("module id");
        let definition = sinomo_config::ConfigDefinition::new(
            "test.config-identical",
            "dynamic-module",
            sinomo_config::ConfigValueType::Object {
                schema_name: "DynamicIdenticalConfig".to_string(),
            },
            "dynamic module config",
            sinomo_config::ConfigClusterMode::LocalOnly,
        )
        .expect("definition")
        .with_schema(sinomo_config::ConfigSchema::object(
            "DynamicIdenticalConfig",
            "Dynamic module settings declared by both the fallback and native catalogs.",
            vec![sinomo_config::ConfigSchemaField::new(
                "enabled",
                "Enabled",
                "Whether the test behavior is enabled.",
                sinomo_config::ConfigSchemaValue::bool(),
            )],
        ))
        .expect("schema")
        .with_module_id(module_id.to_string())
        .expect("module ownership");
        let builder = sinomo_config::ConfigRegistry::builder()
            .register(definition.clone())
            .expect("static definition registration");
        let surfaces = sinomo_module::ModuleSurfaces::default()
            .with_config_definition(definition.clone())
            .expect("dynamic settings surface");

        let registration = sinomo_module::ModuleRegistration::native_rust(
            module_id.clone(),
            "1.0.0",
            "dynamic-module",
            "dynamic-module",
            "dynamic-module-v1",
            sinomo_core::TrustRing::R1TrustedRuntime,
            "dynamic-module",
        )
        .with_surfaces(surfaces);
        let catalog = ComponentCatalog::builder()
            .register_module(registration)
            .expect("register module")
            .build()
            .expect("component catalog");
        let registry = catalog
            .register_config_definitions(builder)
            .expect("compose config")
            .build();

        assert_eq!(
            registry
                .get("test.config-identical")
                .expect("identical definition remains"),
            &definition
        );
    }

    #[test]
    fn dfs_status_service_dispatch_allows_cluster_member_signer() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-status-member-dispatch");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let dfs_factory: crate::DaemonComponentFactory =
            Arc::new(sinomo_dfs::component_factory().expect("DFS component factory"));
        let registry = daemon_service_registry(&paths, None, &[dfs_factory]);
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

        let state = sinomo_module::read_native_dylib_module_loader_state(
            &root,
            &sinomo_module::ModuleId::from_legacy_named(&descriptor.module_id).expect("module id"),
        )
        .expect("read installed module state")
        .expect("installed module state");
        let registration =
            sinomo_module::native_dylib_module_registration_from_loader_state(&root, &state)
                .expect("installed module registration");
        let catalog = sinomo_module::ComponentCatalog::builder()
            .register_module(registration)
            .expect("register installed module")
            .build()
            .expect("component catalog");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let host = DaemonServiceRegistryHost::from_registry_with_internal_modules(
            ServiceRegistry::builder().build(),
            paths,
            None,
            catalog,
            Arc::new(DaemonEventHub::new()),
            None,
        )
        .expect("service registry host");
        let route_descriptor = host
            .object_rpc_route_descriptor(&route.route)
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
        let response = runtime
            .block_on(call_value_service_host_async(
                &host,
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
    fn cluster_registry_hosts_cluster_routes() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-cluster-kv-registry-routes");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let build_factory: crate::DaemonComponentFactory =
            Arc::new(sinomo_build::component_factory().expect("build component factory"));
        let factories = vec![build_factory, test_rollout_component_factory()];
        let registry = daemon_service_registry(&paths, Some(runtime.handle()), &factories);

        for route in CLUSTER_KV_RPC_ROUTES
            .iter()
            .copied()
            .chain(CLUSTER_RAFT_RPC_ROUTES.iter().copied())
            .chain([
                sinomo_core::network::CLUSTER_HEALTH_ROUTE,
                sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
                sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE,
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
            registry.category_for_route(CLUSTER_JOIN_REGISTER_ROUTE),
            Some(CLUSTER_MEMBERSHIP_RPC_CATEGORY)
        );
        assert!(!registry.owns("/dfs/message"));
        assert_eq!(registry.category_for_route("/dfs/message"), None);
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
        assert_eq!(
            object_rpc_forward.request_body_mode,
            ServiceBodyMode::Optional
        );
        assert_eq!(
            object_rpc_forward.response_body_mode,
            ServiceBodyMode::Optional
        );
        let network_chunk_metrics = route_metadata
            .iter()
            .find(|route| route.route == crate::DAEMON_NETWORK_CHUNK_METRICS_ROUTE)
            .expect("daemon network chunk metrics route metadata");
        assert_eq!(network_chunk_metrics.category, crate::DAEMON_RPC_CATEGORY);
        assert_eq!(
            network_chunk_metrics.auth_scope.as_deref(),
            Some("local-command")
        );
        assert_eq!(
            network_chunk_metrics.request_body_mode,
            ServiceBodyMode::None
        );
        assert_eq!(
            network_chunk_metrics.response_body_mode,
            ServiceBodyMode::None
        );
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
        for route in [
            crate::DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
            crate::DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
        ] {
            let descriptor = route_metadata
                .iter()
                .find(|descriptor| descriptor.route == route)
                .unwrap_or_else(|| panic!("main forward route metadata missing for {route}"));
            assert_eq!(descriptor.category, crate::DAEMON_RPC_CATEGORY);
            assert_eq!(descriptor.auth_scope.as_deref(), Some("local-command"));
            assert_eq!(descriptor.request_body_mode, ServiceBodyMode::Optional);
            assert_eq!(descriptor.response_body_mode, ServiceBodyMode::Optional);
        }
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
            .find(|route| route.route == CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE)
            .expect("cluster kv local snapshot route metadata");
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
        assert_eq!(ping.request_body_mode, ServiceBodyMode::Optional);
        assert_eq!(ping.response_body_mode, ServiceBodyMode::Optional);
        assert_eq!(
            ping.request_value_mode,
            sinomo_core::service::ServiceRequestValueMode::Incremental
        );
        assert_eq!(
            ping_module_route.request_body_mode,
            sinomo_module_api::NativeModuleBodyMode::Optional
        );
        assert_eq!(
            ping_module_route.response_body_mode,
            sinomo_module_api::NativeModuleBodyMode::Optional
        );
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
    async fn daemon_services_forward_across_a_stream_native_relay() {
        crate::cluster::ensure_cluster_directory_installed();
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-rpc-forward-bodies");
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
        let relay_listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("relay listener");
        let relay_address = relay_listener
            .local_addr_string()
            .expect("relay listener address");
        let local_node =
            sinomo_core::node::NodeId::from_legacy_named("node_daemon_dfs_rpc_forward_local")
                .expect("local node");
        let relay_node =
            sinomo_core::node::NodeId::from_legacy_named("node_daemon_dfs_rpc_forward_relay")
                .expect("relay node");
        let target_node =
            sinomo_core::node::NodeId::from_legacy_named("node_daemon_dfs_rpc_forward_target")
                .expect("target node");
        let relay_data_dir = SinomoDataDir::new(root.join("relay-node"));
        relay_data_dir
            .ensure_root_layout()
            .expect("relay data directory");
        let relay_identity =
            sinomo_core::node::load_or_create_main_node_identity(&relay_data_dir.root())
                .expect("relay identity");
        let target_data_dir = SinomoDataDir::new(root.join("target-node"));
        target_data_dir
            .ensure_root_layout()
            .expect("target data directory");
        let target_identity =
            sinomo_core::node::load_or_create_main_node_identity(&target_data_dir.root())
                .expect("target identity");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_rpc_forward")
                .expect("cluster id");
        let now = time::OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon DFS RPC forward",
            local_node.clone(),
            "local",
            identity.public_key,
            now,
        );
        let mut relay_member = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            relay_node.clone(),
            "relay",
            relay_identity.public_key,
            now,
        );
        relay_member
            .endpoints
            .push(sinomo_core::ClusterEndpointRecord {
                endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                    "endpoint_daemon_dfs_rpc_forward_relay",
                )
                .expect("endpoint id"),
                node_id: relay_node.clone(),
                transport: sinomo_core::ClusterTransportKind::LanTcp,
                address: relay_address,
                source: sinomo_core::ClusterEndpointSource::Static,
                privacy: sinomo_core::ClusterEndpointPrivacy::Lan,
                priority: 0,
                last_observed_latency_ms: None,
            });
        let relay_endpoint = relay_member.endpoints[0].clone();
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
                    "endpoint_daemon_dfs_rpc_forward_target",
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
        let target_endpoint = target_member.endpoints[0].clone();
        state.members.push(relay_member);
        state.members.push(target_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        // Do not mirror membership into cluster KV here. Accepted join state
        // can lead that projection, and this test must exercise that exact
        // production bootstrap window through the stream-native route.
        let directory = sinomo_core::network::cluster_directory_with_bootstrap_state(
            sinomo_core::network::cluster_directory_for(&cluster_paths),
            &state,
        )
        .snapshot()
        .expect("bootstrap-aware cluster directory snapshot");
        assert!(directory.is_authorized(&local_node));
        assert!(directory.is_authorized(&relay_node));
        assert!(directory.is_authorized(&target_node));
        let local_to_relay = sinomo_core::network::cluster_connectivity_record(
            &state,
            &relay_endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let local_to_target = sinomo_core::network::cluster_connectivity_record(
            &state,
            &target_endpoint,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            None,
            1,
            None,
            Some(now.to_string()),
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_records(
            &cluster_paths,
            &local_node,
            &[local_to_relay, local_to_target],
        )
        .expect("local relay connectivity");
        let mut relay_to_target = sinomo_core::network::cluster_connectivity_record(
            &state,
            &target_endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        relay_to_target.source_node_id = relay_node.clone();
        sinomo_core::network::write_local_cluster_connectivity_records(
            &cluster_paths,
            &relay_node,
            std::slice::from_ref(&relay_to_target),
        )
        .expect("relay target connectivity");
        let relay_cluster_paths = ClusterPaths::new(&relay_data_dir);
        let mut relay_state = state.clone();
        relay_state.local_node_id = relay_node.clone();
        sinomo_core::cluster::write_cluster_state(&relay_cluster_paths, &relay_state)
            .expect("relay cluster state");
        let target_cluster_paths = ClusterPaths::new(&target_data_dir);
        let mut target_state = state.clone();
        target_state.local_node_id = target_node.clone();
        sinomo_core::cluster::write_cluster_state(&target_cluster_paths, &target_state)
            .expect("target cluster state");

        // Every fake network peer below understands only SAT1, routed-session,
        // and stream-native service frames. Any unexpected transport path
        // fails at its first wire prefix instead of satisfying the test.
        // Case zero exercises bodies in both directions. Case one proves that
        // a large typed DFS request keeps its value role and one-pass
        // authentication proof through both proxy directions. Case two
        // carries the membership challenge route used when a bridge relays a
        // downstream join to a state authority.
        let streamed_value_route = "/test/session-forward/streamed-value";
        let streamed_request_value = vec![0x6b_u8; 768 * 1024 + 43];
        let streamed_request_body =
            ObjectRpcRequest::encode_body(streamed_value_route, "test", &streamed_request_value)
                .expect("streamed request value")
                .body;
        let inner_routes = [
            "/test/session-forward/0".to_string(),
            streamed_value_route.to_string(),
            sinomo_core::network::CLUSTER_JOIN_CHALLENGE_ROUTE.to_string(),
        ];
        let request_bodies = [
            vec![0x31_u8; 2 * 1024 * 1024 + 113],
            streamed_request_body,
            Vec::new(),
        ];
        let response_bodies = [vec![0x72_u8; 3 * 1024 * 1024 + 211], Vec::new(), Vec::new()];
        let target_routes = inner_routes.clone();
        let target_expected_requests = request_bodies.clone();
        let target_responses = response_bodies.clone();
        let target_streamed_request_value = streamed_request_value.clone();
        let target_directory = sinomo_core::network::DirectorySnapshot::try_from_records(
            sinomo_core::network::DirectoryVersion(1),
            target_state.members.clone(),
            std::iter::once(target_endpoint),
        )
        .expect("target directory");
        let relay_directory = target_directory.clone();
        let relay_routed_identity = Arc::new(
            sinomo_core::network::DaemonRoutedSessionIdentity::new(
                relay_node.clone(),
                relay_data_dir,
            )
            .expect("relay routed identity"),
        );
        let relay_link_provider = sinomo_core::network::ClusterLinkProvider::new(
            relay_node.clone(),
            relay_routed_identity.clone(),
            sinomo_core::network::NetworkHealthPolicy::default(),
        )
        .expect("relay link provider");
        let server_relay_node = relay_node.clone();
        let relay_source_node = local_node.clone();
        let relay_target_node = target_node.clone();
        let relay_server = tokio::spawn(async move {
            let mut stream = relay_listener
                .accept()
                .await
                .expect("relay accept")
                .into_inner();
            let metadata = sinomo_core::network::read_authenticated_transport_prelude(&mut stream)
                .await
                .expect("relay authenticated transport prelude");
            let source_node =
                sinomo_core::network::verify_builtin_authenticated_transport_metadata(
                    &metadata,
                    &server_relay_node,
                    &relay_directory,
                    relay_routed_identity.as_ref(),
                )
                .expect("relay adjacent source");
            assert_eq!(source_node, relay_source_node);
            let direct = sinomo_core::network::AuthenticatedDirectStream::accepted(
                Box::new(stream),
                server_relay_node.clone(),
                source_node,
                sinomo_core::Id::generate(),
                metadata.channel_binding,
                metadata.capabilities,
            )
            .expect("relay direct stream");
            let report = match sinomo_core::network::accept_or_relay_routed_session(
                direct,
                &relay_directory,
                relay_routed_identity.as_ref(),
                &relay_link_provider,
                None,
            )
            .await
            .expect("relay routed session")
            {
                sinomo_core::network::RoutedSessionDisposition::Relayed(report) => report,
                sinomo_core::network::RoutedSessionDisposition::Destination(_) => {
                    panic!("intermediate node terminated the stream instead of relaying it")
                }
            };
            assert_eq!(report.source_node_id, relay_source_node);
            assert_eq!(report.destination_node_id, relay_target_node);
            assert_eq!(report.relay_node_id, server_relay_node);
            assert_eq!(report.next_node_id, relay_target_node);
            assert!(report.bytes_toward_destination > 0);
            assert!(report.bytes_toward_source > 0);
        });
        let target_routed_identity = Arc::new(
            sinomo_core::network::DaemonRoutedSessionIdentity::new(
                target_node.clone(),
                target_data_dir,
            )
            .expect("target routed identity"),
        );
        let target_link_provider = sinomo_core::network::ClusterLinkProvider::new(
            target_node.clone(),
            target_routed_identity.clone(),
            sinomo_core::network::NetworkHealthPolicy::default(),
        )
        .expect("target link provider");
        let server_target_node = target_node.clone();
        let target_adjacent_node = relay_node.clone();
        let target_source_node = local_node.clone();
        let target_server = tokio::spawn(async move {
            let mut stream = target_listener
                .accept()
                .await
                .expect("target accept")
                .into_inner();
            let metadata = sinomo_core::network::read_authenticated_transport_prelude(&mut stream)
                .await
                .expect("target authenticated transport prelude");
            let source_node =
                sinomo_core::network::verify_builtin_authenticated_transport_metadata(
                    &metadata,
                    &server_target_node,
                    &target_directory,
                    target_routed_identity.as_ref(),
                )
                .expect("target adjacent source");
            assert_eq!(source_node, target_adjacent_node);
            let direct = sinomo_core::network::AuthenticatedDirectStream::accepted(
                Box::new(stream),
                server_target_node.clone(),
                source_node,
                sinomo_core::Id::generate(),
                metadata.channel_binding,
                metadata.capabilities,
            )
            .expect("target direct stream");
            let carrier = match sinomo_core::network::accept_or_relay_routed_session(
                direct,
                &target_directory,
                target_routed_identity.as_ref(),
                &target_link_provider,
                None,
            )
            .await
            .expect("target routed session")
            {
                sinomo_core::network::RoutedSessionDisposition::Destination(stream) => stream,
                sinomo_core::network::RoutedSessionDisposition::Relayed(_) => {
                    panic!("final forwarding target continued as a relay")
                }
            };
            assert_eq!(carrier.peer_node_id, target_source_node);
            let mut acceptor = sinomo_core::network::accept_routed_stream_carrier(carrier);
            for (index, ((expected_route, expected_request), response_body)) in target_routes
                .into_iter()
                .zip(target_expected_requests)
                .zip(target_responses)
                .enumerate()
            {
                let routed = acceptor
                    .accept()
                    .await
                    .expect("accept target logical stream")
                    .expect("target logical stream");
                assert_eq!(routed.peer_node_id, target_source_node);
                let context = sinomo_core::network::RpcPeerContext::authenticated(
                    routed.local_node_id.clone(),
                    routed.peer_node_id.clone(),
                    routed.session_id.clone(),
                    routed.channel_binding,
                )
                .expect("target service context");
                let response_node = server_target_node.clone();
                let expected_streamed_request_value = target_streamed_request_value.clone();
                sinomo_core::network::serve_service_rpc_peer_stream(
                    routed,
                    &context,
                    sinomo_core::network::ObjectFrameIoOptions::default(),
                    move |mut incoming, _peer| async move {
                        assert_eq!(incoming.request.route, expected_route);
                        let request_route = incoming.request.route.clone();
                        assert_eq!(
                            incoming.has_streamed_value(),
                            index != 0,
                            "case {index} used the wrong service body semantics"
                        );
                        match index {
                            0 => {
                                assert!(
                                    incoming.body.total_bytes().is_none_or(|total| {
                                        total == expected_request.len() as u64
                                    }),
                                    "the proxy must preserve a known length or use the authenticated deferred-body contract"
                                );
                                let mut actual_request = Vec::new();
                                incoming
                                    .body
                                    .read_to_end(&mut actual_request)
                                    .await
                                    .expect("target request body");
                                assert_eq!(actual_request, expected_request);
                            }
                            1 => {
                                assert_eq!(
                                    incoming
                                        .decode_body::<Vec<u8>>()
                                        .await
                                        .expect("decode forwarded streamed value"),
                                    expected_streamed_request_value
                                );
                            }
                            2 => {
                                assert_eq!(
                                    incoming
                                        .decode_body::<String>()
                                        .await
                                        .expect("decode forwarded membership value"),
                                    "request-2"
                                );
                            }
                            _ => unreachable!("unexpected forwarding case"),
                        }
                        if index == 1 {
                            let mut outgoing = ObjectRpcOutgoingResponse::encode_value(
                                request_route,
                                "response-1".to_string(),
                            )?;
                            outgoing
                                .response
                                .mark_responder_node_id(response_node.to_string());
                            return Ok(outgoing);
                        }
                        let mut response = ObjectRpcResponse::encode_body(
                            request_route,
                            &format!("response-{index}"),
                        )
                        .expect("target response head");
                        response.mark_responder_node_id(response_node.to_string());
                        let response_bytes = response_body.len() as u64;
                        assert!(index != 0 || response_bytes > 0);
                        let body = ObjectRpcBodySource::bytes(response_body);
                        assert_eq!(body.total_bytes(), Some(response_bytes));
                        let outgoing = ObjectRpcOutgoingResponse::new(response, body);
                        assert_eq!(outgoing.body.total_bytes(), Some(response_bytes));
                        Ok(outgoing)
                    },
                )
                    .await
                    .expect("target stream-native service response");
            }
            assert!(
                acceptor
                    .accept()
                    .await
                    .expect("finish target routed carrier")
                    .is_none(),
                "the source must retire the carrier after the final logical stream"
            );
        });

        let outer_listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("outer listener");
        let outer_address = outer_listener
            .local_addr_string()
            .expect("outer listener address");
        let outer_paths = paths.clone();
        let outer_runtime = tokio::runtime::Handle::current();
        let forward_routes = [
            crate::DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            crate::DAEMON_OBJECT_RPC_FORWARD_ROUTE,
        ];
        let outer_server = tokio::spawn(async move {
            let mut stream = outer_listener
                .accept()
                .await
                .expect("outer accept")
                .into_inner();
            let carrier_context =
                sinomo_core::network::read_local_object_rpc_carrier_prelude(&mut stream)
                    .await
                    .expect("outer local carrier prelude");
            let mut acceptor = sinomo_core::network::accept_local_object_rpc_stream_carrier(
                Box::new(stream),
                &carrier_context,
            )
            .expect("outer local carrier");
            for forward_route in forward_routes {
                let logical = acceptor
                    .accept()
                    .await
                    .expect("accept outer logical stream")
                    .expect("outer logical stream");
                let context = sinomo_core::network::RpcPeerContext::unauthenticated_local(
                    logical.session_id.clone(),
                    logical.channel_binding,
                );
                let handler_paths = outer_paths.clone();
                let handler_runtime = outer_runtime.clone();
                sinomo_core::network::serve_service_rpc_peer_stream(
                    logical,
                    &context,
                    sinomo_core::network::ObjectFrameIoOptions::default(),
                    move |incoming, _peer| async move {
                        assert_eq!(incoming.request.route, forward_route);
                        let payload: crate::DaemonObjectRpcForwardRequest =
                            incoming.request.decode_body().expect("forward payload");
                        let (response, body) = forward_object_rpc_call_from_daemon_control(
                            &handler_paths,
                            payload,
                            incoming.body,
                            &handler_runtime,
                        )
                        .await
                        .expect("forward session");
                        Ok(ObjectRpcOutgoingResponse::new(
                            ObjectRpcResponse::encode_body(forward_route, &response)
                                .expect("forward response head"),
                            body,
                        ))
                    },
                )
                .await
                .expect("outer stream-native forward response");
            }
        });

        let runtime = tokio::runtime::Handle::current();
        let mut host =
            crate::client::DaemonObjectRpcAsyncHost::new(&runtime, false).with_paths(&paths);
        let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
        for (index, ((request_body, expected_response), inner_route)) in request_bodies
            .into_iter()
            .zip(response_bodies)
            .zip(inner_routes)
            .enumerate()
        {
            let forward_route = forward_routes[index];
            let category = if index == 2 {
                sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
            } else if index == 1 {
                sinomo_dfs::DFS_RPC_CATEGORY
            } else {
                "test"
            };
            let inner_request = if index == 1 {
                ObjectRpcRequest::from_encoded_body(inner_route, category, Vec::new())
            } else {
                ObjectRpcRequest::encode_body(inner_route, category, &format!("request-{index}"))
                    .expect("inner request head")
            };
            let payload = crate::DaemonObjectRpcForwardRequest {
                target_node_id: target_node.clone(),
                request: inner_request,
                streamed_value_body: index == 1,
                connect_timeout_millis: 5_000,
                idle_timeout_millis: Some(310_000),
                overall_timeout_millis: None,
                observe_connectivity: false,
                destination_process_affinity: crate::DaemonObjectRpcDestinationProcessAffinity::Any,
            };
            let outer_head =
                ObjectRpcRequest::encode_body(forward_route, crate::DAEMON_RPC_CATEGORY, &payload)
                    .expect("outer request head");
            let request_body = if request_body.is_empty() {
                ObjectRpcBodySource::empty()
            } else {
                ObjectRpcBodySource::bytes(request_body)
            };
            let result = client
                .call_loopback(
                    &data_dir,
                    &outer_address,
                    ObjectRpcCall::new(outer_head, request_body),
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await
                .expect("outer forwarded session");
            let envelope: crate::DaemonObjectRpcForwardResponse = result
                .response
                .decode_body()
                .expect("forward response envelope");
            let crate::DaemonObjectRpcForwardResponse::Success {
                response,
                streamed_value_body,
                ..
            } = envelope
            else {
                panic!(
                    "forward returned a network error: {envelope:?}; local={local_node} target={target_node}"
                );
            };
            assert_eq!(streamed_value_body, index == 1);
            if streamed_value_body {
                assert_eq!(
                    response.responder_node_id(),
                    Some(target_node.as_str()),
                    "the streamed nested response must preserve the verified final target identity"
                );
                let nested = ObjectRpcResult::from_forwarded_body(response, result.body, true)
                    .expect("restore streamed nested response");
                assert_eq!(
                    nested
                        .decode_body::<String>()
                        .await
                        .expect("decode streamed nested value"),
                    "response-1"
                );
                continue;
            }
            assert!(
                result
                    .body
                    .total_bytes()
                    .is_none_or(|total| total == expected_response.len() as u64),
                "the proxy must preserve a known length or use the authenticated deferred-body contract"
            );
            assert_eq!(
                response.responder_node_id(),
                Some(target_node.as_str()),
                "the trusted outer envelope must preserve the verified final target identity"
            );
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
        assert_eq!(
            sinomo_core::network::retire_pooled_routed_streams(&state.cluster_id.to_string()).await,
            1,
            "the three forwarded calls must share one routed carrier"
        );
        relay_server.await.expect("relay server");
        target_server.await.expect("target server");
    }

    #[test]
    fn main_and_shadow_keep_process_specific_service_ownership() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-main-shadow-build-routes");
        let data_dir = SinomoDataDir::new(&root);
        let main_paths = DaemonPaths::new(&data_dir);
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let main_factories: Vec<crate::DaemonComponentFactory> = vec![
            Arc::new(sinomo_dfs::component_factory().expect("DFS component factory")),
            Arc::new(sinomo_sandbox::component_factory().expect("sandbox component factory")),
            Arc::new(sinomo_build::component_factory().expect("build component factory")),
            test_rollout_component_factory(),
        ];
        let main_registry =
            daemon_service_registry(&main_paths, Some(runtime.handle()), &main_factories);
        let shadow_factories = vec![test_rollout_component_factory()];
        let shadow_registry =
            daemon_service_registry(&shadow_paths, Some(runtime.handle()), &shadow_factories);

        let authority_routes = CLUSTER_KV_RPC_ROUTES
            .iter()
            .copied()
            .chain(CLUSTER_RAFT_RPC_ROUTES.iter().copied())
            .chain([
                CLUSTER_JOIN_CHALLENGE_ROUTE,
                CLUSTER_JOIN_REGISTER_ROUTE,
                CLUSTER_INVITE_PUBLISH_ROUTE,
                CLUSTER_MEMBER_PUBLISH_ROUTE,
            ])
            .chain(
                sinomo_core::cluster::CLUSTER_CONTROL_RPC_ROUTES
                    .iter()
                    .copied(),
            )
            .collect::<Vec<_>>();
        for route in &authority_routes {
            assert!(
                main_registry.owns(route),
                "main registry should own cluster authority route {route}"
            );
            assert!(
                !shadow_registry.owns(route),
                "shadow registry must not own cluster authority route {route}"
            );
        }
        assert!(
            main_registry.owns(sinomo_core::network::CLUSTER_HEALTH_ROUTE),
            "main registry should own the authority-backed cluster health route"
        );
        assert!(
            !shadow_registry.owns(sinomo_core::network::CLUSTER_HEALTH_ROUTE),
            "shadow registry must not own the authority-backed cluster health route"
        );
        for route in [
            CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
            CLUSTER_NETWORK_PING_ROUTE,
        ] {
            assert!(
                shadow_registry.owns(route),
                "shadow registry should retain passive network route {route}"
            );
        }
        for route in [
            crate::DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
            crate::DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
        ] {
            assert!(
                main_registry.owns(route),
                "main registry should accept fail-closed local forwarding route {route}"
            );
            assert!(
                shadow_registry.owns(route),
                "shadow registry should relay fail-closed local forwarding route {route} to the main process"
            );
        }
        assert!(
            !main_registry.owns(DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE),
            "main registry must not own the process-specific remote shadow shutdown route"
        );
        assert!(
            shadow_registry.owns(DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE),
            "shadow registry should own its voter-authorized compatibility shutdown route"
        );
        {
            let route = DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE;
            assert!(
                main_registry.owns(route),
                "main registry should own {route}"
            );
            assert!(
                !shadow_registry.owns(route),
                "shadow registry must not own authority-bearing maintenance route {route}"
            );
        }

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
        assert!(
            main_registry.owns(sinomo_sandbox::SANDBOX_STATUS_ROUTE),
            "main registry should own clustered sandbox routes"
        );
        assert!(
            !shadow_registry.owns(sinomo_sandbox::SANDBOX_STATUS_ROUTE),
            "shadow registry must not own sandbox routes backed by DFS"
        );
        for route in [
            sinomo_rollout::ROLLOUT_DIAGNOSTICS_ROUTE,
            sinomo_rollout::ROLLOUT_BINARY_ROUTE,
            sinomo_rollout::ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
            sinomo_rollout::ROLLOUT_BUILD_ROUTE,
            sinomo_rollout::ROLLOUT_FINALIZE_SHADOW_ROUTE,
            sinomo_rollout::ROLLOUT_CLEAR_STATUS_ROUTE,
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
        assert!(
            main_host
                .object_rpc_route_descriptor(DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE)
                .is_none(),
            "main host must not advertise the remote shadow shutdown route"
        );
        let remote_shadow_shutdown = shadow_host
            .object_rpc_route_descriptor(DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE)
            .expect("shadow shutdown route descriptor");
        assert_eq!(
            remote_shadow_shutdown.auth_scope.as_deref(),
            Some("local-or-cluster-voter")
        );
        let generic_shutdown = shadow_host
            .object_rpc_route_descriptor(DAEMON_SHUTDOWN_ROUTE)
            .expect("generic shutdown route descriptor");
        assert_eq!(
            generic_shutdown.auth_scope.as_deref(),
            Some("local-command"),
            "the legacy generic shutdown route must remain local-only at service dispatch"
        );
        assert!(
            shadow_host.service_cluster.is_none(),
            "shadow service dispatch must not expose DaemonClusterAccess to services"
        );
        for route in &authority_routes {
            assert!(
                main_host.object_rpc_route_descriptor(route).is_some(),
                "main host should advertise cluster authority route {route}"
            );
            assert!(
                shadow_host.object_rpc_route_descriptor(route).is_none(),
                "shadow host must not advertise cluster authority route {route}"
            );
        }
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
        assert!(
            main_host
                .object_rpc_route_descriptor(sinomo_sandbox::SANDBOX_STATUS_ROUTE)
                .is_some(),
            "main host should advertise clustered sandbox routes"
        );
        assert!(
            shadow_host
                .object_rpc_route_descriptor(sinomo_sandbox::SANDBOX_STATUS_ROUTE)
                .is_none(),
            "shadow host must not advertise sandbox routes backed by DFS"
        );
    }

    #[test]
    fn daemon_status_and_local_health_leave_cluster_kv_unchanged() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-read-only-rpc-kv-quietness");
        let data_dir = Arc::new(SinomoDataDir::new(&root));
        data_dir.ensure_root_layout().expect("data directory");
        let paths = DaemonPaths::new(data_dir.as_ref());
        paths.ensure().expect("daemon paths");
        let cluster_paths = ClusterPaths::new(data_dir.as_ref());
        cluster_paths.ensure().expect("cluster paths");

        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_daemon_read_only_rpc_kv_quietness",
            )
            .expect("cluster id"),
            "daemon read-only RPC KV quietness",
            sinomo_core::node::NodeId::from_legacy_named("node_daemon_read_only_rpc_kv_quietness")
                .expect("node id"),
            "quiet node",
            "quiet-public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("seed cluster KV");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let builder = crate::host_api::register_hosted_daemon_lifecycle_service(
            ServiceRegistry::builder(),
            Arc::new(DaemonLifecycleServiceHost {
                paths: paths.clone(),
                runtime: Some(runtime.handle().clone()),
                status_contributors: Vec::new(),
            }),
        );
        let registry =
            register_daemon_cluster_network_service(builder, &paths, Some(runtime.handle()), true)
                .build();
        let host = DaemonServiceRegistryHost::from_registry_for_paths(registry, &paths)
            .expect("service host");
        let cluster = crate::daemon_cluster_status(&state, crate::DaemonClusterProcess::Main)
            .expect("daemon cluster status");
        let _daemon_lock =
            crate::DaemonLock::acquire(&paths, &crate::DaemonRunOptions::default(), Some(cluster))
                .expect("hold daemon status fixture lock");
        assert!(paths.status.exists(), "daemon status fixture must exist");
        let before = sinomo_core::cluster::read_cluster_kv_local_snapshot(&cluster_paths, &state)
            .expect("KV snapshot before read-only RPCs");

        for _ in 0..8 {
            let mut status_request =
                ObjectRpcRequest::encode_body(DAEMON_STATUS_ROUTE, DAEMON_RPC_CATEGORY, &())
                    .expect("status request");
            annotate_test_request(&mut status_request, DaemonSignerAuthority::Local);
            let status_response = runtime
                .block_on(call_value_service_host_async(&host, status_request))
                .expect("status response");
            status_response
                .decode_body::<crate::DaemonStatus>()
                .expect("decode daemon status");

            let mut health_request = sinomo_core::cluster::cluster_local_health_rpc_request()
                .expect("local health request");
            annotate_test_request(&mut health_request, DaemonSignerAuthority::Local);
            let health_response = runtime
                .block_on(call_value_service_host_async(&host, health_request))
                .expect("local health response");
            sinomo_core::cluster::decode_cluster_health_rpc_response(&health_response)
                .expect("decode local health response");
        }

        let after = sinomo_core::cluster::read_cluster_kv_local_snapshot(&cluster_paths, &state)
            .expect("KV snapshot after read-only RPCs");
        assert_eq!(
            after, before,
            "daemon status and health observations must not advance or rewrite cluster KV"
        );
    }

    #[test]
    fn voter_authorized_remote_shadow_shutdown_dispatches_only_on_shadow() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-remote-shadow-shutdown-route");
        let data_dir = SinomoDataDir::new(&root);
        let main_paths = DaemonPaths::new(&data_dir);
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let main_host = DaemonServiceRegistryHost::from_registry_for_paths(
            daemon_service_registry(&main_paths, Some(runtime.handle()), &[]),
            &main_paths,
        )
        .expect("main host");
        let shadow_host = DaemonServiceRegistryHost::from_registry_for_paths(
            daemon_service_registry(&shadow_paths, Some(runtime.handle()), &[]),
            &shadow_paths,
        )
        .expect("shadow host");
        let request = || {
            ObjectRpcRequest::encode_body(
                DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE,
                DAEMON_RPC_CATEGORY,
                &(),
            )
            .expect("remote shadow shutdown request")
        };

        let response = runtime
            .block_on(call_authorized_value_service_host_async(
                &shadow_host,
                DaemonSignerAuthority::Voter,
                request(),
            ))
            .expect("voter-authorized shadow shutdown");
        assert_eq!(
            response.decode_body::<String>().expect("shutdown response"),
            "sinomo shadow daemon stopping"
        );
        assert!(
            runtime
                .block_on(call_authorized_value_service_host_async(
                    &main_host,
                    DaemonSignerAuthority::Voter,
                    request(),
                ))
                .is_err(),
            "the same authenticated route must remain unavailable on main"
        );
        let generic_shutdown =
            ObjectRpcRequest::encode_body(DAEMON_SHUTDOWN_ROUTE, DAEMON_RPC_CATEGORY, &())
                .expect("generic shutdown request");
        assert!(
            runtime
                .block_on(call_authorized_value_service_host_async(
                    &shadow_host,
                    DaemonSignerAuthority::Voter,
                    generic_shutdown,
                ))
                .is_err(),
            "granting voters the shadow-specific route must not widen generic shutdown"
        );
    }
}
