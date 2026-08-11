//! Bridges the daemon's control loop to the async, host-agnostic service seam
//! (`sinomo-service`).
//!
//! Routes owned by the [`ServiceRegistry`] are served here; everything else falls
//! through to the daemon's own dispatch. Sandbox live output is produced by the
//! daemon and carried as the standard stream-native object-RPC body;
//! the synchronous command runners remain isolated behind a bounded producer.
//!
//! The registry mixes host-agnostic services from `sinomo-service` and native modules
//! such as `sinomo-rollout` with **daemon-foundational** services defined here
//! ([`DaemonControlService`], [`ClusterKvService`], [`ClusterKvConsensusService`],
//! [`ClusterHealthService`]) that own daemon state
//! (their [`DaemonPaths`], which encode whether this is the main or shadow daemon)
//! and call the daemon's own cluster/control handlers. Consuming services receive a real
//! [`DaemonClusterAccess`] via [`ServiceContext::cluster`] when the node is clustered.

use sinomo_core::cluster::ClusterKvPathsExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::auth::{AuthenticatedDaemonCommand, DaemonSignerAuthority};
use crate::cluster::{
    DaemonClusterJoinRegistrationHost, DaemonClusterMemberMutationHost, DaemonClusterStatusProbe,
    RemoteClusterControlAuthorityFence, cluster_paths_from_daemon_paths,
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
    handle_forwarded_daemon_cluster_kv_txn_request_async_on_runtime,
};
use crate::control::{
    accepted_rollout_should_stop_current_daemon, daemon_data_dir_path,
    daemon_status_response_async, handle_daemon_sandbox_checkpoint_on_runtime,
    handle_daemon_sandbox_create_on_runtime, handle_daemon_sandbox_destroy_on_runtime,
    handle_daemon_sandbox_http_fetch, handle_daemon_sandbox_http_fetch_to_file_on_runtime,
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
    DaemonDfsDataPlaneHost, break_glass_recover_dfs_placement_authority_permit,
    daemon_dfs_control_plane_client_on_runtime, daemon_dfs_peer_transports_on_runtime,
    inspect_dfs_placement_authority_permit, recover_removed_node_dfs_placement_authority_permit,
};
use crate::error::DaemonError;
use crate::event_stream::{DaemonEventHub, DaemonEventService};
use crate::paths::DaemonPaths;
use crate::registry::network_module_surfaces;
use crate::rollout::{
    handle_daemon_clear_rollout_status, handle_daemon_finalize_shadow_rollout,
    handle_daemon_rollout_binary_request, handle_daemon_rollout_build_request,
    handle_daemon_rollout_native_dylib_module_request,
};
use crate::{
    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE, DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
    DAEMON_DFS_BROWSER_DELETE_ROUTE, DAEMON_DFS_BROWSER_LIST_ROUTE,
    DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE, DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE, DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE, DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE, DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_ROUTE, DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
    DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE, DAEMON_DFS_BROWSER_RENAME_ROUTE,
    DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE, DAEMON_DFS_BROWSER_ROUTES,
    DAEMON_DFS_BROWSER_STAT_ROUTE, DAEMON_DFS_BROWSER_STATUS_ROUTE,
    DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE, DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE, DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
    DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE, DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE, DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
    DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ROUTE, DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_NETWORK_CHUNK_METRICS_ROUTE, DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_ROUTE, DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE, DAEMON_RPC_CATEGORY,
    DAEMON_SHUTDOWN_ROUTE, DAEMON_STATUS_ROUTE,
};
use sinomo_api::DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES;
use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_SEARCH_ROUTE, CONFIG_SET_ROUTE, ConfigRegistry,
    ConfigRegistryBuilder, register_services as register_config_services,
};
use sinomo_core::kv::{
    CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE, CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE,
    CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE, CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
    CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE, CLUSTER_KV_LOCAL_ENTRY_ROUTE,
    CLUSTER_KV_LOCAL_READ_WATCH_ROUTES, CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
    CLUSTER_KV_REQUEST_OUTCOME_ROUTE, CLUSTER_KV_RPC_ROUTE_POLICIES, CLUSTER_KV_RPC_ROUTES,
    CLUSTER_KV_TXN_ROUTE, CLUSTER_KV_WATCH_EVENTS_ROUTE, CLUSTER_RAFT_APPEND_ROUTE,
    CLUSTER_RAFT_RPC_ROUTE_POLICIES, CLUSTER_RAFT_RPC_ROUTES, CLUSTER_RAFT_VOTE_ROUTE,
    ClusterKvRpcRoutePolicy, ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse,
    ClusterKvWatchDrainRequest, ClusterKvWatchDrainResponse, ClusterKvWatchPrefixRequest,
    ClusterKvWatchRegistration, ClusterRaftAppendRequest, ClusterRaftVoteRequest,
    KvAuthoritativeRead, KvEntry, KvError, KvOp, KvTxn, KvTxnResponse,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_CHALLENGE_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE,
    CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY,
    CLUSTER_NETWORK_CONNECTIVITY_ROUTE, CLUSTER_NETWORK_PING_ROUTE, ClusterConnectivityGraph,
    ClusterConnectivityState, ClusterPaths, DaemonControlRequest, NetworkError,
    ObjectRpcBodySource, ObjectRpcCall, ObjectRpcCancelRequest, ObjectRpcCancelResponse,
    ObjectRpcIncomingRequest, ObjectRpcOutgoingResponse, ObjectRpcRequest, ObjectRpcResponse,
    ObjectRpcResult, cluster_directory_for, cluster_route_next_hop,
};
#[cfg(test)]
use sinomo_core::service::ServiceRegistryBuilder;
use sinomo_core::service::{
    ClusterAccess, ResponseSink, Service, ServiceBodyMode, ServiceContext, ServiceDisposition,
    ServiceError, ServiceFuture, ServiceRegistry, ServiceRegistrySnapshotStore, ServiceRoute,
    ServiceSigner,
};
use sinomo_dfs::register_services as register_dfs_services;
use sinomo_module::{MODULE_NATIVE_DYLIB_CANCEL_ROUTE, MODULE_RPC_CATEGORY};
use sinomo_rollout::{
    ClusterRolloutBinaryResponse, ROLLOUT_BINARY_ROUTE, ROLLOUT_BUILD_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_FINALIZE_SHADOW_ROUTE, ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
    register_services as register_rollout_services, rollout_module_surfaces,
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

static DAEMON_CLUSTER_NETWORK_PING_NODE_IDS: OnceLock<
    Mutex<HashMap<PathBuf, sinomo_core::node::NodeId>>,
> = OnceLock::new();

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

pub(crate) fn enforce_daemon_object_rpc_forward_route_affinity(
    route: &str,
    payload: &mut crate::DaemonObjectRpcForwardRequest,
) -> Result<(), NetworkError> {
    match route {
        DAEMON_OBJECT_RPC_FORWARD_ROUTE | DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE => {
            if payload.destination_process_affinity
                != crate::DaemonObjectRpcDestinationProcessAffinity::Any
            {
                return Err(NetworkError::InvalidClusterCommand {
                    detail: format!(
                        "explicit main-daemon affinity requires a fail-closed main forwarding route, not legacy route {route}"
                    ),
                });
            }
        }
        DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE | DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE => {
            payload.destination_process_affinity =
                crate::DaemonObjectRpcDestinationProcessAffinity::MainDaemon;
        }
        _ => {
            return Err(NetworkError::UnknownRoute {
                route: route.to_string(),
            });
        }
    }
    Ok(())
}

pub(crate) async fn forward_object_rpc_call_from_daemon_control(
    paths: &crate::DaemonPaths,
    payload: crate::DaemonObjectRpcForwardRequest,
    request_body: sinomo_core::network::ObjectRpcIncomingBody,
    runtime: &tokio::runtime::Handle,
) -> Result<(crate::DaemonObjectRpcForwardResponse, ObjectRpcBodySource), ServiceError> {
    let options = daemon_object_rpc_forward_options(&payload);
    let forward_transport_timeouts = daemon_object_rpc_forward_transport_timeouts(&payload);

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

fn daemon_object_rpc_forward_options(
    payload: &crate::DaemonObjectRpcForwardRequest,
) -> sinomo_core::network::ClusterObjectRpcOptions {
    let transport_timeouts = daemon_object_rpc_forward_transport_timeouts(payload);
    let mut options = sinomo_core::network::ClusterObjectRpcOptions::new(transport_timeouts);
    if let Some(overall_timeout_millis) = payload.overall_timeout_millis {
        options = options.with_overall_timeout(Duration::from_millis(overall_timeout_millis));
    }
    match payload.destination_process_affinity {
        crate::DaemonObjectRpcDestinationProcessAffinity::Any => options,
        crate::DaemonObjectRpcDestinationProcessAffinity::MainDaemon => {
            sinomo_core::cluster::main_daemon_cluster_rpc_options(options)
        }
    }
}

fn daemon_object_rpc_forward_transport_timeouts(
    payload: &crate::DaemonObjectRpcForwardRequest,
) -> sinomo_core::network::DaemonRpcTimeouts {
    let mut timeouts = sinomo_core::network::DaemonRpcTimeouts::default()
        .with_connect_timeout(Some(Duration::from_millis(payload.connect_timeout_millis)));
    if let Some(idle_timeout_millis) = payload.idle_timeout_millis {
        timeouts = timeouts.with_idle_timeout(Some(Duration::from_millis(idle_timeout_millis)));
    }
    timeouts
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

struct DaemonClusterKvLocalReadWatchHost<'a> {
    paths: &'a ClusterPaths,
}

impl sinomo_core::kv::ClusterKvLocalReadWatchHost for DaemonClusterKvLocalReadWatchHost<'_> {
    type Error = ServiceError;

    fn kv_error(error: sinomo_core::kv::KvError) -> Self::Error {
        ServiceError::from(error)
    }

    fn range_prefix(
        &mut self,
        prefix: &str,
    ) -> Result<sinomo_core::kv::KvLocalRead<Vec<KvEntry>>, Self::Error> {
        let node_id = cluster_kv_local_node_id(self.paths)?;
        sinomo_core::kv::read_cluster_kv_local_prefix(&self.paths.kv_paths(), &node_id, prefix)
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

enum ClusterKvServiceResponse {
    LocalEntry(sinomo_core::kv::KvLocalRead<Option<KvEntry>>),
    LocalRange(sinomo_core::kv::KvLocalRead<Vec<KvEntry>>),
    WatchRegistration(ClusterKvWatchRegistration),
    WatchDrain(ClusterKvWatchDrainResponse),
    WatchCancel(ClusterKvWatchCancelResponse),
    WatchEvents(sinomo_core::kv::ClusterKvWatchEventsResponse),
    LocalSnapshot(sinomo_core::kv::KvLocalRead<sinomo_core::kv::KvSnapshot>),
    AuthoritativeEntry(KvAuthoritativeRead<Option<KvEntry>>),
    AuthoritativeEntries(sinomo_core::kv::ClusterKvAuthoritativeEntriesResponse),
    AuthoritativeSnapshot(KvAuthoritativeRead<sinomo_core::kv::KvSnapshot>),
    AuthoritativeRange(KvAuthoritativeRead<Vec<KvEntry>>),
    AuthoritativeWatch(KvAuthoritativeRead<ClusterKvWatchRegistration>),
    RequestOutcome(sinomo_core::kv::ClusterKvRequestOutcomeRead),
    Transaction(sinomo_core::kv::ClusterKvTxnRpcResponse),
}

impl ClusterKvServiceResponse {
    fn respond(self, ctx: &mut ServiceContext) -> Result<(), ServiceError> {
        match self {
            Self::LocalEntry(response) => ctx.respond_value(CLUSTER_KV_LOCAL_ENTRY_ROUTE, response),
            Self::LocalRange(response) => ctx.respond_value(
                sinomo_core::kv::CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
                response,
            ),
            Self::WatchRegistration(response) => ctx.respond_value(
                sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE,
                response,
            ),
            Self::WatchDrain(response) => ctx.respond_value(
                sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
                response,
            ),
            Self::WatchCancel(response) => ctx.respond_value(
                sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
                response,
            ),
            Self::WatchEvents(response) => {
                ctx.respond_value(CLUSTER_KV_WATCH_EVENTS_ROUTE, response)
            }
            Self::LocalSnapshot(response) => {
                ctx.respond_value(CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE, response)
            }
            Self::AuthoritativeEntry(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE, response)
            }
            Self::AuthoritativeEntries(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE, response)
            }
            Self::AuthoritativeSnapshot(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE, response)
            }
            Self::AuthoritativeRange(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE, response)
            }
            Self::AuthoritativeWatch(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE, response)
            }
            Self::RequestOutcome(response) => {
                ctx.respond_value(CLUSTER_KV_REQUEST_OUTCOME_ROUTE, response)
            }
            Self::Transaction(response) => ctx.respond_value(CLUSTER_KV_TXN_ROUTE, response),
        }
    }
}

fn handle_cluster_kv_local_read_watch_request(
    paths: &ClusterPaths,
    request: sinomo_core::kv::ClusterKvLocalReadWatchRequest,
) -> Result<ClusterKvServiceResponse, ServiceError> {
    let mut host = DaemonClusterKvLocalReadWatchHost { paths };
    match request {
        sinomo_core::kv::ClusterKvLocalReadWatchRequest::RangePrefix(prefix) => {
            sinomo_core::kv::ClusterKvLocalReadWatchHost::range_prefix(&mut host, &prefix)
                .map(ClusterKvServiceResponse::LocalRange)
        }
        sinomo_core::kv::ClusterKvLocalReadWatchRequest::CreateWatch(request) => {
            sinomo_core::kv::ClusterKvLocalReadWatchHost::create_watch(&mut host, request)
                .map(ClusterKvServiceResponse::WatchRegistration)
        }
        sinomo_core::kv::ClusterKvLocalReadWatchRequest::DrainWatch(request) => {
            sinomo_core::kv::ClusterKvLocalReadWatchHost::drain_watch(&mut host, request)
                .map(ClusterKvServiceResponse::WatchDrain)
        }
        sinomo_core::kv::ClusterKvLocalReadWatchRequest::CancelWatch(request) => {
            sinomo_core::kv::ClusterKvLocalReadWatchHost::cancel_watch(&mut host, request)
                .map(ClusterKvServiceResponse::WatchCancel)
        }
    }
}

fn handle_cluster_kv_watch_events_request(
    paths: &ClusterPaths,
    request: sinomo_core::kv::ClusterKvWatchEventsRequest,
) -> Result<ClusterKvServiceResponse, ServiceError> {
    let state = sinomo_core::cluster::read_cluster_state(paths)?;
    let response =
        sinomo_core::kv::handle_cluster_kv_watch_events(&paths.kv_paths(), &state, request)?;
    Ok(ClusterKvServiceResponse::WatchEvents(response))
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

fn network_module_admin_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = network_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("network module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown network service route {route}"));
    let service_route = ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
    .with_request_body_mode(native_module_body_mode_to_service(
        descriptor.request_body_mode,
    ))
    .with_response_body_mode(native_module_body_mode_to_service(
        descriptor.response_body_mode,
    ));
    if route == CLUSTER_NETWORK_PING_ROUTE {
        service_route.with_incremental_request_value()
    } else {
        service_route
    }
}

fn rollout_daemon_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = rollout_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("rollout module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown daemon rollout service route {route}"));
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

const DAEMON_CONTROL_MAIN_ROUTES: &[&str] = &[
    DAEMON_STATUS_ROUTE,
    DAEMON_SHUTDOWN_ROUTE,
    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
    DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ROUTE,
    DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
    DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
];
const DAEMON_CONTROL_SHADOW_ROUTES: &[&str] = &[
    DAEMON_STATUS_ROUTE,
    DAEMON_SHUTDOWN_ROUTE,
    DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE,
    DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
];

impl Service for DaemonControlService {
    fn name(&self) -> &'static str {
        "daemon-control"
    }

    fn routes(&self) -> &'static [&'static str] {
        if crate::daemon_paths_are_shadow(&self.paths) {
            DAEMON_CONTROL_SHADOW_ROUTES
        } else {
            DAEMON_CONTROL_MAIN_ROUTES
        }
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        let mut routes = service_route_categories(self.routes(), DAEMON_RPC_CATEGORY);
        for route in &mut routes {
            if route.route == DAEMON_OBJECT_RPC_FORWARD_ROUTE
                || route.route == DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
                || route.route == DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE
                || route.route == DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE
                || route.route == DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE
                || route.route == DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ROUTE
                || route.route == DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE
                || route.route == DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE
                || route.route == DAEMON_NETWORK_CHUNK_METRICS_ROUTE
                || route.route == DAEMON_SHUTDOWN_ROUTE
            {
                route.auth_scope = Some("local-command".to_string());
            }
            if route.route == DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE {
                route
                    .required_capabilities
                    .push("daemon.dfs.mounted-mutation".to_string());
                route.timeout_ms = Some(30_000);
            }
            if route.route == DAEMON_OBJECT_RPC_FORWARD_ROUTE
                || route.route == DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
                || route.route == DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE
                || route.route == DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE
            {
                route.request_body_mode = ServiceBodyMode::Optional;
                route.response_body_mode = ServiceBodyMode::Optional;
            }
            if route.route == DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE {
                route.auth_scope = Some("local-or-cluster-voter".to_string());
            }
        }
        routes
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            if crate::daemon_paths_are_shadow(&paths)
                && matches!(
                    request.route.as_str(),
                    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE
                        | DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ROUTE
                        | DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE
                )
            {
                return Err(ServiceError::UnknownRoute(request.route));
            }
            if request.route == DAEMON_OBJECT_RPC_FORWARD_ROUTE
                || request.route == DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
                || request.route == DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE
                || request.route == DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE
            {
                let forward_route = request.route.clone();
                let mut payload: crate::DaemonObjectRpcForwardRequest =
                    ctx.decode_request_body(&request).await?;
                enforce_daemon_object_rpc_forward_route_affinity(&forward_route, &mut payload)
                    .map_err(ServiceError::Network)?;
                let runtime =
                    require_service_runtime(runtime.as_ref(), "daemon object RPC call forward")?
                        .clone();
                let request_body = ctx.take_request_body();
                let (response, response_body) = forward_object_rpc_call_from_daemon_control(
                    &paths,
                    payload,
                    request_body,
                    &runtime,
                )
                .await?;
                ctx.respond_with_body(
                    ObjectRpcResponse::encode_body(forward_route, &response)?,
                    response_body,
                )?;
                return Ok(ServiceDisposition::Continue);
            }
            let disposition = match request.route.as_str() {
                DAEMON_STATUS_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let status = daemon_status_response_async(&paths, runtime.as_ref())
                        .await
                        .map_err(daemon_error)?;
                    ctx.respond_value(DAEMON_STATUS_ROUTE, status)?;
                    ServiceDisposition::Continue
                }
                DAEMON_SHUTDOWN_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(DAEMON_SHUTDOWN_ROUTE, "sinomo daemon stopping".to_string())?;
                    ServiceDisposition::Stop
                }
                DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    if !crate::daemon_paths_are_shadow(&paths) {
                        return Err(ServiceError::UnknownRoute(request.route));
                    }
                    crate::append_operational_event_best_effort(
                        &paths,
                        crate::OperationalSeverity::Info,
                        "daemon-control",
                        "daemon.lifecycle.remote_shadow_shutdown_requested",
                        "remote shadow shutdown requested by an authenticated cluster voter",
                        None,
                        serde_json::json!({
                            "route": DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE,
                        }),
                    );
                    ctx.respond_value(
                        DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE,
                        "sinomo shadow daemon stopping".to_string(),
                    )?;
                    ServiceDisposition::Stop
                }
                DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE => {
                    let _: crate::DaemonClusterMaintenanceRefreshRequest =
                        ctx.decode_request_body(&request).await?;
                    crate::control::request_daemon_cluster_maintenance(
                        crate::control::DaemonClusterMaintenanceReason::ManualBroad,
                    );
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
                    ctx.respond_value(
                        DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
                        crate::DaemonClusterMaintenanceRefreshResponse,
                    )?;
                    ServiceDisposition::Continue
                }
                DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ROUTE => {
                    let recovery: crate::DaemonDfsRepairQueueRecoveryRequest =
                        ctx.decode_request_body(&request).await?;
                    if recovery.acknowledgement
                        != crate::DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ACKNOWLEDGEMENT
                    {
                        return Err(ServiceError::Service(format!(
                            "DFS repair queue recovery requires acknowledgement `{}`",
                            crate::DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ACKNOWLEDGEMENT
                        )));
                    }
                    let runtime = require_service_runtime(
                        runtime.as_ref(),
                        "DFS repair queue disaster recovery",
                    )?;
                    let response = crate::dfs::run_daemon_dfs_repair_queue_recovery_on_runtime(
                        &paths, runtime,
                    )
                    .await
                    .map_err(daemon_error)?;
                    ctx.respond_value(DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ROUTE, response)?;
                    ServiceDisposition::Continue
                }
                DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE => {
                    let handoff: crate::DaemonDfsMountedMutationHandoffRequest = ctx
                        .decode_request_body_bounded(
                            &request,
                            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES,
                        )
                        .await?;
                    handoff.validate().map_err(ServiceError::Service)?;
                    let runtime = require_service_runtime(
                        runtime.as_ref(),
                        "DFS mounted-mutation durability handoff",
                    )?
                    .clone();
                    let traceparent = request.traceparent().map(ToString::to_string);
                    let handoff_paths = paths.clone();
                    let response = run_blocking_service_task(
                        DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
                        "DFS mounted-mutation durability handoff",
                        move || {
                            crate::dfs::handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
                                &handoff_paths,
                                &runtime,
                                traceparent,
                                handoff,
                            )
                            .map_err(daemon_error)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE, response)?;
                    ServiceDisposition::Continue
                }
                DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE => {
                    let _: crate::DaemonLocalConnectivityReloadRequest =
                        ctx.decode_request_body(&request).await?;
                    let cluster_paths =
                        cluster_paths_from_daemon_paths(&paths).map_err(daemon_error)?;
                    sinomo_core::network::invalidate_local_cluster_connectivity_cache(
                        &cluster_paths,
                    )?;
                    // A repaired route can unblock durable DFS transfers even
                    // when no new filesystem mutation occurs. Re-run only the
                    // bounded reactive queues after invalidating the stale
                    // projection.
                    crate::dfs::notify_daemon_dfs_maintenance_work();
                    ctx.respond_value(
                        DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
                        crate::DaemonLocalConnectivityReloadResponse,
                    )?;
                    ServiceDisposition::Continue
                }
                DAEMON_NETWORK_CHUNK_METRICS_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
                        crate::DaemonNetworkChunkMetricsResponse {
                            chunk_metrics: sinomo_core::network::network_chunk_metrics(),
                        },
                    )?;
                    ServiceDisposition::Continue
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
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

fn deny_remote_cluster_kv_txn_authority(reason: &str) -> ServiceError {
    tracing::warn!(
        target: "sinomo.daemon.cluster_kv_auth",
        reason,
        "remote cluster KV transaction denied by authoritative membership policy"
    );
    ServiceError::Network(NetworkError::AuthDenied)
}

fn remote_cluster_kv_authorization_keys(
    signer: &ServiceSigner,
) -> Result<(String, String), ServiceError> {
    let ServiceSigner::Remote { node_id, .. } = signer else {
        return Err(deny_remote_cluster_kv_txn_authority(
            "authorization fence requires a remote signer",
        ));
    };
    let node_id = node_id
        .parse::<sinomo_core::node::NodeId>()
        .map_err(|_| deny_remote_cluster_kv_txn_authority("signer node id is invalid"))?;
    Ok((
        sinomo_core::node::cluster_member_key(&node_id),
        sinomo_core::node::cluster_capability_policy_key(&node_id),
    ))
}

/// Bind a remote transaction to the exact membership and capability-policy
/// entries observed by the receiving voter.
///
/// Remote transactions deliberately reject failure-branch mutations. A flat
/// KV transaction cannot distinguish a caller compare failure from an
/// authorization-fence failure; permitting a failure branch would let a
/// revoked caller mutate state when one of these appended guards fails.
fn fence_remote_cluster_kv_txn_request_with_entries(
    member_entry: Option<&KvEntry>,
    policy_entry: Option<&KvEntry>,
    signer: &ServiceSigner,
    mut request: sinomo_core::kv::KvTxnRequest,
) -> Result<sinomo_core::kv::KvTxnRequest, ServiceError> {
    let ServiceSigner::Remote {
        node_id,
        public_key,
    } = signer
    else {
        return Err(deny_remote_cluster_kv_txn_authority(
            "authorization fence requires a remote signer",
        ));
    };
    if !request.txn.failure.is_empty() {
        return Err(ServiceError::Service(
            "remote cluster KV transactions cannot contain failure-branch mutations because authorization guards must fail closed"
                .to_string(),
        ));
    }

    let node_id = node_id
        .parse::<sinomo_core::node::NodeId>()
        .map_err(|_| deny_remote_cluster_kv_txn_authority("signer node id is invalid"))?;
    let member_key = sinomo_core::node::cluster_member_key(&node_id);
    let member_entry = member_entry
        .filter(|entry| entry.key == member_key)
        .ok_or_else(|| deny_remote_cluster_kv_txn_authority("signer member is absent"))?;
    let member: sinomo_core::ClusterMemberRecord = member_entry.decode()?;
    if member.node_id != node_id
        || !member.state.is_authorized()
        || member.node_public_key != *public_key
    {
        return Err(deny_remote_cluster_kv_txn_authority(
            "signer member identity is stale or revoked",
        ));
    }

    let policy_key = sinomo_core::node::cluster_capability_policy_key(&node_id);
    let policy_entry = policy_entry
        .filter(|entry| entry.key == policy_key)
        .ok_or_else(|| {
            deny_remote_cluster_kv_txn_authority("signer capability policy is absent")
        })?;
    let policy: sinomo_core::node::ClusterCapabilityPolicyRecord = policy_entry.decode()?;
    let required_capability = sinomo_core::kv::CLUSTER_KV_WRITE_CAPABILITY;
    let advertises_capability = policy
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == required_capability);
    let allows_capability = policy
        .allowed_capabilities
        .iter()
        .any(|capability| capability.as_str() == required_capability);
    if policy.cluster_id != member.cluster_id
        || policy.node_id != node_id
        || !advertises_capability
        || !allows_capability
    {
        return Err(deny_remote_cluster_kv_txn_authority(
            "signer capability policy does not grant cluster.kv.write",
        ));
    }

    let compare_count = request
        .txn
        .compares
        .len()
        .checked_add(4)
        .unwrap_or(usize::MAX);
    if compare_count > sinomo_core::kv::MAX_KV_TXN_COMPARES {
        return Err(KvError::TxnLimit {
            compares: compare_count,
            success_ops: request.txn.success.len(),
            failure_ops: request.txn.failure.len(),
            max_compares: sinomo_core::kv::MAX_KV_TXN_COMPARES,
            max_ops_per_branch: sinomo_core::kv::MAX_KV_TXN_OPS_PER_BRANCH,
        }
        .into());
    }
    request
        .txn
        .compares
        .try_reserve(4)
        .map_err(|_| KvError::ResourceExhausted {
            operation: "remote cluster KV authorization fence compares".to_string(),
            requested_bytes: compare_count
                .saturating_mul(std::mem::size_of::<sinomo_core::kv::KvCompare>().max(1)),
        })?;
    request
        .txn
        .compares
        .push(sinomo_core::kv::KvCompare::ModifyRevisionEquals {
            key: member_key.clone(),
            revision: member_entry.modify_revision,
        });
    request
        .txn
        .compares
        .push(sinomo_core::kv::KvCompare::ValueEquals {
            key: member_key,
            value: member_entry.value.clone(),
        });
    request
        .txn
        .compares
        .push(sinomo_core::kv::KvCompare::ModifyRevisionEquals {
            key: policy_key.clone(),
            revision: policy_entry.modify_revision,
        });
    request
        .txn
        .compares
        .push(sinomo_core::kv::KvCompare::ValueEquals {
            key: policy_key,
            value: policy_entry.value.clone(),
        });
    Ok(request)
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
            // KV reads, barriers, and transactions are also used internally by
            // the maintenance pass itself. They therefore cannot be scheduling
            // edges for that pass: waking here makes every completed pass queue
            // its successor. External app/control boundaries and actual Raft
            // replication progress provide the independent wake sources.
            let cluster_paths = ClusterPaths::new(ctx.data_dir.as_ref());
            let route = request.route.clone();
            if route == CLUSTER_KV_LOCAL_ENTRY_ROUTE {
                let request: sinomo_core::kv::ClusterKvEntryRequest =
                    ctx.decode_request_body(&request).await?;
                let cluster_paths_for_task = cluster_paths.clone();
                let response =
                    run_blocking_service_task(route.clone(), "cluster kv local entry", move || {
                        let node_id = cluster_kv_local_node_id(&cluster_paths_for_task)?;
                        sinomo_core::kv::read_cluster_kv_resident_local_entry(
                            &cluster_paths_for_task.kv_paths(),
                            &node_id,
                            &request.key,
                        )
                        .map(ClusterKvServiceResponse::LocalEntry)
                        .map_err(ServiceError::from)
                    })
                    .await?;
                response.respond(&mut ctx)?;
                return Ok(ServiceDisposition::Continue);
            }
            if CLUSTER_KV_LOCAL_READ_WATCH_ROUTES.contains(&route.as_str()) {
                let request = match route.as_str() {
                    sinomo_core::kv::CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE => {
                        sinomo_core::kv::ClusterKvLocalReadWatchRequest::RangePrefix(
                            ctx.decode_request_body(&request).await?,
                        )
                    }
                    sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE => {
                        sinomo_core::kv::ClusterKvLocalReadWatchRequest::CreateWatch(
                            ctx.decode_request_body(&request).await?,
                        )
                    }
                    sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE => {
                        sinomo_core::kv::ClusterKvLocalReadWatchRequest::DrainWatch(
                            ctx.decode_request_body(&request).await?,
                        )
                    }
                    sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE => {
                        sinomo_core::kv::ClusterKvLocalReadWatchRequest::CancelWatch(
                            ctx.decode_request_body(&request).await?,
                        )
                    }
                    other => return Err(ServiceError::UnknownRoute(other.to_string())),
                };
                let cluster_paths_for_task = cluster_paths.clone();
                let response = run_blocking_service_task(
                    route.clone(),
                    "cluster kv local read/watch",
                    move || {
                        handle_cluster_kv_local_read_watch_request(&cluster_paths_for_task, request)
                    },
                )
                .await?;
                response.respond(&mut ctx)?;
                return Ok(ServiceDisposition::Continue);
            }
            if route == CLUSTER_KV_WATCH_EVENTS_ROUTE {
                let request: sinomo_core::kv::ClusterKvWatchEventsRequest =
                    ctx.decode_request_body(&request).await?;
                let cluster_paths_for_task = cluster_paths.clone();
                let response = run_blocking_service_task(
                    route.clone(),
                    "cluster kv watch-event fanout",
                    move || {
                        handle_cluster_kv_watch_events_request(&cluster_paths_for_task, request)
                    },
                )
                .await?;
                response.respond(&mut ctx)?;
                return Ok(ServiceDisposition::Continue);
            }
            let response = match route.as_str() {
                CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let cluster_paths_for_task = cluster_paths.clone();
                    run_blocking_service_task(
                        route.clone(),
                        "cluster kv local snapshot",
                        move || {
                            let state = sinomo_core::cluster::read_cluster_state_without_kv(
                                &cluster_paths_for_task,
                            )?;
                            let snapshot = sinomo_core::cluster::read_cluster_kv_local_snapshot(
                                &cluster_paths_for_task,
                                &state,
                            )?;
                            Ok(ClusterKvServiceResponse::LocalSnapshot(snapshot))
                        },
                    )
                    .await?
                }
                CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE => {
                    let _: sinomo_core::kv::ClusterKvAuthoritativeSnapshotRequest =
                        ctx.decode_request_body(&request).await?;
                    let runtime = require_service_runtime(
                        runtime.as_ref(),
                        "cluster kv authoritative snapshot",
                    )?;
                    let barrier = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
                    let snapshot =
                        handle_daemon_cluster_kv_authoritative_snapshot_request_async_on_runtime(
                            &paths,
                            &barrier.body,
                            ctx.signer.is_local(),
                            runtime,
                        )
                        .await
                        .map_err(daemon_error)?;
                    ClusterKvServiceResponse::AuthoritativeSnapshot(snapshot)
                }
                CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE => {
                    let entry: sinomo_core::kv::ClusterKvEntryRequest =
                        ctx.decode_request_body(&request).await?;
                    let runtime = require_service_runtime(
                        runtime.as_ref(),
                        "cluster kv authoritative entry",
                    )?;
                    let entry =
                        handle_daemon_cluster_kv_authoritative_entry_request_async_on_runtime(
                            &paths,
                            entry.key,
                            ctx.signer.is_local(),
                            runtime,
                        )
                        .await
                        .map_err(daemon_error)?;
                    ClusterKvServiceResponse::AuthoritativeEntry(entry)
                }
                CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE => {
                    let entries: sinomo_core::kv::ClusterKvAuthoritativeEntriesRequest =
                        ctx.decode_request_body(&request).await?;
                    let keys = entries.keys;
                    let runtime = require_service_runtime(
                        runtime.as_ref(),
                        "cluster kv authoritative exact-key set",
                    )?;
                    let entries =
                        handle_daemon_cluster_kv_authoritative_entries_request_async_on_runtime(
                            &paths,
                            keys.clone(),
                            ctx.signer.is_local(),
                            runtime,
                        )
                        .await
                        .map_err(daemon_error)?;
                    ClusterKvServiceResponse::AuthoritativeEntries(
                        sinomo_core::kv::pack_cluster_kv_authoritative_entries_response(
                            &keys, &entries,
                        )?,
                    )
                }
                CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE => {
                    let range: sinomo_core::kv::ClusterKvAuthoritativeRangePrefixRequest =
                        ctx.decode_request_body(&request).await?;
                    let runtime = require_service_runtime(
                        runtime.as_ref(),
                        "cluster kv authoritative range-prefix",
                    )?;
                    let range =
                        handle_daemon_cluster_kv_authoritative_range_prefix_request_async_on_runtime(
                            &paths,
                            range.prefix,
                            ctx.signer.is_local(),
                            runtime,
                        )
                        .await
                        .map_err(daemon_error)?;
                    ClusterKvServiceResponse::AuthoritativeRange(range)
                }
                CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE => {
                    let watch: sinomo_core::kv::ClusterKvAuthoritativeWatchStartRequest =
                        ctx.decode_request_body(&request).await?;
                    let runtime = require_service_runtime(
                        runtime.as_ref(),
                        "cluster kv authoritative watch start",
                    )?;
                    let barrier = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
                    // Watch registrations are node-local. Until this complete RPC
                    // can be forwarded to the authority as one operation, require
                    // the receiving daemon to establish the barrier itself. A
                    // forwarded barrier followed by local registration could put
                    // the watch on a lagging follower.
                    let snapshot =
                        handle_daemon_cluster_kv_authoritative_snapshot_request_async_on_runtime(
                            &paths,
                            &barrier.body,
                            false,
                            runtime,
                        )
                        .await
                        .map_err(daemon_error)?;
                    let authority = snapshot.authority;
                    let watch_request = ClusterKvWatchPrefixRequest {
                        prefix: watch.prefix,
                        start_revision: authority.revision.next(),
                    };
                    let cluster_paths_for_task = cluster_paths.clone();
                    let registration = run_blocking_service_task(
                        route.clone(),
                        "cluster kv authoritative watch registration",
                        move || {
                            let node_id = cluster_kv_local_node_id(&cluster_paths_for_task)?;
                            sinomo_core::kv::create_cluster_kv_watch_for_node(
                                &cluster_paths_for_task.kv_paths(),
                                node_id,
                                watch_request,
                            )
                            .map_err(ServiceError::from)
                        },
                    )
                    .await?;
                    let response = KvAuthoritativeRead {
                        value: registration,
                        authority,
                    };
                    sinomo_core::kv::validate_cluster_kv_authoritative_watch_start_response(
                        &response,
                    )?;
                    ClusterKvServiceResponse::AuthoritativeWatch(response)
                }
                CLUSTER_KV_REQUEST_OUTCOME_ROUTE => {
                    let request: sinomo_core::kv::ClusterKvRequestOutcomeRequest =
                        ctx.decode_request_body(&request).await?;
                    let request =
                        sinomo_core::kv::cluster_kv_request_outcome_rpc_payload(&request)?;
                    let runtime =
                        require_service_runtime(runtime.as_ref(), "cluster kv request outcome")?;
                    let outcome =
                        handle_daemon_cluster_kv_request_outcome_request_async_on_runtime(
                            &paths,
                            &request.body,
                            ctx.signer.is_local(),
                            runtime,
                        )
                        .await
                        .map_err(daemon_error)?;
                    sinomo_core::kv::validate_cluster_kv_request_outcome_read(&outcome)?;
                    ClusterKvServiceResponse::RequestOutcome(outcome)
                }
                CLUSTER_KV_TXN_ROUTE => {
                    let request: sinomo_core::kv::KvTxnRequest =
                        ctx.decode_request_body(&request).await?;
                    let submitted_command_digest = request
                        .clone()
                        .into_command_request()
                        .digest()
                        .map_err(ServiceError::from)?;
                    let txn_paths = paths.clone();
                    let runtime =
                        require_service_runtime(runtime.as_ref(), "cluster kv transaction")?;
                    let mut result = match &ctx.signer {
                        ServiceSigner::Local | ServiceSigner::LocalNode { .. } => {
                            handle_daemon_cluster_kv_txn_rpc_request_compact_async_on_runtime(
                                &txn_paths, request, true, runtime,
                            )
                            .await
                        }
                        ServiceSigner::Remote { .. } => {
                            let cluster_paths_for_task = cluster_paths.clone();
                            let (member_key, policy_key) =
                                remote_cluster_kv_authorization_keys(&ctx.signer)?;
                            // The authorization rows are appended to the submitted
                            // transaction as value-and-revision compares. Evaluating
                            // them in the same Raft command is the linearization
                            // point: a stale local view fails closed at commit, so a
                            // separate quorum barrier here would only double the
                            // number of consensus rounds for every forwarded write.
                            let (member_entry, policy_entry) = run_blocking_service_task(
                                route.clone(),
                                "cluster kv remote authorization entries",
                                move || {
                                    let state =
                                        sinomo_core::cluster::read_cluster_state_without_kv(
                                            &cluster_paths_for_task,
                                        )?;
                                    let kv_paths = cluster_paths_for_task.kv_paths();
                                    let member_entry =
                                        sinomo_core::kv::read_cluster_kv_local_entry(
                                            &kv_paths,
                                            &state.local_node_id,
                                            &member_key,
                                        )?
                                        .value;
                                    let policy_entry =
                                        sinomo_core::kv::read_cluster_kv_local_entry(
                                            &kv_paths,
                                            &state.local_node_id,
                                            &policy_key,
                                        )?
                                        .value;
                                    Ok((member_entry, policy_entry))
                                },
                            )
                            .await?;
                            let request = fence_remote_cluster_kv_txn_request_with_entries(
                                member_entry.as_ref(),
                                policy_entry.as_ref(),
                                &ctx.signer,
                                request,
                            )?;
                            handle_forwarded_daemon_cluster_kv_txn_request_async_on_runtime(
                                &txn_paths, request, runtime,
                            )
                            .await
                        }
                    }
                    .map_err(daemon_error)?;
                    // A remote voter is allowed to append authenticated
                    // membership/capability compares, so preserve both
                    // identities: the caller's exact submitted transaction
                    // and the fenced command that entered Raft.
                    result.submitted_command_digest = Some(submitted_command_digest);
                    ClusterKvServiceResponse::Transaction(result)
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            response.respond(&mut ctx)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Daemon adapter for the cluster health RPC. `sinomo-network` owns health aggregation;
/// this service only supplies daemon-local probing through the existing cluster host code.
struct ClusterHealthService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
    include_authority_health: bool,
}

const CLUSTER_NETWORK_MAIN_ROUTES: &[&str] = &[
    sinomo_core::network::CLUSTER_HEALTH_ROUTE,
    CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
    CLUSTER_NETWORK_PING_ROUTE,
];
const CLUSTER_NETWORK_PASSIVE_ROUTES: &[&str] = &[
    CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
    CLUSTER_NETWORK_PING_ROUTE,
];

fn validate_cluster_connectivity_snapshot_record_count(
    observed_records: usize,
) -> Result<(), sinomo_core::network::NetworkError> {
    if observed_records > sinomo_core::network::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
        return Err(
            sinomo_core::network::NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records,
                max_records: sinomo_core::network::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
            },
        );
    }
    Ok(())
}

fn read_bounded_cluster_connectivity_snapshot(
    paths: &ClusterPaths,
) -> Result<Vec<sinomo_core::network::ClusterConnectivityRecord>, ServiceError> {
    let records = sinomo_core::network::read_local_cluster_connectivity_records(paths)
        .map_err(ServiceError::from)?;
    validate_cluster_connectivity_snapshot_record_count(records.len())
        .map_err(ServiceError::from)?;
    Ok(records)
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
    if request.has_streamed_value() {
        let decode_started_ns = daemon_service_timeline_started_ns(timeline);
        let ping_request: Result<sinomo_core::cluster::ClusterNetworkPingRequest, _> =
            request.decode_body().await;
        record_daemon_service_timeline_stage(
            timeline,
            sinomo_core::network::network_timeline_stage::PING_REQUEST_DECODE,
            sinomo_core::network::network_timeline_flag::REQUEST,
            decode_started_ns,
            ping_request.is_ok(),
        );
        let ping_request = ping_request?;
        let responder_node_id = cached_cluster_network_ping_node_id(paths, data_dir)?;
        let build_started_ns = daemon_service_timeline_started_ns(timeline);
        let response = sinomo_core::cluster::cluster_network_ping_response_for_node(
            &responder_node_id,
            ping_request,
            time::OffsetDateTime::now_utc(),
        );
        record_daemon_service_timeline_stage(
            timeline,
            sinomo_core::network::network_timeline_stage::PING_RESPONSE_BUILD,
            sinomo_core::network::network_timeline_flag::RESPONSE,
            build_started_ns,
            response.is_ok(),
        );
        let response = response?;
        let encode_started_ns = daemon_service_timeline_started_ns(timeline);
        let encoded = ObjectRpcOutgoingResponse::encode_value(CLUSTER_NETWORK_PING_ROUTE, response);
        record_daemon_service_timeline_stage(
            timeline,
            sinomo_core::network::network_timeline_stage::PING_RESPONSE_ENCODE,
            sinomo_core::network::network_timeline_flag::RESPONSE,
            encode_started_ns,
            encoded.is_ok(),
        );
        return encoded.map_err(ServiceError::from);
    }

    let ObjectRpcIncomingRequest {
        request: request_head,
        body: request_body,
    } = request;
    let decode_started_ns = daemon_service_timeline_started_ns(timeline);
    let ping_request = sinomo_core::cluster::decode_cluster_network_ping_rpc_request(&request_head);
    record_daemon_service_timeline_stage(
        timeline,
        sinomo_core::network::network_timeline_stage::PING_REQUEST_DECODE,
        sinomo_core::network::network_timeline_flag::REQUEST,
        decode_started_ns,
        ping_request.is_ok(),
    );
    let ping_request = ping_request?;

    match ping_request.payload_mode {
        sinomo_core::cluster::ClusterNetworkPingPayloadMode::EmbeddedSobj => {
            if !request_body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "embedded-SOBJ cluster network ping does not accept an attached body"
                        .to_string(),
                }
                .into());
            }
            let responder_node_id = cached_cluster_network_ping_node_id(paths, data_dir)?;
            let build_started_ns = daemon_service_timeline_started_ns(timeline);
            let response = sinomo_core::cluster::cluster_network_ping_response_for_node(
                &responder_node_id,
                ping_request,
                time::OffsetDateTime::now_utc(),
            );
            record_daemon_service_timeline_stage(
                timeline,
                sinomo_core::network::network_timeline_stage::PING_RESPONSE_BUILD,
                sinomo_core::network::network_timeline_flag::RESPONSE,
                build_started_ns,
                response.is_ok(),
            );
            let response = response?;
            let encode_started_ns = daemon_service_timeline_started_ns(timeline);
            let encoded =
                ObjectRpcOutgoingResponse::encode_value(CLUSTER_NETWORK_PING_ROUTE, response);
            record_daemon_service_timeline_stage(
                timeline,
                sinomo_core::network::network_timeline_stage::PING_RESPONSE_ENCODE,
                sinomo_core::network::network_timeline_flag::RESPONSE,
                encode_started_ns,
                encoded.is_ok(),
            );
            encoded.map_err(ServiceError::from)
        }
        sinomo_core::cluster::ClusterNetworkPingPayloadMode::SemanticAttachment => {
            sinomo_core::cluster::validate_cluster_network_ping_attachment_request(&ping_request)?;
            let declared_body_bytes = request_body.total_bytes();
            if declared_body_bytes != Some(ping_request.request_attachment_bytes)
                && !(ping_request.request_attachment_bytes == 0 && request_body.is_empty())
            {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "cluster network ping request attachment descriptor declares {declared_body_bytes:?} bytes, head declares {}",
                        ping_request.request_attachment_bytes
                    ),
                }
                .into());
            }

            // Consume ownership-bearing chunks directly. This keeps the benchmark on the
            // canonical bounded network body path without an application AsyncRead copy loop.
            let actual_request_attachment_bytes = request_body
                .discard_owned_chunks()
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "drain cluster network ping request attachment".to_string(),
                    detail: source.to_string(),
                })?;
            if actual_request_attachment_bytes != ping_request.request_attachment_bytes {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "cluster network ping request attachment delivered {actual_request_attachment_bytes} bytes, head declares {}",
                        ping_request.request_attachment_bytes
                    ),
                }
                .into());
            }

            let responder_node_id = cached_cluster_network_ping_node_id(paths, data_dir)?;
            let response =
                sinomo_core::cluster::cluster_network_ping_attachment_rpc_response_for_node(
                    &responder_node_id,
                    &ping_request,
                    actual_request_attachment_bytes,
                    time::OffsetDateTime::now_utc(),
                )?;
            let response_body = ObjectRpcBodySource::zeroes(ping_request.response_attachment_bytes);
            Ok(ObjectRpcOutgoingResponse::new(response, response_body))
        }
    }
}

#[inline]
fn daemon_service_timeline_started_ns(
    timeline: Option<sinomo_core::network::NetworkTimelineTraceContext>,
) -> u64 {
    timeline
        .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
        .unwrap_or(0)
}

#[inline]
fn record_daemon_service_timeline_stage(
    timeline: Option<sinomo_core::network::NetworkTimelineTraceContext>,
    stage: u16,
    flags: u16,
    started_ns: u64,
    success: bool,
) {
    let Some(trace) = timeline else {
        return;
    };
    sinomo_core::network::record_network_timeline_event_now(
        sinomo_core::network::NetworkTimelineEvent::new(
            trace,
            stage,
            flags
                | if success {
                    sinomo_core::network::network_timeline_flag::SUCCESS
                } else {
                    0
                },
            0,
            0,
            0,
            0,
            0,
            sinomo_core::network::network_timeline_monotonic_ns().saturating_sub(started_ns),
        ),
    );
}

impl Service for ClusterHealthService {
    fn name(&self) -> &'static str {
        "cluster-network"
    }

    fn routes(&self) -> &'static [&'static str] {
        if self.include_authority_health {
            CLUSTER_NETWORK_MAIN_ROUTES
        } else {
            CLUSTER_NETWORK_PASSIVE_ROUTES
        }
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(network_module_admin_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let include_authority_health = self.include_authority_health;
        Box::pin(async move {
            let cluster_paths = ClusterPaths::new(ctx.data_dir.as_ref());
            if request.route == CLUSTER_NETWORK_PING_ROUTE {
                let request_body = ctx.take_request_body();
                let response = handle_daemon_cluster_network_ping_request(
                    &paths,
                    ctx.data_dir.as_ref(),
                    ObjectRpcIncomingRequest::new(request, request_body),
                )
                .await?;
                ctx.respond_with_body(response.response, response.body)?;
                return Ok(ServiceDisposition::Continue);
            }
            if request.route == CLUSTER_NETWORK_CONNECTIVITY_ROUTE {
                let _: () = ctx.decode_request_body(&request).await?;
                let response = run_blocking_service_task(
                    CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
                    "cluster network connectivity",
                    move || read_bounded_cluster_connectivity_snapshot(&cluster_paths),
                )
                .await?;
                ctx.respond_value(CLUSTER_NETWORK_CONNECTIVITY_ROUTE, response)?;
                return Ok(ServiceDisposition::Continue);
            }
            if !include_authority_health {
                return Err(ServiceError::UnknownRoute(request.route));
            }
            let _: () = ctx.decode_request_body(&request).await?;
            let runtime = require_service_runtime(runtime.as_ref(), "cluster health")?;
            let probe = DaemonClusterStatusProbe::new(&cluster_paths, runtime);
            let response =
                sinomo_core::cluster::current_cluster_health_rows_for_decoded_request_async(
                    runtime,
                    &cluster_paths,
                    &probe,
                    &request,
                    time::OffsetDateTime::now_utc(),
                )
                .await
                .map_err(ServiceError::from)?;
            ctx.respond_value(request.route, response)?;
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

fn service_signer_origin_name(signer: &ServiceSigner) -> &'static str {
    if signer.is_local() { "local" } else { "remote" }
}

fn cluster_member_publish_outcome_name(accepted: bool, detail: &str) -> &'static str {
    if !accepted {
        "rejected"
    } else if detail == "member endpoints already current" {
        "already_current"
    } else if detail == "member endpoints published" {
        "published"
    } else {
        "accepted_other"
    }
}

fn observe_cluster_member_publish_response(
    signer_origin: &'static str,
    signer_node_id: &str,
    member_count: usize,
    response: &sinomo_core::cluster::ClusterMemberPublishResponse,
    duration: Duration,
) {
    let outcome = cluster_member_publish_outcome_name(response.accepted, &response.detail);
    let duration_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
    metrics::counter!(
        "sinomo.daemon.cluster.member_publish.responses",
        "origin" => signer_origin,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.daemon.cluster.member_publish.us",
        "origin" => signer_origin,
        "outcome" => outcome
    )
    .record(duration_us as f64);
    tracing::trace!(
        target: "sinomo.daemon.cluster_member_publish",
        signer_origin,
        signer_node_id,
        member_count,
        outcome,
        accepted = response.accepted,
        snapshot_returned = response.kv_snapshot.is_some(),
        duration_us,
        detail = %response.detail,
        "cluster member publish completed"
    );
}

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
            match route.as_str() {
                CLUSTER_RAFT_APPEND_ROUTE => {
                    let payload: ClusterRaftAppendRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = sinomo_core::cluster::handle_cluster_raft_append_request_async(
                        runtime,
                        &cluster_paths,
                        payload,
                        &authenticated_peer,
                    )
                    .await
                    .map_err(ServiceError::from)?;
                    ctx.respond_value(CLUSTER_RAFT_APPEND_ROUTE, response)?;
                }
                CLUSTER_RAFT_VOTE_ROUTE => {
                    let payload: ClusterRaftVoteRequest = ctx.decode_request_body(&request).await?;
                    let response = sinomo_core::cluster::handle_cluster_raft_vote_request_async(
                        runtime,
                        &cluster_paths,
                        payload,
                        &authenticated_peer,
                    )
                    .await
                    .map_err(ServiceError::from)?;
                    ctx.respond_value(CLUSTER_RAFT_VOTE_ROUTE, response)?;
                }
                other => {
                    return Err(ServiceError::UnknownRoute(other.to_string()));
                }
            };
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
/// Server-side budgets for join handling. These are intentionally tolerant of
/// real-world transports; the Docker cluster's stricter formation gate belongs
/// only to its near-instant test environment.
const CLUSTER_JOIN_CHALLENGE_BUDGET: std::time::Duration = std::time::Duration::from_secs(20);
const CLUSTER_JOIN_REGISTRATION_BUDGET: std::time::Duration = std::time::Duration::from_secs(25);

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
        service_route_categories(self.routes(), CLUSTER_MEMBERSHIP_RPC_CATEGORY)
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            if request.route == CLUSTER_JOIN_CHALLENGE_ROUTE {
                let challenge: sinomo_core::cluster::ClusterInviteChallengeRequest =
                    ctx.decode_request_body(&request).await?;
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
                ctx.respond_value(CLUSTER_JOIN_CHALLENGE_ROUTE, proof)?;
                return Ok(ServiceDisposition::Continue);
            }
            let cluster_paths = cluster_paths_from_daemon_paths(&paths).map_err(daemon_error)?;
            if request.route == CLUSTER_JOIN_REGISTER_ROUTE {
                let payload: sinomo_core::cluster::ClusterJoinRegistrationRequest =
                    ctx.decode_request_body(&request).await?;
                let register_started = std::time::Instant::now();
                let invite_id = payload.invite.invite_id.clone();
                crate::cluster_join_trace(format_args!(
                    "register invite={} received with {} member(s)",
                    invite_id,
                    payload.members.len()
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
                    // The cluster join handler has already persisted its
                    // revision-fenced local projection. Wake maintenance here;
                    // a second read/merge/write would only duplicate I/O and
                    // could reapply an older response after a concurrent join.
                    crate::control::request_daemon_cluster_maintenance(
                        crate::control::DaemonClusterMaintenanceReason::JoinAccepted,
                    );
                }
                ctx.respond_value(CLUSTER_JOIN_REGISTER_ROUTE, response)?;
                return Ok(ServiceDisposition::Continue);
            }
            let service_runtime =
                require_service_runtime(runtime.as_ref(), "cluster membership publish")?;
            let mut publish_host = DaemonClusterMemberMutationHost::new_on_runtime(service_runtime);
            match request.route.as_str() {
                CLUSTER_INVITE_PUBLISH_ROUTE => {
                    let payload: sinomo_core::cluster::ClusterInvitePublishRequest =
                        ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(CLUSTER_INVITE_PUBLISH_ROUTE, response)?;
                }
                CLUSTER_MEMBER_PUBLISH_ROUTE => {
                    let payload: sinomo_core::cluster::ClusterMemberPublishRequest =
                        ctx.decode_request_body(&request).await?;
                    let member_count = payload.members.len();
                    let signer_origin = service_signer_origin_name(&ctx.signer);
                    let (signer_node_id, _) =
                        service_signer_cluster_identity(&ctx.signer, CLUSTER_MEMBER_PUBLISH_ROUTE)?;
                    let publish_started = Instant::now();
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
                    observe_cluster_member_publish_response(
                        signer_origin,
                        &signer_node_id,
                        member_count,
                        &response,
                        publish_started.elapsed(),
                    );
                    ctx.respond_value(CLUSTER_MEMBER_PUBLISH_ROUTE, response)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
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

fn deny_remote_cluster_control_authority(reason: &str) -> ServiceError {
    tracing::warn!(
        target: "sinomo.daemon.cluster_control_auth",
        reason,
        "remote cluster-control mutation denied by authoritative membership policy"
    );
    ServiceError::Network(NetworkError::AuthDenied)
}

fn validate_remote_cluster_control_authority(
    snapshot: &sinomo_core::kv::KvSnapshot,
    signer: &ServiceSigner,
) -> Result<RemoteClusterControlAuthorityFence, ServiceError> {
    let ServiceSigner::Remote {
        node_id,
        public_key,
    } = signer
    else {
        return Err(deny_remote_cluster_control_authority(
            "fresh cluster-control authorization requires a remote signer",
        ));
    };
    let node_id = node_id
        .parse::<sinomo_core::node::NodeId>()
        .map_err(|_| deny_remote_cluster_control_authority("signer node id is invalid"))?;
    let member_key = sinomo_core::node::cluster_member_key(&node_id);
    let member_entry = snapshot
        .get(&member_key)
        .ok_or_else(|| deny_remote_cluster_control_authority("signer member is absent"))?;
    let member: sinomo_core::ClusterMemberRecord = member_entry.decode()?;
    if member.node_id != node_id || member.node_public_key != *public_key || !member.has_raft_vote()
    {
        return Err(deny_remote_cluster_control_authority(
            "signer voter identity is stale, demoted, or revoked",
        ));
    }

    let config_entry = snapshot
        .get(&sinomo_core::kv::cluster_kv_raft_config_key())
        .ok_or_else(|| deny_remote_cluster_control_authority("committed Raft config is absent"))?;
    let config: sinomo_core::kv::ClusterKvRaftConfigRecord = config_entry.decode()?;
    if config.cluster_id != member.cluster_id || !config.voters.contains(&node_id) {
        return Err(deny_remote_cluster_control_authority(
            "signer is not a voter in the committed Raft config",
        ));
    }

    let policy_entry = snapshot
        .get(&sinomo_core::node::cluster_capability_policy_key(&node_id))
        .ok_or_else(|| {
            deny_remote_cluster_control_authority("signer capability policy is absent")
        })?;
    let policy: sinomo_core::node::ClusterCapabilityPolicyRecord = policy_entry.decode()?;
    let required = sinomo_core::cluster::CLUSTER_CONTROL_ADMIN_CAPABILITY;
    let advertises_required = policy
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == required);
    let allows_required = policy
        .allowed_capabilities
        .iter()
        .any(|capability| capability.as_str() == required);
    if policy.cluster_id != member.cluster_id
        || policy.node_id != node_id
        || !advertises_required
        || !allows_required
    {
        return Err(deny_remote_cluster_control_authority(
            "signer capability policy does not grant cluster.control.admin",
        ));
    }
    Ok(RemoteClusterControlAuthorityFence::new(
        node_id,
        public_key.clone(),
        member_entry.clone(),
        config_entry.clone(),
        policy_entry.clone(),
    ))
}

async fn authorize_cluster_control_service_request(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    route: &str,
    signer: &ServiceSigner,
) -> Result<Option<RemoteClusterControlAuthorityFence>, ServiceError> {
    let policy = sinomo_core::cluster::cluster_control_rpc_route_policy(route)
        .ok_or_else(|| ServiceError::UnknownRoute(route.to_string()))?;
    if policy.auth == sinomo_core::kv::ClusterKvRouteAuth::LocalOnly {
        return signer
            .is_local()
            .then_some(None)
            .ok_or_else(|| deny_remote_cluster_control_authority("route is local-only"));
    }
    if signer.is_local() {
        return Ok(None);
    }
    if !matches!(signer, ServiceSigner::Remote { .. }) {
        return Err(deny_remote_cluster_control_authority(
            "non-local signer has no authenticated cluster identity",
        ));
    }

    let barrier = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
    let authoritative = handle_daemon_cluster_kv_authoritative_snapshot_request_async_on_runtime(
        paths,
        &barrier.body,
        true,
        runtime,
    )
    .await
    .map_err(daemon_error)?;
    if authoritative.authority.revision != authoritative.value.revision
        || !matches!(
            authoritative.authority.basis,
            sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks,
                required_quorum,
            } if required_quorum > 0 && voter_acks >= required_quorum
        )
    {
        return Err(deny_remote_cluster_control_authority(
            "fresh current-term quorum authority could not be established",
        ));
    }
    validate_remote_cluster_control_authority(&authoritative.value, signer).map(Some)
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
            let request = match route.as_str() {
                sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE => {
                    ClusterControlServiceRequest::Init(ctx.decode_request_body(&request).await?)
                }
                sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE => {
                    ClusterControlServiceRequest::Join(ctx.decode_request_body(&request).await?)
                }
                sinomo_core::cluster::CLUSTER_CONTROL_INVITE_ROUTE => {
                    ClusterControlServiceRequest::Invite(ctx.decode_request_body(&request).await?)
                }
                sinomo_core::cluster::CLUSTER_CONTROL_RENAME_ROUTE => {
                    ClusterControlServiceRequest::Rename(ctx.decode_request_body(&request).await?)
                }
                sinomo_core::cluster::CLUSTER_CONTROL_ROLE_ROUTE => {
                    ClusterControlServiceRequest::Role(ctx.decode_request_body(&request).await?)
                }
                sinomo_core::cluster::CLUSTER_CONTROL_REMOVE_ROUTE => {
                    ClusterControlServiceRequest::Remove(ctx.decode_request_body(&request).await?)
                }
                sinomo_core::cluster::CLUSTER_CONTROL_LEAVE_ROUTE => {
                    ClusterControlServiceRequest::Leave(ctx.decode_request_body(&request).await?)
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            let runtime = require_service_runtime(runtime.as_ref(), "cluster control")?.clone();
            let remote_authority =
                authorize_cluster_control_service_request(&paths, &runtime, &route, &ctx.signer)
                    .await?;
            let response = run_blocking_service_task(route.clone(), "cluster control", move || {
                handle_daemon_cluster_control_request(&paths, request, &runtime, remote_authority)
            })
            .await?;
            response.respond(&mut ctx)?;
            Ok(cluster_control_service_disposition(&route))
        })
    }
}

enum ClusterControlServiceRequest {
    Init(sinomo_core::cluster::ClusterControlInitRequest),
    Join(sinomo_core::cluster::ClusterControlJoinRequest),
    Invite(sinomo_core::cluster::ClusterControlInviteRequest),
    Rename(sinomo_core::cluster::ClusterControlRenameRequest),
    Role(sinomo_core::cluster::ClusterControlRoleRequest),
    Remove(sinomo_core::cluster::ClusterControlRemoveRequest),
    Leave(sinomo_core::cluster::ClusterControlLeaveRequest),
}

enum ClusterControlServiceResponse {
    Init(sinomo_core::cluster::ClusterControlInitResponse),
    Join(sinomo_core::cluster::ClusterControlJoinResponse),
    Invite(sinomo_core::cluster::ClusterControlInviteResponse),
    Rename(sinomo_core::cluster::ClusterControlRenameResponse),
    Role(sinomo_core::cluster::ClusterControlRoleResponse),
    Remove(sinomo_core::cluster::ClusterControlRemoveResponse),
    Leave(sinomo_core::cluster::ClusterControlLeaveResponse),
}

impl ClusterControlServiceResponse {
    fn respond(self, ctx: &mut ServiceContext) -> Result<(), ServiceError> {
        match self {
            Self::Init(response) => {
                ctx.respond_value(sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE, response)
            }
            Self::Join(response) => {
                ctx.respond_value(sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE, response)
            }
            Self::Invite(response) => {
                ctx.respond_value(sinomo_core::cluster::CLUSTER_CONTROL_INVITE_ROUTE, response)
            }
            Self::Rename(response) => {
                ctx.respond_value(sinomo_core::cluster::CLUSTER_CONTROL_RENAME_ROUTE, response)
            }
            Self::Role(response) => {
                ctx.respond_value(sinomo_core::cluster::CLUSTER_CONTROL_ROLE_ROUTE, response)
            }
            Self::Remove(response) => {
                ctx.respond_value(sinomo_core::cluster::CLUSTER_CONTROL_REMOVE_ROUTE, response)
            }
            Self::Leave(response) => {
                ctx.respond_value(sinomo_core::cluster::CLUSTER_CONTROL_LEAVE_ROUTE, response)
            }
        }
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
        && cluster_control_service_restart_required(route)
        && let Some(paths) = paths
    {
        if let Err(error) = crate::rollout::request_supervised_main_restart(paths) {
            tracing::error!(%error, "could not mark cluster identity restart for the shadow supervisor");
        }
    }
}

/// Dispatches one `/cluster/control/*` request to the daemon-side operation.
fn handle_daemon_cluster_control_request(
    paths: &DaemonPaths,
    request: ClusterControlServiceRequest,
    runtime: &tokio::runtime::Handle,
    mut remote_authority: Option<RemoteClusterControlAuthorityFence>,
) -> Result<ClusterControlServiceResponse, ServiceError> {
    match request {
        ClusterControlServiceRequest::Init(payload) => {
            let response = handle_daemon_cluster_control_init_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            Ok(ClusterControlServiceResponse::Init(response))
        }
        ClusterControlServiceRequest::Join(payload) => {
            let response = handle_daemon_cluster_control_join_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            Ok(ClusterControlServiceResponse::Join(response))
        }
        ClusterControlServiceRequest::Invite(payload) => {
            let response = handle_daemon_cluster_control_invite_with_authority_on_runtime(
                paths,
                payload,
                runtime,
                remote_authority.take(),
            )
            .map_err(daemon_error)?;
            Ok(ClusterControlServiceResponse::Invite(response))
        }
        ClusterControlServiceRequest::Rename(payload) => {
            let response = handle_daemon_cluster_control_rename_with_authority_on_runtime(
                paths,
                payload,
                runtime,
                remote_authority.take(),
            )
            .map_err(daemon_error)?;
            Ok(ClusterControlServiceResponse::Rename(response))
        }
        ClusterControlServiceRequest::Role(payload) => {
            let response = handle_daemon_cluster_control_role_with_authority_on_runtime(
                paths,
                payload,
                runtime,
                remote_authority.take(),
            )
            .map_err(daemon_error)?;
            Ok(ClusterControlServiceResponse::Role(response))
        }
        ClusterControlServiceRequest::Remove(payload) => {
            let response = handle_daemon_cluster_control_remove_with_authority_on_runtime(
                paths,
                payload,
                runtime,
                remote_authority.take(),
            )
            .map_err(daemon_error)?;
            Ok(ClusterControlServiceResponse::Remove(response))
        }
        ClusterControlServiceRequest::Leave(payload) => {
            let response = handle_daemon_cluster_control_leave_on_runtime(paths, payload, runtime)
                .map_err(daemon_error)?;
            Ok(ClusterControlServiceResponse::Leave(response))
        }
    }
}

pub(crate) const NETWORK_MODULES_LIST_ROUTE: &str = "/network/modules";
pub(crate) const NETWORK_MODULES_SET_ENABLED_ROUTE: &str = "/network/modules/set-enabled";
const NETWORK_MODULES_RPC_CATEGORY: &str = "network.modules";
const NETWORK_MODULES_ADMIN_CAPABILITY: &str = "network.modules.admin";
const NETWORK_MODULES_RPC_ROUTES: &[&str] = &[
    NETWORK_MODULES_LIST_ROUTE,
    NETWORK_MODULES_SET_ENABLED_ROUTE,
];

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
struct NetworkModuleListResponse {
    modules: Vec<sinomo_core::network::NetworkModuleStatus>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
struct NetworkModuleSetEnabledRequest {
    id: String,
    enabled: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
struct NetworkModuleSetEnabledResponse {
    module: sinomo_core::network::NetworkModuleStatus,
}

enum NetworkModuleServiceRequest {
    List,
    SetEnabled(NetworkModuleSetEnabledRequest),
}

enum NetworkModuleServiceResponse {
    List(NetworkModuleListResponse),
    SetEnabled(NetworkModuleSetEnabledResponse),
}

impl NetworkModuleServiceResponse {
    fn respond(self, ctx: &mut ServiceContext) -> Result<(), ServiceError> {
        match self {
            Self::List(response) => ctx.respond_value(NETWORK_MODULES_LIST_ROUTE, response),
            Self::SetEnabled(response) => {
                ctx.respond_value(NETWORK_MODULES_SET_ENABLED_ROUTE, response)
            }
        }
    }
}

fn network_module_service_route(route: &'static str) -> ServiceRoute {
    ServiceRoute::from_route_metadata(
        route,
        NETWORK_MODULES_RPC_CATEGORY,
        "admin",
        [NETWORK_MODULES_ADMIN_CAPABILITY],
    )
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
        NETWORK_MODULES_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(network_module_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        Box::pin(async move {
            let route = request.route.clone();
            let request = match route.as_str() {
                NETWORK_MODULES_LIST_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    NetworkModuleServiceRequest::List
                }
                NETWORK_MODULES_SET_ENABLED_ROUTE => NetworkModuleServiceRequest::SetEnabled(
                    ctx.decode_request_body(&request).await?,
                ),
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            // Module start/stop hooks block (dylib work on its own runtime);
            // keep them off the async workers like the other daemon services.
            let response = run_blocking_service_task(route.clone(), "network modules", move || {
                handle_daemon_network_modules_request(&paths, &request)
            })
            .await?;
            response.respond(&mut ctx)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Dispatches one `/network/modules*` request against the daemon's loaded
/// network modules.
fn handle_daemon_network_modules_request(
    paths: &DaemonPaths,
    request: &NetworkModuleServiceRequest,
) -> Result<NetworkModuleServiceResponse, ServiceError> {
    let modules = crate::registry::daemon_network_modules().ok_or_else(|| {
        ServiceError::Service("daemon network module registry is not initialized".to_string())
    })?;
    match request {
        NetworkModuleServiceRequest::List => {
            let response = NetworkModuleListResponse {
                modules: modules.registry.snapshot(),
            };
            Ok(NetworkModuleServiceResponse::List(response))
        }
        NetworkModuleServiceRequest::SetEnabled(payload) => {
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
            let response = NetworkModuleSetEnabledResponse { module: status };
            Ok(NetworkModuleServiceResponse::SetEnabled(response))
        }
    }
}

/// Daemon-foundational service: rollout binary staging + source build. An accepted
/// rollout on the main daemon must restart the process, reported via
/// `ServiceDisposition::Stop` (the adapter then drains and stops). A handler error is
/// reported as a `failed` response rather than propagated, matching the prior dispatch.
struct RolloutBinaryService {
    paths: DaemonPaths,
}

/// Daemon-owned rollout maintenance commands registered through the canonical
/// session dispatcher, the sole ordinary RPC entry point.
struct RolloutControlService {
    paths: DaemonPaths,
}

impl Service for RolloutControlService {
    fn name(&self) -> &'static str {
        "rollout-control"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[ROLLOUT_FINALIZE_SHADOW_ROUTE, ROLLOUT_CLEAR_STATUS_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(rollout_daemon_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        Box::pin(async move {
            let _: () = ctx.decode_request_body(&request).await?;
            let route = request.route;
            let response = match route.as_str() {
                ROLLOUT_FINALIZE_SHADOW_ROUTE => {
                    run_blocking_service_task(route.clone(), "rollout finalization", move || {
                        handle_daemon_finalize_shadow_rollout(&paths)
                            .map_err(|error| ServiceError::Service(error.to_string()))
                    })
                    .await?
                }
                ROLLOUT_CLEAR_STATUS_ROUTE => {
                    run_blocking_service_task(route.clone(), "rollout clear-status", move || {
                        handle_daemon_clear_rollout_status(&paths)
                            .map_err(|error| ServiceError::Service(error.to_string()))
                    })
                    .await?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond_value(route, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

impl RolloutBinaryService {
    fn finish(
        paths: &DaemonPaths,
        result: Result<ClusterRolloutBinaryResponse, DaemonError>,
    ) -> Result<(ClusterRolloutBinaryResponse, ServiceDisposition), ServiceError> {
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
        Ok((response, disposition))
    }
}

async fn decode_rollout_build_session_request(
    ctx: &mut ServiceContext,
    request: &ObjectRpcRequest,
) -> Result<sinomo_rollout::ClusterRolloutBuildRequest, ServiceError> {
    let mut payload: sinomo_rollout::ClusterRolloutBuildRequest =
        ctx.decode_request_body(request).await?;
    let mut body = ctx.take_request_body();
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
                let service_route = rollout_daemon_service_route(route);
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
                    let payload = ctx.decode_request_body(&request).await?;
                    let mut body = ctx.take_request_body();
                    let result =
                        handle_daemon_rollout_binary_request(&paths, payload, &mut body).await;
                    Self::finish(&paths, result)?
                }
                ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let rollout_paths = paths.clone();
                    run_blocking_service_task(
                        route.clone(),
                        "rollout native dylib module",
                        move || {
                            Self::finish(
                                &rollout_paths,
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
                    let payload = decode_rollout_build_session_request(&mut ctx, &request).await?;
                    let rollout_paths = paths.clone();
                    run_blocking_service_task(route.clone(), "rollout build", move || {
                        Self::finish(
                            &rollout_paths,
                            handle_daemon_rollout_build_request(&rollout_paths, payload),
                        )
                    })
                    .await?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond_value(route, response)?;
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
    stat_cache: Mutex<HashMap<String, DaemonDfsBrowserCachedStat>>,
}

#[derive(Clone, Debug)]
struct DaemonDfsBrowserCachedStat {
    cached_at: Instant,
    response: crate::DaemonDfsBrowserStatResponse,
}

fn daemon_dfs_browser_read_file_response(
    outcome: sinomo_dfs::DfsPosixDistributedFileReadOutcome,
) -> crate::DaemonDfsBrowserReadFileResponse {
    match outcome {
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::Found { bytes } => {
            crate::DaemonDfsBrowserReadFileResponse::found(bytes)
        }
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::NotFound { path } => {
            crate::DaemonDfsBrowserReadFileResponse::not_found(path)
        }
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail } => {
            crate::DaemonDfsBrowserReadFileResponse::content_unavailable(path, detail)
        }
    }
}

fn daemon_dfs_browser_read_file_prefix_response(
    outcome: sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome,
) -> crate::DaemonDfsBrowserReadFilePrefixResponse {
    match outcome {
        sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::Found { read } => {
            crate::DaemonDfsBrowserReadFilePrefixResponse::found(read.bytes, read.truncated)
        }
        sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::NotFound { path } => {
            crate::DaemonDfsBrowserReadFilePrefixResponse::not_found(path)
        }
        sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable {
            path,
            detail,
        } => crate::DaemonDfsBrowserReadFilePrefixResponse::content_unavailable(path, detail),
    }
}

fn daemon_dfs_browser_bulk_file_read_response(
    requested_path: String,
    outcome: sinomo_dfs::DfsPosixDistributedFileReadOutcome,
) -> crate::DaemonDfsBrowserFileRead {
    match outcome {
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::Found { bytes } => {
            crate::DaemonDfsBrowserFileRead::found(requested_path, bytes)
        }
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::NotFound { path } => {
            let mut response = crate::DaemonDfsBrowserFileRead::not_found(path);
            response.path = requested_path;
            response
        }
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail } => {
            let mut response = crate::DaemonDfsBrowserFileRead::content_unavailable(path, detail);
            response.path = requested_path;
            response
        }
    }
}

#[derive(Clone, Debug)]
struct DaemonDfsBrowserObservedControlPlaneKvClient {
    cluster_paths: ClusterPaths,
}

impl DaemonDfsBrowserObservedControlPlaneKvClient {
    fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
}

impl sinomo_dfs::DfsControlPlaneKvClient for DaemonDfsBrowserObservedControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        sinomo_core::kv::read_cluster_kv_cached_entries_consistently(
            &self.cluster_paths.kv_paths(),
            keys,
        )
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn get_authoritative_consistent(
        &mut self,
        _keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        Err(KvError::AuthorityUnavailable {
            detail: "observational DFS browser KV checkpoint cannot authorize cluster state"
                .to_string(),
        })
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
        let _ = txn;
        Err(self.kv_error("observational DFS browser KV client cannot submit transactions"))
    }
}

#[derive(Default)]
struct DaemonDfsBrowserBackendSlot {
    backend: Mutex<Option<Arc<DaemonDfsBrowserBackend>>>,
}

static DAEMON_DFS_BROWSER_BACKENDS: OnceLock<
    Mutex<HashMap<PathBuf, Arc<DaemonDfsBrowserBackendSlot>>>,
> = OnceLock::new();

#[cfg(test)]
static DAEMON_DFS_BROWSER_BACKEND_OPEN_COUNTS: OnceLock<Mutex<HashMap<PathBuf, usize>>> =
    OnceLock::new();

#[cfg(test)]
fn record_daemon_dfs_browser_backend_open(data_dir: &SinomoDataDir) {
    let counts = DAEMON_DFS_BROWSER_BACKEND_OPEN_COUNTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut counts = counts.lock().expect("DFS browser open-count lock");
    *counts.entry(data_dir.root()).or_default() += 1;
}

#[cfg(test)]
fn daemon_dfs_browser_backend_open_count(data_dir: &SinomoDataDir) -> usize {
    DAEMON_DFS_BROWSER_BACKEND_OPEN_COUNTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("DFS browser open-count lock")
        .get(&data_dir.root())
        .copied()
        .unwrap_or_default()
}

// Browser reads run on blocking workers because the DFS POSIX surface is
// synchronous. A remote content fetch can consume the full 30-second POSIX
// deadline while holding the shared service lock. Do not let the other browser
// workers park behind that request: ordinary short local operations still get
// a small handoff window, while degraded remote reads fail fast and let their
// callers retry without building an in-process queue. This is an admission
// bound only; it does not impose an end-to-end timeout on an admitted DFS read.
const DAEMON_DFS_BROWSER_READ_LOCK_ADMISSION_TIMEOUT: Duration = Duration::from_millis(100);
const DAEMON_DFS_BROWSER_PLACEMENT_IMPACT_PAGE_SIZE: usize = 256;
const DAEMON_DFS_BROWSER_STAT_CACHE_MAX_ENTRIES: usize = 256;

impl DaemonDfsBrowserBackend {
    fn open(
        data_dir: SinomoDataDir,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, ServiceError> {
        #[cfg(test)]
        record_daemon_dfs_browser_backend_open(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let control_plane =
            daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime));
        let (peer_transport, async_peer_transport) =
            daemon_dfs_peer_transports_on_runtime(&data_dir, runtime);
        let opened = sinomo_dfs::open_mounted_sifs_service_with_peer_transports(
            data_dir.clone(),
            peer_transport,
            async_peer_transport,
            control_plane,
        )
        .map_err(dfs_browser_service_error)?;
        Ok(Self {
            data_dir,
            service: Mutex::new(opened.service),
            peer_targets_refreshed_at: Mutex::new(Some(Instant::now())),
            stat_cache: Mutex::new(HashMap::new()),
        })
    }

    fn for_data_dir(
        data_dir: Arc<SinomoDataDir>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Arc<Self>, ServiceError> {
        let root = data_dir.root();
        let cache = DAEMON_DFS_BROWSER_BACKENDS.get_or_init(|| Mutex::new(HashMap::new()));
        let slot = {
            let mut guard = cache.lock().map_err(|source| {
                ServiceError::Service(format!("DFS browser backend cache lock failed: {source}"))
            })?;
            Arc::clone(
                guard
                    .entry(root)
                    .or_insert_with(|| Arc::new(DaemonDfsBrowserBackendSlot::default())),
            )
        };
        let mut initialized = slot.backend.lock().map_err(|source| {
            ServiceError::Service(format!(
                "DFS browser backend initialization lock failed: {source}"
            ))
        })?;
        if let Some(backend) = initialized.as_ref() {
            return Ok(Arc::clone(backend));
        }
        let backend = Arc::new(Self::open((*data_dir).clone(), runtime)?);
        *initialized = Some(Arc::clone(&backend));
        Ok(backend)
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
        let service = self.lock_service_for_read()?;
        let metadata = service.metadata_backend().ok_or_else(|| {
            ServiceError::Service("DFS browser metadata backend is unavailable".to_string())
        })?;
        let status =
            sinomo_dfs::inspect_dfs_status_from_metadata(&self.data_dir, &cluster_paths, metadata)
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
        let include_metadata = request.include_metadata;
        let mut entries: Vec<_> = match service.posix_list_dir_distributed(&path) {
            Ok(entries) => entries,
            Err(sinomo_dfs::DfsPosixError::NotFound { .. }) if request.missing_is_empty => {
                Vec::new()
            }
            Err(error) => return Err(dfs_browser_service_error(error)),
        }
        .into_iter()
        .map(|entry| {
            let metadata = include_metadata.then(|| crate::DaemonDfsBrowserEntryMetadata {
                object_id: entry.object_id.to_string(),
                version: entry.version.to_string(),
                modified_at_unix_nanos: entry.modified_at_unix_nanos,
                policy: entry.policy,
                current_locations: entry
                    .current_locations
                    .into_iter()
                    .map(|node_id| node_id.to_string())
                    .collect(),
                placement: None,
            });
            crate::DaemonDfsBrowserEntry {
                name: entry.name,
                path: entry.path,
                kind: entry.kind,
                size: entry.size_bytes,
                metadata,
            }
        })
        .collect();
        sort_daemon_dfs_browser_entries(&mut entries, &request.sort_key, &request.sort_direction);
        let total_count = entries.len();
        let offset = request.offset.min(total_count);
        let limit = request.limit;
        let mut entries: Vec<crate::DaemonDfsBrowserEntry> = match limit {
            Some(limit) => entries.into_iter().skip(offset).take(limit).collect(),
            None => entries.into_iter().skip(offset).collect(),
        };
        if include_metadata {
            for entry in &mut entries {
                let placement = entry.metadata.as_ref().and_then(|metadata| {
                    daemon_dfs_browser_entry_placement(&mut service, &entry.path, metadata)
                });
                if let Some(metadata) = entry.metadata.as_mut() {
                    metadata.placement = placement;
                }
            }
        }
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
    ) -> Result<crate::DaemonDfsBrowserStatOutcomeResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        if let Some(response) = self.cached_stat(&path, request.max_staleness_millis) {
            return Ok(crate::DaemonDfsBrowserStatOutcomeResponse::found(response));
        }
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let stat = match service.posix_stat_distributed(&path) {
            Ok(stat) => stat,
            Err(sinomo_dfs::DfsPosixError::NotFound { path: missing_path }) => {
                match service.posix_namespace_lstat_distributed(&path) {
                    Err(sinomo_dfs::DfsPosixError::NotFound { .. }) => {
                        return Ok(crate::DaemonDfsBrowserStatOutcomeResponse::not_found(
                            missing_path,
                        ));
                    }
                    // A dangling symbolic link has a confirmed absent target,
                    // which is POSIX stat NotFound. Missing link-body content
                    // is reported separately as SymlinkContentPending.
                    Ok(candidate) if candidate.kind == sinomo_dfs::DfsObjectKind::Symlink => {
                        return Ok(crate::DaemonDfsBrowserStatOutcomeResponse::not_found(
                            missing_path,
                        ));
                    }
                    Ok(candidate) => {
                        return Err(dfs_browser_service_error(format!(
                            "SIFS stat could not resolve {path}, but authoritative namespace metadata still contains {:?} object {}",
                            candidate.kind, candidate.object_id
                        )));
                    }
                    Err(error) => return Err(dfs_browser_service_error(error)),
                }
            }
            Err(error) => return Err(dfs_browser_service_error(error)),
        };
        let placement = service
            .resolved_placement_policy_for_path(
                &stat.path,
                stat.policy.clone(),
                stat.current_locations.len().max(1),
            )
            .ok();
        let health = daemon_dfs_browser_object_health(&service, &stat.object_id);
        let local_pins =
            daemon_dfs_browser_local_pins(&self.data_dir, &stat.file_system_id, &stat.path);
        let local_node_id = service.node_id().clone();
        drop(service);
        let metadata_shard = self
            .observed_metadata_shard_for_stat(&stat.path)
            .ok()
            .flatten()
            .map(|shard| daemon_dfs_browser_metadata_shard(shard, &local_node_id));
        let response =
            daemon_dfs_browser_stat_response(stat, local_pins, health, metadata_shard, placement);
        self.cache_stat(path, response.clone());
        Ok(crate::DaemonDfsBrowserStatOutcomeResponse::found(response))
    }

    fn cached_stat(
        &self,
        path: &str,
        max_staleness_millis: u64,
    ) -> Option<crate::DaemonDfsBrowserStatResponse> {
        if max_staleness_millis == 0 {
            return None;
        }
        let max_staleness = Duration::from_millis(max_staleness_millis);
        let cache = self.stat_cache.lock().ok()?;
        let cached = cache.get(path)?;
        (cached.cached_at.elapsed() <= max_staleness).then(|| cached.response.clone())
    }

    fn cache_stat(&self, path: String, response: crate::DaemonDfsBrowserStatResponse) {
        let Ok(mut cache) = self.stat_cache.lock() else {
            return;
        };
        if cache.len() >= DAEMON_DFS_BROWSER_STAT_CACHE_MAX_ENTRIES
            && !cache.contains_key(&path)
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, cached)| cached.cached_at)
                .map(|(path, _)| path.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert(
            path,
            DaemonDfsBrowserCachedStat {
                cached_at: Instant::now(),
                response,
            },
        );
    }

    fn invalidate_stat_cache(&self) {
        if let Ok(mut cache) = self.stat_cache.lock() {
            cache.clear();
        }
    }

    /// Add best-effort topology context to an already completed `stat` read.
    /// This materialized view never selects a route, authorizes a mutation, or
    /// turns absence into an operational decision; the caller drops any error.
    fn observed_metadata_shard_for_stat(
        &self,
        path: &str,
    ) -> Result<Option<sinomo_dfs::DfsMetadataShardConfig>, ServiceError> {
        let path = normalize_sifs_browser_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Ok(None);
        }
        let mut kv = DaemonDfsBrowserObservedControlPlaneKvClient::new(&cluster_paths);
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
        let outcome = service
            .posix_read_file_distributed_outcome(&path)
            .map_err(dfs_browser_service_error)?;
        Ok(daemon_dfs_browser_read_file_response(outcome))
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
        let outcome = service
            .posix_read_file_prefix_distributed_outcome(&path, max_bytes)
            .map_err(dfs_browser_service_error)?;
        Ok(daemon_dfs_browser_read_file_prefix_response(outcome))
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
        let normalized_paths = paths
            .iter()
            .map(|(_, normalized)| normalized.clone())
            .collect::<Vec<_>>();
        // Keep one browser request as one DFS bulk read. Besides grouping
        // content fetches by source, this lets the DFS layer issue one
        // LookupPaths request per metadata primary instead of holding the
        // resident browser service lock across one routed lookup per file.
        let results = service.posix_read_files_distributed_outcomes(&normalized_paths);
        let files = paths
            .into_iter()
            .zip(results)
            .map(|((requested_path, _), result)| match result {
                Ok(outcome) => daemon_dfs_browser_bulk_file_read_response(requested_path, outcome),
                Err(error) => {
                    crate::DaemonDfsBrowserFileRead::read_failed(requested_path, error.to_string())
                }
            })
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
        let options = sifs_browser_write_options(&mut service, &path, &replica_candidates)?;
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
        let options = match sifs_browser_write_options(&mut service, &path, &replica_candidates) {
            Ok(options) => options,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: Outcome::Rejected {
                        detail: error.to_string(),
                    },
                });
            }
        };
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
        self.invalidate_stat_cache();
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
        let removed = sinomo_dfs::remove_local_dfs_pin(
            sinomo_dfs::default_dfs_pin_root(&self.data_dir),
            &stat.file_system_id,
            &stat.path,
            Some(&node_id),
            Some(&mode),
        )
        .map_err(dfs_browser_service_error)?;
        if removed {
            self.invalidate_stat_cache();
        }
        Ok(removed)
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
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| ServiceError::Service(error.to_string()))?;
        let local_node_id = state.local_node_id;
        let mut kv = daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime))
            .ok_or_else(|| {
                ServiceError::Service(
                    "SIFS metadata primary transfer has no authoritative control plane".to_string(),
                )
            })?;
        let mut store = sinomo_dfs::DfsControlPlaneStore::new(kv.as_mut());
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
        self.invalidate_stat_cache();
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

    fn placement_preview(
        &self,
        request: crate::DaemonDfsBrowserPlacementPreviewRequest,
        runtime: &tokio::runtime::Handle,
    ) -> Result<crate::DaemonDfsBrowserPlacementPreviewResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err(ServiceError::Service(
                "managed DFS placement requires a cluster".to_string(),
            ));
        }
        let mut kv = daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime))
            .ok_or_else(|| {
                ServiceError::Service(
                    "managed DFS placement preview has no authoritative control plane".to_string(),
                )
            })?;
        let base_config = sinomo_dfs::DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&sinomo_dfs::sifs_file_system_id())
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service("SIFS cluster policy is unavailable".to_string())
            })?;
        let mut config = base_config.clone();
        mutate_daemon_dfs_browser_placement_config(
            &mut config,
            &path,
            request.scope,
            request.policy.clone(),
        )?;
        // `config` already contains the proposed mutation, including a
        // `policy=None` removal. Passing a materialized config through the
        // shared helper prevents a second override from restoring that rule.
        let (preview, _, _) = self.preview_placement_for_config(
            &cluster_paths,
            &base_config,
            &config,
            &path,
            request.scope,
        )?;
        Ok(crate::DaemonDfsBrowserPlacementPreviewResponse { preview })
    }

    fn placement_update_v2(
        &self,
        request: crate::DaemonDfsBrowserPlacementUpdateV2Request,
        runtime: &tokio::runtime::Handle,
    ) -> Result<crate::DaemonDfsBrowserPlacementUpdateResponse, ServiceError> {
        self.placement_update_inner(
            request.path,
            request.policy,
            request.scope,
            request.expected_preview_token,
            runtime,
        )
    }

    fn placement_update_inner(
        &self,
        path: String,
        policy: Option<sinomo_dfs::DfsPlacementPolicy>,
        scope: sinomo_dfs::DfsPlacementScope,
        expected_preview_token: sinomo_dfs::DfsPlacementPreviewToken,
        runtime: &tokio::runtime::Handle,
    ) -> Result<crate::DaemonDfsBrowserPlacementUpdateResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err(ServiceError::Service(
                "managed DFS placement requires a cluster".to_string(),
            ));
        }
        let mut kv = daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime))
            .ok_or_else(|| {
                ServiceError::Service(
                    "managed DFS placement update has no authoritative control plane".to_string(),
                )
            })?;
        let mut store = sinomo_dfs::DfsControlPlaneStore::new(kv.as_mut());
        let expected = store
            .get_file_system_config(&sinomo_dfs::sifs_file_system_id())
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service("SIFS cluster policy is unavailable".to_string())
            })?;
        let mut desired = expected.clone();
        mutate_daemon_dfs_browser_placement_config(&mut desired, &path, scope, policy)?;
        let (preview, unavailable_node_ids, candidate_snapshot) =
            self.preview_placement_for_config(&cluster_paths, &expected, &desired, &path, scope)?;
        require_matching_daemon_dfs_browser_placement_preview_token(
            &expected_preview_token,
            &preview.preview_token,
        )?;
        require_satisfiable_daemon_dfs_browser_placement(&preview)?;
        let now = time::OffsetDateTime::now_utc()
            .unix_timestamp_nanos()
            .try_into()
            .unwrap_or(1_000_000_000);
        let reconciliation = sinomo_dfs::DfsPlacementReconciliationJob {
            schema_version: sinomo_dfs::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: sinomo_core::Id::generate().to_string(),
            file_system_id: desired.file_system_id.clone(),
            path,
            scope,
            unavailable_node_ids,
            candidate_snapshot,
            accepted_config: Some(desired.clone()),
            state: sinomo_dfs::DfsPlacementReconciliationState::Pending,
            cursor: None,
            total_object_count: 0,
            scanned_object_count: 0,
            matched_object_count: 0,
            updated_object_count: 0,
            scheduled_repair_count: 0,
            completed_repair_count: 0,
            pending_repair_count: 0,
            revision: 0,
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
            last_error: None,
        };
        if !store
            .compare_and_put_placement_update(&expected, &desired, &reconciliation)
            .map_err(dfs_browser_service_error)?
        {
            return Err(ServiceError::Service(
                "SIFS placement policy changed concurrently; refresh the preview and retry"
                    .to_string(),
            ));
        }
        self.invalidate_stat_cache();
        let reconciliation = store
            .placement_reconciliation(&desired.file_system_id, &reconciliation.job_id)
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service(
                    "accepted SIFS placement job is missing from the control plane".to_string(),
                )
            })?;
        crate::dfs::notify_daemon_dfs_maintenance_work();
        Ok(crate::DaemonDfsBrowserPlacementUpdateResponse {
            preview,
            reconciliation,
        })
    }

    fn preview_placement_for_config(
        &self,
        cluster_paths: &ClusterPaths,
        base_config: &sinomo_dfs::DfsFileSystemConfig,
        config: &sinomo_dfs::DfsFileSystemConfig,
        path: &str,
        scope: sinomo_dfs::DfsPlacementScope,
    ) -> Result<
        (
            sinomo_dfs::DfsPlacementPreview,
            BTreeSet<sinomo_core::node::NodeId>,
            Vec<sinomo_dfs::DfsNodeAdvertisement>,
        ),
        ServiceError,
    > {
        let (stat, observer_node_id, candidates) = {
            let mut service = self.lock_service_for_read()?;
            self.refresh_peer_targets(&mut service);
            let stat = service
                .posix_stat_distributed(path)
                .map_err(dfs_browser_service_error)?;
            let observer_node_id = service.node_id().clone();
            let candidates = service
                .placement_content_candidates()
                .map_err(dfs_browser_service_error)?;
            (stat, observer_node_id, candidates)
        };
        if candidates.is_empty() {
            return Err(ServiceError::Service(
                "cannot apply SIFS placement without content candidates".to_string(),
            ));
        }
        let object_fence = sinomo_dfs::DfsPlacementPreviewObjectFence::from(&stat);
        let initial_connectivity =
            daemon_dfs_browser_placement_connectivity_snapshot(cluster_paths);
        let unavailable_node_ids = initial_connectivity.explicitly_unavailable_node_ids();
        let mut preview = sinomo_dfs::preview_placement_with_unavailable_nodes(
            config,
            &stat.path,
            stat.object_id.clone(),
            stat.version.clone(),
            stat.policy.clone(),
            observer_node_id,
            stat.current_locations.clone(),
            candidates.clone(),
            None,
            scope,
            &unavailable_node_ids,
        )
        .map_err(dfs_browser_service_error)?;

        let mut aggregate_impact = sinomo_dfs::DfsPlacementImpact::default();
        let mut aggregate_diagnostics = Vec::new();
        let mut cursor = None;
        loop {
            // The cached service is shared by all browser calls. Holding its
            // mutex for one bounded metadata page keeps large subtree previews
            // from starving unrelated reads and writes.
            let page = {
                let service = self.lock_service_for_read()?;
                service
                    .placement_impact_page_for_config_with_unavailable_nodes(
                        config,
                        &stat.path,
                        scope,
                        &candidates,
                        cursor.as_ref(),
                        DAEMON_DFS_BROWSER_PLACEMENT_IMPACT_PAGE_SIZE,
                        &unavailable_node_ids,
                    )
                    .map_err(dfs_browser_service_error)?
            };
            sinomo_dfs::merge_placement_impact(&mut aggregate_impact, &page.impact);
            extend_unique_daemon_dfs_browser_placement_diagnostics(
                &mut aggregate_diagnostics,
                page.blocking_diagnostics,
            );
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        preview.impact = aggregate_impact;
        extend_daemon_dfs_browser_preview_diagnostics(&mut preview, aggregate_diagnostics);

        // Connectivity can change while a multi-page impact scan is running.
        // Re-read it after releasing the final service guard and fail closed if
        // a node selected by the stable plan has since become explicitly down.
        let final_connectivity = daemon_dfs_browser_placement_connectivity_snapshot(cluster_paths);
        enrich_daemon_dfs_browser_placement_connectivity_from_snapshot(
            &mut preview,
            &final_connectivity,
        );
        let compatibility_state = sinomo_core::cluster::read_cluster_state_without_kv(
            cluster_paths,
        )
        .map_err(|error| {
            ServiceError::Service(format!(
                "cannot verify placement daemon compatibility: {error}"
            ))
        })?;
        if let Some(diagnostic) =
            sinomo_dfs::placement_reconciliation_compatibility_diagnostic(&compatibility_state)
            && !preview.blocking_diagnostics.contains(&diagnostic)
        {
            preview.blocking_diagnostics.push(diagnostic);
        }
        if !preview.blocking_diagnostics.is_empty() {
            preview.impact.complete = false;
        }
        // Keep the planning exclusions stable across the scan. A candidate
        // that recovers before the final observational refresh must not be
        // silently reintroduced by reconciliation, while newly unavailable
        // optional candidates should remain excluded as well.
        let mut reconciliation_unavailable_node_ids = unavailable_node_ids;
        reconciliation_unavailable_node_ids.extend(preview.unavailable_node_ids());
        sinomo_dfs::bind_placement_preview_token(
            &mut preview,
            base_config,
            config,
            path,
            scope,
            &object_fence,
            &candidates,
            &reconciliation_unavailable_node_ids,
        )
        .map_err(dfs_browser_service_error)?;
        Ok((preview, reconciliation_unavailable_node_ids, candidates))
    }

    fn placement_progress(
        &self,
        request: crate::DaemonDfsBrowserPlacementProgressRequest,
    ) -> Result<crate::DaemonDfsBrowserPlacementProgressResponse, ServiceError> {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let mut kv = DaemonDfsBrowserObservedControlPlaneKvClient::new(&cluster_paths);
        let reconciliation = sinomo_dfs::DfsControlPlaneStore::new(&mut kv)
            .placement_reconciliation(&sinomo_dfs::sifs_file_system_id(), &request.job_id)
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service(format!(
                    "unknown DFS placement reconciliation job: {}",
                    request.job_id
                ))
            })?;
        Ok(crate::DaemonDfsBrowserPlacementProgressResponse { reconciliation })
    }

    fn placement_activity(
        &self,
        request: crate::DaemonDfsBrowserPlacementActivityRequest,
    ) -> Result<crate::DaemonDfsBrowserPlacementActivityResponse, ServiceError> {
        if request
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.job_id.trim().is_empty())
        {
            return Err(ServiceError::Service(
                "DFS placement activity cursor has an empty job id".to_string(),
            ));
        }
        let path_prefix = request
            .path_prefix
            .as_deref()
            .map(normalize_sifs_browser_path)
            .transpose()?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let mut kv = DaemonDfsBrowserObservedControlPlaneKvClient::new(&cluster_paths);
        let page = sinomo_dfs::DfsControlPlaneStore::new(&mut kv)
            .placement_reconciliation_activity(
                &sinomo_dfs::sifs_file_system_id(),
                &request.statuses,
                path_prefix.as_deref(),
                request.cursor.as_ref(),
                request.limit,
            )
            .map_err(dfs_browser_service_error)?;
        Ok(crate::DaemonDfsBrowserPlacementActivityResponse { page })
    }

    fn flush_after_mutation(
        &self,
        service: &mut sinomo_dfs::MountedSifsService,
    ) -> Result<(), ServiceError> {
        self.invalidate_stat_cache();
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
            match route.as_str() {
                DAEMON_DFS_BROWSER_STATUS_ROUTE => {
                    let payload: crate::DaemonDfsBrowserStatusRequest =
                        ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(
                        DAEMON_DFS_BROWSER_STATUS_ROUTE,
                        crate::DaemonDfsBrowserStatusResponse {
                            node_id: backend.node_id()?,
                            health,
                            health_error,
                        },
                    )?;
                }
                DAEMON_DFS_BROWSER_LIST_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser list", move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?.list(payload)
                        })
                        .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_LIST_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_STAT_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser stat", move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?.stat(payload)
                        })
                        .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_STAT_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_FILE_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser read file",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .read_file(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_FILE_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser read object version",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .read_object_version(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser read file prefix",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .read_file_prefix(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_FILES_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser read files",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .read_files(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_FILES_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE => {
                    let payload: crate::DaemonDfsBrowserWriteFileRequest =
                        ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE => {
                    let payload: crate::DaemonDfsBrowserReplaceFileIfVersionRequest =
                        ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE => {
                    let payload: crate::DaemonDfsBrowserCreateDirAllRequest =
                        ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_RENAME_ROUTE => {
                    let payload: crate::DaemonDfsBrowserRenameRequest =
                        ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(DAEMON_DFS_BROWSER_RENAME_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_DELETE_ROUTE => {
                    let payload: crate::DaemonDfsBrowserDeleteRequest =
                        ctx.decode_request_body(&request).await?;
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
                    ctx.respond_value(DAEMON_DFS_BROWSER_DELETE_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE => {
                    let payload: crate::DaemonDfsBrowserPinMetadataRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser pin metadata",
                        move || {
                            let backend =
                                DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?;
                            let result = backend.pin_metadata(payload)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    event_hub.publish_dfs_placement_change(
                        result.1.clone(),
                        "pin-metadata",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE => {
                    let payload: crate::DaemonDfsBrowserUnpinMetadataRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser unpin metadata",
                        move || {
                            let backend =
                                DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?;
                            let result = backend.unpin_metadata(payload)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    if result.0.removed {
                        event_hub.publish_dfs_placement_change(
                            result.1.clone(),
                            "unpin-metadata",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE => {
                    let payload: crate::DaemonDfsBrowserPinContentRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser pin content",
                        move || {
                            let backend =
                                DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?;
                            let result = backend.pin_content(payload)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    event_hub.publish_dfs_placement_change(
                        result.1.clone(),
                        "pin-content",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE => {
                    let payload: crate::DaemonDfsBrowserUnpinContentRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser unpin content",
                        move || {
                            let backend =
                                DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?;
                            let result = backend.unpin_content(payload)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    if result.0.removed {
                        event_hub.publish_dfs_placement_change(
                            result.1.clone(),
                            "unpin-content",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE => {
                    let payload: crate::DaemonDfsBrowserTransferMetadataPrimaryRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser metadata primary transfer",
                        move || {
                            let backend =
                                DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?;
                            let result = backend.transfer_metadata_primary(payload, &runtime)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    if result.0.changed {
                        event_hub.publish_dfs_placement_change(
                            result.1.clone(),
                            "transfer-metadata-primary",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(
                        DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
                        result.0,
                    )?;
                }
                DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser placement preview",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .placement_preview(payload, &runtime)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE => {
                    let _: crate::DaemonDfsBrowserPlacementUpdateRequest =
                        ctx.decode_request_body(&request).await?;
                    return Err(legacy_daemon_dfs_browser_placement_update_error());
                }
                DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE => {
                    let payload: crate::DaemonDfsBrowserPlacementUpdateV2Request =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser placement update v2",
                        move || {
                            let backend =
                                DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?;
                            let result = backend.placement_update_v2(payload, &runtime)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    event_hub.publish_dfs_placement_change(
                        result.1.clone(),
                        "update-policy",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser placement progress",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .placement_progress(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser placement activity",
                        move || {
                            DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)?
                                .placement_activity(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE, result)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            Ok(ServiceDisposition::Continue)
        })
    }
}

struct DaemonDfsPlacementAuthorityAdminService {
    paths: DaemonPaths,
    runtime: Option<tokio::runtime::Handle>,
}

fn require_local_dfs_placement_authority_break_glass_signer(
    signer: &ServiceSigner,
) -> Result<(), ServiceError> {
    signer.is_local().then_some(()).ok_or_else(|| {
        ServiceError::Service(
            "DFS placement-authority break-glass recovery requires a local signer".to_string(),
        )
    })
}

impl Service for DaemonDfsPlacementAuthorityAdminService {
    fn name(&self) -> &'static str {
        "dfs-placement-authority-admin"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(
                DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
                DAEMON_RPC_CATEGORY,
            )
            .with_auth_scope("admin")
            .require_capability("dfs.placement-authority.admin")
            .with_concurrency_limit(1)
            .with_timeout(Duration::from_secs(30)),
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "DFS placement authority admin")?.clone();
            let payload: crate::DaemonDfsPlacementAuthorityAdminRequest =
                ctx.decode_request_body(&request).await?;
            let response = match payload {
                crate::DaemonDfsPlacementAuthorityAdminRequest::Inspect { file_system_id } => {
                    let permit = run_blocking_service_task(
                        DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
                        "DFS placement authority inspection",
                        move || {
                            inspect_dfs_placement_authority_permit(
                                &paths,
                                &runtime,
                                &file_system_id,
                            )
                            .map_err(|error| ServiceError::Service(error.to_string()))
                        },
                    )
                    .await?;
                    crate::DaemonDfsPlacementAuthorityAdminResponse::Inspection { permit }
                }
                crate::DaemonDfsPlacementAuthorityAdminRequest::RecoverRemovedNode {
                    file_system_id,
                    expected_recovery_token,
                    acknowledge_owner_stopped,
                } => {
                    let outcome = run_blocking_service_task(
                        DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
                        "DFS placement authority removed-node recovery",
                        move || {
                            recover_removed_node_dfs_placement_authority_permit(
                                &paths,
                                &runtime,
                                &file_system_id,
                                &expected_recovery_token,
                                acknowledge_owner_stopped,
                            )
                            .map_err(|error| ServiceError::Service(error.to_string()))
                        },
                    )
                    .await?;
                    crate::DaemonDfsPlacementAuthorityAdminResponse::Recovery { outcome }
                }
                crate::DaemonDfsPlacementAuthorityAdminRequest::BreakGlassRecover {
                    file_system_id,
                    expected_recovery_token,
                    acknowledgement,
                } => {
                    require_local_dfs_placement_authority_break_glass_signer(&ctx.signer)?;
                    let outcome = run_blocking_service_task(
                        DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
                        "DFS placement authority break-glass recovery",
                        move || {
                            break_glass_recover_dfs_placement_authority_permit(
                                &paths,
                                &runtime,
                                &file_system_id,
                                &expected_recovery_token,
                                &acknowledgement,
                            )
                            .map_err(|error| ServiceError::Service(error.to_string()))
                        },
                    )
                    .await?;
                    crate::DaemonDfsPlacementAuthorityAdminResponse::Recovery { outcome }
                }
            };
            ctx.respond_value(DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn daemon_dfs_browser_stat_response(
    stat: sinomo_dfs::DfsStat,
    local_pins: Vec<crate::DaemonDfsBrowserPinRecord>,
    health: Option<crate::DaemonDfsBrowserObjectHealth>,
    metadata_shard: Option<crate::DaemonDfsBrowserMetadataShard>,
    placement: Option<sinomo_dfs::DfsResolvedPlacementPolicy>,
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
        placement,
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

fn daemon_dfs_browser_entry_placement(
    service: &mut sinomo_dfs::MountedSifsService,
    path: &str,
    metadata: &crate::DaemonDfsBrowserEntryMetadata,
) -> Option<crate::DaemonDfsBrowserEntryPlacement> {
    let object_id = sinomo_dfs::DfsObjectId::new(&metadata.object_id).ok()?;
    let current_replicas = metadata.current_locations.len();
    let desired_replicas = service
        .resolved_placement_policy_for_path(path, metadata.policy.clone(), current_replicas.max(1))
        .map(|placement| placement.policy.desired_replicas)
        .unwrap_or_else(|_| current_replicas.max(1))
        .max(1);
    let agent_state = daemon_dfs_browser_object_health(service, &object_id)
        .map(|health| health.agent_state)
        .unwrap_or_else(|| {
            if current_replicas == 0 {
                sinomo_dfs::DfsAgentObjectState::MetadataOnly
            } else if current_replicas < desired_replicas {
                sinomo_dfs::DfsAgentObjectState::PendingRepair
            } else {
                sinomo_dfs::DfsAgentObjectState::Current
            }
        });
    Some(crate::DaemonDfsBrowserEntryPlacement {
        agent_state,
        desired_replicas,
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
    service: &mut sinomo_dfs::MountedSifsService,
    path: &str,
    replica_candidates: &[sinomo_core::node::NodeId],
) -> Result<sinomo_dfs::DfsWriteOptions, ServiceError> {
    let object_policy = service
        .posix_stat(path)
        .map(|stat| stat.policy)
        .unwrap_or(sinomo_core::DfsObjectPolicy::ReplicatedEventual);
    let (_, _, policy) = service
        .managed_replication_policy_for_path(path, object_policy, replica_candidates.len())
        .map_err(dfs_browser_service_error)?;
    let candidates = sifs_browser_write_candidates(replica_candidates, &policy);
    let options = sinomo_dfs::DfsWriteOptions {
        policy,
        replica_candidates: candidates,
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    Ok(options)
}

fn sifs_browser_write_candidates(
    replica_candidates: &[sinomo_core::node::NodeId],
    policy: &sinomo_dfs::DfsReplicationPolicy,
) -> Vec<sinomo_core::node::NodeId> {
    let mut candidates = replica_candidates.to_vec();
    for required in &policy.required_node_ids {
        if !candidates.contains(required) {
            candidates.push(required.clone());
        }
    }
    candidates
}

fn dfs_browser_service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}

fn mutate_daemon_dfs_browser_placement_config(
    config: &mut sinomo_dfs::DfsFileSystemConfig,
    path: &str,
    scope: sinomo_dfs::DfsPlacementScope,
    policy: Option<sinomo_dfs::DfsPlacementPolicy>,
) -> Result<(), ServiceError> {
    match policy {
        Some(policy) => {
            let rule = sinomo_dfs::normalize_placement_rule(path, scope, policy)
                .map_err(dfs_browser_service_error)?;
            sinomo_dfs::upsert_placement_rule(&mut config.subtree_policies, rule);
        }
        None => {
            sinomo_dfs::remove_placement_rule(&mut config.subtree_policies, path, scope)
                .map_err(dfs_browser_service_error)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct DaemonDfsBrowserPlacementConnectivitySnapshot {
    local_node_id: Option<sinomo_core::node::NodeId>,
    graph: Option<ClusterConnectivityGraph>,
    advertised_endpoints: Vec<sinomo_core::ClusterEndpointRecord>,
}

impl DaemonDfsBrowserPlacementConnectivitySnapshot {
    fn reachability(
        &self,
        node_id: &sinomo_core::node::NodeId,
    ) -> sinomo_dfs::DfsPlacementNodeReachability {
        let Some(local_node_id) = self.local_node_id.as_ref() else {
            return sinomo_dfs::DfsPlacementNodeReachability::Unknown;
        };
        if node_id == local_node_id {
            return sinomo_dfs::DfsPlacementNodeReachability::Local;
        }
        let Some(graph) = self.graph.as_ref() else {
            return sinomo_dfs::DfsPlacementNodeReachability::Unknown;
        };
        daemon_dfs_browser_placement_node_reachability(
            local_node_id,
            node_id,
            graph,
            &self.advertised_endpoints,
        )
    }

    fn explicitly_unavailable_node_ids(&self) -> BTreeSet<sinomo_core::node::NodeId> {
        self.advertised_endpoints
            .iter()
            .map(|endpoint| endpoint.node_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|node_id| {
                self.reachability(node_id) == sinomo_dfs::DfsPlacementNodeReachability::Unreachable
            })
            .collect()
    }
}

fn daemon_dfs_browser_placement_connectivity_snapshot(
    cluster_paths: &ClusterPaths,
) -> DaemonDfsBrowserPlacementConnectivitySnapshot {
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths) else {
        return DaemonDfsBrowserPlacementConnectivitySnapshot::default();
    };
    let mut advertised_endpoints = cluster_directory_for(cluster_paths)
        .endpoints()
        .unwrap_or_default();
    if advertised_endpoints.is_empty() {
        advertised_endpoints.extend(
            state
                .members
                .iter()
                .flat_map(|member| member.endpoints.iter().cloned()),
        );
    }
    let graph = ClusterConnectivityGraph::try_read_for_local(cluster_paths, &state).ok();
    DaemonDfsBrowserPlacementConnectivitySnapshot {
        local_node_id: Some(state.local_node_id),
        graph,
        advertised_endpoints,
    }
}

fn enrich_daemon_dfs_browser_placement_connectivity_from_snapshot(
    preview: &mut sinomo_dfs::DfsPlacementPreview,
    connectivity: &DaemonDfsBrowserPlacementConnectivitySnapshot,
) {
    const DESIRED_BECAME_UNREACHABLE: &str =
        "blocked: this desired replica became explicitly unreachable before apply";

    let mut new_diagnostics = Vec::new();
    for node in &mut preview.nodes {
        node.reachability = connectivity.reachability(&node.node_id);
        if node.reachability != sinomo_dfs::DfsPlacementNodeReachability::Unreachable {
            continue;
        }
        if node.required {
            new_diagnostics.push(
                sinomo_dfs::DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                    node_id: node.node_id.clone(),
                },
            );
        }
        if node.desired_replica {
            new_diagnostics.push(
                sinomo_dfs::DfsPlacementBlockingDiagnostic::DesiredNodeUnavailable {
                    node_id: node.node_id.clone(),
                },
            );
            if !node.explanation.contains(DESIRED_BECAME_UNREACHABLE) {
                if !node.explanation.is_empty() {
                    node.explanation.push_str("; ");
                }
                node.explanation.push_str(DESIRED_BECAME_UNREACHABLE);
            }
        }
    }
    extend_unique_daemon_dfs_browser_placement_diagnostics(
        &mut preview.blocking_diagnostics,
        new_diagnostics,
    );
}

#[cfg(test)]
fn merge_daemon_dfs_browser_placement_impact(
    aggregate: &mut sinomo_dfs::DfsPlacementImpact,
    page: &sinomo_dfs::DfsPlacementImpact,
) {
    sinomo_dfs::merge_placement_impact(aggregate, page);
}

fn extend_unique_daemon_dfs_browser_placement_diagnostics(
    diagnostics: &mut Vec<sinomo_dfs::DfsPlacementBlockingDiagnostic>,
    additional: impl IntoIterator<Item = sinomo_dfs::DfsPlacementBlockingDiagnostic>,
) {
    for diagnostic in additional {
        if !diagnostics.contains(&diagnostic) {
            diagnostics.push(diagnostic);
        }
    }
}

fn extend_daemon_dfs_browser_preview_diagnostics(
    preview: &mut sinomo_dfs::DfsPlacementPreview,
    additional: impl IntoIterator<Item = sinomo_dfs::DfsPlacementBlockingDiagnostic>,
) {
    let anchor_source = preview
        .resolved
        .source_path
        .clone()
        .zip(preview.resolved.source_scope)
        .unwrap_or_else(|| {
            (
                preview.resolved.path.clone(),
                sinomo_dfs::DfsPlacementScope::SelfOnly,
            )
        });
    for diagnostic in additional {
        let duplicates_anchor = matches!(
            &diagnostic,
            sinomo_dfs::DfsPlacementBlockingDiagnostic::ScopedPolicy {
                path,
                scope,
                diagnostic,
            } if path == &anchor_source.0
                && scope == &anchor_source.1
                && preview
                    .blocking_diagnostics
                    .iter()
                    .any(|existing| existing == diagnostic.as_ref())
        );
        if !duplicates_anchor && !preview.blocking_diagnostics.contains(&diagnostic) {
            preview.blocking_diagnostics.push(diagnostic);
        }
    }
}

fn daemon_dfs_browser_placement_node_reachability(
    local_node_id: &sinomo_core::node::NodeId,
    node_id: &sinomo_core::node::NodeId,
    graph: &ClusterConnectivityGraph,
    advertised_endpoints: &[sinomo_core::ClusterEndpointRecord],
) -> sinomo_dfs::DfsPlacementNodeReachability {
    use sinomo_dfs::DfsPlacementNodeReachability as Reachability;

    if node_id == local_node_id {
        return Reachability::Local;
    }
    if cluster_route_next_hop(graph, local_node_id, node_id).is_some() {
        return Reachability::Reachable;
    }
    let mut node_endpoints = advertised_endpoints
        .iter()
        .filter(|endpoint| &endpoint.node_id == node_id)
        .peekable();
    if node_endpoints.peek().is_some()
        && node_endpoints.all(|endpoint| {
            matches!(
                graph.state_for_endpoint(endpoint),
                Some(ClusterConnectivityState::Unreachable)
            )
        })
    {
        Reachability::Unreachable
    } else {
        Reachability::Unknown
    }
}

fn require_satisfiable_daemon_dfs_browser_placement(
    preview: &sinomo_dfs::DfsPlacementPreview,
) -> Result<(), ServiceError> {
    if preview.is_satisfiable() {
        return Ok(());
    }
    let diagnostics = preview
        .blocking_diagnostics
        .iter()
        .map(sinomo_dfs::DfsPlacementBlockingDiagnostic::message)
        .collect::<Vec<_>>()
        .join("; ");
    Err(ServiceError::Service(format!(
        "cannot apply unsatisfiable SIFS placement policy: {diagnostics}"
    )))
}

fn legacy_daemon_dfs_browser_placement_update_error() -> ServiceError {
    ServiceError::Service(
        "legacy SIFS placement Apply is disabled because it cannot verify the reviewed preview; refresh the preview and retry with a client that uses /daemon/dfs/browser/placement-update-v2"
            .to_string(),
    )
}

fn require_matching_daemon_dfs_browser_placement_preview_token(
    expected: &sinomo_dfs::DfsPlacementPreviewToken,
    current: &sinomo_dfs::DfsPlacementPreviewToken,
) -> Result<(), ServiceError> {
    if expected.is_well_formed() && current.is_well_formed() && expected == current {
        return Ok(());
    }
    Err(ServiceError::Service(
        "SIFS placement preview is stale; refresh the preview and review the updated plan before applying"
            .to_string(),
    ))
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
    let is_shadow = crate::daemon_paths_are_shadow(paths);
    let event_hub = Arc::new(DaemonEventHub::new());
    let mut builder = ServiceRegistry::builder();
    if observe_module_should_register(paths) {
        builder = sinomo_observe::register_services(builder);
    }
    // Config definitions and arbitrary extension hooks may receive
    // `DaemonClusterAccess`; keep them with the main authority owner. The
    // shadow remains a bounded passive/recovery endpoint.
    if !is_shadow {
        let config_registry = daemon_config_registry(paths, service_hooks);
        builder = register_config_services(builder, config_registry);
    }
    builder = register_rollout_services(builder)
        .register(DaemonControlService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
        })
        .register(DaemonEventService::new(
            service_runtime.cloned(),
            Arc::clone(&event_hub),
        ))
        .register(ClusterHealthService {
            paths: paths.clone(),
            runtime: service_runtime.cloned(),
            include_authority_health: !is_shadow,
        })
        .register(NetworkModulesService {
            paths: paths.clone(),
        })
        .register(RolloutControlService {
            paths: paths.clone(),
        })
        .register(RolloutBinaryService {
            paths: paths.clone(),
        });
    if !is_shadow {
        builder = builder
            .register(ClusterKvService {
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
            });
    }
    // Keep the peer DFS protocol routes on both registries as a fail-closed
    // mixed-version surface. Current DFS callers require a main-daemon terminal;
    // a legacy affinity-neutral replica mutation that reaches the shadow must
    // receive the explicit data-owner rejection instead of an ambiguous
    // "unknown route".
    builder = if let Some(runtime) = service_runtime {
        sinomo_dfs::register_services_with_data_plane_host(
            builder,
            Arc::new(DaemonDfsDataPlaneHost::new_on_runtime(
                paths.clone(),
                runtime,
            )),
        )
    } else {
        register_dfs_services(builder)
    };
    // Main and shadow share the data-dir build root. Only the main daemon owns
    // local UI/control services whose state is not part of the peer DFS protocol.
    if !is_shadow {
        builder = builder
            .register(DaemonDfsBrowserService {
                runtime: service_runtime.cloned(),
                event_hub: Arc::clone(&event_hub),
            })
            .register(crate::build_service::DaemonBuildService::new(paths))
            .register(DaemonDfsPlacementAuthorityAdminService {
                paths: paths.clone(),
                runtime: service_runtime.cloned(),
            });
        builder = sinomo_sandbox::register_services_with_factory(
            builder,
            Arc::new(DaemonSandboxBackendFactory {
                paths: paths.clone(),
                runtime: service_runtime.cloned(),
            }),
        );
        for hook in service_hooks {
            builder = hook.register(builder);
        }
    }
    builder.build()
}

#[derive(Clone)]
pub(crate) struct DaemonServiceRegistryHost {
    paths: DaemonPaths,
    service_data_dir: Arc<SinomoDataDir>,
    service_cluster: Option<Arc<dyn ClusterAccess>>,
    /// Live service-registry snapshot store; the daemon maintenance loop
    /// publishes its active snapshot's instances into cluster KV.
    service_registry: ServiceRegistrySnapshotStore,
}

static DAEMON_SERVICE_REGISTRY_HOSTS: OnceLock<Mutex<HashMap<PathBuf, DaemonServiceRegistryHost>>> =
    OnceLock::new();

impl DaemonServiceRegistryHost {
    #[cfg(test)]
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

    #[cfg(test)]
    pub(crate) fn from_registry_for_test(
        registry: ServiceRegistry,
        paths: &DaemonPaths,
    ) -> Result<Self, DaemonError> {
        Self::from_registry_for_paths(registry, paths)
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
        let module_snapshot =
            daemon_internal_module_snapshot(&module_snapshots).map_err(|source| {
                DaemonError::Network {
                    address: "daemon service route registry".to_string(),
                    detail: source.to_string(),
                }
            })?;
        let registry =
            daemon_service_registry_with_module_snapshot(registry, module_snapshot, paths.clone())
                .map_err(|source| DaemonError::Network {
                    address: "daemon service route registry".to_string(),
                    detail: source.to_string(),
                })?;
        let service_data_dir = Arc::new(SinomoDataDir::new(data_dir_root));
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
        if !crate::daemon_paths_are_shadow(&paths)
            && let Some(runtime) = service_runtime.as_ref()
        {
            crate::dfs::replay_daemon_dfs_mounted_mutation_committed_work_on_runtime(
                &paths, runtime,
            )?;
        }
        Ok(Self {
            paths,
            service_data_dir,
            service_cluster,
            service_registry: ServiceRegistrySnapshotStore::new(registry),
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

    pub(crate) fn object_rpc_route_descriptor(&self, route: &str) -> Option<ServiceRoute> {
        self.service_registry
            .active_snapshot()
            .registry()
            .route(route)
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
    descriptor: &ServiceRoute,
) -> bool {
    descriptor
        .auth_scope
        .as_deref()
        .is_none_or(|scope| daemon_signer_authorizes_route_scope(&authenticated.signer, scope))
        && daemon_route_local_permissions_are_granted(paths, descriptor)
}

/// Authenticates a request against the live route catalog. Catalog policy is
/// authoritative for dynamic routes; routes without policy metadata use the
/// static authorization table in `auth.rs`.
pub(crate) fn authenticate_daemon_control_request_via_registry(
    paths: &DaemonPaths,
    request: &DaemonControlRequest,
    service_host: &DaemonServiceRegistryHost,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    let authenticate = |authorize_route| {
        if authorize_route {
            crate::auth::authenticate_daemon_control_request(paths, request)
        } else {
            crate::auth::authenticate_daemon_control_request_without_route_authorization(
                paths, request,
            )
        }
    };
    let Some(descriptor) = service_host.object_rpc_route_descriptor(&request.route) else {
        return authenticate(true);
    };
    if descriptor.auth_scope.is_none() {
        let authenticated = authenticate(true)?;
        return daemon_route_local_permissions_are_granted(paths, &descriptor)
            .then_some(authenticated)
            .ok_or(DaemonError::AuthDenied);
    }
    let authenticated = authenticate(false)?;
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
    routes: Vec<ServiceRoute>,
    native_dylib_routes: BTreeMap<String, DaemonNativeDylibRouteTarget>,
}

#[derive(Clone, Debug)]
struct DaemonNativeDylibRouteTarget {
    module_id: sinomo_module::ModuleId,
    operation: String,
    request_body_mode: ServiceBodyMode,
    response_body_mode: ServiceBodyMode,
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
    data_dir_root: PathBuf,
}

impl sinomo_module::NativeModuleHostCallbacks for NativeDylibModuleInvocationCallbacks {
    fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    fn local_permission_granted(&self, permission: &str) -> bool {
        let Ok(permission) = sinomo_core::node::LocalNodePermission::try_from(permission) else {
            return false;
        };
        sinomo_core::node::LocalNodePermissionStore::new(&self.data_dir_root)
            .permission_granted(permission)
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
) -> ServiceRoute {
    let mut route = ServiceRoute::describe(
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
    route.request_body_mode = native_module_body_mode_to_service(descriptor.request_body_mode);
    route.response_body_mode = native_module_body_mode_to_service(descriptor.response_body_mode);
    route
}

fn native_module_body_mode_to_service(
    mode: sinomo_module_api::NativeModuleBodyMode,
) -> ServiceBodyMode {
    match mode {
        sinomo_module_api::NativeModuleBodyMode::None => ServiceBodyMode::None,
        sinomo_module_api::NativeModuleBodyMode::Optional => ServiceBodyMode::Optional,
        sinomo_module_api::NativeModuleBodyMode::Required => ServiceBodyMode::Required,
    }
}

fn daemon_module_snapshot_route_descriptors(
    snapshot: &sinomo_module::ModuleRegistrySnapshot,
) -> Result<
    (
        Vec<ServiceRoute>,
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
            let request_body_mode =
                native_module_body_mode_to_service(descriptor.request_body_mode);
            let response_body_mode =
                native_module_body_mode_to_service(descriptor.response_body_mode);
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
                        request_body_mode,
                        response_body_mode,
                    },
                );
            }
            descriptors.push(descriptor);
        }
    }
    Ok((descriptors, native_dylib_routes))
}

fn validate_module_service_route_contract(
    descriptor: &ServiceRoute,
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

struct NativeDylibOperationCancellationRegistration {
    active_operations: Arc<Mutex<BTreeMap<String, DaemonOperationCancellationToken>>>,
    operation_id: Option<String>,
    token: DaemonOperationCancellationToken,
}

impl NativeDylibOperationCancellationRegistration {
    fn register(
        active_operations: Arc<Mutex<BTreeMap<String, DaemonOperationCancellationToken>>>,
        operation_id: Option<String>,
    ) -> Result<Self, NetworkError> {
        let token = register_native_dylib_operation_cancellation(
            &active_operations,
            operation_id.as_deref(),
        )?;
        Ok(Self {
            active_operations,
            operation_id,
            token,
        })
    }

    fn token(&self) -> DaemonOperationCancellationToken {
        self.token.clone()
    }
}

impl Drop for NativeDylibOperationCancellationRegistration {
    fn drop(&mut self) {
        // Dropping the async dispatch future (disconnect, timeout, or caller
        // cancellation) detaches any active blocking invocation. Signal its
        // per-invocation callbacks before removing the registry entry so a
        // large synchronous copy stops promptly and releases staged files.
        self.token.cancel();
        unregister_native_dylib_operation_cancellation(
            &self.active_operations,
            self.operation_id.as_deref(),
        );
    }
}

fn dispatch_native_dylib_module_cancel_request(
    active_operations: &Arc<Mutex<BTreeMap<String, DaemonOperationCancellationToken>>>,
    metadata_operation_id: Option<String>,
    payload: ObjectRpcCancelRequest,
) -> Result<ObjectRpcCancelResponse, NetworkError> {
    let operation_id = payload.operation_id;
    if metadata_operation_id
        .as_deref()
        .is_some_and(|metadata| metadata != operation_id)
    {
        return Ok(ObjectRpcCancelResponse {
            operation_id,
            cancelled: false,
            detail: "cancel metadata operation id does not match request body".to_string(),
        });
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
    Ok(ObjectRpcCancelResponse {
        operation_id,
        cancelled,
        detail,
    })
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

#[derive(Debug)]
struct NativeDylibSessionStaging {
    root: PathBuf,
    request_body_path: Option<PathBuf>,
    response_body_path: Option<PathBuf>,
}

impl NativeDylibSessionStaging {
    fn create(
        data_dir_root: &std::path::Path,
        request_body_mode: ServiceBodyMode,
        response_body_mode: ServiceBodyMode,
    ) -> Result<Self, NetworkError> {
        let runtime_root = data_dir_root.join("module").join("runtime");
        std::fs::create_dir_all(&runtime_root).map_err(|source| NetworkError::Io {
            operation: format!(
                "create native dylib module runtime root {}",
                runtime_root.display()
            ),
            detail: source.to_string(),
        })?;
        let parent = runtime_root.join("native-dylib-sessions");
        sinomo_core::storage::durable_file::ensure_private_directory(&parent).map_err(
            |source| NetworkError::Io {
                operation: format!(
                    "create private native dylib session staging root {}",
                    parent.display()
                ),
                detail: source.to_string(),
            },
        )?;

        let root = (0..8)
            .find_map(|_| {
                let candidate = parent.join(sinomo_core::Id::generate().as_str());
                match sinomo_core::storage::durable_file::create_private_directory(&candidate) {
                    Ok(()) => Some(Ok(candidate)),
                    Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => None,
                    Err(source) => Some(Err(NetworkError::Io {
                        operation: format!(
                            "create native dylib session staging {}",
                            candidate.display()
                        ),
                        detail: source.to_string(),
                    })),
                }
            })
            .transpose()?
            .ok_or_else(|| NetworkError::Io {
                operation: "create native dylib session staging".to_string(),
                detail: "could not allocate a unique private staging directory".to_string(),
            })?;

        let request_body_path =
            (request_body_mode != ServiceBodyMode::None).then(|| root.join("request.body"));
        let response_body_path =
            (response_body_mode != ServiceBodyMode::None).then(|| root.join("response.body"));
        // Construct the cleanup guard before creating either staged body. If a
        // later file creation fails, `Drop` removes the partially initialized
        // private session instead of leaving it behind indefinitely.
        let staging = Self {
            root,
            request_body_path,
            response_body_path,
        };
        for path in staging
            .request_body_path
            .iter()
            .chain(staging.response_body_path.iter())
        {
            sinomo_core::storage::durable_file::create_private_staged_file(path).map_err(
                |source| NetworkError::Io {
                    operation: format!("create private native dylib body file {}", path.display()),
                    detail: source.to_string(),
                },
            )?;
        }
        Ok(staging)
    }
}

impl Drop for NativeDylibSessionStaging {
    fn drop(&mut self) {
        if let Err(source) = std::fs::remove_dir_all(&self.root)
            && source.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %self.root.display(),
                error = %source,
                "failed to clean native dylib session staging"
            );
        }
    }
}

fn native_dylib_staged_body_path(path: &std::path::Path) -> Result<String, NetworkError> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| NetworkError::Serialization {
            detail: format!(
                "native dylib session staging path is not valid UTF-8: {}",
                path.display()
            ),
        })
}

async fn stage_native_dylib_request_body(
    mut body: sinomo_core::network::ObjectRpcIncomingBody,
    path: &std::path::Path,
    cancellation_token: &DaemonOperationCancellationToken,
) -> Result<sinomo_module_api::NativeModuleStagedBody, NetworkError> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(|source| NetworkError::Io {
            operation: format!("open native dylib request body staging {}", path.display()),
            detail: source.to_string(),
        })?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; 256 * 1024];
    loop {
        if cancellation_token.is_cancelled() {
            return Err(NetworkError::Io {
                operation: format!("stage native dylib request body for {}", path.display()),
                detail: "native dylib operation was cancelled while receiving its request body"
                    .to_string(),
            });
        }
        let read = body
            .read(&mut buffer)
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("stream native dylib request body to {}", path.display()),
                detail: source.to_string(),
            })?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("write native dylib request body staging {}", path.display()),
                detail: source.to_string(),
            })?;
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| NetworkError::Serialization {
                detail: "native dylib request body length exceeds u64".to_string(),
            })?;
    }
    file.flush().await.map_err(|source| NetworkError::Io {
        operation: format!("flush native dylib request body staging {}", path.display()),
        detail: source.to_string(),
    })?;
    drop(file);
    Ok(sinomo_module_api::NativeModuleStagedBody::new(
        native_dylib_staged_body_path(path)?,
        length,
        format!("blake3:{}", hasher.finalize().to_hex()),
    ))
}

fn inspect_native_dylib_staged_body(
    path: &std::path::Path,
) -> Result<sinomo_module_api::NativeModuleStagedBody, NetworkError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| NetworkError::Io {
        operation: format!("inspect native dylib response body {}", path.display()),
        detail: source.to_string(),
    })?;
    if !metadata.file_type().is_file() {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib response body is not a regular file: {}",
                path.display()
            ),
        });
    }
    let mut file = std::fs::File::open(path).map_err(|source| NetworkError::Io {
        operation: format!("open native dylib response body {}", path.display()),
        detail: source.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; 256 * 1024];
    loop {
        let read =
            std::io::Read::read(&mut file, &mut buffer).map_err(|source| NetworkError::Io {
                operation: format!("hash native dylib response body {}", path.display()),
                detail: source.to_string(),
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| NetworkError::Serialization {
                detail: "native dylib response body length exceeds u64".to_string(),
            })?;
    }
    Ok(sinomo_module_api::NativeModuleStagedBody::new(
        native_dylib_staged_body_path(path)?,
        length,
        format!("blake3:{}", hasher.finalize().to_hex()),
    ))
}

fn validate_native_dylib_response_body(
    mode: ServiceBodyMode,
    expected_path: Option<&std::path::Path>,
    reported: Option<sinomo_module_api::NativeModuleStagedBody>,
) -> Result<Option<PathBuf>, NetworkError> {
    let Some(reported) = reported else {
        return if mode == ServiceBodyMode::Required {
            Err(NetworkError::Serialization {
                detail: "native dylib route requires a non-empty response body".to_string(),
            })
        } else {
            Ok(None)
        };
    };
    if mode == ServiceBodyMode::None {
        return Err(NetworkError::Serialization {
            detail: "native dylib route does not declare a response body".to_string(),
        });
    }
    let expected_path = expected_path.ok_or_else(|| NetworkError::Serialization {
        detail: "native dylib response body has no host-assigned staging path".to_string(),
    })?;
    if std::path::Path::new(&reported.path) != expected_path {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib response body path `{}` does not match host-assigned path `{}`",
                reported.path,
                expected_path.display()
            ),
        });
    }
    let actual = inspect_native_dylib_staged_body(expected_path)?;
    if reported.length != actual.length {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib response body length mismatch: module reported {}, host measured {}",
                reported.length, actual.length
            ),
        });
    }
    if reported.blake3 != actual.blake3 {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib response body BLAKE3 mismatch: module reported {}, host measured {}",
                reported.blake3, actual.blake3
            ),
        });
    }
    if mode == ServiceBodyMode::Required && actual.length == 0 {
        return Err(NetworkError::Serialization {
            detail: "native dylib route requires a non-empty response body".to_string(),
        });
    }
    Ok(Some(expected_path.to_path_buf()))
}

fn validate_native_dylib_request_body_mode(
    route: &str,
    mode: ServiceBodyMode,
    has_body: bool,
) -> Result<(), NetworkError> {
    match mode {
        ServiceBodyMode::None if has_body => Err(NetworkError::Serialization {
            detail: format!("native dylib route {route} does not accept a request body"),
        }),
        ServiceBodyMode::Required if !has_body => Err(NetworkError::Serialization {
            detail: format!("native dylib route {route} requires a non-empty request body"),
        }),
        _ => Ok(()),
    }
}

fn validate_native_dylib_staged_request_body(
    route: &str,
    mode: ServiceBodyMode,
    body: Option<&sinomo_module_api::NativeModuleStagedBody>,
) -> Result<(), NetworkError> {
    match (mode, body) {
        (ServiceBodyMode::None, Some(_)) => Err(NetworkError::Serialization {
            detail: format!("native dylib route {route} does not accept a request body"),
        }),
        (ServiceBodyMode::Required, None)
        | (
            ServiceBodyMode::Required,
            Some(sinomo_module_api::NativeModuleStagedBody { length: 0, .. }),
        ) => Err(NetworkError::Serialization {
            detail: format!("native dylib route {route} requires a non-empty request body"),
        }),
        _ => Ok(()),
    }
}

async fn dispatch_native_dylib_module_request(
    paths: DaemonPaths,
    target: DaemonNativeDylibRouteTarget,
    active_operations: Arc<Mutex<BTreeMap<String, DaemonOperationCancellationToken>>>,
    request: ObjectRpcIncomingRequest,
) -> Result<ObjectRpcOutgoingResponse, NetworkError> {
    let route_for_error = request.request.route.clone();
    let operation_id = request.request.operation_id().map(str::to_string);
    let has_request_body = !request.body.is_empty();
    validate_native_dylib_request_body_mode(
        &route_for_error,
        target.request_body_mode,
        has_request_body,
    )?;
    let cancellation_registration = NativeDylibOperationCancellationRegistration::register(
        Arc::clone(&active_operations),
        operation_id.clone(),
    )?;
    let cancellation_token = cancellation_registration.token();
    let data_dir_root = daemon_data_dir_path(&paths).map_err(daemon_error_to_network_error)?;
    let staging = (target.request_body_mode != ServiceBodyMode::None
        || target.response_body_mode != ServiceBodyMode::None)
        .then(|| {
            NativeDylibSessionStaging::create(
                &data_dir_root,
                target.request_body_mode,
                target.response_body_mode,
            )
        })
        .transpose()?;
    let staged_request_body = if has_request_body {
        let request_path = staging
            .as_ref()
            .and_then(|staging| staging.request_body_path.as_deref())
            .ok_or_else(|| NetworkError::Serialization {
                detail: format!("native dylib route {route_for_error} has no request staging path"),
            })?;
        Some(
            stage_native_dylib_request_body(request.body, request_path, &cancellation_token)
                .await?,
        )
    } else {
        None
    };
    validate_native_dylib_staged_request_body(
        &route_for_error,
        target.request_body_mode,
        staged_request_body.as_ref(),
    )?;
    let response_body_path = staging
        .as_ref()
        .and_then(|staging| staging.response_body_path.as_deref())
        .map(native_dylib_staged_body_path)
        .transpose()?;
    let mut request = request.request;
    inject_native_dylib_host_authority(&data_dir_root, &target, &mut request)?;
    let (response, response_body_path, staging) = tokio::task::spawn_blocking(move || {
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
        let callbacks: Arc<dyn sinomo_module::NativeModuleHostCallbacks> =
            Arc::new(NativeDylibModuleInvocationCallbacks {
                cancellation_token: cancellation_token.clone(),
                data_dir_root: data_dir_root.clone(),
            });
        let native_response = module
            .invoke_with_callbacks(
                &sinomo_module_api::NativeModuleRequest::Invoke(
                    sinomo_module_api::NativeModuleInvocationRequest {
                        operation: target.operation.clone(),
                        payload_sobj: request.body,
                        request_body: staged_request_body,
                        response_body_path,
                    },
                ),
                Some(callbacks),
            )
            .map_err(module_error_to_network_error);
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
                let response_body_path = validate_native_dylib_response_body(
                    target.response_body_mode,
                    staging
                        .as_ref()
                        .and_then(|staging| staging.response_body_path.as_deref()),
                    response.response_body,
                )?;
                Ok((
                    ObjectRpcResponse::from_encoded_body(request.route, response.payload_sobj),
                    response_body_path,
                    staging,
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
    })??;
    let body = if let Some(path) = response_body_path {
        let staging = staging.ok_or_else(|| NetworkError::Serialization {
            detail: "native dylib response body lost its staging lifetime".to_string(),
        })?;
        ObjectRpcBodySource::file(path)?.with_lifetime_guard(staging)?
    } else {
        drop(staging);
        ObjectRpcBodySource::empty()
    };
    Ok(ObjectRpcOutgoingResponse::new(response, body))
}

fn daemon_service_registry_with_module_snapshot(
    registry: ServiceRegistry,
    module_snapshot: DaemonInternalModuleSnapshot,
    paths: DaemonPaths,
) -> Result<ServiceRegistry, NetworkError> {
    let mut builder = registry.to_builder();
    let active_native_dylib_operations = Arc::new(Mutex::new(BTreeMap::<
        String,
        DaemonOperationCancellationToken,
    >::new()));
    let module_snapshot_version = module_snapshot.snapshot.version().to_string();

    for route in registry.route_categories() {
        builder = builder.replace_route_metadata(
            route
                .observable("daemon.service_registry", "true")
                .observable("daemon.service_registry.snapshot_version", "1"),
        );
    }

    for mut descriptor in module_snapshot.routes {
        let native_target = module_snapshot
            .native_dylib_routes
            .get(&descriptor.route)
            .cloned();
        if let Some(native_target) = native_target {
            if registry.owns(&descriptor.route) {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "native dylib module route {} collides with a daemon service route",
                        descriptor.route
                    ),
                });
            }
            descriptor.concurrency_limit = Some(4);
            descriptor.timeout_ms = Some(310_000);
            descriptor = descriptor
                // The native-module ABI deliberately accepts one contiguous
                // opaque `payload_sobj`. Until that ABI becomes chunk-native,
                // make its required compatibility materialization explicit.
                .with_buffered_request_value()
                .observable("daemon.native_dylib_module_route", "true")
                .observable(
                    "module.registry.snapshot_version",
                    module_snapshot_version.clone(),
                );
            let handler_paths = paths.clone();
            let active_operations = Arc::clone(&active_native_dylib_operations);
            builder =
                builder.register_dynamic("native-dylib", descriptor, move |mut ctx, request| {
                    let paths = handler_paths.clone();
                    let target = native_target.clone();
                    let active_operations = Arc::clone(&active_operations);
                    async move {
                        let request =
                            ObjectRpcIncomingRequest::new(request, ctx.take_request_body());
                        let response = dispatch_native_dylib_module_request(
                            paths,
                            target,
                            active_operations,
                            request,
                        )
                        .await?;
                        ctx.respond_with_body(response.response, response.body)?;
                        Ok(ServiceDisposition::Continue)
                    }
                });
        } else if let Some(mut service_route) = registry.route(&descriptor.route) {
            validate_module_service_route_contract(&descriptor, &service_route)?;
            service_route.observability.extend(descriptor.observability);
            service_route = service_route
                .observable("daemon.service_registry", "true")
                .observable("daemon.service_registry.snapshot_version", "1")
                .observable(
                    "module.registry.snapshot_version",
                    module_snapshot_version.clone(),
                );
            builder = builder.replace_route_metadata(service_route);
        }
    }

    let active_operations = Arc::clone(&active_native_dylib_operations);
    builder = builder.register_dynamic(
        "native-dylib",
        native_dylib_cancel_route_descriptor()
            .observable("module.registry.snapshot_version", module_snapshot_version),
        move |mut ctx, request| {
            let active_operations = Arc::clone(&active_operations);
            async move {
                let metadata_operation_id = request.cancels_operation_id().map(str::to_string);
                let payload: ObjectRpcCancelRequest = ctx.decode_request_body(&request).await?;
                let response = dispatch_native_dylib_module_cancel_request(
                    &active_operations,
                    metadata_operation_id,
                    payload,
                )?;
                ctx.respond_value(MODULE_NATIVE_DYLIB_CANCEL_ROUTE, response)?;
                Ok(ServiceDisposition::Continue)
            }
        },
    );
    Ok(builder.build())
}

fn native_dylib_cancel_route_descriptor() -> ServiceRoute {
    ServiceRoute::describe(
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
        dfs_data_plane = sinomo_dfs::dfs_route_is_message_data_plane(&route),
        "daemon service rpc dispatch start"
    );
    annotate_daemon_service_request(&mut request.request, authenticated);
    let descriptor = service_host
        .object_rpc_route_descriptor(&route)
        .ok_or_else(|| NetworkError::UnknownRoute {
            route: route.clone(),
        })?;
    if let Some(auth_scope) = descriptor.auth_scope.as_deref()
        && !daemon_signer_authorizes_route_scope(&authenticated.signer, auth_scope)
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
    if !daemon_nonlocal_config_permission_is_granted(
        &service_host.paths,
        &authenticated.signer,
        &route,
    ) || !daemon_route_local_permissions_are_granted(&service_host.paths, &descriptor)
    {
        return Err(DaemonError::AuthDenied);
    }
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

    #[test]
    fn dfs_browser_read_response_does_not_collapse_content_unavailability_into_absence() {
        let missing = daemon_dfs_browser_read_file_response(
            sinomo_dfs::DfsPosixDistributedFileReadOutcome::NotFound {
                path: "/missing".to_string(),
            },
        );
        assert!(matches!(
            missing.into_result(),
            Err(sinomo_api::DaemonDfsBrowserReadFileFailure::NotFound { .. })
        ));

        for detail in [
            "content is pending replication",
            "content confirmation has not reached the placement requirement",
            "the content mirror has no readable replica",
        ] {
            let unavailable = daemon_dfs_browser_read_file_response(
                sinomo_dfs::DfsPosixDistributedFileReadOutcome::ContentUnavailable {
                    path: "/services/skills/catalog.sobj".to_string(),
                    detail: detail.to_string(),
                },
            );
            assert!(matches!(
                unavailable.into_result(),
                Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ContentUnavailable { .. })
            ));
        }

        let requested_path = "/services/sinomo-agent/channels/record.json";
        let missing = daemon_dfs_browser_bulk_file_read_response(
            requested_path.to_string(),
            sinomo_dfs::DfsPosixDistributedFileReadOutcome::NotFound {
                path: requested_path.to_string(),
            },
        );
        assert_eq!(missing.path, requested_path);
        assert!(matches!(
            missing.into_result(),
            Err(sinomo_api::DaemonDfsBrowserReadFileFailure::NotFound { .. })
        ));

        let unavailable = daemon_dfs_browser_bulk_file_read_response(
            requested_path.to_string(),
            sinomo_dfs::DfsPosixDistributedFileReadOutcome::ContentUnavailable {
                path: requested_path.to_string(),
                detail: "bulk source has no readable exact version".to_string(),
            },
        );
        assert!(matches!(
            unavailable.into_result(),
            Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ContentUnavailable { .. })
        ));

        let prefix_found = daemon_dfs_browser_read_file_prefix_response(
            sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::Found {
                read: sinomo_dfs::DfsPosixFilePrefixRead {
                    bytes: vec![1, 2, 3],
                    truncated: true,
                },
            },
        );
        assert_eq!(
            prefix_found.into_result().expect("found prefix"),
            (vec![1, 2, 3], true)
        );

        let prefix_missing = daemon_dfs_browser_read_file_prefix_response(
            sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::NotFound {
                path: "/missing-preview.bin".to_string(),
            },
        );
        assert!(matches!(
            prefix_missing.into_result(),
            Err(sinomo_api::DaemonDfsBrowserReadFileFailure::NotFound { .. })
        ));

        let prefix_unavailable = daemon_dfs_browser_read_file_prefix_response(
            sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable {
                path: "/preview.bin".to_string(),
                detail: "exact prefix content version has no readable replica".to_string(),
            },
        );
        assert!(matches!(
            prefix_unavailable.into_result(),
            Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ContentUnavailable { .. })
        ));
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
    fn daemon_forward_transport_preserves_distinct_connect_and_idle_timeouts() {
        let payload = crate::DaemonObjectRpcForwardRequest {
            target_node_id: sinomo_core::node::NodeId::from_legacy_named(
                "node_daemon_forward_timeouts",
            )
            .expect("node id"),
            request: sinomo_core::network::ObjectRpcRequest::encode_body(
                "/test/daemon-forward-timeouts",
                "test",
                &(),
            )
            .expect("request"),
            streamed_value_body: false,
            connect_timeout_millis: 5_000,
            idle_timeout_millis: Some(310_000),
            overall_timeout_millis: None,
            observe_connectivity: true,
            destination_process_affinity: crate::DaemonObjectRpcDestinationProcessAffinity::Any,
        };

        let timeouts = daemon_object_rpc_forward_transport_timeouts(&payload);

        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(5)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(310)));
    }

    #[test]
    fn daemon_forward_transport_uses_default_idle_timeout_when_omitted() {
        let payload = crate::DaemonObjectRpcForwardRequest {
            target_node_id: sinomo_core::node::NodeId::from_legacy_named(
                "node_daemon_forward_default_idle",
            )
            .expect("node id"),
            request: sinomo_core::network::ObjectRpcRequest::encode_body(
                "/test/daemon-forward-default-idle",
                "test",
                &(),
            )
            .expect("request"),
            streamed_value_body: false,
            connect_timeout_millis: 7_000,
            idle_timeout_millis: None,
            overall_timeout_millis: None,
            observe_connectivity: false,
            destination_process_affinity: crate::DaemonObjectRpcDestinationProcessAffinity::Any,
        };

        let timeouts = daemon_object_rpc_forward_transport_timeouts(&payload);

        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(7)));
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn daemon_forward_preserves_explicit_destination_process_affinity() {
        let mut payload = crate::DaemonObjectRpcForwardRequest {
            target_node_id: sinomo_core::node::NodeId::from_legacy_named(
                "node_daemon_forward_affinity",
            )
            .expect("node id"),
            request: sinomo_core::network::ObjectRpcRequest::encode_body(
                "/test/daemon-forward-affinity",
                "test",
                &(),
            )
            .expect("request"),
            streamed_value_body: false,
            connect_timeout_millis: 5_000,
            idle_timeout_millis: None,
            overall_timeout_millis: Some(15_000),
            observe_connectivity: false,
            destination_process_affinity: crate::DaemonObjectRpcDestinationProcessAffinity::Any,
        };

        assert_eq!(
            daemon_object_rpc_forward_options(&payload).destination_process_affinity(),
            sinomo_core::network::DestinationProcessAffinity::Any,
            "legacy and explicitly process-agnostic traffic must not be rewritten"
        );
        payload.destination_process_affinity =
            crate::DaemonObjectRpcDestinationProcessAffinity::MainDaemon;
        assert_eq!(
            daemon_object_rpc_forward_options(&payload).destination_process_affinity(),
            sinomo_core::network::DestinationProcessAffinity::MainDaemon
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

    #[tokio::test]
    async fn mounted_mutation_handoff_rejects_oversized_values_before_outbox_mutation() {
        let inline_root = sinomo_testdir::TestDir::new(
            "sinomo-daemon-mounted-mutation-handoff-inline-payload-bound",
        );
        let inline_data_dir = Arc::new(SinomoDataDir::new(&inline_root));
        let inline_paths = DaemonPaths::new(inline_data_dir.as_ref());
        let inline_response = Arc::new(Mutex::new(None));
        let inline_context = ServiceContext::new(
            Arc::clone(&inline_data_dir),
            None,
            ServiceSigner::Local,
            sinomo_core::network::ObjectRpcIncomingBody::default(),
            Box::new(BufferSink {
                response: Arc::clone(&inline_response),
            }),
        );
        let inline_request = ObjectRpcRequest::from_encoded_body(
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
            DAEMON_RPC_CATEGORY,
            vec![0; DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES + 1],
        );
        let inline_service = DaemonControlService {
            paths: inline_paths,
            runtime: Some(tokio::runtime::Handle::current()),
        };

        let inline_error = inline_service
            .handle(inline_context, inline_request)
            .await
            .expect_err("reject oversized inline handoff value");
        assert!(matches!(
            inline_error,
            ServiceError::Network(NetworkError::FrameTooLarge {
                actual_bytes,
                max_bytes,
            }) if actual_bytes == DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES + 1
                && max_bytes == DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES
        ));
        assert!(inline_response.lock().expect("inline response").is_none());
        assert!(
            !inline_root
                .join("dfs")
                .join("mounted-mutation-outbox")
                .exists(),
            "an oversized inline value must be rejected before the handoff handler opens its outbox",
        );

        let streamed_root = sinomo_testdir::TestDir::new(
            "sinomo-daemon-mounted-mutation-handoff-streamed-payload-bound",
        );
        let streamed_data_dir = Arc::new(SinomoDataDir::new(&streamed_root));
        let streamed_paths = DaemonPaths::new(streamed_data_dir.as_ref());
        let streamed_response = Arc::new(Mutex::new(None));
        let (_sender, streamed_body) = sinomo_core::network::ObjectRpcIncomingBody::test_channel(
            Some((DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES + 1) as u64),
            1,
        );
        let streamed_context = ServiceContext::new(
            Arc::clone(&streamed_data_dir),
            None,
            ServiceSigner::Local,
            streamed_body.with_test_streamed_value_role(),
            Box::new(BufferSink {
                response: Arc::clone(&streamed_response),
            }),
        );
        let streamed_request = ObjectRpcRequest::from_encoded_body(
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
            DAEMON_RPC_CATEGORY,
            Vec::new(),
        );
        let streamed_service = DaemonControlService {
            paths: streamed_paths,
            runtime: Some(tokio::runtime::Handle::current()),
        };

        let streamed_error = tokio::time::timeout(
            Duration::from_secs(1),
            streamed_service.handle(streamed_context, streamed_request),
        )
        .await
        .expect("declared-length rejection must not await a streamed chunk")
        .expect_err("reject oversized streamed handoff declaration");
        assert!(matches!(
            streamed_error,
            ServiceError::Network(NetworkError::FrameTooLarge {
                actual_bytes,
                max_bytes,
            }) if actual_bytes == DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES + 1
                && max_bytes == DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES
        ));
        assert!(
            streamed_response
                .lock()
                .expect("streamed response")
                .is_none(),
        );
        assert!(
            !streamed_root
                .join("dfs")
                .join("mounted-mutation-outbox")
                .exists(),
            "an oversized streamed declaration must be rejected before the handoff handler opens its outbox",
        );
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
        let staging_root = staging.root.clone();
        let request_path = staging.request_body_path.as_deref().expect("request path");
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
        let request_path = staging.request_body_path.as_deref().expect("request path");
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
        let active = Arc::new(Mutex::new(BTreeMap::new()));
        let registration = NativeDylibOperationCancellationRegistration::register(
            Arc::clone(&active),
            Some("operation.drop".to_string()),
        )
        .expect("register operation");
        let token = registration.token();
        assert!(!token.is_cancelled());
        assert!(
            active
                .lock()
                .expect("active operations")
                .contains_key("operation.drop")
        );

        drop(registration);

        assert!(token.is_cancelled());
        assert!(active.lock().expect("active operations").is_empty());
    }

    #[test]
    fn native_dylib_final_permission_callback_observes_live_local_revocation() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-native-permission-callback");
        let store = sinomo_core::node::LocalNodePermissionStore::new(&root);
        let callbacks = NativeDylibModuleInvocationCallbacks {
            cancellation_token: DaemonOperationCancellationToken::new(),
            data_dir_root: root.to_path_buf(),
        };
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
        let response_path = staging
            .response_body_path
            .as_deref()
            .expect("response path");
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
        let staging_root = staging.root.clone();
        let response_path = staging.response_body_path.clone().expect("response path");
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
            host_override: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-build-session-body");
        let mut context = ServiceContext::new(
            Arc::new(SinomoDataDir::new(&root)),
            None,
            ServiceSigner::Local,
            sinomo_core::network::ObjectRpcIncomingBody::bytes(expected_bundle.clone()),
            Box::new(BufferSink {
                response: Arc::new(Mutex::new(None)),
            }),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let decoded = runtime
            .block_on(decode_rollout_build_session_request(&mut context, &request))
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

        let policy = sinomo_dfs::DfsReplicationPolicy::new(1, 2).expect("policy");
        let candidates = sifs_browser_write_candidates(&[first.clone(), second.clone()], &policy);

        assert_eq!(candidates, vec![first, second]);
    }

    fn placement_test_node(name: &str) -> sinomo_core::node::NodeId {
        sinomo_core::node::NodeId::from_legacy_named(name).expect("placement test node")
    }

    fn placement_test_endpoint(
        node_id: sinomo_core::node::NodeId,
        name: &str,
        address: &str,
    ) -> sinomo_core::ClusterEndpointRecord {
        sinomo_core::ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(name)
                .expect("placement test endpoint"),
            node_id,
            transport: sinomo_core::ClusterTransportKind::LanTcp,
            address: address.to_string(),
            source: sinomo_core::ClusterEndpointSource::Static,
            privacy: sinomo_core::ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        }
    }

    #[test]
    fn dfs_browser_policy_none_removes_only_the_selected_scope() {
        let local_node_id = placement_test_node("node_dfs_browser_placement_remove_local");
        let policy = sinomo_dfs::DfsPlacementPolicy::new(
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            1,
            2,
        )
        .expect("placement policy");
        let mut config = sinomo_dfs::DfsFileSystemConfig {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            display_name: "SIFS".to_string(),
            owner_node_id: local_node_id,
            subtree_policies: vec![
                sinomo_dfs::normalize_placement_rule(
                    "/projects",
                    sinomo_dfs::DfsPlacementScope::SelfOnly,
                    policy.clone(),
                )
                .expect("self rule"),
                sinomo_dfs::normalize_placement_rule(
                    "/projects",
                    sinomo_dfs::DfsPlacementScope::Subtree,
                    policy,
                )
                .expect("subtree rule"),
            ],
        };

        mutate_daemon_dfs_browser_placement_config(
            &mut config,
            "/projects",
            sinomo_dfs::DfsPlacementScope::SelfOnly,
            None,
        )
        .expect("remove self rule");

        assert_eq!(config.subtree_policies.len(), 1);
        assert_eq!(
            config.subtree_policies[0].scope,
            sinomo_dfs::DfsPlacementScope::Subtree
        );
    }

    #[test]
    fn dfs_browser_legacy_placement_update_route_fails_closed_before_opening_dfs() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-browser-legacy-placement");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let service = DaemonDfsBrowserService {
            runtime: Some(runtime.handle().clone()),
            event_hub: Arc::new(DaemonEventHub::new()),
        };
        let response = Arc::new(Mutex::new(None));
        let context = ServiceContext::new(
            Arc::new(SinomoDataDir::new(&root)),
            None,
            ServiceSigner::Local,
            sinomo_core::network::ObjectRpcIncomingBody::default(),
            Box::new(BufferSink {
                response: Arc::clone(&response),
            }),
        );
        let request = ObjectRpcRequest::encode_body(
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE,
            DAEMON_RPC_CATEGORY,
            &crate::DaemonDfsBrowserPlacementUpdateRequest {
                path: "/projects".to_string(),
                policy: None,
                scope: sinomo_dfs::DfsPlacementScope::SelfOnly,
            },
        )
        .expect("legacy placement request");

        let error = runtime
            .block_on(service.handle(context, request))
            .expect_err("legacy placement Apply must be rejected");

        assert!(
            error
                .to_string()
                .contains("legacy SIFS placement Apply is disabled")
        );
        assert!(error.to_string().contains("placement-update-v2"));
        assert!(response.lock().expect("response slot").is_none());
        assert!(
            !root.join("dfs").exists(),
            "the fail-closed compatibility route must not even open the DFS backend"
        );
    }

    #[test]
    fn dfs_browser_concurrent_first_open_initializes_one_backend_per_data_root() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-browser-concurrent-first-open");
        let data_dir = Arc::new(SinomoDataDir::new(&root));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let worker_count = 8;
        let start = Arc::new(std::sync::Barrier::new(worker_count));
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let data_dir = Arc::clone(&data_dir);
            let start = Arc::clone(&start);
            let runtime = runtime.handle().clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                DaemonDfsBrowserBackend::for_data_dir(data_dir, &runtime)
                    .expect("open cached DFS browser backend")
            }));
        }
        let backends = workers
            .into_iter()
            .map(|worker| worker.join().expect("browser open worker"))
            .collect::<Vec<_>>();

        assert!(
            backends
                .iter()
                .skip(1)
                .all(|backend| Arc::ptr_eq(&backends[0], backend)),
            "every concurrent caller must receive the same retained backend"
        );
        assert_eq!(
            daemon_dfs_browser_backend_open_count(data_dir.as_ref()),
            1,
            "the per-root initialization slot must prevent discarded duplicate backend opens"
        );
        backends[0]
            .health_summary()
            .expect("health reuses the retained browser metadata backend");
        backends[0]
            .health_summary()
            .expect("repeated health reuses the retained browser metadata backend");
        assert_eq!(daemon_dfs_browser_backend_open_count(data_dir.as_ref()), 1);
    }

    #[test]
    fn dfs_browser_cluster_authority_ignores_missing_and_stale_materialized_checkpoint() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-daemon-dfs-browser-authority-without-materialized-checkpoint",
        );
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node = sinomo_core::node::NodeId::from_legacy_named(
            "node_dfs_browser_authority_without_checkpoint",
        )
        .expect("local node");
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_browser_authority_without_checkpoint",
        )
        .expect("cluster id");
        let now = time::OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "DFS browser authority without checkpoint",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        state.term = 9;
        let capabilities = sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        let raft_config = sinomo_core::kv::ClusterKvRaftConfigRecord {
            cluster_id,
            term: state.term,
            kv_revision: 1,
            voters: vec![local_node.clone()],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: now,
        };
        let mut node = cluster_paths
            .kv_paths()
            .raft_store()
            .open_node(&local_node)
            .expect("open local Raft node");
        node.become_leader(state.term)
            .expect("establish local leader");
        let config_entry = node
            .append_local_command(
                state.term,
                sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                    key: sinomo_core::kv::cluster_kv_raft_config_key(),
                    value: sinomo_core::kv::encode_kv_value(&raft_config)
                        .expect("encode Raft config"),
                    lease_id: None,
                }),
            )
            .expect("append Raft config");
        node.commit_to(config_entry.index)
            .expect("commit Raft config");
        let stale_materialized = node.store().snapshot().expect("stale checkpoint");
        let advertisement = sinomo_dfs::DfsNodeAdvertisement::content(local_node.clone());
        let advertisement_entry = node
            .append_local_command(
                state.term,
                sinomo_core::kv::KvCommand::Op(
                    sinomo_core::kv::KvOp::put_serde(
                        sinomo_dfs::content_service_key(&local_node),
                        &advertisement,
                        None,
                    )
                    .expect("content advertisement op"),
                ),
            )
            .expect("append content advertisement");
        node.commit_to(advertisement_entry.index)
            .expect("commit content advertisement");
        state.kv_revision = node.store().revision().0;
        drop(node);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        assert!(
            !cluster_paths.kv_paths().snapshot.exists(),
            "the first browser authority read must have no materialized checkpoint"
        );
        let missing_checkpoint_client =
            daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime.handle()))
                .expect("cluster state must attach browser authority without a checkpoint");
        assert_eq!(
            missing_checkpoint_client
                .range_prefix(sinomo_dfs::content_services_prefix())
                .expect("resident browser prefix authority")
                .into_iter()
                .map(|entry| entry.decode::<sinomo_dfs::DfsNodeAdvertisement>())
                .collect::<Result<Vec<_>, _>>()
                .expect("decode resident advertisements"),
            vec![advertisement.clone()],
        );

        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &stale_materialized)
            .expect("write stale materialized checkpoint");
        assert!(
            sinomo_core::kv::read_cluster_kv_prefix_entries(
                &cluster_paths.kv_paths(),
                sinomo_dfs::content_services_prefix(),
            )
            .expect("read stale browser checkpoint")
            .is_empty(),
        );
        let stale_checkpoint_client =
            daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime.handle()))
                .expect("stale checkpoint must not disable browser authority");
        assert_eq!(
            stale_checkpoint_client
                .range_prefix(sinomo_dfs::content_services_prefix())
                .expect("browser authority ahead of stale checkpoint")
                .into_iter()
                .map(|entry| entry.decode::<sinomo_dfs::DfsNodeAdvertisement>())
                .collect::<Result<Vec<_>, _>>()
                .expect("decode authoritative advertisements"),
            vec![advertisement],
        );

        let mut observed = DaemonDfsBrowserObservedControlPlaneKvClient::new(&cluster_paths);
        assert!(matches!(
            sinomo_dfs::DfsControlPlaneKvClient::get_authoritative_consistent(
                &mut observed,
                &[sinomo_dfs::content_service_key(&local_node)],
            ),
            Err(KvError::AuthorityUnavailable { .. })
        ));
    }

    #[test]
    fn dfs_browser_placement_apply_rejects_a_stale_well_formed_preview_token() {
        let current = sinomo_dfs::DfsPlacementPreviewToken {
            schema_version: sinomo_dfs::DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION,
            digest: format!("dfs-placement-preview-v1:blake3:{}", "a".repeat(64)),
        };
        let stale = sinomo_dfs::DfsPlacementPreviewToken {
            schema_version: sinomo_dfs::DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION,
            digest: format!("dfs-placement-preview-v1:blake3:{}", "b".repeat(64)),
        };
        assert!(current.is_well_formed());
        assert!(stale.is_well_formed());
        require_matching_daemon_dfs_browser_placement_preview_token(&current, &current)
            .expect("the exact reviewed preview token");

        let stale_error =
            require_matching_daemon_dfs_browser_placement_preview_token(&stale, &current)
                .expect_err("a stale, otherwise valid token must be rejected");
        assert!(stale_error.to_string().contains("preview is stale"));
        assert!(
            require_matching_daemon_dfs_browser_placement_preview_token(
                &sinomo_dfs::DfsPlacementPreviewToken::default(),
                &current,
            )
            .is_err(),
            "an absent or malformed precondition must also fail closed"
        );
    }

    #[test]
    fn dfs_browser_placement_preview_enriches_connectivity_and_blocks_required_unreachable_node() {
        let local_node_id = placement_test_node("node_dfs_browser_placement_local");
        let reachable_node_id = placement_test_node("node_dfs_browser_placement_reachable");
        let unreachable_node_id = placement_test_node("node_dfs_browser_placement_unreachable");
        let unknown_node_id = placement_test_node("node_dfs_browser_placement_unknown");
        let reachable_endpoint = placement_test_endpoint(
            reachable_node_id.clone(),
            "endpoint_dfs_browser_placement_reachable",
            "192.0.2.31:21353",
        );
        let unreachable_endpoint = placement_test_endpoint(
            unreachable_node_id.clone(),
            "endpoint_dfs_browser_placement_unreachable",
            "192.0.2.32:21353",
        );
        let unknown_endpoint = placement_test_endpoint(
            unknown_node_id.clone(),
            "endpoint_dfs_browser_placement_unknown",
            "192.0.2.33:21353",
        );
        let now = time::OffsetDateTime::now_utc();
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_dfs_browser_placement_connectivity",
            )
            .expect("cluster id"),
            "placement connectivity",
            local_node_id.clone(),
            "local",
            "test-public-key",
            now,
        );
        let graph = ClusterConnectivityGraph::from_records(
            local_node_id.clone(),
            [
                sinomo_core::network::cluster_connectivity_record(
                    &state,
                    &reachable_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(2),
                    0,
                    Some(now.to_string()),
                    None,
                    now,
                    sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
                ),
                sinomo_core::network::cluster_connectivity_record(
                    &state,
                    &unreachable_endpoint,
                    ClusterConnectivityState::Unreachable,
                    None,
                    3,
                    None,
                    Some(now.to_string()),
                    now,
                    sinomo_core::network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
                ),
            ],
        );
        let mut policy = sinomo_dfs::DfsPlacementPolicy::new(
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            1,
            2,
        )
        .expect("placement policy");
        policy.required_node_ids = vec![unreachable_node_id.clone()];
        let config = sinomo_dfs::DfsFileSystemConfig {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            display_name: "SIFS".to_string(),
            owner_node_id: local_node_id.clone(),
            subtree_policies: Vec::new(),
        };
        let candidates = [
            local_node_id.clone(),
            reachable_node_id.clone(),
            unreachable_node_id.clone(),
            unknown_node_id.clone(),
        ]
        .into_iter()
        .map(sinomo_dfs::DfsNodeAdvertisement::content)
        .collect();
        let endpoints = vec![reachable_endpoint, unreachable_endpoint, unknown_endpoint];
        let connectivity = DaemonDfsBrowserPlacementConnectivitySnapshot {
            local_node_id: Some(local_node_id.clone()),
            graph: Some(graph),
            advertised_endpoints: endpoints,
        };
        let unavailable_node_ids = connectivity.explicitly_unavailable_node_ids();
        assert_eq!(
            unavailable_node_ids,
            BTreeSet::from([unreachable_node_id.clone()]),
            "only a node with advertised endpoints that are all explicitly down is unavailable"
        );
        let mut preview = sinomo_dfs::preview_placement_with_unavailable_nodes(
            &config,
            "/projects/data.bin",
            sinomo_dfs::DfsObjectId::from_legacy_named("object_dfs_browser_placement_preview")
                .expect("object id"),
            sinomo_dfs::DfsVersion::from_legacy_named("version_dfs_browser_placement_preview")
                .expect("version"),
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            local_node_id.clone(),
            vec![local_node_id.clone()],
            candidates,
            Some(policy),
            sinomo_dfs::DfsPlacementScope::SelfOnly,
            &unavailable_node_ids,
        )
        .expect("base preview");
        assert!(!preview.is_satisfiable());
        assert!(
            !preview
                .placement
                .plan
                .desired_replica_nodes
                .contains(&unreachable_node_id),
            "an explicitly unavailable optional or required node must not satisfy N"
        );
        let required_unavailable =
            sinomo_dfs::DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                node_id: unreachable_node_id.clone(),
            };
        assert!(preview.blocking_diagnostics.contains(&required_unavailable));
        let anchor_diagnostic_count = preview.blocking_diagnostics.len();
        extend_daemon_dfs_browser_preview_diagnostics(
            &mut preview,
            [sinomo_dfs::DfsPlacementBlockingDiagnostic::ScopedPolicy {
                path: "/projects/data.bin".to_string(),
                scope: sinomo_dfs::DfsPlacementScope::SelfOnly,
                diagnostic: Box::new(required_unavailable),
            }],
        );
        assert_eq!(
            preview.blocking_diagnostics.len(),
            anchor_diagnostic_count,
            "the matching page-level wrapper must not duplicate an anchor diagnostic"
        );

        enrich_daemon_dfs_browser_placement_connectivity_from_snapshot(&mut preview, &connectivity);
        // Enrichment may run again after a cached preview refresh; diagnostics
        // must remain stable rather than accumulating duplicates.
        enrich_daemon_dfs_browser_placement_connectivity_from_snapshot(&mut preview, &connectivity);

        let reachability = |node_id: &sinomo_core::node::NodeId| {
            preview
                .nodes
                .iter()
                .find(|node| &node.node_id == node_id)
                .map(|node| node.reachability)
                .expect("preview node")
        };
        assert_eq!(
            reachability(&local_node_id),
            sinomo_dfs::DfsPlacementNodeReachability::Local
        );
        assert_eq!(
            reachability(&reachable_node_id),
            sinomo_dfs::DfsPlacementNodeReachability::Reachable
        );
        assert_eq!(
            reachability(&unreachable_node_id),
            sinomo_dfs::DfsPlacementNodeReachability::Unreachable
        );
        assert_eq!(
            reachability(&unknown_node_id),
            sinomo_dfs::DfsPlacementNodeReachability::Unknown
        );
        assert_eq!(
            preview
                .blocking_diagnostics
                .iter()
                .filter(|diagnostic| {
                    **diagnostic
                        == sinomo_dfs::DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                            node_id: unreachable_node_id.clone(),
                        }
                })
                .count(),
            1
        );
        let error = require_satisfiable_daemon_dfs_browser_placement(&preview)
            .expect_err("unreachable required node must block update");
        assert!(error.to_string().contains("unsatisfiable"));
        assert!(error.to_string().contains(unreachable_node_id.as_str()));
    }

    #[test]
    fn dfs_browser_placement_enrichment_blocks_a_desired_node_that_became_unreachable() {
        let local_node_id = placement_test_node("node_dfs_browser_placement_race_local");
        let remote_node_id = placement_test_node("node_dfs_browser_placement_race_remote");
        let remote_endpoint = placement_test_endpoint(
            remote_node_id.clone(),
            "endpoint_dfs_browser_placement_race_remote",
            "192.0.2.41:21353",
        );
        let now = time::OffsetDateTime::now_utc();
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_browser_placement_race")
                .expect("cluster id"),
            "placement race",
            local_node_id.clone(),
            "local",
            "test-public-key",
            now,
        );
        let graph = ClusterConnectivityGraph::from_records(
            local_node_id.clone(),
            [sinomo_core::network::cluster_connectivity_record(
                &state,
                &remote_endpoint,
                ClusterConnectivityState::Unreachable,
                None,
                1,
                None,
                Some(now.to_string()),
                now,
                sinomo_core::network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
            )],
        );
        let config = sinomo_dfs::DfsFileSystemConfig {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            display_name: "SIFS".to_string(),
            owner_node_id: local_node_id.clone(),
            subtree_policies: Vec::new(),
        };
        let policy = sinomo_dfs::DfsPlacementPolicy::new(
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            1,
            2,
        )
        .expect("placement policy");
        let mut preview = sinomo_dfs::preview_placement(
            &config,
            "/projects/race.bin",
            sinomo_dfs::DfsObjectId::from_legacy_named("object_dfs_browser_placement_race")
                .expect("object id"),
            sinomo_dfs::DfsVersion::from_legacy_named("version_dfs_browser_placement_race")
                .expect("version"),
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            local_node_id.clone(),
            vec![local_node_id.clone()],
            vec![
                sinomo_dfs::DfsNodeAdvertisement::content(local_node_id.clone()),
                sinomo_dfs::DfsNodeAdvertisement::content(remote_node_id.clone()),
            ],
            Some(policy),
            sinomo_dfs::DfsPlacementScope::SelfOnly,
        )
        .expect("initial reachable plan");
        assert!(
            preview
                .placement
                .plan
                .desired_replica_nodes
                .contains(&remote_node_id)
        );

        enrich_daemon_dfs_browser_placement_connectivity_from_snapshot(
            &mut preview,
            &DaemonDfsBrowserPlacementConnectivitySnapshot {
                local_node_id: Some(local_node_id),
                graph: Some(graph),
                advertised_endpoints: vec![remote_endpoint],
            },
        );

        assert!(preview.blocking_diagnostics.contains(
            &sinomo_dfs::DfsPlacementBlockingDiagnostic::DesiredNodeUnavailable {
                node_id: remote_node_id.clone(),
            }
        ));
        let remote = preview
            .nodes
            .iter()
            .find(|node| node.node_id == remote_node_id)
            .expect("remote node row");
        assert!(remote.explanation.contains("explicitly unreachable"));
        assert!(require_satisfiable_daemon_dfs_browser_placement(&preview).is_err());
    }

    #[test]
    fn dfs_browser_placement_impact_merge_saturates_and_preserves_incompleteness() {
        let mut aggregate = sinomo_dfs::DfsPlacementImpact {
            affected_object_count: u64::MAX - 1,
            affected_bytes: 5,
            unknown_size_object_count: 1,
            copies_added: 2,
            copies_removed: 3,
            metadata_snapshot_digest: String::new(),
            complete: true,
        };
        merge_daemon_dfs_browser_placement_impact(
            &mut aggregate,
            &sinomo_dfs::DfsPlacementImpact {
                affected_object_count: 2,
                affected_bytes: 7,
                unknown_size_object_count: 4,
                copies_added: 6,
                copies_removed: 8,
                metadata_snapshot_digest: String::new(),
                complete: false,
            },
        );

        assert_eq!(aggregate.affected_object_count, u64::MAX);
        assert_eq!(aggregate.affected_bytes, 12);
        assert_eq!(aggregate.unknown_size_object_count, 5);
        assert_eq!(aggregate.copies_added, 8);
        assert_eq!(aggregate.copies_removed, 11);
        assert!(!aggregate.complete);
    }

    #[test]
    fn dfs_browser_placement_activity_pages_and_filters_compact_cluster_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-browser-placement-activity");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let owner = placement_test_node("node_dfs_browser_placement_activity_owner");
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let base = sinomo_dfs::DfsFileSystemConfig {
            file_system_id: file_system_id.clone(),
            display_name: "SIFS".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let policy = sinomo_dfs::DfsPlacementPolicy::new(
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            1,
            1,
        )
        .expect("activity policy");
        let mut first_config = base.clone();
        first_config.subtree_policies.push(
            sinomo_dfs::normalize_placement_rule(
                "/team/first",
                sinomo_dfs::DfsPlacementScope::SelfOnly,
                policy.clone(),
            )
            .expect("first activity rule"),
        );
        let mut second_config = first_config.clone();
        second_config.subtree_policies.push(
            sinomo_dfs::normalize_placement_rule(
                "/team/second",
                sinomo_dfs::DfsPlacementScope::SelfOnly,
                policy,
            )
            .expect("second activity rule"),
        );
        let make_job = |job_id: &str, path: &str, created_at_unix_nanos: u64| {
            sinomo_dfs::DfsPlacementReconciliationJob {
                schema_version: sinomo_dfs::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
                job_id: job_id.to_string(),
                file_system_id: file_system_id.clone(),
                path: path.to_string(),
                scope: sinomo_dfs::DfsPlacementScope::SelfOnly,
                unavailable_node_ids: BTreeSet::new(),
                candidate_snapshot: vec![sinomo_dfs::DfsNodeAdvertisement::content(owner.clone())],
                accepted_config: None,
                state: sinomo_dfs::DfsPlacementReconciliationState::Pending,
                cursor: None,
                total_object_count: 0,
                scanned_object_count: 0,
                matched_object_count: 0,
                updated_object_count: 0,
                scheduled_repair_count: 0,
                completed_repair_count: 0,
                pending_repair_count: 0,
                revision: 0,
                created_at_unix_nanos,
                updated_at_unix_nanos: created_at_unix_nanos,
                last_error: None,
            }
        };

        let mut kv = sinomo_core::kv::InProcessKvStore::new();
        let mut store = sinomo_dfs::DfsControlPlaneStore::new(&mut kv);
        store
            .put_file_system_config(&base)
            .expect("activity base config");
        let first_job = make_job("activity-first", "/team/first", 10);
        assert!(
            store
                .compare_and_put_placement_update(&base, &first_config, &first_job)
                .expect("first activity Apply")
        );
        let durable_first = store
            .placement_reconciliation(&file_system_id, &first_job.job_id)
            .expect("first activity read")
            .expect("durable first activity");
        let mut completed_first = durable_first.clone();
        completed_first.state = sinomo_dfs::DfsPlacementReconciliationState::Completed;
        completed_first.revision = 1;
        completed_first.updated_at_unix_nanos = 11;
        assert!(
            store
                .compare_and_put_placement_reconciliation(&durable_first, &completed_first)
                .expect("complete first activity")
        );
        let mut second_job = make_job("activity-second", "/team/second", 20);
        second_job.last_error = Some("temporary route failure".to_string());
        assert!(
            store
                .compare_and_put_placement_update(&first_config, &second_config, &second_job)
                .expect("second activity Apply")
        );
        drop(store);
        sinomo_core::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &kv.try_snapshot().expect("activity KV snapshot"),
        )
        .expect("write activity KV snapshot");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let backend = DaemonDfsBrowserBackend::open(data_dir, runtime.handle())
            .expect("open activity browser backend");
        let retrying = backend
            .placement_activity(crate::DaemonDfsBrowserPlacementActivityRequest {
                statuses: vec![sinomo_dfs::DfsPlacementReconciliationActivityStatus::Retrying],
                path_prefix: Some("/team/../team".to_string()),
                cursor: None,
                limit: 10,
            })
            .expect("filtered retrying activity")
            .page;
        assert_eq!(retrying.activities.len(), 1);
        assert_eq!(retrying.activities[0].job_id, second_job.job_id);
        assert!(
            retrying.partial,
            "without cluster capability state the endpoint must not claim complete history"
        );
        assert!(!retrying.history_truncated);

        let first_page = backend
            .placement_activity(crate::DaemonDfsBrowserPlacementActivityRequest {
                limit: 1,
                ..Default::default()
            })
            .expect("first activity page")
            .page;
        assert_eq!(first_page.activities[0].job_id, second_job.job_id);
        let second_page = backend
            .placement_activity(crate::DaemonDfsBrowserPlacementActivityRequest {
                cursor: first_page.next_cursor.clone(),
                limit: 1,
                ..Default::default()
            })
            .expect("second activity page")
            .page;
        assert_eq!(second_page.activities.len(), 1);
        assert_eq!(second_page.activities[0].job_id, first_job.job_id);
        assert!(second_page.next_cursor.is_none());
    }

    #[test]
    fn dfs_browser_list_can_return_compact_metadata_without_per_entry_stats() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-browser-list-metadata");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let backend = DaemonDfsBrowserBackend::open(SinomoDataDir::new(&root), runtime.handle())
            .expect("open DFS browser backend");
        backend
            .write_file(crate::DaemonDfsBrowserWriteFileRequest {
                path: "/users/report.txt".to_string(),
                bytes: b"report body".to_vec(),
            })
            .expect("write browser file");
        let stat = backend
            .stat(crate::DaemonDfsBrowserStatRequest {
                path: "/users/report.txt".to_string(),
                max_staleness_millis: 0,
            })
            .expect("stat browser file")
            .into_result()
            .expect("browser file is present");
        let missing = backend
            .stat(crate::DaemonDfsBrowserStatRequest {
                path: "/users/missing-report.txt".to_string(),
                max_staleness_millis: 0,
            })
            .expect("missing stat is a typed response");
        assert!(matches!(
            missing.into_result(),
            Err(sinomo_api::DaemonDfsBrowserReadFileFailure::NotFound { .. })
        ));

        let response = backend
            .list(crate::DaemonDfsBrowserListRequest {
                path: "/users".to_string(),
                shallow: true,
                missing_is_empty: false,
                sort_key: crate::DaemonDfsBrowserSortKey::Name,
                sort_direction: crate::DaemonDfsBrowserSortDirection::Ascending,
                offset: 0,
                limit: Some(32),
                include_metadata: true,
            })
            .expect("list browser directory");

        assert_eq!(response.entries.len(), 1);
        let entry = &response.entries[0];
        let metadata = entry.metadata.as_ref().expect("row metadata");
        assert_eq!(entry.size, Some(11));
        assert_eq!(metadata.object_id, stat.object_id);
        assert_eq!(metadata.version, stat.version);
        assert_eq!(metadata.modified_at_unix_nanos, stat.modified_at_unix_nanos);
        assert_eq!(metadata.policy, stat.policy);
        assert_eq!(metadata.current_locations, stat.current_locations);
        let placement = metadata
            .placement
            .as_ref()
            .expect("live row placement snapshot");
        assert_eq!(
            placement.agent_state,
            sinomo_dfs::DfsAgentObjectState::Current
        );
        assert_eq!(placement.desired_replicas, 1);
    }

    #[test]
    fn dfs_browser_stat_reuses_only_explicitly_bounded_results() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-browser-bounded-stat");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let backend = DaemonDfsBrowserBackend::open(SinomoDataDir::new(&root), runtime.handle())
            .expect("open DFS browser backend");
        backend
            .write_file(crate::DaemonDfsBrowserWriteFileRequest {
                path: "/users/bounded.txt".to_string(),
                bytes: b"bounded".to_vec(),
            })
            .expect("write browser file");
        let request = crate::DaemonDfsBrowserStatRequest {
            path: "/users/bounded.txt".to_string(),
            max_staleness_millis: 1_000,
        };
        let expected = backend.stat(request.clone()).expect("seed bounded stat");

        let held_service = backend.lock_service().expect("hold DFS browser service");
        assert_eq!(
            backend
                .stat(request.clone())
                .expect("bounded stat bypasses busy service"),
            expected
        );
        let strict_error = backend
            .stat(crate::DaemonDfsBrowserStatRequest {
                path: "/users/bounded.txt".to_string(),
                max_staleness_millis: 0,
            })
            .expect_err("strict stat must not use the bounded cache");
        assert!(strict_error.to_string().contains("service is busy"));
        drop(held_service);

        backend
            .pin_metadata(crate::DaemonDfsBrowserPinMetadataRequest {
                path: "/users/bounded.txt".to_string(),
            })
            .expect("pin browser metadata");
        let held_service = backend.lock_service().expect("hold DFS browser service");
        let invalidated_error = backend
            .stat(request)
            .expect_err("pinning must invalidate the bounded stat cache");
        assert!(invalidated_error.to_string().contains("service is busy"));
        drop(held_service);
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
                    include_metadata: false,
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

        let registry = ServiceRegistry::builder()
            .register(ClusterHealthService {
                paths: paths.clone(),
                runtime: Some(tokio::runtime::Handle::current()),
                include_authority_health: true,
            })
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
                paths: paths.clone(),
                runtime: Some(runtime.handle().clone()),
            })
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

        runtime.block_on(async {
            let dfs_notify = crate::dfs::daemon_dfs_maintenance_notify();
            while tokio::time::timeout(Duration::from_millis(1), dfs_notify.notified())
                .await
                .is_ok()
            {}
            let notified = dfs_notify.notified();
            tokio::pin!(notified);
            let response = call_value_service_host_async(&host, request)
                .await
                .expect("maintenance refresh response");
            response.error_detail().expect("maintenance refresh ok");
            tokio::time::timeout(Duration::from_secs(1), &mut notified)
                .await
                .expect("DFS maintenance notification");
        });
    }

    #[test]
    fn daemon_dfs_repair_queue_recovery_rejects_missing_acknowledgement() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-dfs-repair-recovery-ack");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let data_dir = SinomoDataDir::new(&root);
        let paths = DaemonPaths::new(&data_dir);
        let registry = ServiceRegistry::builder()
            .register(DaemonControlService {
                paths: paths.clone(),
                runtime: Some(runtime.handle().clone()),
            })
            .build();
        let host = test_service_host(&root, registry);
        let mut request = ObjectRpcRequest::encode_body(
            DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ROUTE,
            DAEMON_RPC_CATEGORY,
            &crate::DaemonDfsRepairQueueRecoveryRequest {
                acknowledgement: String::new(),
            },
        )
        .expect("request");
        request.auth = Some(
            crate::auth::sign_daemon_rpc_request_from_data_dir(&data_dir, &request)
                .expect("sign DFS repair recovery request"),
        );
        let authenticated =
            authenticate_daemon_control_request_via_registry(&paths, &request, &host)
                .expect("authenticate local DFS repair recovery request");
        assert_eq!(authenticated.signer, DaemonSignerAuthority::Local);
        annotate_daemon_service_request(&mut request, &authenticated);

        let error = runtime
            .block_on(call_value_service_host_async(&host, request))
            .expect_err("missing recovery acknowledgement must fail");
        assert!(
            error
                .to_string()
                .contains("DFS repair queue recovery requires acknowledgement"),
            "unexpected error: {error}"
        );
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
        let initial_dfs_wakes = crate::dfs::daemon_dfs_maintenance_wake_count();

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
        assert!(
            crate::dfs::daemon_dfs_maintenance_wake_count() > initial_dfs_wakes,
            "a connectivity reload must wake connectivity-blocked DFS repair work"
        );
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
            .register(DaemonControlService {
                paths,
                runtime: Some(runtime.handle().clone()),
            })
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
        assert!(matches!(
            call(DaemonSignerAuthority::Voter)
                .expect_err("remote config must be denied before a local grant"),
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
        assert!(matches!(
            call(DaemonSignerAuthority::Voter)
                .expect_err("final dispatch must observe local revocation"),
            DaemonError::AuthDenied
        ));
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
            request_body_mode: ServiceBodyMode::None,
            response_body_mode: ServiceBodyMode::None,
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
        let registry = ServiceRegistry::builder()
            .register(ClusterKvService {
                paths: paths.clone(),
                runtime: Some(runtime.handle().clone()),
            })
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
        assert_eq!(init_route.auth_scope.as_deref(), Some("local-command"));
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
    fn cluster_control_identity_change_handler_boundary_is_local_only() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-control-local-only");
        let paths = DaemonPaths::new(&SinomoDataDir::new(&root));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let remote = ServiceSigner::Remote {
            node_id: "stale-remote".to_string(),
            public_key: "stale-public-key".to_string(),
        };
        for route in [
            sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_LEAVE_ROUTE,
        ] {
            assert_remote_cluster_kv_auth_denied(
                runtime
                    .block_on(authorize_cluster_control_service_request(
                        &paths,
                        runtime.handle(),
                        route,
                        &remote,
                    ))
                    .expect_err("remote identity change must be denied before dispatch"),
            );
            runtime
                .block_on(authorize_cluster_control_service_request(
                    &paths,
                    runtime.handle(),
                    route,
                    &ServiceSigner::Local,
                ))
                .expect("local identity change remains authorized");
        }
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
                .object_rpc_route_descriptor(sinomo_observe::OBSERVE_EVENTS_ROUTE)
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
                .service_registry
                .active_snapshot()
                .registry()
                .owns(sinomo_observe::OBSERVE_EVENTS_ROUTE),
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
        let dfs_status = host
            .object_rpc_route_descriptor(sinomo_dfs::DFS_STATUS_ROUTE)
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

        let daemon_status = host
            .object_rpc_route_descriptor(DAEMON_STATUS_ROUTE)
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
        let connectivity_reload = host
            .object_rpc_route_descriptor(DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE)
            .expect("local connectivity reload route");
        assert_eq!(
            connectivity_reload.auth_scope.as_deref(),
            Some("local-command")
        );
        let maintenance_refresh = host
            .object_rpc_route_descriptor(DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE)
            .expect("cluster maintenance refresh route");
        assert_eq!(
            maintenance_refresh.auth_scope.as_deref(),
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
    fn placement_authority_break_glass_requires_a_local_service_signer() {
        for signer in [
            ServiceSigner::Local,
            ServiceSigner::LocalNode {
                node_id: "local-node".to_string(),
                public_key: "local-key".to_string(),
            },
        ] {
            require_local_dfs_placement_authority_break_glass_signer(&signer)
                .expect("local signer");
        }
        let remote = ServiceSigner::Remote {
            node_id: "remote-voter".to_string(),
            public_key: "remote-key".to_string(),
        };
        assert!(require_local_dfs_placement_authority_break_glass_signer(&remote).is_err());
    }

    #[test]
    fn main_and_shadow_share_peer_dfs_routes_but_only_main_hosts_local_services() {
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

        let authority_routes = CLUSTER_KV_RPC_ROUTES
            .iter()
            .copied()
            .chain(CLUSTER_RAFT_RPC_ROUTES.iter().copied())
            .chain(
                [
                    CLUSTER_JOIN_CHALLENGE_ROUTE,
                    CLUSTER_JOIN_REGISTER_ROUTE,
                    CLUSTER_INVITE_PUBLISH_ROUTE,
                    CLUSTER_MEMBER_PUBLISH_ROUTE,
                ]
                .into_iter(),
            )
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
        for route in [
            DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
            DAEMON_DFS_REPAIR_QUEUE_RECOVERY_ROUTE,
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
        ] {
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
        for route in sinomo_dfs::DFS_OBJECT_RPC_ROUTES {
            assert!(
                main_registry.owns(route),
                "main registry should own DFS route {route}"
            );
            assert!(
                shadow_registry.owns(route),
                "shadow registry should own peer DFS route {route}"
            );
        }
        for route in DAEMON_DFS_BROWSER_ROUTES {
            assert!(
                main_registry.owns(route),
                "main registry should own DFS browser route {route}"
            );
            assert!(
                !shadow_registry.owns(route),
                "shadow registry must not own DFS browser route {route}"
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
        assert!(
            main_registry.owns(DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE),
            "main registry should own DFS placement-authority administration"
        );
        assert!(
            !shadow_registry.owns(DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE),
            "shadow registry must not own DFS placement-authority administration"
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
        let mounted_mutation_handoff = main_host
            .object_rpc_route_descriptor(DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE)
            .expect("main mounted-mutation handoff route descriptor");
        assert_eq!(
            mounted_mutation_handoff.auth_scope.as_deref(),
            Some("local-command")
        );
        assert_eq!(
            mounted_mutation_handoff.required_capabilities,
            vec!["daemon.dfs.mounted-mutation".to_string()]
        );
        assert_eq!(mounted_mutation_handoff.timeout_ms, Some(30_000));
        assert!(
            shadow_host
                .object_rpc_route_descriptor(DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE)
                .is_none(),
            "shadow host must not advertise mounted-mutation ownership"
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
        for route in sinomo_dfs::DFS_OBJECT_RPC_ROUTES {
            assert!(
                main_host.object_rpc_route_descriptor(route).is_some(),
                "main host should advertise DFS route {route}"
            );
            assert!(
                shadow_host.object_rpc_route_descriptor(route).is_some(),
                "shadow host should advertise peer DFS route {route}"
            );
        }
        for route in DAEMON_DFS_BROWSER_ROUTES {
            assert!(
                main_host.object_rpc_route_descriptor(route).is_some(),
                "main host should advertise DFS browser route {route}"
            );
            assert!(
                shadow_host.object_rpc_route_descriptor(route).is_none(),
                "shadow host must not advertise DFS browser route {route}"
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
        let placement_authority = main_host
            .object_rpc_route_descriptor(DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE)
            .expect("main placement-authority descriptor");
        assert_eq!(placement_authority.auth_scope.as_deref(), Some("admin"));
        assert!(
            shadow_host
                .object_rpc_route_descriptor(DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE)
                .is_none(),
            "shadow host must not advertise DFS placement-authority administration"
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
        let registry = ServiceRegistry::builder()
            .register(DaemonControlService {
                paths: paths.clone(),
                runtime: Some(runtime.handle().clone()),
            })
            .register(ClusterHealthService {
                paths: paths.clone(),
                runtime: Some(runtime.handle().clone()),
                include_authority_health: true,
            })
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
