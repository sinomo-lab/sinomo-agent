mod host_api;
pub use host_api::{
    DaemonEventHub, DaemonEventMirror, DaemonEventService, DaemonLifecycleFuture,
    DaemonLifecycleHost, DaemonOperationsFuture, DaemonOperationsHost, DaemonShutdownKind,
    HostedDaemonLifecycleService, HostedDaemonOperationsService, ReplayNonceCachePoisoned,
    ScopedReplayNonceCache, daemon_object_rpc_forward_options,
    daemon_object_rpc_forward_transport_timeouts, enforce_daemon_object_rpc_forward_route_affinity,
    register_daemon_event_service, register_hosted_daemon_lifecycle_service,
    register_hosted_daemon_operations_service,
};
mod api;
mod auth;
mod client;
mod cluster;
mod control;
mod error;
mod observe;
mod paths;
mod process;
pub mod registry;
pub mod service_adapter;
mod status;

pub use sinomo_api::{
    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE, DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE, DAEMON_EVENT_SUBSCRIBE_ROUTE,
    DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE, DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE, DAEMON_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE, DAEMON_RPC_CATEGORY, DAEMON_SHUTDOWN_ROUTE,
    DAEMON_STATUS_ROUTE, DaemonApiError, DaemonClusterMaintenanceRefreshRequest,
    DaemonClusterMaintenanceRefreshResponse, DaemonClusterStatus, DaemonEventStreamItem,
    DaemonEventSubscribeRequest, DaemonEventSubscribeResponse,
    DaemonLocalConnectivityReloadRequest, DaemonLocalConnectivityReloadResponse,
    DaemonNetworkChunkMetricsResponse, DaemonObjectRpcDestinationProcessAffinity,
    DaemonObjectRpcForwardDeliveryState, DaemonObjectRpcForwardRequest,
    DaemonObjectRpcForwardResponse, DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonStatus,
    IdleStatsSnapshot, SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT,
    SINOMO_SHADOW_DAEMON_DEFAULT_PORT, cluster_node_status_from_daemon_status, daemon_bind_address,
    daemon_connect_address, daemon_git_commit, daemon_hostname, daemon_options_from_status,
    daemon_package_version, daemon_paths_from_data_dir, daemon_status_matches_options,
    default_main_daemon_status, probe_daemon_status, probe_daemon_status_async,
    probe_daemon_status_at_address, read_daemon_status, shadow_daemon_paths_from_data_dir,
    write_daemon_status,
};

pub(crate) fn cluster_join_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(
        target: "sinomo.cluster.formation",
        detail = %args,
        "cluster join stage"
    );
    let enabled = std::env::var("SINOMO_CLUSTER_JOIN_TRACE")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"));
    if enabled {
        eprintln!("[sinomo cluster join] {args}");
    }
}

/// Application and built-in modules enter the daemon through the same
/// backend-neutral component factory contract.
pub type DaemonComponentFactory = std::sync::Arc<dyn sinomo_module::ComponentFactory>;
/// Complete component/module composition supplied to one daemon host.
pub type DaemonComponentCatalog = sinomo_module::ComponentCatalog;

pub use api::{DAEMON_API_PROBE_TIMEOUT, DaemonApiClient};
pub use auth::{
    AuthenticatedDaemonCommand, DaemonSignerAuthority, SignedDaemonCommand,
    authenticate_daemon_control_request, authorize_authenticated_daemon_command,
    daemon_rpc_body_sha256, sign_daemon_rpc_request_digest_for_local_node,
    sign_daemon_rpc_request_for_local_node, sign_daemon_rpc_request_from_data_dir,
};
pub use client::{
    DaemonObjectRpcAsyncHost, RemoteConfigClient, block_on_daemon_client_on_owner_runtime,
    block_on_daemon_client_on_process_runtime, block_on_daemon_client_session,
    cancel_cluster_kv_watch_from_endpoint_async, cancel_cluster_kv_watch_from_endpoint_on_runtime,
    cancel_cluster_kv_watch_from_node_async, drain_cluster_kv_watch_from_endpoint_async,
    drain_cluster_kv_watch_from_endpoint_on_runtime, drain_cluster_kv_watch_from_node_async,
    fetch_cluster_kv_authoritative_range_prefix_from_endpoint_on_runtime,
    fetch_cluster_kv_local_entry_from_endpoint_async, fetch_cluster_kv_local_entry_from_node_async,
    fetch_cluster_kv_local_snapshot_from_endpoint_async, fetch_daemon_status_from_address,
    fetch_daemon_status_from_endpoint, fetch_daemon_status_from_endpoint_async,
    local_session_cluster_state, local_session_data_dir, local_session_endpoint_for_address,
    object_rpc_transfer_timeouts, send_daemon_cluster_maintenance_refresh,
    send_daemon_cluster_maintenance_refresh_to_endpoint,
    send_daemon_remote_shadow_shutdown_request, send_daemon_rpc, send_daemon_rpc_async,
    send_daemon_rpc_call_async, send_daemon_rpc_shared_value,
    send_daemon_rpc_shared_value_to_endpoint, send_daemon_rpc_shared_value_to_target,
    send_daemon_rpc_stream, send_daemon_rpc_stream_async, send_daemon_rpc_to_endpoint,
    send_daemon_rpc_to_endpoint_async, send_daemon_rpc_to_endpoint_with_options,
    send_daemon_rpc_to_endpoint_with_timeouts, send_daemon_rpc_value,
    send_daemon_rpc_value_to_endpoint, send_daemon_rpc_with_options, send_daemon_rpc_with_timeouts,
    send_daemon_shutdown_request,
    send_object_rpc_call_to_target_async_with_cluster_paths_and_options,
    send_object_rpc_call_to_target_async_with_options, send_object_rpc_call_to_target_with_options,
    send_object_rpc_to_target, send_object_rpc_to_target_async,
    send_object_rpc_to_target_async_with_options, send_object_rpc_to_target_with_options,
    send_object_rpc_to_target_with_timeouts,
    start_cluster_kv_authoritative_watch_from_endpoint_async,
    start_cluster_kv_authoritative_watch_from_endpoint_on_runtime,
    start_cluster_kv_authoritative_watch_from_node_async,
};
pub(crate) use client::{
    cluster_paths_from_env, fetch_cluster_kv_local_snapshot_from_endpoint_on_runtime,
    fetch_daemon_status_from_endpoint_on_runtime, send_cluster_member_publish_async,
    send_cluster_raft_append_on_runtime, send_cluster_raft_vote_on_runtime,
};
pub use cluster::{
    CLUSTER_HEALTH_PROBE_TIMEOUT, cluster_state_authority_watch_targets,
    commit_cluster_kv_txn_with_quorum, deny_shadow_daemon_local_raft_access,
    refresh_daemon_node_status_in_kv, start_daemon_network_module_context_task,
};
pub use control::{
    daemon_data_dir_path, run_daemon_control_accept_loop, run_daemon_control_loop,
    run_daemon_control_loop_with_catalog, run_daemon_control_loop_with_catalog_and_profile,
    run_daemon_control_loop_with_catalog_and_runtime, run_daemon_control_loop_with_components,
    run_daemon_control_loop_with_components_and_profile,
    run_daemon_control_loop_with_prepared_listener,
};
pub use error::DaemonError;
pub use observe::{
    OperationalSeverity, append_operational_event_best_effort, append_operational_log_best_effort,
};
pub(crate) use process::build_daemon_async_runtime_with_profile;
#[cfg(windows)]
pub use process::prepare_noninteractive_command;
#[cfg(not(unix))]
pub use process::spawn_shadow_rollout_completion_helper_process;
pub use process::{
    CandidateMainDiagnostics, build_daemon_async_runtime, request_supervised_main_restart,
    spawn_candidate_main_daemon_process, spawn_main_daemon_process,
    spawn_shadow_rollout_recovery_helper_process,
};
pub use process::{
    DaemonExecutionProfile, DaemonRuntimeConfig, PreparedDaemonControlListener,
    daemon_main_binary_for_shadow_start, prepare_background_command,
    prepare_daemon_control_listener, start_daemon_command_process,
};
pub fn daemon_paths_are_shadow(paths: &DaemonPaths) -> bool {
    paths.is_shadow()
}
pub(crate) use sinomo_api::{
    IdleStatsAtomics, install_process_idle_stats, process_idle_stats, register_io_observer,
};
pub use status::{
    DaemonClusterProcess, DaemonClusterRegistration, DaemonEndpointAdvertisementConfig, DaemonLock,
    DaemonManualEndpointConfig, current_shadow_daemon_options,
    daemon_advertised_endpoints_for_state, daemon_advertised_endpoints_for_state_async,
    daemon_advertised_endpoints_for_state_with_manual_config,
    daemon_advertised_endpoints_for_state_with_manual_config_async, daemon_cluster_registration,
    daemon_cluster_status, daemon_endpoint_advertisement_config_file,
    invite_introducers_from_daemon_status, read_daemon_endpoint_advertisement_config,
};
pub(crate) use status::{
    current_daemon_cluster_options, current_shadow_daemon_options_on_runtime,
    daemon_cluster_options_from_status, daemon_cluster_process_from_paths,
    daemon_cluster_status_matches,
};

pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    use sinomo_core::storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    for relative_path in ["daemon/status.sobj", "shadow-daemon/status.sobj"] {
        layout.register_sinomo_object(
            "sinomo-daemon",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new("DaemonStatus", STORAGE_CONTRACT_OBJECT_VERSION)
                .field("pid", "u32")
                .field("address", "String")
                .field("api_protocol_version", "u32")
                .field("app_launch_id", "Option<String>")
                .field("started_at", "String")
                .field("cluster", "Option<DaemonClusterStatus>"),
        )?;
    }
    for relative_path in [
        "daemon/daemon.lock",
        "daemon/data-owner.lock",
        "daemon/sifs-mount.lock",
        "shadow-daemon/daemon.lock",
    ] {
        layout.register_binary("sinomo-daemon", StorageRing::R1, relative_path)?;
    }
    for relative_path in ["daemon/auth-nonces.sobj", "shadow-daemon/auth-nonces.sobj"] {
        layout.register_sinomo_object(
            "sinomo-daemon",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new(
                "BTreeMap<DaemonAuthNonceKey, Timestamp>",
                STORAGE_CONTRACT_OBJECT_VERSION,
            ),
        )?;
    }
    for relative_path in [
        "daemon/auth-nonces.sobjlog",
        "shadow-daemon/auth-nonces.sobjlog",
    ] {
        layout.register_object_log("sinomo-daemon", StorageRing::R1, relative_path)?;
    }
    for relative_path in ["daemon/main.stdout.log", "daemon/main.stderr.log"] {
        layout.register_human_text("sinomo-daemon", StorageRing::R1, relative_path)?;
    }
    layout.register_escape_path(
        "sinomo-daemon",
        StorageRing::Application,
        "startup",
        "installed OS boot entries and launch scripts",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::DaemonComponentFactory;

    #[test]
    fn component_host_ready_is_an_explicit_one_shot_edge() {
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&notifications);
        let factory: DaemonComponentFactory = Arc::new(
            sinomo_module::StaticComponentFactory::new(
                sinomo_module::ComponentDescriptor::new("test.ready", "1.0.0"),
                move |_, scope| {
                    let observed = Arc::clone(&observed);
                    scope.on_host_ready(move || {
                        observed.fetch_add(1, Ordering::SeqCst);
                    });
                    Ok(())
                },
            )
            .expect("component factory"),
        );

        let data_dir = Arc::new(sinomo_core::local::SinomoDataDir::new("/tmp/ready-test"));
        let mut reconciler = sinomo_module::ComponentReconciler::new(
            sinomo_module::ComponentContext::new(data_dir),
            sinomo_core::service::ServiceRegistry::default(),
        );
        reconciler.reconcile(&[factory]).expect("reconcile");
        assert_eq!(notifications.load(Ordering::SeqCst), 0);

        reconciler.notify_host_ready();
        assert_eq!(notifications.load(Ordering::SeqCst), 1);
    }
}
