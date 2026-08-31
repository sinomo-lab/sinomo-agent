mod api;
mod auth;
mod client;
mod cluster;
mod control;
mod dfs;
mod error;
mod observe;
mod paths;
mod process;
pub mod registry;
mod rollout;
mod service_adapter;
mod status;

pub(crate) const DAEMON_SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR: &str = "sandbox-lifecycle-recovery";

pub use sinomo_api::{
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
    DAEMON_EVENT_SUBSCRIBE_ROUTE, DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_NETWORK_CHUNK_METRICS_ROUTE, DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_ROUTE, DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE, DAEMON_RPC_CATEGORY,
    DAEMON_SHUTDOWN_ROUTE, DAEMON_STATUS_ROUTE, DFS_NAMESPACE_CHANGED_EVENT_KIND,
    DFS_PLACEMENT_CHANGED_EVENT_KIND, DaemonApiError, DaemonClusterMaintenanceRefreshRequest,
    DaemonClusterMaintenanceRefreshResponse, DaemonClusterStatus,
    DaemonDfsBrowserCreateDirAllRequest, DaemonDfsBrowserCreateDirAllResponse,
    DaemonDfsBrowserDeleteRequest, DaemonDfsBrowserDeleteResponse, DaemonDfsBrowserEntry,
    DaemonDfsBrowserEntryMetadata, DaemonDfsBrowserEntryPlacement, DaemonDfsBrowserFileRead,
    DaemonDfsBrowserHealthSummary, DaemonDfsBrowserListRequest, DaemonDfsBrowserListResponse,
    DaemonDfsBrowserMetadataShard, DaemonDfsBrowserObjectHealth, DaemonDfsBrowserPinContentRequest,
    DaemonDfsBrowserPinContentResponse, DaemonDfsBrowserPinMetadataRequest,
    DaemonDfsBrowserPinMetadataResponse, DaemonDfsBrowserPinRecord,
    DaemonDfsBrowserPlacementActivityRequest, DaemonDfsBrowserPlacementActivityResponse,
    DaemonDfsBrowserPlacementPreviewRequest, DaemonDfsBrowserPlacementPreviewResponse,
    DaemonDfsBrowserPlacementProgressRequest, DaemonDfsBrowserPlacementProgressResponse,
    DaemonDfsBrowserPlacementUpdateRequest, DaemonDfsBrowserPlacementUpdateResponse,
    DaemonDfsBrowserPlacementUpdateV2Request, DaemonDfsBrowserReadFilePrefixRequest,
    DaemonDfsBrowserReadFilePrefixResponse, DaemonDfsBrowserReadFileRequest,
    DaemonDfsBrowserReadFileResponse, DaemonDfsBrowserReadFilesRequest,
    DaemonDfsBrowserReadFilesResponse, DaemonDfsBrowserReadObjectVersionRequest,
    DaemonDfsBrowserReadObjectVersionResponse, DaemonDfsBrowserRenameRequest,
    DaemonDfsBrowserRenameResponse, DaemonDfsBrowserReplaceFileIfVersionOutcome,
    DaemonDfsBrowserReplaceFileIfVersionRequest, DaemonDfsBrowserReplaceFileIfVersionResponse,
    DaemonDfsBrowserSortDirection, DaemonDfsBrowserSortKey, DaemonDfsBrowserStatOutcomeResponse,
    DaemonDfsBrowserStatRequest, DaemonDfsBrowserStatResponse, DaemonDfsBrowserStatusRequest,
    DaemonDfsBrowserStatusResponse, DaemonDfsBrowserTransferMetadataPrimaryRequest,
    DaemonDfsBrowserTransferMetadataPrimaryResponse, DaemonDfsBrowserUnpinContentRequest,
    DaemonDfsBrowserUnpinContentResponse, DaemonDfsBrowserUnpinMetadataRequest,
    DaemonDfsBrowserUnpinMetadataResponse, DaemonDfsBrowserWriteFileRequest,
    DaemonDfsBrowserWriteFileResponse, DaemonDfsBrowserWriteReceipt,
    DaemonDfsMountedMutationHandoffPhase, DaemonDfsMountedMutationHandoffRequest,
    DaemonDfsMountedMutationHandoffResponse, DaemonDfsObjectRpcForwardRequest,
    DaemonDfsPlacementAuthorityAdminRequest, DaemonDfsPlacementAuthorityAdminResponse,
    DaemonEventStreamItem, DaemonEventSubscribeRequest, DaemonEventSubscribeResponse,
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
pub(crate) use client::{
    DaemonObjectRpcAsyncHost, cluster_paths_from_env,
    fetch_cluster_kv_local_snapshot_from_endpoint_on_runtime,
    fetch_daemon_status_from_endpoint_on_runtime, send_cluster_member_publish_async,
    send_cluster_raft_append_on_runtime, send_cluster_raft_vote_on_runtime,
};
pub use client::{
    RemoteConfigClient, cancel_cluster_kv_watch_from_endpoint_on_runtime,
    drain_cluster_kv_watch_from_endpoint_on_runtime,
    fetch_cluster_kv_authoritative_range_prefix_from_endpoint_on_runtime,
    fetch_daemon_status_from_address, fetch_daemon_status_from_endpoint,
    fetch_daemon_status_from_endpoint_async, send_daemon_build_artifact,
    send_daemon_build_artifact_to_endpoint, send_daemon_build_artifact_to_target,
    send_daemon_build_capabilities, send_daemon_build_capabilities_to_endpoint,
    send_daemon_build_capabilities_to_target, send_daemon_build_status,
    send_daemon_build_status_to_endpoint, send_daemon_build_status_to_target,
    send_daemon_build_submit, send_daemon_build_submit_to_endpoint,
    send_daemon_build_submit_to_target, send_daemon_cluster_maintenance_refresh,
    send_daemon_cluster_maintenance_refresh_to_endpoint,
    send_daemon_remote_shadow_shutdown_request, send_daemon_rollout_binary,
    send_daemon_rollout_binary_to_endpoint, send_daemon_rollout_binary_to_target,
    send_daemon_rollout_build, send_daemon_rollout_build_to_endpoint,
    send_daemon_rollout_build_to_target, send_daemon_rollout_clear_status,
    send_daemon_rollout_clear_status_to_endpoint, send_daemon_rollout_diagnostics,
    send_daemon_rollout_diagnostics_to_endpoint, send_daemon_rollout_native_dylib_module,
    send_daemon_rollout_native_dylib_module_to_endpoint,
    send_daemon_rollout_native_dylib_module_to_target, send_daemon_rpc, send_daemon_rpc_async,
    send_daemon_rpc_call_async, send_daemon_rpc_stream, send_daemon_rpc_stream_async,
    send_daemon_rpc_to_endpoint, send_daemon_rpc_to_endpoint_async,
    send_daemon_rpc_to_endpoint_with_options, send_daemon_rpc_to_endpoint_with_timeouts,
    send_daemon_rpc_with_options, send_daemon_rpc_with_timeouts, send_daemon_shutdown_request,
    send_dfs_metadata_journal_batch_to_endpoint_async,
    send_dfs_metadata_snapshot_install_to_endpoint_async,
    send_object_rpc_call_to_target_async_with_cluster_paths_and_options,
    send_object_rpc_call_to_target_async_with_options, send_object_rpc_call_to_target_with_options,
    send_object_rpc_to_target, send_object_rpc_to_target_async,
    send_object_rpc_to_target_async_with_options, send_object_rpc_to_target_with_options,
    send_object_rpc_to_target_with_timeouts,
    start_cluster_kv_authoritative_watch_from_endpoint_on_runtime,
};
pub use cluster::commit_cluster_kv_txn_with_quorum;
pub use control::{
    handle_daemon_sandbox_checkpoint, handle_daemon_sandbox_create, handle_daemon_sandbox_destroy,
    handle_daemon_sandbox_http_fetch, handle_daemon_sandbox_http_fetch_to_file,
    handle_daemon_sandbox_import_busybox, handle_daemon_sandbox_list, handle_daemon_sandbox_read,
    handle_daemon_sandbox_reboot, handle_daemon_sandbox_resume,
    handle_daemon_sandbox_run_process_plan, handle_daemon_sandbox_run_toolchain,
    handle_daemon_sandbox_status, handle_daemon_sandbox_toolchain_add_artifact,
    handle_daemon_sandbox_toolchain_add_command, handle_daemon_sandbox_toolchain_import_package,
    handle_daemon_sandbox_update, handle_daemon_sandbox_write, open_daemon_sandbox_dfs_service,
    run_daemon_control_loop, run_daemon_control_loop_with_catalog,
    run_daemon_control_loop_with_catalog_and_profile,
    run_daemon_control_loop_with_catalog_and_runtime, run_daemon_control_loop_with_components,
    run_daemon_control_loop_with_components_and_profile,
};
pub use error::DaemonError;
pub(crate) use observe::{
    OperationalSeverity, append_operational_event_best_effort, append_operational_log_best_effort,
};
#[cfg(windows)]
pub use process::prepare_noninteractive_command;
#[cfg(not(unix))]
pub(crate) use process::spawn_shadow_rollout_completion_helper_process;
pub(crate) use process::{
    CandidateMainDiagnostics, build_daemon_async_runtime, build_daemon_async_runtime_with_profile,
    main_exit_observation_from_status, spawn_candidate_main_daemon_process,
    spawn_main_daemon_process, spawn_shadow_rollout_recovery_helper_process,
};
pub use process::{
    DaemonExecutionProfile, DaemonRuntimeConfig, daemon_main_binary_for_shadow_start,
    prepare_background_command, start_daemon_command_process,
};
pub use rollout::{
    LocalRolloutMainActivationPlan, handle_daemon_rollout_build_request,
    handle_daemon_rollout_native_dylib_module_request, handle_deferred_shadow_rollout_completion,
    prepare_local_rollout_main_activation, run_shadow_daemon_supervision_loop,
    shadow_main_binary_from_env, shadow_main_options_from_env, sleep_for_shadow_start_delay,
    stage_rollout_main_activation, wait_for_local_shadow_rollout_completion,
    wait_for_main_daemon_health,
};
pub(crate) use rollout::{daemon_paths_are_shadow, handle_daemon_finalize_shadow_rollout};
pub(crate) use sinomo_observe::{
    IdleStatsAtomics, install_process_idle_stats, process_idle_stats, register_io_observer,
};
pub use status::{
    DaemonClusterProcess, DaemonClusterRegistration, DaemonDataOwnerLock,
    DaemonEndpointAdvertisementConfig, DaemonLock, DaemonManualEndpointConfig,
    current_shadow_daemon_options, daemon_advertised_endpoints_for_state,
    daemon_advertised_endpoints_for_state_async,
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
    for daemon_root in ["daemon", "shadow-daemon"] {
        layout.register_escape_path(
            "sinomo-daemon",
            StorageRing::R1,
            format!("{daemon_root}/{DAEMON_SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR}"),
            "durable sandbox lifecycle recovery markers and sweep state",
        )?;
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
