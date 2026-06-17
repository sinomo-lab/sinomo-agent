mod auth;
mod client;
mod cluster;
mod control;
mod dfs;
mod error;
mod idle_stats;
mod install;
mod observe;
mod paths;
mod process;
mod rollout;
mod service_adapter;
mod status;

#[derive(Clone)]
pub struct DaemonServiceRegistryHook {
    register: std::sync::Arc<
        dyn Fn(sinomo_service::ServiceRegistryBuilder) -> sinomo_service::ServiceRegistryBuilder
            + Send
            + Sync,
    >,
}

impl DaemonServiceRegistryHook {
    pub fn new(
        register: impl Fn(
            sinomo_service::ServiceRegistryBuilder,
        ) -> sinomo_service::ServiceRegistryBuilder
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            register: std::sync::Arc::new(register),
        }
    }

    pub fn register(
        &self,
        builder: sinomo_service::ServiceRegistryBuilder,
    ) -> sinomo_service::ServiceRegistryBuilder {
        (self.register)(builder)
    }
}

pub use auth::{
    AuthenticatedDaemonCommand, DaemonSignerAuthority, SignedDaemonCommand,
    authenticate_daemon_control_request, authorize_authenticated_daemon_command,
    sign_daemon_rpc_request_for_local_node, sign_daemon_rpc_request_from_data_dir,
};
#[cfg(test)]
pub(crate) use client::send_daemon_rpc_raw;
pub use client::{
    RemoteConfigClient, fetch_daemon_status_from_address, fetch_daemon_status_from_endpoint,
    fetch_daemon_status_from_endpoint_async, send_daemon_rollout_binary,
    send_daemon_rollout_binary_to_endpoint, send_daemon_rollout_binary_upload_chunk,
    send_daemon_rollout_binary_upload_chunk_to_endpoint,
    send_daemon_rollout_binary_upload_finalize,
    send_daemon_rollout_binary_upload_finalize_to_endpoint, send_daemon_rollout_build,
    send_daemon_rollout_build_to_endpoint, send_daemon_rollout_clear_status,
    send_daemon_rollout_diagnostics, send_daemon_rollout_native_dylib_module,
    send_daemon_rollout_native_dylib_module_to_endpoint, send_daemon_rpc, send_daemon_rpc_async,
    send_daemon_rpc_direct_to_endpoint, send_daemon_rpc_direct_to_endpoint_async,
    send_daemon_rpc_stream, send_daemon_rpc_stream_async, send_daemon_rpc_to_endpoint,
    send_daemon_rpc_to_endpoint_async, send_daemon_rpc_with_timeouts, send_daemon_shutdown_request,
    send_dfs_metadata_journal_batch_to_endpoint_async,
    send_dfs_metadata_snapshot_install_to_endpoint_async, send_dfs_protocol_frame_to_endpoint,
    send_dfs_protocol_frame_to_endpoint_async,
};
pub(crate) use client::{
    fetch_cluster_kv_snapshot_from_endpoint_async,
    fetch_cluster_kv_snapshot_from_endpoint_on_runtime,
    fetch_daemon_status_from_endpoint_on_runtime, send_cluster_kv_txn_async,
    send_cluster_kv_txn_on_runtime, send_cluster_kv_watch_events_async,
    send_cluster_member_publish_async, send_cluster_member_publish_on_runtime,
    send_cluster_raft_append_async, send_cluster_raft_append_on_runtime,
    send_cluster_raft_vote_async, send_cluster_raft_vote_on_runtime, send_daemon_rpc_on_runtime,
    send_daemon_rpc_to_endpoint_on_runtime, send_dfs_metadata_journal_batch_to_endpoint_on_runtime,
    send_dfs_metadata_snapshot_install_to_endpoint_on_runtime,
    send_dfs_protocol_frame_to_endpoint_on_runtime,
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
    run_daemon_control_loop, run_daemon_control_loop_with_services,
};
pub use error::DaemonError;
pub use idle_stats::IdleStatsSnapshot;
pub(crate) use idle_stats::{
    IdleStatsAtomics, install_process_idle_stats, process_idle_stats, register_io_observer,
};
pub use install::{
    DaemonStopBeforeInstallStart, DaemonStopBeforeInstallStatus, DaemonStopIfRunningStatus,
    InstallLayout, InstalledDaemonInstallPlan, InstalledDaemonInstallPlanRequest,
    InstalledDaemonPaths, InstalledDaemonStartSpec, InstalledDaemonStartStatus,
    ManagedSourceRepoInstallStatus, activate_boot_entries, deactivate_boot_entries,
    default_boot_dir, ensure_install_layout_dirs, git_is_available, install_managed_source_repo,
    installed_daemon_paths, managed_source_git_status_lines, prepare_installed_daemon_install_plan,
    remove_boot_entry,
    start_installed_sinomo_daemon as daemon_install_start_installed_sinomo_daemon,
    stop_daemon_before_install as daemon_install_stop_daemon_before_install,
    stop_daemon_if_running as daemon_install_stop_daemon_if_running, write_install_boot_entries,
};
#[cfg(not(windows))]
pub use install::{LocalBinSymlinkStatus, SystemctlUserOutcome, ensure_local_bin_symlink};
#[cfg(windows)]
pub use install::{UserPathUpdate, ensure_windows_user_path_contains};
pub(crate) use observe::{
    OperationalSeverity, append_operational_event_best_effort, append_operational_log_best_effort,
};
pub use paths::DaemonPaths;
pub use process::start_daemon_command_process;
pub(crate) use process::{
    CandidateMainDiagnostics, build_daemon_async_runtime, main_exit_observation_from_status,
    prepare_background_command, spawn_candidate_main_daemon_process, spawn_main_daemon_process,
    spawn_shadow_rollout_recovery_helper_process,
};
pub use rollout::{
    LocalRolloutMainActivationPlan, handle_daemon_rollout_build_request,
    handle_daemon_rollout_native_dylib_module_request, handle_deferred_shadow_rollout_completion,
    prepare_local_rollout_main_activation, run_shadow_daemon_supervision_loop,
    shadow_main_binary_from_env, shadow_main_options_from_env, sleep_for_shadow_start_delay,
    stage_rollout_main_activation, wait_for_local_shadow_rollout_completion,
    wait_for_main_daemon_health,
};
pub(crate) use rollout::{
    daemon_paths_are_shadow, handle_daemon_clear_rollout_status,
    handle_daemon_finalize_shadow_rollout,
};
pub use status::{
    DAEMON_MAIN_CLUSTER_ROLE, DAEMON_SHADOW_CLUSTER_ROLE, DaemonClusterProcess,
    DaemonClusterRegistration, DaemonClusterStatus, DaemonDataOwnerLock, DaemonLock,
    DaemonProbeStatus, DaemonRunOptions, DaemonStatus, SINOMO_DAEMON_DEFAULT_HOST,
    SINOMO_DAEMON_DEFAULT_PORT, SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
    cluster_node_status_from_daemon_status, current_shadow_daemon_options,
    daemon_advertised_endpoints_for_state, daemon_advertised_endpoints_for_state_async,
    daemon_bind_address, daemon_cluster_registration, daemon_cluster_status,
    daemon_connect_address, daemon_options_from_status, daemon_package_version,
    daemon_status_matches_options, default_main_daemon_status,
    invite_introducers_from_daemon_status, probe_daemon_status, write_daemon_status,
};
pub(crate) use status::{
    current_daemon_cluster_options, current_shadow_daemon_options_on_runtime,
    daemon_cluster_options_from_status, daemon_cluster_process_from_paths,
    daemon_cluster_status_matches, daemon_probe_status_needs_restart_for_options,
    probe_daemon_status_async, probe_daemon_status_at_address, read_daemon_status,
};

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    for relative_path in ["daemon/status.sobj", "shadow-daemon/status.sobj"] {
        layout.register_sinomo_object(
            "sinomo-daemon",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new("DaemonStatus", STORAGE_CONTRACT_OBJECT_VERSION)
                .field("pid", "u32")
                .field("address", "String")
                .field("started_at", "String")
                .field("cluster", "Option<DaemonClusterStatus>"),
        )?;
    }
    for relative_path in [
        "daemon/daemon.lock",
        "daemon/data-owner.lock",
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
