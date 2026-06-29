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

pub const DAEMON_RPC_CATEGORY: &str = "daemon";
pub const DAEMON_STATUS_ROUTE: &str = "/daemon/status";
pub const DAEMON_SHUTDOWN_ROUTE: &str = "/daemon/shutdown";
pub const DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE: &str = "/daemon/cluster/maintenance-refresh";
pub const DAEMON_DFS_BROWSER_STATUS_ROUTE: &str = "/daemon/dfs/browser/status";
pub const DAEMON_DFS_BROWSER_STAT_ROUTE: &str = "/daemon/dfs/browser/stat";
pub const DAEMON_DFS_BROWSER_LIST_ROUTE: &str = "/daemon/dfs/browser/list";
pub const DAEMON_DFS_BROWSER_READ_FILE_ROUTE: &str = "/daemon/dfs/browser/read-file";
pub const DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE: &str = "/daemon/dfs/browser/read-file-prefix";
pub const DAEMON_DFS_BROWSER_READ_FILES_ROUTE: &str = "/daemon/dfs/browser/read-files";
pub const DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE: &str = "/daemon/dfs/browser/write-file";
pub const DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE: &str = "/daemon/dfs/browser/create-dir-all";
pub const DAEMON_DFS_BROWSER_RENAME_ROUTE: &str = "/daemon/dfs/browser/rename";
pub const DAEMON_DFS_BROWSER_DELETE_ROUTE: &str = "/daemon/dfs/browser/delete";
pub const DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE: &str = "/daemon/dfs/browser/pin-metadata";
pub const DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE: &str = "/daemon/dfs/browser/unpin-metadata";
pub const DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE: &str = "/daemon/dfs/browser/pin-content";
pub const DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE: &str = "/daemon/dfs/browser/unpin-content";
pub const DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE: &str =
    "/daemon/dfs/browser/transfer-metadata-primary";
pub const DAEMON_DFS_BROWSER_ROUTES: &[&str] = &[
    DAEMON_DFS_BROWSER_STATUS_ROUTE,
    DAEMON_DFS_BROWSER_STAT_ROUTE,
    DAEMON_DFS_BROWSER_LIST_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
    DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
    DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
    DAEMON_DFS_BROWSER_RENAME_ROUTE,
    DAEMON_DFS_BROWSER_DELETE_ROUTE,
    DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
];

pub(crate) fn cluster_join_trace(args: std::fmt::Arguments<'_>) {
    let enabled = std::env::var("SINOMO_CLUSTER_JOIN_TRACE")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"));
    if enabled {
        eprintln!("[sinomo cluster join] {args}");
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatusRequest;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatusResponse {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<DaemonDfsBrowserHealthSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_error: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserHealthSummary {
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub pending_write_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub pending_namespace_job_count: usize,
    pub failed_namespace_job_count: usize,
    pub metadata_shard_count: usize,
    pub repair_attempt_count: usize,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserListRequest {
    pub path: String,
    #[serde(default)]
    pub shallow: bool,
    #[serde(default)]
    pub missing_is_empty: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserListResponse {
    pub node_id: String,
    pub entries: Vec<DaemonDfsBrowserEntry>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserEntry {
    pub name: String,
    pub path: String,
    pub kind: sinomo_dfs::DfsObjectKind,
    pub size: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatResponse {
    pub file_system_id: String,
    pub object_id: String,
    pub path: String,
    pub kind: sinomo_dfs::DfsObjectKind,
    pub version: String,
    pub content_hash: Option<String>,
    pub size_bytes: Option<u64>,
    pub link_count: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub accessed_at_unix_nanos: u64,
    pub modified_at_unix_nanos: u64,
    pub created_at_unix_nanos: u64,
    pub changed_at_unix_nanos: u64,
    pub policy: sinomo_core::DfsObjectPolicy,
    pub current_locations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub local_pins: Vec<DaemonDfsBrowserPinRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<DaemonDfsBrowserObjectHealth>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_shard: Option<DaemonDfsBrowserMetadataShard>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserMetadataShard {
    pub shard_id: String,
    pub primary_node_id: String,
    pub replica_node_ids: Vec<String>,
    pub epoch: u64,
    pub local_node_id: String,
    pub local_is_primary: bool,
    pub local_is_replica: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinRecord {
    pub pinned_by_node_id: String,
    pub mode: sinomo_dfs::DfsPinMode,
    pub created_at_unix_nanos: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserObjectHealth {
    pub agent_state: sinomo_dfs::DfsAgentObjectState,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub missing_replica_nodes: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFileRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFileResponse {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFilePrefixRequest {
    pub path: String,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFilePrefixResponse {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFilesRequest {
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFilesResponse {
    pub files: Vec<DaemonDfsBrowserFileRead>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserFileRead {
    pub path: String,
    pub bytes: Option<Vec<u8>>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserWriteFileRequest {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserWriteFileResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserCreateDirAllRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserCreateDirAllResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserRenameRequest {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserRenameResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserDeleteRequest {
    pub path: String,
    pub directory: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserDeleteResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinMetadataRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinMetadataResponse {
    pub node_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserUnpinMetadataRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserUnpinMetadataResponse {
    pub removed: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinContentRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinContentResponse {
    pub node_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserUnpinContentRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserUnpinContentResponse {
    pub removed: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserTransferMetadataPrimaryRequest {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_epoch: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserTransferMetadataPrimaryResponse {
    pub file_system_id: String,
    pub shard_id: String,
    pub previous_primary_node_id: String,
    pub new_primary_node_id: String,
    pub previous_epoch: u64,
    pub new_epoch: u64,
    pub changed: bool,
    pub metadata_shard: DaemonDfsBrowserMetadataShard,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonClusterMaintenanceRefreshRequest;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonClusterMaintenanceRefreshResponse;

#[derive(Clone)]
pub struct DaemonServiceRegistryHook {
    register: std::sync::Arc<
        dyn Fn(sinomo_service::ServiceRegistryBuilder) -> sinomo_service::ServiceRegistryBuilder
            + Send
            + Sync,
    >,
    register_config_definitions: Option<
        std::sync::Arc<
            dyn Fn(
                    sinomo_config::ConfigRegistryBuilder,
                ) -> Result<sinomo_config::ConfigRegistryBuilder, String>
                + Send
                + Sync,
        >,
    >,
    register_observe_metric_exporters: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
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
            register_config_definitions: None,
            register_observe_metric_exporters: None,
        }
    }

    pub fn with_config_definitions(
        mut self,
        register: impl Fn(
            sinomo_config::ConfigRegistryBuilder,
        ) -> Result<sinomo_config::ConfigRegistryBuilder, String>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.register_config_definitions = Some(std::sync::Arc::new(register));
        self
    }

    pub fn with_observe_metric_exporters(
        mut self,
        register: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        self.register_observe_metric_exporters = Some(std::sync::Arc::new(register));
        self
    }

    pub fn register_config_definitions(
        &self,
        builder: sinomo_config::ConfigRegistryBuilder,
    ) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
        match &self.register_config_definitions {
            Some(register) => register(builder),
            None => Ok(builder),
        }
    }

    pub fn register(
        &self,
        builder: sinomo_service::ServiceRegistryBuilder,
    ) -> sinomo_service::ServiceRegistryBuilder {
        (self.register)(builder)
    }

    pub fn register_observe_metric_exporters(&self) {
        if let Some(register) = &self.register_observe_metric_exporters {
            register();
        }
    }
}

pub use auth::{
    AuthenticatedDaemonCommand, DaemonSignerAuthority, SignedDaemonCommand,
    authenticate_daemon_control_request, authorize_authenticated_daemon_command,
    daemon_rpc_body_sha256, sign_daemon_rpc_request_digest_for_local_node,
    sign_daemon_rpc_request_for_local_node, sign_daemon_rpc_request_from_data_dir,
};
#[cfg(test)]
pub(crate) use client::send_daemon_rpc_raw;
pub(crate) use client::{
    DaemonSignedEndpointRpcAsyncHost, cluster_paths_from_env,
    fetch_cluster_kv_snapshot_from_endpoint_on_runtime,
    fetch_daemon_status_from_endpoint_on_runtime, send_cluster_kv_watch_events_async,
    send_cluster_member_publish_async, send_cluster_raft_append_async,
    send_cluster_raft_append_on_runtime, send_cluster_raft_vote_async,
    send_cluster_raft_vote_on_runtime, send_daemon_rpc_on_runtime,
    send_daemon_rpc_to_endpoint_on_runtime, send_dfs_object_rpc_to_target_on_runtime,
};
pub use client::{
    RemoteConfigClient, fetch_daemon_status_from_address, fetch_daemon_status_from_endpoint,
    fetch_daemon_status_from_endpoint_async, send_daemon_cluster_maintenance_refresh,
    send_daemon_cluster_maintenance_refresh_to_endpoint, send_daemon_rollout_binary,
    send_daemon_rollout_binary_to_endpoint, send_daemon_rollout_binary_upload_chunk,
    send_daemon_rollout_binary_upload_chunk_to_endpoint,
    send_daemon_rollout_binary_upload_finalize,
    send_daemon_rollout_binary_upload_finalize_to_endpoint, send_daemon_rollout_build,
    send_daemon_rollout_build_to_endpoint, send_daemon_rollout_clear_status,
    send_daemon_rollout_clear_status_to_endpoint, send_daemon_rollout_diagnostics,
    send_daemon_rollout_diagnostics_to_endpoint, send_daemon_rollout_native_dylib_module,
    send_daemon_rollout_native_dylib_module_to_endpoint, send_daemon_rpc, send_daemon_rpc_async,
    send_daemon_rpc_stream, send_daemon_rpc_stream_async, send_daemon_rpc_to_endpoint,
    send_daemon_rpc_to_endpoint_async, send_daemon_rpc_with_timeouts, send_daemon_shutdown_request,
    send_dfs_metadata_journal_batch_to_endpoint_async,
    send_dfs_metadata_snapshot_install_to_endpoint_async, send_dfs_object_rpc_to_target,
    send_dfs_object_rpc_to_target_async,
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
    DaemonClusterProcess, DaemonClusterRegistration, DaemonClusterStatus, DaemonDataOwnerLock,
    DaemonLock, DaemonProbeStatus, DaemonRunOptions, DaemonStatus, SINOMO_DAEMON_DEFAULT_HOST,
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
