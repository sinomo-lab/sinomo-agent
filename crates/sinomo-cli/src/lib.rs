//! Shared Sinomo command-line handlers.

macro_rules! print {
    ($($arg:tt)*) => {{
        $crate::write_cli_stdout(format_args!($($arg)*));
    }};
}

macro_rules! println {
    () => {{
        $crate::write_cli_stdout(format_args!("\n"));
    }};
    ($($arg:tt)*) => {{
        $crate::write_cli_stdout(format_args!($($arg)*));
        $crate::write_cli_stdout(format_args!("\n"));
    }};
}

macro_rules! eprintln {
    () => {{
        $crate::write_cli_stderr(format_args!("\n"));
    }};
    ($($arg:tt)*) => {{
        $crate::write_cli_stderr(format_args!($($arg)*));
        $crate::write_cli_stderr(format_args!("\n"));
    }};
}

#[cfg(test)]
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sinomo_agent::{
    Agent, AgentDefinition, AgentEventPayload, AgentLoopDecision, AgentLoopInput, AgentModelEvent,
    AgentOp, AgentRole, AgentRuntimeEvent, AgentRuntimeEventCursor, AgentRuntimeEventPayload,
    AgentSession, AgentSessionRuntime, AgentToolProgressEvent, BoundedContextBuilder,
    ConfigAgentToolConfig, EventEnvelopeExt, InteractionModel, InteractionOutput,
    InteractionSession, InteractionSurface, InteractionSurfaceKind, interaction_event,
    interaction_model_request, user_interaction_input_event,
};
use sinomo_lm::{
    AnthropicProvider, CODEX_DEFAULT_MODEL, CODEX_PROVIDER_ID, CurrentDefaultModel,
    DEFAULT_PROVIDER_ACCOUNT, DeepSeekProvider, GeminiProvider, GitHubCopilotProvider,
    LM_PROVIDER_CONFIG_KEY, LM_PROVIDER_CONFIG_KV_KEY, LM_RUNTIME_CONFIG_KEY,
    LM_RUNTIME_CONFIG_KV_KEY, LanguageModelError, LanguageModelModelOptions,
    LanguageModelProviderOptions, LanguageModelProviderUsage, LanguageModelRequest,
    LanguageModelResponse, ModelConfig, ModelPaths, ModelRuntimeConfig, NvidiaProvider,
    OpenAiProvider, ProviderAccountConfig, ProviderReasoningEffort, ProviderTextVerbosity,
    codex_provider, collect_provider_model_catalogs, collect_provider_usage,
    complete_configured_model_request_with_account,
    current_default_model as resolve_current_default_model, default_model_provider_ids,
    github_copilot_provider, load_model_config, model_config_cluster_kv_txn,
    model_config_definition, model_config_from_cluster_kv_snapshot, model_config_store,
    model_runtime_config_cluster_kv_txn, model_runtime_config_definition,
    model_runtime_config_from_cluster_kv_snapshot, normalize_model_id_for_provider,
    normalize_model_provider, normalize_provider_account_name, provider_account_config,
    provider_api_key_from_env, provider_options_for_model, resolve_default_model,
    save_provider_api_key, save_provider_api_key_in_model_config, subscription_usage_provider_ids,
    test_provider_account_credentials, test_provider_credentials, write_default_model_config,
};
use sinomo_pytorch::{
    MINICPM_DEFAULT_MODEL, MINICPM_PROVIDER_ID, ManagedPytorchServer, ManagedPytorchServerConfig,
    PytorchDevice, PytorchMode, RemotePytorchRuntime, RemotePytorchRuntimeConfig,
    is_loopback_http_endpoint,
};
use sinomo_runtime::cli_registry::{
    CliCommandContext, CliCommandRegistry, CliParseError, RegisteredCliCommand,
};
use sinomo_runtime::core::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterJoinInvite, ClusterMemberRecord,
    ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability, ClusterNodeRole,
    ClusterNodeRolloutStatus, ClusterNodeStatusRecord, ClusterTransportKind, EvaluationScorecard,
    EventEnvelope, RollbackTrigger,
};
#[cfg(test)]
use sinomo_runtime::daemon::{
    AuthenticatedDaemonCommand, DAEMON_MAIN_CLUSTER_ROLE, DaemonError, DaemonSignerAuthority,
    SignedDaemonCommand, authenticate_daemon_control_request,
    authorize_authenticated_daemon_command, daemon_package_version,
    sign_daemon_rpc_request_from_data_dir, write_daemon_status,
};
use sinomo_runtime::daemon::{
    DaemonClusterProcess, DaemonClusterRegistration, DaemonClusterStatus, DaemonPaths,
    DaemonProbeStatus, DaemonRunOptions, DaemonStatus, DaemonStopBeforeInstallStart,
    DaemonStopBeforeInstallStatus, DaemonStopIfRunningStatus, IdleStatsSnapshot, InstallLayout,
    InstalledDaemonInstallPlanRequest, InstalledDaemonStartStatus, ManagedSourceRepoInstallStatus,
    SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT, SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
    activate_boot_entries as daemon_activate_boot_entries, cluster_node_status_from_daemon_status,
    current_shadow_daemon_options, daemon_advertised_endpoints_for_state, daemon_bind_address,
    daemon_cluster_registration, daemon_cluster_status, daemon_connect_address,
    daemon_install_start_installed_sinomo_daemon, daemon_install_stop_daemon_before_install,
    daemon_install_stop_daemon_if_running, daemon_options_from_status,
    deactivate_boot_entries as daemon_deactivate_boot_entries,
    default_boot_dir as daemon_default_boot_dir,
    default_main_daemon_status as daemon_default_main_daemon_status,
    ensure_install_layout_dirs as daemon_ensure_install_layout_dirs,
    fetch_cluster_kv_snapshot as daemon_fetch_cluster_kv_snapshot,
    fetch_cluster_kv_snapshot_from_endpoint as daemon_fetch_cluster_kv_snapshot_from_endpoint,
    fetch_daemon_status_from_address as daemon_fetch_daemon_status_from_address,
    fetch_daemon_status_from_endpoint as daemon_fetch_daemon_status_from_endpoint,
    git_is_available as daemon_git_is_available,
    handle_daemon_connection as daemon_handle_daemon_connection,
    install_managed_source_repo as daemon_install_managed_source_repo,
    installed_daemon_paths as daemon_installed_daemon_paths, invite_introducers_from_daemon_status,
    managed_source_git_status_lines as daemon_managed_source_git_status_lines,
    prepare_installed_daemon_install_plan as daemon_prepare_installed_daemon_install_plan,
    prepare_local_rollout_main_activation as daemon_prepare_local_rollout_main_activation,
    probe_daemon_status, remove_boot_entry as daemon_remove_boot_entry,
    run_daemon_control_loop as daemon_run_daemon_control_loop,
    run_shadow_daemon_supervision_loop as daemon_run_shadow_daemon_supervision_loop,
    send_daemon_rollout_binary as daemon_send_daemon_rollout_binary,
    send_daemon_rollout_binary_to_endpoint as daemon_send_daemon_rollout_binary_to_endpoint,
    send_daemon_rollout_binary_upload_chunk as daemon_send_daemon_rollout_binary_upload_chunk,
    send_daemon_rollout_binary_upload_chunk_to_endpoint as daemon_send_daemon_rollout_binary_upload_chunk_to_endpoint,
    send_daemon_rollout_binary_upload_finalize as daemon_send_daemon_rollout_binary_upload_finalize,
    send_daemon_rollout_binary_upload_finalize_to_endpoint as daemon_send_daemon_rollout_binary_upload_finalize_to_endpoint,
    send_daemon_rollout_build as daemon_send_daemon_rollout_build,
    send_daemon_rollout_build_to_endpoint as daemon_send_daemon_rollout_build_to_endpoint,
    send_daemon_rollout_clear_status as daemon_send_daemon_rollout_clear_status,
    send_daemon_rollout_diagnostics as daemon_send_daemon_rollout_diagnostics,
    send_daemon_rpc as daemon_send_daemon_rpc,
    send_daemon_rpc_to_endpoint as daemon_send_daemon_rpc_to_endpoint,
    send_daemon_shutdown_request,
    shadow_main_binary_from_env as daemon_shadow_main_binary_from_env,
    shadow_main_options_from_env as daemon_shadow_main_options_from_env,
    sleep_for_shadow_start_delay as daemon_sleep_for_shadow_start_delay,
    stage_rollout_main_activation as daemon_stage_rollout_main_activation,
    start_daemon_command_process as daemon_start_daemon_command_process,
    wait_for_local_shadow_rollout_completion as daemon_wait_for_local_shadow_rollout_completion,
    wait_for_main_daemon_health as daemon_wait_for_main_daemon_health,
    write_install_boot_entries as daemon_write_install_boot_entries,
};
#[cfg(test)]
use sinomo_runtime::daemon::{
    DaemonLock, daemon_rpc_command,
    handle_daemon_rollout_build_request as daemon_handle_daemon_rollout_build_request,
    handle_daemon_sandbox_http_fetch as daemon_handle_daemon_sandbox_http_fetch,
};
#[cfg(not(windows))]
use sinomo_runtime::daemon::{
    LocalBinSymlinkStatus, SystemctlUserOutcome,
    ensure_local_bin_symlink as daemon_ensure_local_bin_symlink,
};
#[cfg(windows)]
use sinomo_runtime::daemon::{
    UserPathUpdate, ensure_windows_user_path_contains as daemon_ensure_windows_user_path_contains,
};
#[cfg(all(test, feature = "sandbox"))]
use sinomo_runtime::daemon::{
    handle_daemon_sandbox_checkpoint as daemon_handle_daemon_sandbox_checkpoint,
    handle_daemon_sandbox_create as daemon_handle_daemon_sandbox_create,
    handle_daemon_sandbox_destroy as daemon_handle_daemon_sandbox_destroy,
    handle_daemon_sandbox_http_fetch_to_file as daemon_handle_daemon_sandbox_http_fetch_to_file,
    handle_daemon_sandbox_import_busybox as daemon_handle_daemon_sandbox_import_busybox,
    handle_daemon_sandbox_list as daemon_handle_daemon_sandbox_list,
    handle_daemon_sandbox_read as daemon_handle_daemon_sandbox_read,
    handle_daemon_sandbox_reboot as daemon_handle_daemon_sandbox_reboot,
    handle_daemon_sandbox_resume as daemon_handle_daemon_sandbox_resume,
    handle_daemon_sandbox_run_process_plan as daemon_handle_daemon_sandbox_run_process_plan,
    handle_daemon_sandbox_run_process_plan_stream as daemon_handle_daemon_sandbox_run_process_plan_stream,
    handle_daemon_sandbox_run_toolchain as daemon_handle_daemon_sandbox_run_toolchain,
    handle_daemon_sandbox_run_toolchain_stream as daemon_handle_daemon_sandbox_run_toolchain_stream,
    handle_daemon_sandbox_status as daemon_handle_daemon_sandbox_status,
    handle_daemon_sandbox_toolchain_add_artifact as daemon_handle_daemon_sandbox_toolchain_add_artifact,
    handle_daemon_sandbox_toolchain_add_command as daemon_handle_daemon_sandbox_toolchain_add_command,
    handle_daemon_sandbox_toolchain_import_package as daemon_handle_daemon_sandbox_toolchain_import_package,
    handle_daemon_sandbox_update as daemon_handle_daemon_sandbox_update,
    handle_daemon_sandbox_write as daemon_handle_daemon_sandbox_write,
    open_daemon_sandbox_dfs_service as daemon_open_daemon_sandbox_dfs_service,
};
use sinomo_runtime::dfs::{DaemonDfsInspectionRequest, DaemonDfsInspectionResponse};
use sinomo_runtime::dfs::{
    DfsConflictRecord, DfsFileSystemId, DfsMetadataShardConfig, DfsObjectId,
    DfsOperatorInspectionCursor, DfsOperatorInspector, DfsOperatorRepairView,
    LocalShardedMetadataBackend, LocalSobjMetadataBackend,
};
use sinomo_runtime::dfs::{
    DfsExposeAddRequest, DfsPinAddRequest, DfsPinMode, add_dfs_expose_entry, add_local_dfs_pin,
    default_dfs_metadata_root, default_dfs_metadata_shards_root, dfs_expose_config_path,
    ensure_default_dfs_expose_config, fallback_dfs_node_id, list_local_dfs_pins,
    load_dfs_expose_config, metadata_shards_prefix, open_local_sharded_metadata_backend,
    open_root_local_metadata_backend, remove_dfs_expose_entry, remove_local_dfs_pin,
    resolve_dfs_metadata_root, resolve_dfs_pin_root, sifs_file_system_id,
};
use sinomo_runtime::events::{EventId, JsonlEventStore, SessionId};
use sinomo_runtime::events::{EventStore, InMemoryEventStore};
use sinomo_runtime::kv::{InProcessKvStore, KvError, KvOp, KvSnapshot, KvTxn, KvTxnResponse};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::module::ModuleReplayReport;
use sinomo_runtime::network::ClusterJoinRegistrationRequest;
use sinomo_runtime::network::ClusterJoinRegistrationResponse;
use sinomo_runtime::network::{self, ClusterPaths, cluster_doctor_findings};
use sinomo_runtime::network::{
    CLUSTER_INVITE_CHALLENGE_PROTOCOL, ClusterDoctorFinding, ClusterHealthRow, ClusterId,
    ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    verify_cluster_invite_challenge_proof,
};
use sinomo_runtime::node::{
    NodeId, load_or_create_main_node_identity, load_or_create_shadow_node_identity,
    shadow_daemon_node_id,
};
#[cfg(test)]
use sinomo_runtime::rollout::read_rollout_status;
#[cfg(test)]
use sinomo_runtime::rollout::rollout_main_activation_path;
#[cfg(test)]
use sinomo_runtime::rollout::write_rollout_status;
use sinomo_runtime::rollout::{
    AppliedRolloutTransition, ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse,
    ClusterRolloutBinaryUploadChunkRequest, ClusterRolloutBinaryUploadFinalizeRequest,
    ClusterRolloutBinaryUploadManifest, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleUploadSpec, ClusterRolloutUploadArtifactSpec,
    DEFAULT_SHADOW_TELEMETRY_LIMIT, LocalRolloutNativeDylibModuleArtifact, ManagedSourceStatus,
    RolloutActivationStrategy, RolloutController, RolloutError, RolloutMainActivation,
    RolloutObservation, RolloutRuntimeState, RolloutTelemetryGateConfig, SelfRolloutPaths,
    clear_rollout_status, ensure_rollout_layout, ensure_rollout_source_repo,
    install_current_executable,
    install_local_rollout_native_dylib_modules as rollout_install_local_rollout_native_dylib_modules,
    managed_source_status, prepare_cluster_rollout_binary_request,
    prepare_cluster_rollout_build_request, prepare_local_rollout_artifacts,
    prepare_rollout_dev_artifacts, prepare_rollout_source_bundle, read_rollout_runtime_state,
    read_rollout_telemetry_gate_config, rollout_source_head, rollout_status_view,
    shadow_telemetry_scorecard_from_event_store, source_heads_match,
    sync_external_repo_from_managed, unique_rollout_targets, write_rollout_telemetry_gate_config,
};
use sinomo_runtime::rollout::{
    SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV, SHADOW_MAIN_PORT_ENV,
    cluster_rollout_should_use_remote_build as daemon_cluster_rollout_should_use_remote_build,
};
use std::cell::{Cell, RefCell};
use std::env;
use std::fmt;
use std::fs;
use std::io::{self, BufRead, Read, Write};
#[cfg(unix)]
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

mod error;
mod help;
mod providers;
mod sandbox_host;
#[cfg(test)]
mod tests;
mod tui;

/// GUI-facing surface over the main-agent chat runtime (used by `sinomo-app`).
pub mod app_api;

use error::CliError;
use help::*;
pub use providers::*;
use sandbox_host::SinomoCliSandboxHost;
use time::format_description::well_known::Rfc3339;
pub use tui::*;

struct CliClusterVoterReachabilityHost;

impl network::ClusterVoterReachabilityHost for CliClusterVoterReachabilityHost {
    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool {
        daemon_fetch_daemon_status_from_endpoint(endpoint, timeout).is_ok()
    }
}

fn cli_cluster_voter_reachability_options() -> network::ClusterVoterReachabilityOptions {
    network::ClusterVoterReachabilityOptions {
        timeout: Duration::from_secs(5),
        tcp_command_timeout: Duration::from_secs(5),
        iroh_command_timeout: Duration::from_secs(3),
        tcp_foreground_attempt_timeout: Duration::from_secs(1),
        public_foreground_attempt_timeout: Duration::from_secs(4),
        iroh_foreground_attempt_timeout: Duration::from_millis(1500),
    }
}

fn cluster_has_local_quorum(paths: &ClusterPaths, state: &ClusterBootstrapState) -> bool {
    network::cluster_has_local_quorum_with_host(
        paths,
        state,
        cli_cluster_voter_reachability_options(),
        &mut CliClusterVoterReachabilityHost,
    )
}

fn cluster_reachable_voter_count(paths: &ClusterPaths, state: &ClusterBootstrapState) -> usize {
    network::cluster_reachable_voter_count_with_host(
        paths,
        state,
        cli_cluster_voter_reachability_options(),
        &mut CliClusterVoterReachabilityHost,
    )
}

struct CliClusterStatusProbe<'a> {
    paths: &'a ClusterPaths,
}

impl network::ClusterStatusProbe for CliClusterStatusProbe<'_> {
    fn probe_member_status(
        &self,
        state: &ClusterBootstrapState,
        member: &ClusterMemberRecord,
    ) -> Option<ClusterNodeStatusRecord> {
        cli_probe_member_daemon_status(self.paths, state, member)
    }

    fn peer_local_health_rows(
        &self,
        state: &ClusterBootstrapState,
        member: &ClusterMemberRecord,
    ) -> Option<Vec<ClusterHealthRow>> {
        cli_fetch_local_cluster_health_rows_from_member(self.paths, state, member)
    }
}

fn cli_probe_member_daemon_status(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<ClusterNodeStatusRecord> {
    if network::cluster_member_is_local_process(state, member)
        && let Some(status) = cli_probe_local_member_daemon_status(paths, state, member)
    {
        return Some(status);
    }
    for endpoint in
        network::cluster_member_daemon_status_targets_with_connectivity(paths, state, member)
    {
        let Ok(status) =
            daemon_fetch_daemon_status_from_endpoint(&endpoint, CLUSTER_HEALTH_PROBE_TIMEOUT)
        else {
            continue;
        };
        let Some(cluster) = status.cluster.as_ref() else {
            continue;
        };
        if cluster.cluster_id != state.cluster_id || cluster.node_id != member.node_id {
            continue;
        }
        return Some(cluster_node_status_from_daemon_status(
            &status,
            cluster,
            None,
            OffsetDateTime::now_utc(),
        ));
    }
    None
}

fn cli_probe_local_member_daemon_status(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<ClusterNodeStatusRecord> {
    let data_dir = paths.root.parent()?;
    let target_paths = if member.node_id == state.local_node_id {
        DaemonPaths::with_root(data_dir.join("daemon"))
    } else if member.node_id == shadow_daemon_node_id(&state.local_node_id) {
        DaemonPaths::with_root(data_dir.join("shadow-daemon"))
    } else {
        return None;
    };
    let status = match probe_daemon_status(&target_paths, CLUSTER_HEALTH_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => status,
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => return None,
    };
    let cluster = status.cluster.as_ref()?;
    if cluster.cluster_id != state.cluster_id || cluster.node_id != member.node_id {
        return None;
    }
    Some(cluster_node_status_from_daemon_status(
        &status,
        cluster,
        None,
        OffsetDateTime::now_utc(),
    ))
}

fn cli_fetch_local_cluster_health_rows_from_member(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<Vec<ClusterHealthRow>> {
    for endpoint in
        network::cluster_member_daemon_status_targets_with_connectivity(paths, state, member)
    {
        let Ok(request) = network::cluster_local_health_rpc_request() else {
            return None;
        };
        let Ok(response) = daemon_send_daemon_rpc_to_endpoint(
            &endpoint,
            &request,
            CLUSTER_HEALTH_PEER_ROWS_TIMEOUT,
        ) else {
            continue;
        };
        let Ok(rows) = network::decode_cluster_health_rpc_response(&response) else {
            continue;
        };
        return Some(rows);
    }
    None
}

fn cli_fetch_cluster_health_rows(
    address: &str,
    timeout: Duration,
) -> Result<Vec<ClusterHealthRow>, CliError> {
    let request = network::cluster_health_rpc_request()?;
    let response = daemon_send_daemon_rpc(address, &request, timeout)?;
    network::decode_cluster_health_rpc_response(&response).map_err(Into::into)
}

mod commands;
mod completion;
mod file_manager;
mod sifs_file_manager;
mod sobj_file_preview;
pub(crate) use commands::*;
pub(crate) use completion::*;

thread_local! {
    static CLI_FORCE_LOCAL_MODE: Cell<bool> = const { Cell::new(false) };
    static CLI_OUTPUT_CAPTURE: RefCell<Option<CliOutputBuffer>> = const { RefCell::new(None) };
}

static CLI_RUNTIME_CONFIG: OnceLock<CliRuntimeConfig> = OnceLock::new();

#[cfg(unix)]
static PROCESS_STDIO_CAPTURE_LOCK: Mutex<()> = Mutex::new(());

pub type CliConfigRegistryHook = Box<
    dyn Fn(
            sinomo_runtime::config::ConfigRegistryBuilder,
        ) -> Result<sinomo_runtime::config::ConfigRegistryBuilder, String>
        + Send
        + Sync,
>;

pub type CliStorageLayoutHook = Box<
    dyn Fn(
            &mut sinomo_runtime::storage::StorageLayout,
        ) -> Result<(), sinomo_runtime::storage::StorageContractError>
        + Send
        + Sync,
>;

pub struct CliRuntimeConfig {
    command_registry: CliCommandRegistry,
    config_registry_hooks: Vec<CliConfigRegistryHook>,
    storage_layout_hooks: Vec<CliStorageLayoutHook>,
}

impl CliRuntimeConfig {
    pub fn with_command_handler(
        mut self,
        handler: impl sinomo_runtime::cli_registry::CliCommandHandler + 'static,
    ) -> Self {
        self.command_registry = self.command_registry.register(handler);
        self
    }

    pub fn with_native_rust_module_binding(
        mut self,
        binding: sinomo_runtime::module::NativeRustModuleBinding,
    ) -> Self {
        if let Some(register_cli_command) = binding.register_cli_command {
            self.command_registry = register_cli_command(self.command_registry);
        }
        if let Some(register_config_definitions) = binding.register_config_definitions {
            self.config_registry_hooks
                .push(Box::new(register_config_definitions));
        }
        if let Some(register_storage_paths) = binding.register_storage_paths {
            self.storage_layout_hooks
                .push(Box::new(register_storage_paths));
        }
        self
    }

    pub fn with_native_rust_module_bindings(
        mut self,
        bindings: &[sinomo_runtime::module::NativeRustModuleBinding],
    ) -> Self {
        for binding in bindings {
            self = self.with_native_rust_module_binding(*binding);
        }
        self
    }

    pub fn with_config_registry_hook(
        mut self,
        hook: impl Fn(
            sinomo_runtime::config::ConfigRegistryBuilder,
        ) -> Result<sinomo_runtime::config::ConfigRegistryBuilder, String>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.config_registry_hooks.push(Box::new(hook));
        self
    }

    pub fn with_storage_layout_hook(
        mut self,
        hook: impl Fn(
            &mut sinomo_runtime::storage::StorageLayout,
        ) -> Result<(), sinomo_runtime::storage::StorageContractError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.storage_layout_hooks.push(Box::new(hook));
        self
    }
}

pub fn default_cli_runtime_config() -> CliRuntimeConfig {
    CliRuntimeConfig {
        command_registry: default_cli_command_registry(),
        config_registry_hooks: Vec::new(),
        storage_layout_hooks: Vec::new(),
    }
}

pub fn install_cli_runtime_config(config: CliRuntimeConfig) -> Result<(), &'static str> {
    CLI_RUNTIME_CONFIG
        .set(config)
        .map_err(|_| "CLI runtime config was already installed")
}

fn cli_runtime_config() -> &'static CliRuntimeConfig {
    CLI_RUNTIME_CONFIG.get_or_init(default_cli_runtime_config)
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct CliCommandOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CliCommandStatus {
    Completed,
    Failed { error: String },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CapturedCliCommand {
    pub output: CliCommandOutput,
    pub status: CliCommandStatus,
}

#[derive(Debug, Default)]
struct CliOutputBuffer {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

enum CliOutputStream {
    Stdout,
    Stderr,
}

pub(crate) fn write_cli_stdout(args: fmt::Arguments<'_>) {
    write_cli_output(CliOutputStream::Stdout, args.to_string());
}

pub(crate) fn write_cli_stderr(args: fmt::Arguments<'_>) {
    write_cli_output(CliOutputStream::Stderr, args.to_string());
}

pub(crate) fn write_cli_stdout_bytes(bytes: &[u8]) -> io::Result<()> {
    write_cli_output_bytes(CliOutputStream::Stdout, bytes)
}

pub(crate) fn write_cli_stderr_bytes(bytes: &[u8]) -> io::Result<()> {
    write_cli_output_bytes(CliOutputStream::Stderr, bytes)
}

fn write_cli_output(stream: CliOutputStream, text: String) {
    let _ = write_cli_output_bytes(stream, text.as_bytes());
}

fn write_cli_output_bytes(stream: CliOutputStream, bytes: &[u8]) -> io::Result<()> {
    let captured = CLI_OUTPUT_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(buffer) = capture.as_mut() else {
            return false;
        };
        match stream {
            CliOutputStream::Stdout => buffer.stdout.extend_from_slice(bytes),
            CliOutputStream::Stderr => buffer.stderr.extend_from_slice(bytes),
        }
        true
    });
    if captured {
        return Ok(());
    }

    match stream {
        CliOutputStream::Stdout => {
            let mut stdout = io::stdout().lock();
            stdout.write_all(bytes)?;
            stdout.flush()
        }
        CliOutputStream::Stderr => {
            let mut stderr = io::stderr().lock();
            stderr.write_all(bytes)?;
            stderr.flush()
        }
    }
}

struct CliStdoutWriter;

impl Write for CliStdoutWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        write_cli_stdout_bytes(bytes)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct CliStderrWriter;

impl Write for CliStderrWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        write_cli_stderr_bytes(bytes)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn execute_cli_command_captured(args: &[String]) -> CapturedCliCommand {
    let execution = run_cli_command_with_capture(args);
    let status = match execution.result {
        Ok(()) => CliCommandStatus::Completed,
        Err(error) => CliCommandStatus::Failed {
            error: error.to_string(),
        },
    };
    CapturedCliCommand {
        output: execution.output,
        status,
    }
}

struct CliCommandCaptureExecution {
    output: CliCommandOutput,
    result: Result<(), CliError>,
}

fn run_cli_command_with_capture(args: &[String]) -> CliCommandCaptureExecution {
    run_cli_closure_with_capture(|| run_cli_command(args))
}

fn run_cli_closure_with_capture(
    f: impl FnOnce() -> Result<(), CliError>,
) -> CliCommandCaptureExecution {
    let previous = CLI_OUTPUT_CAPTURE.with(|capture| {
        capture.replace(Some(CliOutputBuffer {
            stdout: Vec::new(),
            stderr: Vec::new(),
        }))
    });
    let (result, raw_output) = run_with_process_stdio_capture(f);
    let buffer = CLI_OUTPUT_CAPTURE
        .with(|capture| capture.replace(previous))
        .expect("CLI output capture installed");
    let mut stdout = buffer.stdout;
    stdout.extend_from_slice(&raw_output.stdout);
    let mut stderr = buffer.stderr;
    stderr.extend_from_slice(&raw_output.stderr);
    CliCommandCaptureExecution {
        output: CliCommandOutput { stdout, stderr },
        result,
    }
}

fn run_with_process_stdio_capture<T>(f: impl FnOnce() -> T) -> (T, CliOutputBuffer) {
    #[cfg(unix)]
    {
        let _lock = PROCESS_STDIO_CAPTURE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match UnixProcessStdioCapture::start() {
            Ok(capture) => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
                let output = capture.finish().unwrap_or_else(|error| CliOutputBuffer {
                    stdout: Vec::new(),
                    stderr: format!("warning: failed to restore captured stdio: {error}\n")
                        .into_bytes(),
                });
                match result {
                    Ok(result) => (result, output),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
            Err(error) => {
                let result = f();
                (
                    result,
                    CliOutputBuffer {
                        stdout: Vec::new(),
                        stderr: format!(
                            "warning: failed to capture process stdio for embedded command: {error}\n"
                        )
                        .into_bytes(),
                    },
                )
            }
        }
    }
    #[cfg(not(unix))]
    {
        (f(), CliOutputBuffer::default())
    }
}

#[cfg(unix)]
struct UnixProcessStdioCapture {
    saved_stdout: RawFd,
    saved_stderr: RawFd,
    stop_readers: Arc<AtomicBool>,
    stdout_reader: thread::JoinHandle<io::Result<Vec<u8>>>,
    stderr_reader: thread::JoinHandle<io::Result<Vec<u8>>>,
}

#[cfg(unix)]
impl UnixProcessStdioCapture {
    fn start() -> io::Result<Self> {
        let saved_stdout = dup_fd(libc::STDOUT_FILENO)?;
        let saved_stderr = match dup_fd(libc::STDERR_FILENO) {
            Ok(fd) => fd,
            Err(error) => {
                close_fd_quietly(saved_stdout);
                return Err(error);
            }
        };
        let (stdout_read, stdout_write) = match create_pipe() {
            Ok(pipe) => pipe,
            Err(error) => {
                close_fd_quietly(saved_stdout);
                close_fd_quietly(saved_stderr);
                return Err(error);
            }
        };
        let (stderr_read, stderr_write) = match create_pipe() {
            Ok(pipe) => pipe,
            Err(error) => {
                close_fd_quietly(stdout_read);
                close_fd_quietly(stdout_write);
                close_fd_quietly(saved_stdout);
                close_fd_quietly(saved_stderr);
                return Err(error);
            }
        };
        if let Err(error) =
            set_fd_nonblocking(stdout_read).and_then(|()| set_fd_nonblocking(stderr_read))
        {
            close_fd_quietly(stdout_read);
            close_fd_quietly(stdout_write);
            close_fd_quietly(stderr_read);
            close_fd_quietly(stderr_write);
            close_fd_quietly(saved_stdout);
            close_fd_quietly(saved_stderr);
            return Err(error);
        }

        if let Err(error) = dup2_fd(stdout_write, libc::STDOUT_FILENO) {
            close_fd_quietly(stdout_read);
            close_fd_quietly(stdout_write);
            close_fd_quietly(stderr_read);
            close_fd_quietly(stderr_write);
            close_fd_quietly(saved_stdout);
            close_fd_quietly(saved_stderr);
            return Err(error);
        }
        if let Err(error) = dup2_fd(stderr_write, libc::STDERR_FILENO) {
            let _ = dup2_fd(saved_stdout, libc::STDOUT_FILENO);
            close_fd_quietly(stdout_read);
            close_fd_quietly(stdout_write);
            close_fd_quietly(stderr_read);
            close_fd_quietly(stderr_write);
            close_fd_quietly(saved_stdout);
            close_fd_quietly(saved_stderr);
            return Err(error);
        }
        close_fd_quietly(stdout_write);
        close_fd_quietly(stderr_write);

        let stop_readers = Arc::new(AtomicBool::new(false));
        let stdout_reader = spawn_stdio_capture_reader(stdout_read, Arc::clone(&stop_readers));
        let stderr_reader = spawn_stdio_capture_reader(stderr_read, Arc::clone(&stop_readers));

        Ok(Self {
            saved_stdout,
            saved_stderr,
            stop_readers,
            stdout_reader,
            stderr_reader,
        })
    }

    fn finish(self) -> io::Result<CliOutputBuffer> {
        let _ = io::stdout().flush();
        let _ = io::stderr().flush();
        let stdout_restore = dup2_fd(self.saved_stdout, libc::STDOUT_FILENO);
        let stderr_restore = dup2_fd(self.saved_stderr, libc::STDERR_FILENO);
        close_fd_quietly(self.saved_stdout);
        close_fd_quietly(self.saved_stderr);

        self.stop_readers.store(true, Ordering::SeqCst);
        let stdout = join_stdio_capture_reader(self.stdout_reader)?;
        let stderr = join_stdio_capture_reader(self.stderr_reader)?;

        stdout_restore?;
        stderr_restore?;

        Ok(CliOutputBuffer { stdout, stderr })
    }
}

#[cfg(unix)]
fn create_pipe() -> io::Result<(RawFd, RawFd)> {
    let mut fds = [0; 2];
    // SAFETY: `fds` points to two valid `RawFd` slots for libc to initialize.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((fds[0], fds[1]))
}

#[cfg(unix)]
fn dup_fd(fd: RawFd) -> io::Result<RawFd> {
    // SAFETY: `dup` does not take ownership of `fd`; it returns a new descriptor or -1.
    let duplicated = unsafe { libc::dup(fd) };
    if duplicated < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(duplicated)
    }
}

#[cfg(unix)]
fn dup2_fd(from: RawFd, to: RawFd) -> io::Result<()> {
    // SAFETY: `dup2` atomically duplicates `from` onto descriptor number `to`.
    if unsafe { libc::dup2(from, to) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn set_fd_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fcntl` reads and updates flags for a valid file descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fcntl` writes the updated flag bitset back to the same descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn close_fd_quietly(fd: RawFd) {
    // SAFETY: closing an owned descriptor is safe; errors are intentionally ignored here.
    let _ = unsafe { libc::close(fd) };
}

#[cfg(unix)]
fn spawn_stdio_capture_reader(
    fd: RawFd,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<io::Result<Vec<u8>>> {
    thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            // SAFETY: `buffer` is valid for writes and `fd` is owned by this reader thread.
            let read =
                unsafe { libc::read(fd, buffer.as_mut_ptr().cast::<libc::c_void>(), buffer.len()) };
            if read > 0 {
                output.extend_from_slice(&buffer[..read as usize]);
                continue;
            }
            if read == 0 {
                close_fd_quietly(fd);
                return Ok(output);
            }
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(code)
                    if code == libc::EINTR || code == libc::EAGAIN || code == libc::EWOULDBLOCK =>
                {
                    if stop.load(Ordering::SeqCst)
                        && matches!(
                            error.raw_os_error(),
                            Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK
                        )
                    {
                        close_fd_quietly(fd);
                        return Ok(output);
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                _ => {
                    close_fd_quietly(fd);
                    return Err(error);
                }
            }
        }
    })
}

#[cfg(unix)]
fn join_stdio_capture_reader(
    reader: thread::JoinHandle<io::Result<Vec<u8>>>,
) -> io::Result<Vec<u8>> {
    reader.join().unwrap_or_else(|payload| {
        std::panic::resume_unwind(payload);
    })
}

pub(crate) fn cli_command_requires_cluster_state(command: &CliCommand) -> bool {
    match command {
        CliCommand::Dfs(command) => dfs_command_requires_cluster_state(command),
        CliCommand::Registered(command) => command.policy().requires_cluster_state,
        _ => false,
    }
}

pub(crate) fn cli_command_supports_degraded_local_mode(command: &CliCommand) -> bool {
    match command {
        CliCommand::Tui
        | CliCommand::Models(_)
        | CliCommand::Providers(_)
        | CliCommand::Agent(_) => true,
        CliCommand::Dfs(command) => !dfs_command_requires_cluster_state(command),
        CliCommand::Registered(command) => command.policy().supports_degraded_local_mode,
        _ => false,
    }
}

fn cli_command_needs_daemon_probe(command: &CliCommand) -> bool {
    cli_command_requires_cluster_state(command) || cli_command_supports_degraded_local_mode(command)
}

fn cli_command_enters_degraded_local_mode(
    command: &CliCommand,
    daemon_available: bool,
    local_cluster_quorum: Option<bool>,
) -> bool {
    cli_command_supports_degraded_local_mode(command)
        && (!daemon_available || local_cluster_quorum == Some(false))
}

pub(crate) fn cli_invocation_force_local_mode(
    options: CliRuntimeOptions,
    command: &CliCommand,
    daemon_available: bool,
    local_cluster_quorum: Option<bool>,
) -> bool {
    options.force_local
        || cli_command_enters_degraded_local_mode(command, daemon_available, local_cluster_quorum)
}

fn cli_local_cluster_quorum_if_relevant(
    command: &CliCommand,
    daemon: Option<&DaemonStatus>,
) -> Result<Option<bool>, CliError> {
    if daemon.is_none() || !cli_command_supports_degraded_local_mode(command) {
        return Ok(None);
    }
    let paths = cluster_paths_from_defaults()?;
    match read_cluster_state(&paths) {
        Ok(state) => Ok(Some(cluster_has_local_quorum(&paths, &state))),
        Err(CliError::ClusterOrphan) => Ok(None),
        Err(error) => Err(error),
    }
}

fn dfs_command_requires_cluster_state(command: &DfsCliCommand) -> bool {
    match command {
        DfsCliCommand::Status(options) => options.cluster,
        DfsCliCommand::Services { .. } | DfsCliCommand::MetadataPublish { .. } => true,
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct CliRuntimeOptions {
    pub(crate) force_local: bool,
}

#[derive(Debug)]
pub(crate) struct CliInvocation {
    pub(crate) command: CliCommand,
    pub(crate) options: CliRuntimeOptions,
}

struct CliLocalModeGuard {
    previous: bool,
}

impl Drop for CliLocalModeGuard {
    fn drop(&mut self) {
        CLI_FORCE_LOCAL_MODE.with(|mode| mode.set(self.previous));
    }
}

pub(crate) fn cli_force_local_mode() -> bool {
    CLI_FORCE_LOCAL_MODE.with(Cell::get)
}

fn enter_cli_force_local_mode(force_local: bool) -> CliLocalModeGuard {
    let previous = cli_force_local_mode();
    CLI_FORCE_LOCAL_MODE.with(|mode| mode.set(previous || force_local));
    CliLocalModeGuard { previous }
}

#[cfg(test)]
pub(crate) fn with_cli_force_local_mode<T>(force_local: bool, f: impl FnOnce() -> T) -> T {
    let _guard = enter_cli_force_local_mode(force_local);
    f()
}

#[derive(Debug)]
pub(crate) enum CliCommand {
    Tui,
    Help,
    Cluster(ClusterCliCommand),
    InstallHelp,
    Install(InstallOptions),
    Uninstall(UninstallOptions),
    Daemon(DaemonCliCommand),
    Debug(DebugCliCommand),
    Rollout(RolloutCliCommand),
    Dfs(DfsCliCommand),
    Models(ModelsCliCommand),
    Providers(ProviderCliCommand),
    Agent(AgentCliCommand),
    Registered(RegisteredCliCommand),
    Usage(UsageOptions),
}

pub fn default_cli_command_registry() -> CliCommandRegistry {
    let module_config = sinomo_runtime::module::ModuleCliCommandConfig::new()
        .with_toggleable_native_rust_module_name(
            sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID,
            "operational observation module",
        )
        .expect("observe module id is valid")
        .with_wasix(sinomo_runtime::module::ModuleWasixCliConfig {
            module_id: sinomo_runtime::sandbox_wasix_api::SINOMO_SANDBOX_WASIX_MODULE_ID,
            interface_version:
                sinomo_runtime::sandbox_wasix_api::SINOMO_SANDBOX_WASIX_INTERFACE_VERSION,
        });
    CliCommandRegistry::new()
        .register(sinomo_runtime::module::module_cli_command_handler(
            module_config,
        ))
        .register(sinomo_runtime::observe::observe_cli_command_handler())
        .register(sinomo_runtime::sandbox::sandbox_cli_command_handler(
            Arc::new(SinomoCliSandboxHost),
        ))
}

fn cli_command_registry() -> &'static CliCommandRegistry {
    &cli_runtime_config().command_registry
}

pub(crate) fn parse_cli_invocation(args: &[String]) -> Result<CliInvocation, CliError> {
    let mut force_local = false;
    let mut command_start = 0;
    while matches!(args.get(command_start).map(String::as_str), Some("--local")) {
        force_local = true;
        command_start += 1;
    }
    Ok(CliInvocation {
        command: parse_cli_command(&args[command_start..])?,
        options: CliRuntimeOptions { force_local },
    })
}

pub(crate) fn parse_cli_command(args: &[String]) -> Result<CliCommand, CliError> {
    match args.split_first() {
        None => Ok(CliCommand::Tui),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(CliCommand::Help)
        }
        Some((command, rest)) if command == "cluster" => {
            Ok(CliCommand::Cluster(parse_cluster_command(rest)?))
        }
        Some((command, rest)) if command == "install" && is_help_args(rest) => {
            Ok(CliCommand::InstallHelp)
        }
        Some((command, rest)) if command == "install" => {
            Ok(CliCommand::Install(parse_install_options(rest)?))
        }
        Some((command, rest)) if command == "uninstall" && is_help_args(rest) => {
            Ok(CliCommand::InstallHelp)
        }
        Some((command, rest)) if command == "uninstall" => {
            Ok(CliCommand::Uninstall(parse_uninstall_options(rest)?))
        }
        Some((command, rest)) if command == "daemon" => {
            Ok(CliCommand::Daemon(parse_daemon_command(rest)?))
        }
        Some((command, rest)) if command == "debug" => {
            Ok(CliCommand::Debug(parse_debug_command(rest)?))
        }
        Some((command, rest)) if command == "rollout" => {
            Ok(CliCommand::Rollout(parse_rollout_command(rest)?))
        }
        Some((command, rest)) if command == "dfs" => Ok(CliCommand::Dfs(parse_dfs_command(rest)?)),
        Some((command, rest)) if matches!(command.as_str(), "models" | "model") => {
            Ok(CliCommand::Models(parse_models_command(rest)?))
        }
        Some((command, rest)) if command == "providers" => {
            Ok(CliCommand::Providers(parse_provider_command(rest)?))
        }
        Some((command, rest)) if command == "agent" => {
            Ok(CliCommand::Agent(parse_agent_command(rest)?))
        }
        Some((command, rest)) if command == "usage" => {
            Ok(CliCommand::Usage(parse_usage_options(rest)?))
        }
        _ => match cli_command_registry().parse(args)? {
            Some(command) => Ok(CliCommand::Registered(command)),
            None => Err(CliError::Usage),
        },
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum AgentCliCommand {
    Run(AgentRunOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentRunOptions {
    pub prompt: String,
    pub provider: String,
    pub model: String,
    pub account: Option<String>,
}

pub(crate) fn parse_agent_command(args: &[String]) -> Result<AgentCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "run" => {
            Ok(AgentCliCommand::Run(parse_agent_run_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_agent_run_options(args: &[String]) -> Result<AgentRunOptions, CliError> {
    let mut prompt = None;
    let mut provider = CODEX_PROVIDER_ID.to_string();
    let mut model = CODEX_DEFAULT_MODEL.to_string();
    let mut account = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--prompt" => {
                index += 1;
                prompt = args.get(index).cloned();
            }
            "--provider" => {
                index += 1;
                provider = args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--provider requires a value".to_string(),
                })?;
            }
            "--model" => {
                index += 1;
                model = args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--model requires a value".to_string(),
                })?;
            }
            "--account" => {
                index += 1;
                account = Some(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--account requires a value".to_string(),
                })?);
            }
            value if prompt.is_none() => {
                prompt = Some(value.to_string());
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent run argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(AgentRunOptions {
        prompt: prompt.ok_or_else(|| CliError::Parse {
            detail: "agent run requires --prompt <text>".to_string(),
        })?,
        provider,
        model,
        account,
    })
}

struct ConfiguredInteractionModel {
    data_dir: SinomoDataDir,
    provider: String,
    model: String,
    account: Option<sinomo_lm::ProviderAccountConfig>,
    runtime_config: ModelRuntimeConfig,
    managed_pytorch: Mutex<Option<ManagedPytorchServer>>,
}

impl InteractionModel for ConfiguredInteractionModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if self.provider == MINICPM_PROVIDER_ID {
            let model = if self.model == sinomo_lm::PROVIDER_DEFAULT_MODEL {
                MINICPM_DEFAULT_MODEL.to_string()
            } else {
                self.model.clone()
            };
            if let Some(endpoint) = self.runtime_config.pytorch_bridge.endpoint.clone() {
                ensure_local_pytorch_endpoint(&endpoint, &model)?;
                let provider = RemotePytorchRuntime::new(RemotePytorchRuntimeConfig {
                    endpoint,
                    timeout_seconds: self
                        .runtime_config
                        .pytorch_bridge
                        .timeout_seconds
                        .unwrap_or(300),
                    auth_token: None,
                });
                return sinomo_lm::LanguageModelProvider::complete(
                    &provider,
                    &LanguageModelRequest {
                        provider: self.provider.clone(),
                        model,
                        ..request.clone()
                    },
                );
            }
            return self.complete_with_managed_pytorch(request, &model);
        }
        complete_configured_model_request_with_account(
            &self.data_dir,
            &self.provider,
            &self.model,
            self.account.as_ref(),
            None,
            request,
        )
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if self.provider == MINICPM_PROVIDER_ID
            && let Some(endpoint) = self.runtime_config.pytorch_bridge.endpoint.clone()
        {
            let model = if self.model == sinomo_lm::PROVIDER_DEFAULT_MODEL {
                MINICPM_DEFAULT_MODEL.to_string()
            } else {
                self.model.clone()
            };
            ensure_local_pytorch_endpoint(&endpoint, &model)?;
            let provider = RemotePytorchRuntime::new(RemotePytorchRuntimeConfig {
                endpoint,
                timeout_seconds: self
                    .runtime_config
                    .pytorch_bridge
                    .timeout_seconds
                    .unwrap_or(300),
                auth_token: None,
            });
            let response = provider
                .complete_streaming(
                    sinomo_pytorch::PytorchRequest::from_language_model_request(
                        &LanguageModelRequest {
                            provider: self.provider.clone(),
                            model,
                            ..request.clone()
                        },
                    ),
                    on_delta,
                )
                .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                    provider: MINICPM_PROVIDER_ID.to_string(),
                    model: self.model.clone(),
                    detail: source.to_string(),
                })?;
            return Ok(LanguageModelResponse {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: self.model.clone(),
                content: response.content,
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: response.usage,
            });
        }
        if self.provider == MINICPM_PROVIDER_ID {
            let model = if self.model == sinomo_lm::PROVIDER_DEFAULT_MODEL {
                MINICPM_DEFAULT_MODEL.to_string()
            } else {
                self.model.clone()
            };
            return self.complete_streaming_with_managed_pytorch(request, &model, on_delta);
        }
        self.complete(request)
    }
}

impl ConfiguredInteractionModel {
    fn complete_with_managed_pytorch(
        &self,
        request: &LanguageModelRequest,
        model: &str,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let mut server = self.managed_pytorch.lock().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: model.to_string(),
                detail: format!("managed PyTorch runtime lock failed: {source}"),
            }
        })?;
        let server = server.get_or_insert_with(|| self.managed_pytorch_server(model));
        let response = server
            .complete(sinomo_pytorch::PytorchRequest::from_language_model_request(
                &LanguageModelRequest {
                    provider: self.provider.clone(),
                    model: model.to_string(),
                    ..request.clone()
                },
            ))
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: model.to_string(),
                detail: source.to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: MINICPM_PROVIDER_ID.to_string(),
            model: model.to_string(),
            content: response.content,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: response.usage,
        })
    }

    fn complete_streaming_with_managed_pytorch(
        &self,
        request: &LanguageModelRequest,
        model: &str,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let mut server = self.managed_pytorch.lock().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: model.to_string(),
                detail: format!("managed PyTorch runtime lock failed: {source}"),
            }
        })?;
        let server = server.get_or_insert_with(|| self.managed_pytorch_server(model));
        let response = server
            .complete_streaming(
                sinomo_pytorch::PytorchRequest::from_language_model_request(
                    &LanguageModelRequest {
                        provider: self.provider.clone(),
                        model: model.to_string(),
                        ..request.clone()
                    },
                ),
                on_delta,
            )
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: model.to_string(),
                detail: source.to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: MINICPM_PROVIDER_ID.to_string(),
            model: model.to_string(),
            content: response.content,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: response.usage,
        })
    }

    fn managed_pytorch_server(&self, model: &str) -> ManagedPytorchServer {
        ManagedPytorchServer::new(ManagedPytorchServerConfig {
            python: self
                .runtime_config
                .pytorch_bridge
                .python
                .as_deref()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| default_managed_pytorch_python(&self.data_dir)),
            model: model.to_string(),
            device: match self.runtime_config.pytorch_bridge.device.as_deref() {
                Some("cpu") => PytorchDevice::Cpu,
                Some("cuda") => PytorchDevice::Cuda,
                _ => PytorchDevice::Auto,
            },
            mode: PytorchMode::Vision,
            timeout_seconds: self
                .runtime_config
                .pytorch_bridge
                .timeout_seconds
                .unwrap_or(300),
            log_dir: self.data_dir.root().join("logs").join("pytorch"),
            mock: self.runtime_config.pytorch_bridge.mock.unwrap_or(false),
            ..ManagedPytorchServerConfig::default()
        })
    }
}

fn default_managed_pytorch_python(data_dir: &SinomoDataDir) -> PathBuf {
    let root = data_dir.root();
    #[cfg(windows)]
    {
        let venv_python = root.join("minicpm-venv").join("Scripts").join("python.exe");
        if venv_python.exists() {
            return venv_python;
        }
        PathBuf::from("python.exe")
    }
    #[cfg(not(windows))]
    {
        let venv_python = root.join("minicpm-venv").join("bin").join("python");
        if venv_python.exists() {
            return venv_python;
        }
        PathBuf::from("python3")
    }
}

fn ensure_local_pytorch_endpoint(endpoint: &str, model: &str) -> Result<(), LanguageModelError> {
    if is_loopback_http_endpoint(endpoint).unwrap_or(false) {
        return Ok(());
    }
    Err(LanguageModelError::ProviderInvocationFailed {
        provider: MINICPM_PROVIDER_ID.to_string(),
        model: model.to_string(),
        detail: format!(
            "configured PyTorch endpoint `{endpoint}` is not local loopback; cluster model traffic must go through sinomo-network, not direct HTTP"
        ),
    })
}

pub(crate) fn run_agent_command(command: AgentCliCommand) -> Result<(), CliError> {
    match command {
        AgentCliCommand::Run(options) => run_agent_once(options),
    }
}

pub(crate) fn run_agent_once(options: AgentRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let account = if let Some(account_name) = options.account.as_deref() {
        let config = load_model_config(&ModelPaths::new(&data_dir))?;
        provider_account_config(&config, &options.provider, Some(account_name)).cloned()
    } else {
        None
    };
    let session = InteractionSession::new(
        SessionId::generate(),
        NodeId::from_legacy_named("node_agent_cli").expect("legacy node id"),
        InteractionSurface::new("cli.agent", InteractionSurfaceKind::CommandLine),
    );
    let mut store = InMemoryEventStore::new();
    let input_event = user_interaction_input_event(&session, &options.prompt);
    store.append(input_event.clone())?;
    let context = BoundedContextBuilder::new(12).build(&store, session.session_id.clone())?;
    let input = AgentLoopInput {
        session,
        latest_event: input_event,
        context,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    };
    let provider = options.provider.clone();
    let model = options.model.clone();
    let (runtime_config, _) = load_model_runtime_config_for_node(&data_dir)?;
    let tool_registry = main_agent_config_tool_registry(&data_dir)?;
    let loop_model = ConfiguredInteractionModel {
        data_dir: data_dir.clone(),
        provider: provider.clone(),
        model: model.clone(),
        account,
        runtime_config,
        managed_pytorch: Mutex::new(None),
    };
    let agent = Agent::new(loop_model, provider, model)
        .with_agent_definition(AgentDefinition::main())
        .with_tool_registry(tool_registry)
        .with_policy(sinomo_agent::AgentPolicyBundle::default_for_role(
            AgentRole::Main,
        ));
    let runtime = AgentSessionRuntime::new(
        agent,
        AgentSession::new(input.session.session_id.clone(), AgentRole::Main),
    );
    let op = AgentOp::start_turn(input);
    let op_id = op.op_id.clone();
    let submit = runtime.submit_op(op)?;
    if !submit.accepted {
        let reason = submit
            .events
            .iter()
            .find_map(|event| match &event.payload {
                AgentEventPayload::OperationRejected { reason, .. } => Some(reason.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "main agent turn was rejected".to_string());
        return Err(CliError::Parse { detail: reason });
    }
    runtime.join_active_turn()?;
    let decision = runtime
        .turn_result(&op_id)?
        .and_then(|outcome| outcome.run)
        .map(|run| run.final_decision)
        .ok_or_else(|| CliError::Parse {
            detail: format!("main agent runtime turn `{op_id}` completed without a decision"),
        })?;
    if decision.text.trim().is_empty() {
        println!("{}", agent_decision_json(&decision)?);
    } else {
        println!("{}", decision.text);
        if !decision.tool_calls.is_empty() || decision.background_handoff.is_some() {
            println!("{}", agent_decision_json(&decision)?);
        }
    }
    Ok(())
}

pub(crate) fn main_agent_config_tool_registry(
    data_dir: &SinomoDataDir,
) -> Result<sinomo_agent::AgentToolRegistry, CliError> {
    let store = main_agent_config_store()?;
    Ok(sinomo_agent::AgentToolRegistry::config_management(
        ConfigAgentToolConfig::new(data_dir.clone(), store),
    ))
}

pub(crate) fn main_agent_config_store() -> Result<sinomo_runtime::config::ConfigStore, CliError> {
    let builder = sinomo_runtime::config::ConfigRegistry::builder();
    let builder = sinomo_runtime::dfs::register_dfs_config_definitions(builder)?;
    let builder =
        sinomo_runtime::rollout::register_config_definitions(builder).map_err(CliError::from)?;
    let builder = sinomo_runtime::sandbox::register_config_definitions(builder)?;
    let builder =
        sinomo_lm::register_config_definitions(builder).map_err(CliError::LanguageModel)?;
    let mut builder = builder;
    for hook in &cli_runtime_config().config_registry_hooks {
        builder = hook(builder).map_err(|detail| CliError::Parse { detail })?;
    }
    Ok(sinomo_runtime::config::ConfigStore::new(builder.build()))
}

fn agent_decision_json(decision: &sinomo_agent::AgentLoopDecision) -> Result<String, CliError> {
    serde_json::to_string_pretty(decision).map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })
}

pub(crate) fn is_help_args(args: &[String]) -> bool {
    matches!(args, [arg] if matches!(arg.as_str(), "help" | "--help" | "-h"))
}

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    run_parsed_cli_invocation(parse_cli_invocation(&args)?)
}

fn run_cli_command(args: &[String]) -> Result<(), CliError> {
    run_parsed_cli_invocation(parse_cli_invocation(args)?)
}

fn run_parsed_cli_invocation(invocation: CliInvocation) -> Result<(), CliError> {
    let command = invocation.command;
    let daemon = if invocation.options.force_local || !cli_command_needs_daemon_probe(&command) {
        None
    } else {
        default_main_daemon_status()?
    };
    let local_cluster_quorum = if invocation.options.force_local {
        None
    } else {
        cli_local_cluster_quorum_if_relevant(&command, daemon.as_ref())?
    };
    let force_local = cli_invocation_force_local_mode(
        invocation.options,
        &command,
        daemon.is_some(),
        local_cluster_quorum,
    );
    let _guard = enter_cli_force_local_mode(force_local);

    if !cli_force_local_mode() && cli_command_requires_cluster_state(&command) {
        let paths = cluster_paths_from_defaults()?;
        require_cluster_runtime_quorum_with_paths_and_daemon(&paths, daemon.as_ref())?;
    }

    match command {
        CliCommand::Tui => run_tui(),
        CliCommand::Help => {
            print_usage();
            Ok(())
        }
        CliCommand::Cluster(command) => run_cluster_cli_command(command),
        CliCommand::InstallHelp => {
            print_install_help();
            Ok(())
        }
        CliCommand::Install(options) => install(options),
        CliCommand::Uninstall(options) => uninstall(options),
        CliCommand::Daemon(command) => run_daemon_cli_command(command),
        CliCommand::Debug(command) => run_debug_cli_command(command),
        CliCommand::Rollout(command) => run_rollout_cli_command(command),
        CliCommand::Dfs(command) => run_dfs_cli_command(command),
        CliCommand::Models(command) => run_models_command(command),
        CliCommand::Providers(command) => run_provider_command(command),
        CliCommand::Agent(command) => run_agent_command(command),
        CliCommand::Registered(command) => {
            let command_name = command.name().to_string();
            let mut stdout = CliStdoutWriter;
            let mut stderr = CliStderrWriter;
            let mut context =
                CliCommandContext::new(&mut stdout, &mut stderr).with_force_local(force_local);
            command
                .run(&mut context)
                .map_err(|error| CliError::ExternalCommand {
                    command: command_name,
                    detail: error.to_string(),
                })
        }
        CliCommand::Usage(options) => usage(options),
    }
}

const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
const DAEMON_ROLLOUT_TIMEOUT: Duration = Duration::from_secs(60);
const DAEMON_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const DAEMON_CLUSTER_KV_TXN_TIMEOUT: Duration = Duration::from_secs(15);
const IROH_DAEMON_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const CLUSTER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const CLUSTER_HEALTH_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
const CLUSTER_HEALTH_PEER_ROWS_TIMEOUT: Duration = Duration::from_secs(8);
#[cfg(test)]
const SINOMO_JOIN_URI_PREFIX: &str = network::SINOMO_JOIN_URI_PREFIX;
const CLUSTER_INVITE_DEFAULT_TTL_SECONDS: i64 = 24 * 60 * 60;
const INSTALL_DEFAULT_HOST: &str = "0.0.0.0";
