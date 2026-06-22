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
    AGENT_RPC_CATEGORY, AGENT_SESSION_FOLLOW_UP_ROUTE, AGENT_SESSION_INSPECT_ROUTE,
    AGENT_SESSION_INTERRUPT_ROUTE, AGENT_SESSION_PROGRESS_ROUTE, AGENT_SESSION_RESUME_ROUTE,
    AGENT_SESSION_STEER_ROUTE, AGENT_SESSION_TERMINAL_RESULTS_ROUTE, Agent, AgentCatalog,
    AgentDefinition, AgentDefinitionStore, AgentEventPayload, AgentLoopDecision, AgentLoopInput,
    AgentModelEvent, AgentOp, AgentPackageProvenance, AgentPackageSource,
    AgentReadinessCheckStatus, AgentReadinessReport, AgentRole, AgentRuntimeEvent,
    AgentRuntimeEventCursor, AgentRuntimeEventPayload, AgentSession,
    AgentSessionControlEventCursor, AgentSessionControlResponse, AgentSessionFollowUpRequest,
    AgentSessionInspectRequest, AgentSessionInspectResponse, AgentSessionInterruptRequest,
    AgentSessionProgressRequest, AgentSessionProgressResponse, AgentSessionResumeRequest,
    AgentSessionResumeResponse, AgentSessionRuntime, AgentSessionSteerRequest,
    AgentSessionTerminalResultsRequest, AgentSessionTerminalResultsResponse,
    AgentTaskTerminalResult, AgentToolProgressEvent, ConfigAgentToolConfig, EventEnvelopeExt,
    FileAgentDefinitionStore, InteractionModel, InteractionOutput, InteractionSession,
    InteractionSurface, InteractionSurfaceKind, TaskId, interaction_event,
    run_local_agent_readiness_gate, user_interaction_input_event,
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
    save_provider_api_key, save_provider_api_key_in_model_config, stored_provider_api_key,
    subscription_usage_provider_ids, test_provider_account_credentials, test_provider_credentials,
    write_default_model_config,
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
    ClusterMemberRole, ClusterMemberState, ClusterMigrationMode, ClusterMigrationPhase,
    ClusterMigrationRecord, ClusterNodeAvailability, ClusterNodeRole, ClusterNodeRolloutStatus,
    ClusterNodeStatusRecord, ClusterTransportKind, EvaluationScorecard, EventEnvelope, Id,
    RollbackTrigger, cluster_migration_key,
};
#[cfg(test)]
use sinomo_runtime::daemon::{
    AuthenticatedDaemonCommand, DaemonError, DaemonSignerAuthority, SignedDaemonCommand,
    authenticate_daemon_control_request, authorize_authenticated_daemon_command,
    daemon_package_version, sign_daemon_rpc_request_from_data_dir, write_daemon_status,
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
    fetch_daemon_status_from_address as daemon_fetch_daemon_status_from_address,
    fetch_daemon_status_from_endpoint as daemon_fetch_daemon_status_from_endpoint,
    git_is_available as daemon_git_is_available,
    handle_deferred_shadow_rollout_completion as daemon_handle_deferred_shadow_rollout_completion,
    install_managed_source_repo as daemon_install_managed_source_repo,
    installed_daemon_paths as daemon_installed_daemon_paths, invite_introducers_from_daemon_status,
    managed_source_git_status_lines as daemon_managed_source_git_status_lines,
    prepare_installed_daemon_install_plan as daemon_prepare_installed_daemon_install_plan,
    prepare_local_rollout_main_activation as daemon_prepare_local_rollout_main_activation,
    probe_daemon_status, remove_boot_entry as daemon_remove_boot_entry,
    run_daemon_control_loop_with_services as daemon_run_daemon_control_loop_with_services,
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
    send_daemon_rollout_native_dylib_module as daemon_send_daemon_rollout_native_dylib_module,
    send_daemon_rollout_native_dylib_module_to_endpoint as daemon_send_daemon_rollout_native_dylib_module_to_endpoint,
    send_daemon_rpc as daemon_send_daemon_rpc,
    send_daemon_rpc_to_endpoint as daemon_send_daemon_rpc_to_endpoint,
    send_daemon_rpc_with_timeouts as daemon_send_daemon_rpc_with_timeouts,
    send_daemon_shutdown_request,
    send_dfs_object_rpc_to_target as daemon_send_object_rpc_to_target,
    shadow_main_binary_from_env as daemon_shadow_main_binary_from_env,
    shadow_main_options_from_env as daemon_shadow_main_options_from_env,
    sign_daemon_rpc_request_for_local_node as daemon_sign_daemon_rpc_request_for_local_node,
    sleep_for_shadow_start_delay as daemon_sleep_for_shadow_start_delay,
    stage_rollout_main_activation as daemon_stage_rollout_main_activation,
    start_daemon_command_process as daemon_start_daemon_command_process,
    wait_for_local_shadow_rollout_completion as daemon_wait_for_local_shadow_rollout_completion,
    wait_for_main_daemon_health as daemon_wait_for_main_daemon_health,
    write_install_boot_entries as daemon_write_install_boot_entries,
};
#[cfg(test)]
use sinomo_runtime::daemon::{
    DaemonLock, handle_daemon_rollout_build_request as daemon_handle_daemon_rollout_build_request,
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
    handle_daemon_sandbox_http_fetch_to_file as daemon_handle_daemon_sandbox_http_fetch_to_file,
    handle_daemon_sandbox_import_busybox as daemon_handle_daemon_sandbox_import_busybox,
    handle_daemon_sandbox_read as daemon_handle_daemon_sandbox_read,
    handle_daemon_sandbox_toolchain_add_artifact as daemon_handle_daemon_sandbox_toolchain_add_artifact,
    handle_daemon_sandbox_toolchain_add_command as daemon_handle_daemon_sandbox_toolchain_add_command,
    handle_daemon_sandbox_toolchain_import_package as daemon_handle_daemon_sandbox_toolchain_import_package,
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
    dfs_import_metadata_root, dfs_import_namespace_root, ensure_default_dfs_expose_config,
    fallback_dfs_node_id, list_local_dfs_pins, load_dfs_expose_config, metadata_shards_prefix,
    open_local_sharded_metadata_backend, open_root_local_metadata_backend, remove_dfs_expose_entry,
    remove_local_dfs_pin, resolve_dfs_metadata_root, resolve_dfs_pin_root, sifs_file_system_id,
};
use sinomo_runtime::events::{EventId, JsonlEventStore, SessionId};
use sinomo_runtime::events::{EventStore, InMemoryEventStore};
use sinomo_runtime::kv::{InProcessKvStore, KvError, KvOp, KvSnapshot, KvTxn, KvTxnResponse};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::module::ModuleReplayReport;
#[cfg(test)]
use sinomo_runtime::network::ClusterJoinRegistrationRequest;
#[cfg(test)]
use sinomo_runtime::network::ClusterJoinRegistrationResponse;
use sinomo_runtime::network::{self, ClusterPaths, cluster_doctor_findings};
use sinomo_runtime::network::{ClusterDoctorFinding, ClusterHealthRow};
#[cfg(test)]
use sinomo_runtime::network::{ClusterInviteChallengeProof, ClusterInviteChallengeRequest};
#[cfg(test)]
use sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE;
use sinomo_runtime::node::{
    ClusterId, ClusterInviteId, NodeId, cluster_member_expected_daemon_process,
    cluster_member_is_local_process, load_or_create_main_node_identity,
    load_or_create_shadow_node_identity, shadow_daemon_node_id,
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
    ClusterRolloutNativeDylibModuleRequest, ClusterRolloutNativeDylibModuleUploadSpec,
    ClusterRolloutUploadArtifactSpec, DEFAULT_SHADOW_TELEMETRY_LIMIT,
    LocalRolloutNativeDylibModuleArtifact, ManagedSourceDeployOptions, ManagedSourceStatus,
    RolloutActivationStrategy, RolloutController, RolloutError, RolloutMainActivation,
    RolloutObservation, RolloutRuntimeState, RolloutTelemetryGateConfig, SelfRolloutPaths,
    clear_rollout_status, ensure_rollout_layout, ensure_rollout_source_repo_with_options,
    install_current_executable,
    install_local_rollout_native_dylib_modules as rollout_install_local_rollout_native_dylib_modules,
    managed_source_status, prepare_cluster_rollout_binary_request,
    prepare_cluster_rollout_build_request, prepare_cluster_rollout_native_dylib_module_request,
    prepare_local_rollout_artifacts_with_options, prepare_rollout_dev_artifacts,
    prepare_rollout_source_bundle, read_rollout_runtime_state, read_rollout_telemetry_gate_config,
    rollout_build_dev_native_dylib_module, rollout_source_head, rollout_status_view,
    shadow_telemetry_scorecard_from_event_store, source_heads_match,
    sync_external_repo_from_managed, unique_rollout_targets, write_rollout_telemetry_gate_config,
};
use sinomo_runtime::rollout::{
    SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV, SHADOW_MAIN_PORT_ENV,
    cluster_rollout_should_use_remote_build as daemon_cluster_rollout_should_use_remote_build,
};
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::env;
use std::fmt;
use std::fs;
use std::io::{self, BufRead, Read, Write};
#[cfg(unix)]
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::str::FromStr;
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
mod voice;

/// GUI-facing surface over the main-agent chat runtime (used by `sinomo-app`).
pub mod app_api;

use error::CliError;
use help::*;
pub use providers::*;
use sandbox_host::SinomoCliSandboxHost;
use time::format_description::well_known::Rfc3339;
pub use tui::*;
pub(crate) use voice::*;

struct CliClusterStateAuthorityReachabilityHost;

impl network::ClusterStateAuthorityReachabilityHost for CliClusterStateAuthorityReachabilityHost {
    fn probe_cluster_state_authority(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> bool {
        daemon_fetch_daemon_status_from_endpoint(endpoint, timeout).is_ok()
    }
}

fn cli_cluster_state_authority_reachability_options()
-> network::ClusterStateAuthorityReachabilityOptions {
    network::ClusterStateAuthorityReachabilityOptions {
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
        cli_cluster_state_authority_reachability_options(),
        &mut CliClusterStateAuthorityReachabilityHost,
    )
}

fn cluster_reachable_state_authority_count(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> usize {
    network::cluster_reachable_state_authority_count_with_host(
        paths,
        state,
        cli_cluster_state_authority_reachability_options(),
        &mut CliClusterStateAuthorityReachabilityHost,
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
    if cluster_member_is_local_process(state, member)
        && let Some(status) = cli_probe_local_member_daemon_status(paths, state, member)
    {
        return Some(status);
    }
    for endpoint in network::ClusterEndpointSelector::new(paths, state)
        .select(network::ClusterEndpointSelectionPurpose::MemberDaemonStatus)
        .into_iter()
        .filter(|endpoint| endpoint.node_id == member.node_id)
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
        let status = cluster_node_status_from_daemon_status(
            &status,
            cluster,
            None,
            OffsetDateTime::now_utc(),
        );
        if cluster_member_expected_daemon_process(member)
            .is_some_and(|process| status.process != process)
        {
            continue;
        }
        return Some(status);
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
    let status =
        cluster_node_status_from_daemon_status(&status, cluster, None, OffsetDateTime::now_utc());
    if cluster_member_expected_daemon_process(member)
        .is_some_and(|process| status.process != process)
    {
        return None;
    }
    Some(status)
}

fn cli_fetch_local_cluster_health_rows_from_member(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<Vec<ClusterHealthRow>> {
    for endpoint in network::ClusterEndpointSelector::new(paths, state)
        .select(network::ClusterEndpointSelectionPurpose::MemberDaemonStatus)
        .into_iter()
        .filter(|endpoint| endpoint.node_id == member.node_id)
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

pub type CliTestRegistryHook = Box<
    dyn Fn(
            sinomo_runtime::test::SinomoTestRegistryBuilder,
        ) -> sinomo_runtime::test::SinomoTestRegistryBuilder
        + Send
        + Sync,
>;

pub struct CliRuntimeConfig {
    command_registry: CliCommandRegistry,
    config_registry_hooks: Vec<CliConfigRegistryHook>,
    storage_layout_hooks: Vec<CliStorageLayoutHook>,
    test_registry_hooks: Vec<CliTestRegistryHook>,
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

    pub fn with_test_cases(
        mut self,
        hook: impl Fn(
            sinomo_runtime::test::SinomoTestRegistryBuilder,
        ) -> sinomo_runtime::test::SinomoTestRegistryBuilder
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.test_registry_hooks.push(Box::new(hook));
        self
    }
}

pub fn default_cli_runtime_config() -> CliRuntimeConfig {
    CliRuntimeConfig {
        command_registry: default_cli_command_registry(),
        config_registry_hooks: Vec::new(),
        storage_layout_hooks: Vec::new(),
        test_registry_hooks: Vec::new(),
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
    run_cli_closure_with_cli_output_capture(f, false)
}

#[cfg(test)]
fn run_cli_closure_with_process_stdio_capture(
    f: impl FnOnce() -> Result<(), CliError>,
) -> CliCommandCaptureExecution {
    run_cli_closure_with_cli_output_capture(f, true)
}

fn run_cli_closure_with_cli_output_capture(
    f: impl FnOnce() -> Result<(), CliError>,
    capture_process_stdio: bool,
) -> CliCommandCaptureExecution {
    let previous = CLI_OUTPUT_CAPTURE.with(|capture| {
        capture.replace(Some(CliOutputBuffer {
            stdout: Vec::new(),
            stderr: Vec::new(),
        }))
    });
    let (result, raw_output) = if capture_process_stdio {
        run_with_process_stdio_capture(f)
    } else {
        (f(), CliOutputBuffer::default())
    };
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
        CliCommand::Test(command) => test_command_requires_cluster_state(command),
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
        CliCommand::Test(command) => !test_command_requires_cluster_state(command),
        CliCommand::Dfs(command) => !dfs_command_requires_cluster_state(command),
        CliCommand::Registered(command) => command.policy().supports_degraded_local_mode,
        _ => false,
    }
}

pub(crate) fn cli_command_is_help(command: &CliCommand) -> bool {
    match command {
        CliCommand::Help | CliCommand::InstallHelp => true,
        CliCommand::Cluster(ClusterCliCommand::Help)
        | CliCommand::Daemon(DaemonCliCommand::Help)
        | CliCommand::Debug(DebugCliCommand::Help)
        | CliCommand::Rollout(RolloutCliCommand::Help)
        | CliCommand::Dfs(DfsCliCommand::Help)
        | CliCommand::Models(ModelsCliCommand::Help)
        | CliCommand::Providers(ProviderCliCommand::Help)
        | CliCommand::Voice(VoiceCliCommand::Help)
        | CliCommand::Test(TestCliCommand::Help)
        | CliCommand::Agent(AgentCliCommand::Help)
        | CliCommand::Agent(AgentCliCommand::Session(AgentSessionCliCommand::Help)) => true,
        CliCommand::Registered(command) => command.is_help(),
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
        Ok(state) => Ok(Some(
            daemon.is_some_and(|daemon| local_daemon_can_serve_cluster_kv_snapshot(daemon, &state))
                || cluster_has_local_quorum(&paths, &state),
        )),
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
    Voice(VoiceCliCommand),
    Test(TestCliCommand),
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
    let module_config = SinomoDataDir::from_env_or_home()
        .ok()
        .and_then(|data_dir| {
            module_config
                .clone()
                .with_installed_native_dylib_modules_from_data_dir(&data_dir.root())
                .ok()
        })
        .unwrap_or(module_config);
    CliCommandRegistry::new()
        .register(sinomo_runtime::module::module_cli_command_handler(
            module_config,
        ))
        .register(
            sinomo_runtime::observe::observe_cli_command_handler_with_cluster_rpc(
                observe_cli_rpc,
                observe_cli_cluster_targets,
            ),
        )
        .register(sinomo_runtime::sandbox::sandbox_cli_command_handler(
            Arc::new(SinomoCliSandboxHost),
        ))
}

fn cli_command_registry() -> &'static CliCommandRegistry {
    &cli_runtime_config().command_registry
}

fn observe_cli_rpc(
    target_node_id: Option<&str>,
    request: network::ObjectRpcRequest,
) -> Result<network::ObjectRpcResponse, String> {
    const OBSERVE_RPC_TIMEOUT: Duration = Duration::from_secs(5);
    if let Some(target_node_id) = target_node_id {
        let target_node_id = NodeId::new(target_node_id.to_string()).map_err(|source| {
            format!("invalid observe target node id `{target_node_id}`: {source}")
        })?;
        return daemon_send_object_rpc_to_target(
            &target_node_id,
            &request,
            OBSERVE_RPC_TIMEOUT,
            true,
        )
        .map_err(|source| source.to_string());
    }
    let status = daemon_default_main_daemon_status(OBSERVE_RPC_TIMEOUT)
        .map_err(|source| source.to_string())?
        .ok_or_else(|| "observe live queries require the local daemon to be running".to_string())?;
    daemon_send_daemon_rpc(&status.address, &request, OBSERVE_RPC_TIMEOUT)
        .map_err(|source| source.to_string())
}

fn observe_cli_cluster_targets() -> Result<Vec<String>, String> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(|source| source.to_string())?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = network::read_cluster_state_without_kv(&cluster_paths)
        .map_err(|source| source.to_string())?;
    let mut targets = BTreeSet::from([state.local_node_id.to_string()]);
    targets.extend(
        state
            .members
            .iter()
            .filter(|member| member.state.is_authorized())
            .map(|member| member.node_id.to_string()),
    );
    Ok(targets.into_iter().collect())
}

pub(crate) fn parse_cli_invocation(args: &[String]) -> Result<CliInvocation, CliError> {
    if args.iter().any(|arg| arg == "--local") {
        return Err(CliError::Parse {
            detail:
                "`--local` has been removed; start the main daemon and use cluster-backed commands"
                    .to_string(),
        });
    }
    Ok(CliInvocation {
        command: parse_cli_command(args)?,
        options: CliRuntimeOptions { force_local: false },
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
        Some((command, rest)) if command == "voice" => {
            Ok(CliCommand::Voice(parse_voice_command(rest)?))
        }
        Some((command, rest)) if command == "test" => {
            Ok(CliCommand::Test(parse_test_command(rest)?))
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
    Help,
    Export(AgentExportOptions),
    Import(AgentImportOptions),
    Inspect(AgentInspectOptions),
    Readiness(AgentReadinessOptions),
    Run(AgentRunOptions),
    Session(AgentSessionCliCommand),
    Validate(AgentValidateOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum AgentSessionCliCommand {
    Help,
    Inspect(AgentSessionInspectOptions),
    Progress(AgentSessionProgressOptions),
    TerminalResults(AgentSessionTerminalResultsOptions),
    Steer(AgentSessionMessageOptions),
    FollowUp(AgentSessionMessageOptions),
    Interrupt(AgentSessionMessageOptions),
    Resume(AgentSessionResumeOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentInspectOptions {
    pub agent: String,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentExportOptions {
    pub agent: String,
    pub output: Option<PathBuf>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentImportOptions {
    pub path: PathBuf,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentValidateOptions {
    pub agent: String,
    pub file: Option<PathBuf>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentReadinessOptions {
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentRunOptions {
    pub prompt: String,
    pub provider: String,
    pub model: String,
    pub account: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentSessionInspectOptions {
    pub session_id: SessionId,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentSessionProgressOptions {
    pub session_id: SessionId,
    pub cursor: Option<AgentSessionControlEventCursor>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentSessionTerminalResultsOptions {
    pub session_id: SessionId,
    pub task_id: Option<TaskId>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentSessionMessageOptions {
    pub session_id: SessionId,
    pub message: String,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AgentSessionResumeOptions {
    pub session_id: SessionId,
    pub json: bool,
}

pub(crate) fn parse_agent_command(args: &[String]) -> Result<AgentCliCommand, CliError> {
    match args.split_first() {
        None => Ok(AgentCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(AgentCliCommand::Help)
        }
        Some((command, rest)) if command == "export" => {
            Ok(AgentCliCommand::Export(parse_agent_export_options(rest)?))
        }
        Some((command, rest)) if command == "import" => {
            Ok(AgentCliCommand::Import(parse_agent_import_options(rest)?))
        }
        Some((command, rest)) if command == "inspect" => {
            Ok(AgentCliCommand::Inspect(parse_agent_inspect_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "readiness" | "ready") => Ok(
            AgentCliCommand::Readiness(parse_agent_readiness_options(rest)?),
        ),
        Some((command, rest)) if command == "run" => {
            Ok(AgentCliCommand::Run(parse_agent_run_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "session" | "sessions") => {
            Ok(AgentCliCommand::Session(parse_agent_session_command(rest)?))
        }
        Some((command, rest)) if command == "validate" => Ok(AgentCliCommand::Validate(
            parse_agent_validate_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_agent_session_command(
    args: &[String],
) -> Result<AgentSessionCliCommand, CliError> {
    match args.split_first() {
        None => Ok(AgentSessionCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(AgentSessionCliCommand::Help)
        }
        Some((command, rest)) if command == "inspect" => Ok(AgentSessionCliCommand::Inspect(
            parse_agent_session_inspect_options(rest)?,
        )),
        Some((command, rest)) if command == "progress" => Ok(AgentSessionCliCommand::Progress(
            parse_agent_session_progress_options(rest)?,
        )),
        Some((command, rest)) if matches!(command.as_str(), "terminal-results" | "results") => {
            Ok(AgentSessionCliCommand::TerminalResults(
                parse_agent_session_terminal_results_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "steer" => Ok(AgentSessionCliCommand::Steer(
            parse_agent_session_message_options(rest, "steer")?,
        )),
        Some((command, rest)) if matches!(command.as_str(), "follow-up" | "followup") => {
            Ok(AgentSessionCliCommand::FollowUp(
                parse_agent_session_message_options(rest, "follow-up")?,
            ))
        }
        Some((command, rest)) if command == "interrupt" => Ok(AgentSessionCliCommand::Interrupt(
            parse_agent_session_message_options(rest, "interrupt")?,
        )),
        Some((command, rest)) if command == "resume" => Ok(AgentSessionCliCommand::Resume(
            parse_agent_session_resume_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_agent_inspect_options(
    args: &[String],
) -> Result<AgentInspectOptions, CliError> {
    let mut agent = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => {
                json = true;
            }
            "--agent" => {
                index += 1;
                agent = Some(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--agent requires a value".to_string(),
                })?);
            }
            value if agent.is_none() => {
                agent = Some(value.to_string());
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent inspect argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(AgentInspectOptions {
        agent: agent.unwrap_or_else(|| "main-agent".to_string()),
        json,
    })
}

pub(crate) fn parse_agent_session_inspect_options(
    args: &[String],
) -> Result<AgentSessionInspectOptions, CliError> {
    let mut session_id = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--session" | "--session-id" => {
                index += 1;
                session_id = Some(parse_agent_session_id_arg(args.get(index), "--session")?);
            }
            value if session_id.is_none() => {
                session_id = Some(parse_agent_session_id(value)?);
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent session inspect argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(AgentSessionInspectOptions {
        session_id: session_id.ok_or_else(|| CliError::Parse {
            detail: "agent session inspect requires a session id".to_string(),
        })?,
        json,
    })
}

pub(crate) fn parse_agent_session_progress_options(
    args: &[String],
) -> Result<AgentSessionProgressOptions, CliError> {
    let mut session_id = None;
    let mut offset = None;
    let mut last_event_id = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--session" | "--session-id" => {
                index += 1;
                session_id = Some(parse_agent_session_id_arg(args.get(index), "--session")?);
            }
            "--cursor" | "--offset" => {
                index += 1;
                offset = Some(parse_usize_arg(args.get(index), "--cursor")?);
            }
            "--last-event" | "--last-event-id" => {
                index += 1;
                last_event_id = Some(parse_agent_task_id_arg(args.get(index), "--last-event")?);
            }
            value if session_id.is_none() => {
                session_id = Some(parse_agent_session_id(value)?);
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent session progress argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(AgentSessionProgressOptions {
        session_id: session_id.ok_or_else(|| CliError::Parse {
            detail: "agent session progress requires a session id".to_string(),
        })?,
        cursor: if offset.is_some() || last_event_id.is_some() {
            Some(AgentSessionControlEventCursor {
                offset: offset.unwrap_or_default(),
                last_event_id,
            })
        } else {
            None
        },
        json,
    })
}

pub(crate) fn parse_agent_session_terminal_results_options(
    args: &[String],
) -> Result<AgentSessionTerminalResultsOptions, CliError> {
    let mut session_id = None;
    let mut task_id = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--session" | "--session-id" => {
                index += 1;
                session_id = Some(parse_agent_session_id_arg(args.get(index), "--session")?);
            }
            "--task" | "--task-id" => {
                index += 1;
                task_id = Some(parse_agent_task_id_arg(args.get(index), "--task")?);
            }
            value if session_id.is_none() => {
                session_id = Some(parse_agent_session_id(value)?);
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent session terminal-results argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(AgentSessionTerminalResultsOptions {
        session_id: session_id.ok_or_else(|| CliError::Parse {
            detail: "agent session terminal-results requires a session id".to_string(),
        })?,
        task_id,
        json,
    })
}

pub(crate) fn parse_agent_session_message_options(
    args: &[String],
    command: &str,
) -> Result<AgentSessionMessageOptions, CliError> {
    let mut session_id = None;
    let mut message_parts = Vec::new();
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--session" | "--session-id" => {
                index += 1;
                session_id = Some(parse_agent_session_id_arg(args.get(index), "--session")?);
            }
            "--message" | "--reason" => {
                index += 1;
                message_parts.push(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: format!("{} requires a value", args[index - 1]),
                })?);
            }
            value if session_id.is_none() => {
                session_id = Some(parse_agent_session_id(value)?);
            }
            value => {
                message_parts.push(value.to_string());
            }
        }
        index += 1;
    }
    let message = message_parts.join(" ");
    if message.trim().is_empty() {
        return Err(CliError::Parse {
            detail: format!("agent session {command} requires a message"),
        });
    }
    Ok(AgentSessionMessageOptions {
        session_id: session_id.ok_or_else(|| CliError::Parse {
            detail: format!("agent session {command} requires a session id"),
        })?,
        message,
        json,
    })
}

pub(crate) fn parse_agent_session_resume_options(
    args: &[String],
) -> Result<AgentSessionResumeOptions, CliError> {
    let mut session_id = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--session" | "--session-id" => {
                index += 1;
                session_id = Some(parse_agent_session_id_arg(args.get(index), "--session")?);
            }
            value if session_id.is_none() => {
                session_id = Some(parse_agent_session_id(value)?);
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent session resume argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(AgentSessionResumeOptions {
        session_id: session_id.ok_or_else(|| CliError::Parse {
            detail: "agent session resume requires a session id".to_string(),
        })?,
        json,
    })
}

fn parse_agent_session_id_arg(value: Option<&String>, option: &str) -> Result<SessionId, CliError> {
    parse_agent_session_id(value.ok_or_else(|| CliError::Parse {
        detail: format!("{option} requires a value"),
    })?)
}

fn parse_agent_session_id(value: &str) -> Result<SessionId, CliError> {
    SessionId::from_str(value)
        .or_else(|_| SessionId::from_legacy_named(value))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid agent session id `{value}`: {source}"),
        })
}

fn parse_agent_task_id_arg(value: Option<&String>, option: &str) -> Result<TaskId, CliError> {
    let value = value.ok_or_else(|| CliError::Parse {
        detail: format!("{option} requires a value"),
    })?;
    TaskId::from_str(value).map_err(|source| CliError::Parse {
        detail: format!("invalid agent task id `{value}`: {source}"),
    })
}

fn parse_usize_arg(value: Option<&String>, option: &str) -> Result<usize, CliError> {
    let value = value.ok_or_else(|| CliError::Parse {
        detail: format!("{option} requires a value"),
    })?;
    value.parse::<usize>().map_err(|source| CliError::Parse {
        detail: format!("invalid {option} value `{value}`: {source}"),
    })
}

pub(crate) fn parse_agent_export_options(args: &[String]) -> Result<AgentExportOptions, CliError> {
    let mut agent = None;
    let mut output = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--output" | "-o" => {
                index += 1;
                output =
                    Some(
                        args.get(index)
                            .map(PathBuf::from)
                            .ok_or_else(|| CliError::Parse {
                                detail: "--output requires a value".to_string(),
                            })?,
                    );
            }
            value if agent.is_none() => {
                agent = Some(value.to_string());
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent export argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(AgentExportOptions {
        agent: agent.unwrap_or_else(|| "main-agent".to_string()),
        output,
    })
}

pub(crate) fn parse_agent_import_options(args: &[String]) -> Result<AgentImportOptions, CliError> {
    let mut path = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => {
                json = true;
            }
            "--file" => {
                index += 1;
                path = Some(
                    args.get(index)
                        .map(PathBuf::from)
                        .ok_or_else(|| CliError::Parse {
                            detail: "--file requires a value".to_string(),
                        })?,
                );
            }
            value if path.is_none() => {
                path = Some(PathBuf::from(value));
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent import argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    let path = path.ok_or_else(|| CliError::Parse {
        detail: "agent import requires a package JSON path".to_string(),
    })?;
    Ok(AgentImportOptions { path, json })
}

pub(crate) fn parse_agent_readiness_options(
    args: &[String],
) -> Result<AgentReadinessOptions, CliError> {
    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent readiness argument `{value}`"),
                });
            }
        }
    }
    Ok(AgentReadinessOptions { json })
}

pub(crate) fn parse_agent_validate_options(
    args: &[String],
) -> Result<AgentValidateOptions, CliError> {
    let mut agent = None;
    let mut file = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => {
                json = true;
            }
            "--file" => {
                index += 1;
                file = Some(
                    args.get(index)
                        .map(PathBuf::from)
                        .ok_or_else(|| CliError::Parse {
                            detail: "--file requires a value".to_string(),
                        })?,
                );
            }
            value if agent.is_none() && file.is_none() => {
                agent = Some(value.to_string());
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected agent validate argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(AgentValidateOptions {
        agent: agent.unwrap_or_else(|| "main-agent".to_string()),
        file,
        json,
    })
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
        AgentCliCommand::Help => {
            print_agent_help();
            Ok(())
        }
        AgentCliCommand::Export(options) => export_agent_package(options),
        AgentCliCommand::Import(options) => import_agent_package(options),
        AgentCliCommand::Inspect(options) => inspect_agent_package(options),
        AgentCliCommand::Readiness(options) => run_agent_readiness(options),
        AgentCliCommand::Run(options) => run_agent_once(options),
        AgentCliCommand::Session(command) => run_agent_session_command(command),
        AgentCliCommand::Validate(options) => validate_agent_package(options),
    }
}

pub(crate) fn run_agent_session_command(command: AgentSessionCliCommand) -> Result<(), CliError> {
    match command {
        AgentSessionCliCommand::Help => {
            print_agent_session_help();
            Ok(())
        }
        AgentSessionCliCommand::Inspect(options) => {
            let response: AgentSessionInspectResponse = send_agent_session_rpc(
                AGENT_SESSION_INSPECT_ROUTE,
                &AgentSessionInspectRequest {
                    session_id: options.session_id,
                },
            )?;
            if options.json {
                print_agent_session_json("<agent session inspect>", &response)?;
            } else {
                print!("{}", agent_session_inspection_text(&response));
            }
            Ok(())
        }
        AgentSessionCliCommand::Progress(options) => {
            let response: AgentSessionProgressResponse = send_agent_session_rpc(
                AGENT_SESSION_PROGRESS_ROUTE,
                &AgentSessionProgressRequest {
                    session_id: options.session_id,
                    cursor: options.cursor,
                },
            )?;
            if options.json {
                print_agent_session_json("<agent session progress>", &response)?;
            } else {
                print!("{}", agent_session_progress_text(&response));
            }
            Ok(())
        }
        AgentSessionCliCommand::TerminalResults(options) => {
            let response: AgentSessionTerminalResultsResponse = send_agent_session_rpc(
                AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
                &AgentSessionTerminalResultsRequest {
                    session_id: options.session_id,
                    task_id: options.task_id,
                },
            )?;
            if options.json {
                print_agent_session_json("<agent session terminal-results>", &response)?;
            } else {
                print!("{}", agent_session_terminal_results_text(&response));
            }
            Ok(())
        }
        AgentSessionCliCommand::Steer(options) => {
            let response: AgentSessionControlResponse = send_agent_session_rpc(
                AGENT_SESSION_STEER_ROUTE,
                &AgentSessionSteerRequest {
                    session_id: options.session_id,
                    message: options.message,
                },
            )?;
            if options.json {
                print_agent_session_json("<agent session steer>", &response)?;
            } else {
                print!("{}", agent_session_control_text("steer", &response));
            }
            Ok(())
        }
        AgentSessionCliCommand::FollowUp(options) => {
            let response: AgentSessionControlResponse = send_agent_session_rpc(
                AGENT_SESSION_FOLLOW_UP_ROUTE,
                &AgentSessionFollowUpRequest {
                    session_id: options.session_id,
                    message: options.message,
                },
            )?;
            if options.json {
                print_agent_session_json("<agent session follow-up>", &response)?;
            } else {
                print!("{}", agent_session_control_text("follow-up", &response));
            }
            Ok(())
        }
        AgentSessionCliCommand::Interrupt(options) => {
            let response: AgentSessionControlResponse = send_agent_session_rpc(
                AGENT_SESSION_INTERRUPT_ROUTE,
                &AgentSessionInterruptRequest {
                    session_id: options.session_id,
                    reason: options.message,
                },
            )?;
            if options.json {
                print_agent_session_json("<agent session interrupt>", &response)?;
            } else {
                print!("{}", agent_session_control_text("interrupt", &response));
            }
            Ok(())
        }
        AgentSessionCliCommand::Resume(options) => {
            let response: AgentSessionResumeResponse = send_agent_session_rpc(
                AGENT_SESSION_RESUME_ROUTE,
                &AgentSessionResumeRequest {
                    session_id: options.session_id,
                },
            )?;
            if options.json {
                print_agent_session_json("<agent session resume>", &response)?;
            } else {
                print!("{}", agent_session_resume_text(&response));
            }
            Ok(())
        }
    }
}

fn send_agent_session_rpc<Req, Resp>(route: &str, body: &Req) -> Result<Resp, CliError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "agent session commands require the local daemon RPC service".to_string(),
        });
    };
    let request = network::ObjectRpcRequest::encode_body(route, AGENT_RPC_CATEGORY, body)
        .map_err(CliError::from)?;
    let response = daemon_send_daemon_rpc(&status.address, &request, DAEMON_AGENT_SESSION_TIMEOUT)?;
    response.decode_body().map_err(CliError::from)
}

fn print_agent_session_json<T: serde::Serialize>(path: &str, value: &T) -> Result<(), CliError> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|source| CliError::Json {
            path: path.to_string(),
            detail: source.to_string(),
        })?
    );
    Ok(())
}

pub(crate) fn agent_session_inspection_text(response: &AgentSessionInspectResponse) -> String {
    let mut output = String::new();
    output.push_str(&format!("Agent session: {}\n", response.session_id));
    match &response.session {
        Some(session) => {
            output.push_str(&format!("  role: {:?}\n", session.role));
            output.push_str(&format!(
                "  transcript items: {}\n",
                session.transcript.items.len()
            ));
            output.push_str(&format!(
                "  raw transcript items: {}\n",
                session.raw_transcript.items.len()
            ));
            output.push_str(&format!(
                "  queued steering: {}\n",
                session.steering_queue.len()
            ));
            output.push_str(&format!(
                "  queued follow-ups: {}\n",
                session.follow_up_queue.len()
            ));
        }
        None => output.push_str("  session: not found\n"),
    }
    if let Some(snapshot) = &response.control_snapshot {
        output.push_str(&format!(
            "  active turn: {}\n",
            snapshot.active_turn.is_some()
        ));
        output.push_str(&format!("  tasks: {}\n", snapshot.tasks.len()));
        output.push_str(&format!(
            "  buffered events: {}\n",
            snapshot.buffered_event_count
        ));
    } else {
        output.push_str("  control snapshot: not found\n");
    }
    output.push_str(&format!(
        "  persisted events: {}\n",
        response.persisted_event_count
    ));
    output.push_str(&format!(
        "  event cursor: {}\n",
        agent_session_cursor_label(&response.event_cursor)
    ));
    output.push_str(&format!(
        "  terminal results: {}\n",
        response.terminal_results.len()
    ));
    output
}

pub(crate) fn agent_session_progress_text(response: &AgentSessionProgressResponse) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "Agent session progress: {}\n",
        response.session_id
    ));
    output.push_str(&format!(
        "  cursor: {} -> {}\n",
        agent_session_cursor_label(&response.batch.cursor),
        agent_session_cursor_label(&response.batch.next_cursor)
    ));
    output.push_str(&format!("  events: {}\n", response.batch.events.len()));
    output.push_str(&format!(
        "  task updates: {}\n",
        response.batch.task_updates.len()
    ));
    output.push_str(&format!(
        "  terminal results: {}\n",
        response.batch.terminal_results.len()
    ));
    for task in &response.batch.task_updates {
        output.push_str(&format!(
            "  - {} {} {}\n",
            task.task.task_id,
            agent_task_status_label(&task.status),
            task.task.summary
        ));
    }
    output
}

pub(crate) fn agent_session_terminal_results_text(
    response: &AgentSessionTerminalResultsResponse,
) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "Agent session terminal results: {}\n",
        response.session_id
    ));
    output.push_str(&format!("  results: {}\n", response.terminal_results.len()));
    for result in &response.terminal_results {
        output.push_str(&agent_terminal_result_line(result));
    }
    output
}

fn agent_session_control_text(action: &str, response: &AgentSessionControlResponse) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "Agent session {action}: {}\n",
        response.session_id
    ));
    output.push_str(&format!("  accepted: {}\n", response.result.accepted));
    output.push_str(&format!("  sequence: {}\n", response.result.sequence));
    output.push_str(&format!(
        "  active turn: {}\n",
        response.result.snapshot.active_turn.is_some()
    ));
    output.push_str(&format!(
        "  tasks: {}\n",
        response.result.snapshot.tasks.len()
    ));
    output.push_str(&format!("  events: {}\n", response.result.events.len()));
    output.push_str(&format!(
        "  terminal results: {}\n",
        response.result.terminal_results().len()
    ));
    output
}

pub(crate) fn agent_session_resume_text(response: &AgentSessionResumeResponse) -> String {
    let mut output = String::new();
    output.push_str(&format!("Agent session resume: {}\n", response.session_id));
    output.push_str(&format!("  accepted: {}\n", response.accepted));
    output.push_str(&format!(
        "  runtime attached: {}\n",
        response.runtime_attached
    ));
    output.push_str(&format!(
        "  reattach state: {}\n",
        agent_reattach_state_label(&response.reattach.state)
    ));
    output.push_str(&format!(
        "  persisted active turn: {}\n",
        response.reattach.active_turn_persisted
    ));
    output.push_str(&format!(
        "  running tasks: {}\n",
        response.reattach.running_task_count
    ));
    output.push_str(&format!(
        "  persisted events: {}\n",
        response.reattach.persisted_event_count
    ));
    if let Some(session) = &response.session {
        output.push_str(&format!("  role: {:?}\n", session.role));
        output.push_str(&format!(
            "  queued steering: {}\n",
            session.steering_queue.len()
        ));
        output.push_str(&format!(
            "  queued follow-ups: {}\n",
            session.follow_up_queue.len()
        ));
    }
    if let Some(snapshot) = &response.control_snapshot {
        output.push_str(&format!(
            "  active turn: {}\n",
            snapshot.active_turn.is_some()
        ));
        output.push_str(&format!("  tasks: {}\n", snapshot.tasks.len()));
    }
    output.push_str(&format!(
        "  event cursor: {}\n",
        agent_session_cursor_label(&response.event_cursor)
    ));
    if let Some(detail) = response.detail.as_deref() {
        output.push_str(&format!("  detail: {detail}\n"));
    }
    output.push_str(&format!(
        "  reattach reason: {}\n",
        response.reattach.reason
    ));
    output
}

fn agent_reattach_state_label(state: &sinomo_agent::AgentSessionReattachState) -> &'static str {
    match state {
        sinomo_agent::AgentSessionReattachState::LiveRuntimeAttached => "live_runtime_attached",
        sinomo_agent::AgentSessionReattachState::PersistedMetadataOnly => "persisted_metadata_only",
        sinomo_agent::AgentSessionReattachState::Missing => "missing",
    }
}

fn agent_session_cursor_label(cursor: &AgentSessionControlEventCursor) -> String {
    match &cursor.last_event_id {
        Some(event_id) => format!("offset {}, last event {}", cursor.offset, event_id),
        None => format!("offset {}, last event none", cursor.offset),
    }
}

fn agent_terminal_result_line(result: &AgentTaskTerminalResult) -> String {
    let mut line = format!(
        "  - {} {} {}",
        result.task.task_id,
        agent_task_status_label(&result.status),
        result.task.summary
    );
    if let Some(summary) = result.progress_summary.as_deref() {
        line.push_str(&format!(": {summary}"));
    }
    if let Some(result_ref) = result.result_ref.as_deref() {
        line.push_str(&format!(" [{result_ref}]"));
    }
    line.push('\n');
    line
}

fn agent_task_status_label(status: &sinomo_agent::AgentTaskLifecycleStatus) -> &'static str {
    match status {
        sinomo_agent::AgentTaskLifecycleStatus::Pending => "pending",
        sinomo_agent::AgentTaskLifecycleStatus::Running => "running",
        sinomo_agent::AgentTaskLifecycleStatus::Completed => "completed",
        sinomo_agent::AgentTaskLifecycleStatus::Failed => "failed",
        sinomo_agent::AgentTaskLifecycleStatus::Cancelled => "cancelled",
        sinomo_agent::AgentTaskLifecycleStatus::TimedOut => "timed-out",
    }
}

pub(crate) fn inspect_agent_package(options: AgentInspectOptions) -> Result<(), CliError> {
    let catalog = load_agent_catalog_for_cli()?;
    let definition = agent_definition_from_catalog(&catalog, &options.agent)?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&agent_package_inspection_json(definition)).map_err(
                |source| CliError::Json {
                    path: "<agent inspect>".to_string(),
                    detail: source.to_string(),
                },
            )?
        );
    } else {
        print!("{}", agent_package_inspection_text(definition));
    }
    Ok(())
}

pub(crate) fn export_agent_package(options: AgentExportOptions) -> Result<(), CliError> {
    let catalog = load_agent_catalog_for_cli()?;
    let definition = agent_definition_from_catalog(&catalog, &options.agent)?;
    let bytes = serde_json::to_vec_pretty(definition).map_err(|source| CliError::Json {
        path: "<agent export>".to_string(),
        detail: source.to_string(),
    })?;
    if let Some(path) = options.output {
        fs::write(&path, bytes).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        println!("exported agent package: {}", path.display());
    } else {
        println!(
            "{}",
            String::from_utf8(bytes).map_err(|source| CliError::Json {
                path: "<agent export>".to_string(),
                detail: source.to_string(),
            })?
        );
    }
    Ok(())
}

pub(crate) fn import_agent_package(options: AgentImportOptions) -> Result<(), CliError> {
    let source_path = agent_package_source_path(&options.path);
    let mut definition = read_agent_package_definition(&options.path)?;
    definition.package.provenance = AgentPackageProvenance::imported(source_path.clone());
    definition.validate().map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let store = FileAgentDefinitionStore::for_data_dir(&data_dir);
    let saved_path = store
        .save_definition(&definition)
        .map_err(agent_definition_store_cli_error)?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "imported",
                "id": definition.key(),
                "source_path": source_path,
                "stored_path": saved_path,
                "package": definition.package,
            }))
            .map_err(|source| CliError::Json {
                path: "<agent import>".to_string(),
                detail: source.to_string(),
            })?
        );
    } else {
        println!(
            "imported agent package `{}` to {}",
            definition.key(),
            saved_path.display()
        );
    }
    Ok(())
}

pub(crate) fn validate_agent_package(options: AgentValidateOptions) -> Result<(), CliError> {
    let (definition, source, path) = if let Some(path) = options.file {
        let definition = read_agent_package_definition(&path)?;
        definition.validate().map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })?;
        (
            definition,
            "file".to_string(),
            Some(agent_package_source_path(&path)),
        )
    } else {
        let catalog = load_agent_catalog_for_cli()?;
        let definition = agent_definition_from_catalog(&catalog, &options.agent)?.clone();
        definition.validate().map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })?;
        (definition, "catalog".to_string(), None)
    };
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "valid",
                "id": definition.key(),
                "source": source,
                "path": path,
                "package": definition.package,
            }))
            .map_err(|source| CliError::Json {
                path: "<agent validate>".to_string(),
                detail: source.to_string(),
            })?
        );
    } else {
        println!(
            "valid agent package `{}` from {}",
            definition.key(),
            path.unwrap_or(source)
        );
    }
    Ok(())
}

fn load_agent_catalog_for_cli() -> Result<AgentCatalog, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    AgentCatalog::load_from_data_dir(&data_dir).map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })
}

fn agent_definition_from_catalog<'a>(
    catalog: &'a AgentCatalog,
    agent: &str,
) -> Result<&'a AgentDefinition, CliError> {
    catalog.get(agent).ok_or_else(|| CliError::Parse {
        detail: format!("unknown agent `{agent}`"),
    })
}

fn read_agent_package_definition(path: &Path) -> Result<AgentDefinition, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut definition: AgentDefinition =
        serde_json::from_str(&content).map_err(|source| CliError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    if definition.id.is_empty() {
        definition.id = definition.name.clone();
    }
    Ok(definition)
}

fn agent_definition_store_cli_error(error: sinomo_agent::AgentDefinitionStoreError) -> CliError {
    match error {
        sinomo_agent::AgentDefinitionStoreError::Io { path, detail } => {
            CliError::Io { path, detail }
        }
        sinomo_agent::AgentDefinitionStoreError::Json { path, detail } => {
            CliError::Json { path, detail }
        }
        sinomo_agent::AgentDefinitionStoreError::Validation(error) => CliError::Parse {
            detail: error.to_string(),
        },
    }
}

fn agent_package_source_path(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

pub(crate) fn run_agent_readiness(options: AgentReadinessOptions) -> Result<(), CliError> {
    let workspace_root = env::current_dir().map_err(|source| CliError::Io {
        path: ".".to_string(),
        detail: source.to_string(),
    })?;
    let report = run_local_agent_readiness_gate(workspace_root);
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|source| CliError::Json {
                path: "<agent readiness>".to_string(),
                detail: source.to_string(),
            })?
        );
    } else {
        print!("{}", agent_readiness_text(&report));
    }
    if !report.is_ready() {
        return Err(CliError::Parse {
            detail: agent_readiness_failure_detail(&report),
        });
    }
    Ok(())
}

pub(crate) fn agent_readiness_text(report: &AgentReadinessReport) -> String {
    let mut output = String::new();
    output.push_str(&format!("Agent readiness: {:?}\n", report.status));
    output.push_str(&format!("  workspace root: {}\n", report.workspace_root));
    for check in &report.checks {
        output.push_str(&format!(
            "  [{}] {}: {}\n",
            agent_readiness_check_label(&check.status),
            check.id,
            check.summary
        ));
    }
    output
}

fn agent_readiness_check_label(status: &AgentReadinessCheckStatus) -> &'static str {
    match status {
        AgentReadinessCheckStatus::Passed => "pass",
        AgentReadinessCheckStatus::Failed => "fail",
    }
}

fn agent_readiness_failure_detail(report: &AgentReadinessReport) -> String {
    let failed = report
        .failed_checks()
        .map(|check| check.id.as_str())
        .collect::<Vec<_>>();
    if failed.is_empty() {
        "agent readiness failed".to_string()
    } else {
        format!("agent readiness failed: {}", failed.join(", "))
    }
}

pub(crate) fn agent_package_inspection_json(definition: &AgentDefinition) -> serde_json::Value {
    serde_json::json!({
        "id": definition.key(),
        "name": definition.name,
        "description": definition.description,
        "role": definition.role,
        "package": definition.package,
        "system_prompt": definition.system_prompt,
        "tools": definition.tools.iter().map(|tool| serde_json::json!({
            "name": tool.name,
            "kind": tool.kind,
            "async_default": tool.async_default,
        })).collect::<Vec<_>>(),
        "tool_surface": definition.tool_surface,
        "skills": definition.skills,
        "context": definition.context,
        "compaction": definition.compaction,
        "model_policy": definition.model_policy,
        "effective_model_policy": {
            "provider": definition.model_policy.provider.clone(),
            "model": definition.model_policy.model.clone(),
            "temperature": effective_agent_temperature(definition),
            "max_output_tokens": effective_agent_max_output_tokens(definition),
            "vendor_options": definition.model_policy.vendor_options.clone(),
        },
        "runtime_limits": definition.runtime_limits,
        "output_format": definition.output_format,
    })
}

pub(crate) fn agent_package_inspection_text(definition: &AgentDefinition) -> String {
    let mut output = String::new();
    output.push_str(&format!("Agent package: {}\n", definition.key()));
    output.push_str(&format!("  name: {}\n", definition.name));
    if !definition.description.is_empty() {
        output.push_str(&format!("  description: {}\n", definition.description));
    }
    output.push_str(&format!("  role: {:?}\n", definition.role));
    output.push_str(&format!(
        "  output format: {:?}\n",
        definition.output_format
    ));
    output.push_str("  package:\n");
    output.push_str(&format!("    version: {}\n", definition.package.version));
    output.push_str(&format!(
        "    provenance: {}\n",
        agent_package_source_label(&definition.package.provenance.source)
    ));
    if let Some(module_id) = definition.package.provenance.module_id.as_deref() {
        output.push_str(&format!("    module id: {module_id}\n"));
    }
    if let Some(module_version) = definition.package.provenance.module_version.as_deref() {
        output.push_str(&format!("    module version: {module_version}\n"));
    }
    if let Some(path) = definition.package.provenance.path.as_deref() {
        output.push_str(&format!("    path: {path}\n"));
    }
    if let Some(revision) = definition.package.provenance.revision.as_deref() {
        output.push_str(&format!("    revision: {revision}\n"));
    }
    output.push_str("  provider preferences:\n");
    output.push_str(&format!(
        "    provider: {}\n",
        definition
            .model_policy
            .provider
            .as_deref()
            .unwrap_or("runtime-default")
    ));
    output.push_str(&format!(
        "    model: {}\n",
        definition
            .model_policy
            .model
            .as_deref()
            .unwrap_or("runtime-default")
    ));
    output.push_str(&format!(
        "    temperature: {}\n",
        format_optional_value(effective_agent_temperature(definition))
    ));
    output.push_str(&format!(
        "    max output tokens: {}\n",
        format_optional_value(effective_agent_max_output_tokens(definition))
    ));
    output.push_str("  runtime limits:\n");
    output.push_str(&format!(
        "    max steps: {}\n",
        format_unbounded_runtime_limit(definition.runtime_limits.max_steps)
    ));
    output.push_str(&format!(
        "    max tool calls per step: {}\n",
        format_unbounded_runtime_limit(definition.runtime_limits.max_tool_calls_per_step)
    ));
    output.push_str(&format!(
        "    max model repair attempts: {}\n",
        format_unbounded_runtime_limit(definition.runtime_limits.max_model_repair_attempts)
    ));
    output.push_str("  tool surface:\n");
    output.push_str(&format!(
        "    visible toolsets: {}\n",
        format_string_list(&definition.tool_surface.visible_toolsets)
    ));
    output.push_str(&format!(
        "    allow tools: {}\n",
        format_string_list(&definition.tool_surface.allow_tools)
    ));
    output.push_str(&format!(
        "    deny tools: {}\n",
        format_string_list(&definition.tool_surface.deny_tools)
    ));
    output.push_str(&format!(
        "  skills: {}\n",
        format_string_list(&definition.skills)
    ));
    output.push_str("  context:\n");
    output.push_str(&format!(
        "    strategy: {}\n",
        definition.context.pipeline_id
    ));
    output.push_str(&format!(
        "    providers: {}\n",
        format_string_list(&definition.context.providers)
    ));
    output.push_str("  compaction:\n");
    output.push_str(&format!(
        "    strategy: {}\n",
        definition.compaction.strategy_id
    ));
    output.push_str(&format!(
        "    retained tail items: {}\n",
        format_optional_value(definition.compaction.retained_tail_items)
    ));
    output.push_str(&format!(
        "  tools: {}\n",
        format_string_list(
            &definition
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect::<Vec<_>>()
        )
    ));
    output.push_str("  system prompt:\n");
    for line in definition.system_prompt.lines() {
        output.push_str("    ");
        output.push_str(line);
        output.push('\n');
    }
    output
}

fn effective_agent_temperature(definition: &AgentDefinition) -> Option<f32> {
    definition
        .temperature
        .or(definition.model_policy.temperature)
}

fn effective_agent_max_output_tokens(definition: &AgentDefinition) -> Option<u32> {
    definition
        .max_output_tokens
        .or(definition.model_policy.max_output_tokens)
}

fn agent_package_source_label(source: &AgentPackageSource) -> &'static str {
    match source {
        AgentPackageSource::BuiltIn => "built-in",
        AgentPackageSource::Local => "local",
        AgentPackageSource::Module => "module",
        AgentPackageSource::Imported => "imported",
        AgentPackageSource::Unknown => "unknown",
    }
}

fn format_string_list(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values.join(", ")
    }
}

fn format_optional_value<T: std::fmt::Display>(value: Option<T>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "runtime-default".to_string())
}

fn format_unbounded_runtime_limit<T: std::fmt::Display>(value: Option<T>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unbounded".to_string())
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
    let input_event = user_interaction_input_event(&session, &options.prompt);
    let input = AgentLoopInput {
        session,
        latest_event: input_event,
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
        .with_tool_surface(sinomo_agent::AgentToolSurface::default_for_role(
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
    let command_is_help = cli_command_is_help(&command);
    let daemon = if invocation.options.force_local
        || command_is_help
        || !cli_command_needs_daemon_probe(&command)
    {
        None
    } else {
        default_main_daemon_status()?
    };
    let local_cluster_quorum = if invocation.options.force_local || command_is_help {
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

    if !cli_force_local_mode() && !command_is_help && cli_command_requires_cluster_state(&command) {
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
        CliCommand::Voice(command) => run_voice_command(command),
        CliCommand::Test(command) => run_test_command(command),
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
const DAEMON_AGENT_SESSION_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const SINOMO_JOIN_URI_PREFIX: &str = network::SINOMO_JOIN_URI_PREFIX;
const CLUSTER_INVITE_DEFAULT_TTL_SECONDS: i64 = 24 * 60 * 60;
const INSTALL_DEFAULT_HOST: &str = "0.0.0.0";

fn daemon_fetch_cluster_kv_snapshot(
    address: &str,
    timeout: Duration,
) -> Result<KvSnapshot, sinomo_runtime::daemon::DaemonError> {
    let request = cluster_kv_snapshot_rpc_request()?;
    let response = daemon_send_daemon_rpc(address, &request, timeout)?;
    decode_cluster_kv_snapshot_rpc_response(&response).map_err(Into::into)
}

fn daemon_fetch_cluster_kv_snapshot_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<KvSnapshot, sinomo_runtime::daemon::DaemonError> {
    let request = cluster_kv_snapshot_rpc_request()?;
    let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &request, timeout)?;
    decode_cluster_kv_snapshot_rpc_response(&response).map_err(Into::into)
}

fn cluster_kv_snapshot_rpc_request() -> Result<network::ObjectRpcRequest, KvError> {
    let payload = sinomo_runtime::kv::cluster_kv_snapshot_rpc_payload()?;
    Ok(network::ObjectRpcRequest::from_encoded_body(
        payload.route,
        payload.category,
        payload.body,
    ))
}

fn decode_cluster_kv_snapshot_rpc_response(
    response: &network::ObjectRpcResponse,
) -> Result<KvSnapshot, network::NetworkError> {
    response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_kv_snapshot_rpc_body(&response.body).map_err(Into::into)
}
