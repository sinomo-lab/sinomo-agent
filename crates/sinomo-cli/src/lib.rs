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

use sinomo_agent::{
    AGENT_RPC_CATEGORY, AGENT_SESSION_FOLLOW_UP_ROUTE, AGENT_SESSION_INSPECT_ROUTE,
    AGENT_SESSION_INTERRUPT_ROUTE, AGENT_SESSION_PROGRESS_ROUTE, AGENT_SESSION_RESUME_ROUTE,
    AGENT_SESSION_STEER_ROUTE, AGENT_SESSION_TERMINAL_RESULTS_ROUTE, Agent, AgentCancellationToken,
    AgentDefinition, AgentDefinitionSifsStore, AgentDefinitionStore, AgentEventPayload,
    AgentLoopDecision, AgentLoopInput, AgentModelEvent, AgentOp, AgentPackageProvenance,
    AgentPackageSource, AgentReadinessCheckStatus, AgentReadinessReport, AgentRole,
    AgentRuntimeEvent, AgentRuntimeEventCursor, AgentRuntimeEventPayload, AgentRuntimeStorage,
    AgentSession, AgentSessionControlEventCursor, AgentSessionControlResponse,
    AgentSessionFollowUpRequest, AgentSessionInspectRequest, AgentSessionInspectResponse,
    AgentSessionInterruptRequest, AgentSessionProgressRequest, AgentSessionProgressResponse,
    AgentSessionResumeRequest, AgentSessionResumeResponse, AgentSessionRuntime,
    AgentSessionSteerRequest, AgentSessionStore, AgentSessionTerminalResultsRequest,
    AgentSessionTerminalResultsResponse, AgentTaskTerminalResult, AgentTool, AgentToolDescriptor,
    AgentToolEvidence, AgentToolKind, AgentToolProgressEvent, AgentToolRegistry, AgentToolRequest,
    AgentToolResult, AgentToolSpec, ConfigAgentToolConfig, EventEnvelopeExt, FileAgentSessionStore,
    FileAgentTraceStore, HostedModelAgentToolConfig, HostedModelToolBackend, InteractionModel,
    InteractionOutput, InteractionSession, InteractionSurface, InteractionSurfaceKind,
    LocalWorkspaceAgentConfig, SkillActivationScope, SkillCatalogEntry, SkillCatalogSifsStore,
    SkillCatalogSource, SkillCatalogSourceKind, SkillCatalogStore, SkillCatalogWrite, TaskId,
    WorkspaceRef, interaction_event, read_agent_skill_package, register_hosted_model_tools,
    render_common_skill_markdown, run_local_agent_readiness_gate, user_interaction_input_event,
    write_agent_skill_package,
};
use sinomo_lm::{
    AnthropicProvider, CODEX_DEFAULT_MODEL, CODEX_PROVIDER_ID, ClaudeProvider, CurrentDefaultModel,
    DEFAULT_PROVIDER_ACCOUNT, DeepSeekProvider, GeminiProvider, GitHubCopilotProvider,
    LM_PROVIDER_CONFIG_KEY, LM_PROVIDER_CONFIG_KV_KEY, LM_RUNTIME_CONFIG_KEY,
    LM_RUNTIME_CONFIG_KV_KEY, LM_STUDIO_PROVIDER_ID, LanguageModelCancellation, LanguageModelError,
    LanguageModelModelOptions, LanguageModelProviderOptions, LanguageModelProviderUsage,
    LanguageModelRequest, LanguageModelResponse, LanguageModelStreamEvent, LocalResponsesConfig,
    LocalResponsesProvider, LocalResponsesRuntime, ModelConfig, ModelPaths, ModelRuntimeConfig,
    NvidiaProvider, OLLAMA_PROVIDER_ID, OpenAiConfig, OpenAiProvider, OpenAiResponsesCodexClient,
    OpenAiResponsesCodexConfig, ProviderAccountConfig, ProviderReasoningEffort,
    ProviderTextVerbosity, claude_provider, codex_credentials_file_in_data_dir, codex_provider,
    collect_provider_model_catalogs, collect_provider_usage,
    complete_configured_model_request_streaming_events_with_account,
    complete_configured_model_request_streaming_events_with_account_and_cancellation,
    complete_configured_model_request_with_account,
    current_default_model as resolve_current_default_model, default_model_provider_ids,
    github_copilot_provider, load_model_config, model_config_cluster_kv_txn,
    model_config_definition, model_config_from_cluster_kv_snapshot, model_config_store,
    model_runtime_config_cluster_kv_txn, model_runtime_config_definition,
    model_runtime_config_from_cluster_kv_snapshot, normalize_model_id_for_provider,
    normalize_model_provider, normalize_provider_account_name, provider_account_config,
    provider_api_key_from_env, provider_options_for_model, resolve_default_model,
    save_provider_api_key_in_model_config, stored_provider_api_key,
    stored_provider_api_key_in_data_dir, subscription_usage_provider_ids,
    test_provider_account_credentials, test_provider_credentials, upsert_provider_account_config,
    write_default_model_config,
};
use sinomo_pytorch::{
    MINICPM_DEFAULT_MODEL, MINICPM_PROVIDER_ID, ManagedPytorchServer, ManagedPytorchServerConfig,
    PytorchDevice, PytorchMode, RemotePytorchRuntime, RemotePytorchRuntimeConfig,
    is_loopback_http_endpoint,
};
use sinomo_runtime::api::{
    DaemonApiClient, DaemonClusterStatus, DaemonPaths, DaemonProbeStatus, DaemonRunOptions,
    DaemonStatus, IdleStatsSnapshot, SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT,
    SINOMO_SHADOW_DAEMON_DEFAULT_PORT, cluster_node_status_from_daemon_status, daemon_bind_address,
    daemon_connect_address, daemon_options_from_status,
    default_main_daemon_status as daemon_default_main_daemon_status, probe_daemon_status,
    probe_daemon_status_async,
};
#[cfg(test)]
use sinomo_runtime::api::{daemon_package_version, write_daemon_status};
use sinomo_runtime::cli_registry::{
    CliAutocompleteChoice, CliCommandContext, CliCommandMatches, CliCommandPolicy,
    CliCommandRegistry, CliCommandSpec, CliOptionSpec, CliParseError, CliPositionalSpec,
    CliValueSpec, RegisteredCliCommand,
};
use sinomo_runtime::cluster;
#[cfg(test)]
use sinomo_runtime::cluster::ClusterJoinRegistrationRequest;
#[cfg(test)]
use sinomo_runtime::cluster::ClusterJoinRegistrationResponse;
use sinomo_runtime::cluster::cluster_doctor_findings;
use sinomo_runtime::cluster::{ClusterDoctorFinding, ClusterHealthRow};
use sinomo_runtime::core::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterJoinInvite, ClusterMemberRecord,
    ClusterMemberRole, ClusterMemberState, ClusterMigrationMode, ClusterMigrationPhase,
    ClusterNodeAvailability, ClusterNodeRole, ClusterNodeRolloutStatus, ClusterNodeStatusRecord,
    ClusterTransportKind, EvaluationScorecard, EventEnvelope, Id, RollbackTrigger,
};
#[cfg(test)]
use sinomo_runtime::daemon::DaemonLock;
#[cfg(test)]
use sinomo_runtime::daemon::{
    AuthenticatedDaemonCommand, DaemonError, DaemonSignerAuthority,
    authenticate_daemon_control_request, authorize_authenticated_daemon_command,
    sign_daemon_rpc_request_from_data_dir,
};
use sinomo_runtime::daemon::{
    DaemonClusterProcess, DaemonClusterRegistration, current_shadow_daemon_options,
    daemon_advertised_endpoints_for_state_with_manual_config, daemon_cluster_registration,
    daemon_cluster_status, daemon_main_binary_for_shadow_start,
    fetch_daemon_status_from_address as daemon_fetch_daemon_status_from_address,
    fetch_daemon_status_from_endpoint as daemon_fetch_daemon_status_from_endpoint,
    invite_introducers_from_daemon_status,
    prepare_daemon_control_listener as daemon_prepare_daemon_control_listener,
    read_daemon_endpoint_advertisement_config,
    run_daemon_control_loop_with_prepared_listener as daemon_run_daemon_control_loop_with_prepared_listener,
    send_daemon_cluster_maintenance_refresh as daemon_send_daemon_cluster_maintenance_refresh,
    send_daemon_cluster_maintenance_refresh_to_endpoint as daemon_send_daemon_cluster_maintenance_refresh_to_endpoint,
    send_daemon_remote_shadow_shutdown_request,
    send_daemon_rollout_binary as daemon_send_daemon_rollout_binary,
    send_daemon_rollout_binary_to_endpoint as daemon_send_daemon_rollout_binary_to_endpoint,
    send_daemon_rollout_binary_to_target as daemon_send_daemon_rollout_binary_to_target,
    send_daemon_rollout_build as daemon_send_daemon_rollout_build,
    send_daemon_rollout_build_to_endpoint as daemon_send_daemon_rollout_build_to_endpoint,
    send_daemon_rollout_build_to_target as daemon_send_daemon_rollout_build_to_target,
    send_daemon_rollout_clear_status as daemon_send_daemon_rollout_clear_status,
    send_daemon_rollout_clear_status_to_endpoint as daemon_send_daemon_rollout_clear_status_to_endpoint,
    send_daemon_rollout_diagnostics as daemon_send_daemon_rollout_diagnostics,
    send_daemon_rollout_diagnostics_to_endpoint as daemon_send_daemon_rollout_diagnostics_to_endpoint,
    send_daemon_rollout_native_dylib_module as daemon_send_daemon_rollout_native_dylib_module,
    send_daemon_rollout_native_dylib_module_to_endpoint as daemon_send_daemon_rollout_native_dylib_module_to_endpoint,
    send_daemon_rollout_native_dylib_module_to_target as daemon_send_daemon_rollout_native_dylib_module_to_target,
    send_daemon_rpc as daemon_send_daemon_rpc,
    send_daemon_rpc_call_async as daemon_send_daemon_rpc_call_async,
    send_daemon_rpc_to_endpoint as daemon_send_daemon_rpc_to_endpoint,
    send_daemon_rpc_with_timeouts as daemon_send_daemon_rpc_with_timeouts,
    send_daemon_shutdown_request,
    start_daemon_command_process as daemon_start_daemon_command_process,
};
#[cfg(test)]
use sinomo_runtime::daemon::{
    daemon_advertised_endpoints_for_state, daemon_endpoint_advertisement_config_file,
};
use sinomo_runtime::dfs::{DaemonDfsInspectionRequest, DaemonDfsInspectionResponse};
use sinomo_runtime::dfs::{
    DfsConflictRecord, DfsFileSystemId, DfsMetadataBackend, DfsMetadataShardConfig, DfsObjectId,
    DfsOperatorConflictVersionView, DfsOperatorConflictVersionsReport, DfsOperatorInspectionCursor,
    DfsOperatorInspector, DfsOperatorRepairView, DfsVersion, LocalShardedMetadataBackend,
    LocalSobjMetadataBackend,
};
use sinomo_runtime::dfs::{
    DfsPinAddRequest, DfsPinMode, add_local_dfs_pin, default_dfs_metadata_root,
    default_dfs_metadata_shards_root, dfs_import_metadata_root, dfs_import_namespace_root,
    fallback_dfs_node_id, list_local_dfs_pins, metadata_shards_prefix,
    open_local_sharded_metadata_backend, open_root_local_metadata_backend, remove_local_dfs_pin,
    resolve_dfs_metadata_root, resolve_dfs_pin_root, sifs_file_system_id,
};
use sinomo_runtime::events::{EventId, JsonlEventStore, SessionId};
use sinomo_runtime::events::{EventStore, InMemoryEventStore};
use sinomo_runtime::installer::{
    DaemonStopBeforeInstallStart, DaemonStopBeforeInstallStatus, DaemonStopIfRunningStatus,
    InstallLayout, InstalledDaemonInstallPlanRequest, InstalledDaemonStartStatus,
    ManagedSourceRepoInstallStatus, activate_boot_entries as daemon_activate_boot_entries,
    deactivate_boot_entries as daemon_deactivate_boot_entries,
    default_boot_dir as daemon_default_boot_dir,
    ensure_install_layout_dirs as daemon_ensure_install_layout_dirs,
    git_is_available as daemon_git_is_available,
    install_managed_source_repo as daemon_install_managed_source_repo,
    installed_daemon_paths as daemon_installed_daemon_paths,
    managed_source_git_status_lines as daemon_managed_source_git_status_lines,
    prepare_installed_daemon_install_plan as daemon_prepare_installed_daemon_install_plan,
    remove_boot_entry as daemon_remove_boot_entry,
    start_installed_sinomo_daemon as daemon_install_start_installed_sinomo_daemon,
    stop_daemon_before_install as daemon_install_stop_daemon_before_install,
    stop_daemon_if_running as daemon_install_stop_daemon_if_running,
    write_install_boot_entries as daemon_write_install_boot_entries,
};
#[cfg(not(windows))]
use sinomo_runtime::installer::{
    LocalBinSymlinkStatus, SystemctlUserOutcome,
    ensure_local_bin_symlink as daemon_ensure_local_bin_symlink,
};
#[cfg(windows)]
use sinomo_runtime::installer::{
    UserPathUpdate, ensure_windows_user_path_contains as daemon_ensure_windows_user_path_contains,
};
#[cfg(test)]
use sinomo_runtime::kv::KvTxnResponse;
use sinomo_runtime::kv::{
    ClusterKvTxnCommitResult, KvAuthoritativeRead, KvError, KvLocalRead, KvSnapshot, KvTxn,
    KvTxnRequest,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::module::ModuleReplayReport;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::{
    ClusterId, DAEMON_MAIN_CLUSTER_ROLE, LocalNodePermission, LocalNodePermissionStore, NodeId,
    cluster_member_expected_daemon_process, cluster_member_is_local_process,
    load_or_create_main_node_identity, load_or_create_shadow_node_identity, shadow_daemon_node_id,
};
#[cfg(not(test))]
use sinomo_runtime::rollout::install_current_executable;
#[cfg(test)]
use sinomo_runtime::rollout::read_rollout_status;
#[cfg(test)]
use sinomo_runtime::rollout::rollout_main_activation_path;
#[cfg(test)]
use sinomo_runtime::rollout::write_rollout_status;
use sinomo_runtime::rollout::{
    AppliedRolloutTransition, ClusterRolloutBinaryManifest, ClusterRolloutBinaryRequest,
    ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleRequest, DEFAULT_SHADOW_TELEMETRY_LIMIT,
    LocalRolloutNativeDylibModuleArtifact, ManagedSourceDeployOptions, ManagedSourceStatus,
    RolloutActivationStrategy, RolloutController, RolloutError, RolloutMainActivation,
    RolloutObservation, RolloutRuntimeState, RolloutTelemetryGateConfig, SelfRolloutPaths,
    acquire_rollout_activation_guard, clear_rollout_status, ensure_rollout_layout,
    ensure_rollout_source_repo_with_options,
    install_local_rollout_native_dylib_modules as rollout_install_local_rollout_native_dylib_modules,
    managed_source_status, prepare_cluster_rollout_binary_manifest_for_target,
    prepare_cluster_rollout_build_request, prepare_cluster_rollout_native_dylib_module_request,
    prepare_local_rollout_artifacts_with_options, prepare_rollout_artifacts,
    prepare_rollout_source_bundle, read_rollout_runtime_state, read_rollout_telemetry_gate_config,
    reconcile_rollout_native_dylib_platforms, restore_native_dylib_loader_state_for_activation,
    rollout_source_head, rollout_status_view, shadow_telemetry_scorecard_from_event_store,
    source_heads_match, stage_native_dylib_loader_state_rollback, sync_external_repo_from_managed,
    unique_rollout_targets, write_rollout_telemetry_gate_config,
};
use sinomo_runtime::rollout::{
    SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV, SHADOW_MAIN_PORT_ENV,
    cluster_rollout_should_use_remote_build as daemon_cluster_rollout_should_use_remote_build,
};
#[cfg(test)]
use sinomo_runtime::rollout_host::handle_daemon_rollout_build_request as daemon_handle_daemon_rollout_build_request;
use sinomo_runtime::rollout_host::{
    handle_deferred_shadow_rollout_completion as daemon_handle_deferred_shadow_rollout_completion,
    prepare_local_rollout_main_activation as daemon_prepare_local_rollout_main_activation,
    run_shadow_daemon_supervision_loop_with_prepared_listener as daemon_run_shadow_daemon_supervision_loop_with_prepared_listener,
    shadow_main_binary_from_env as daemon_shadow_main_binary_from_env,
    shadow_main_options_from_env as daemon_shadow_main_options_from_env,
    sleep_for_shadow_start_delay as daemon_sleep_for_shadow_start_delay,
    stage_rollout_main_activation as daemon_stage_rollout_main_activation,
    wait_for_local_shadow_rollout_completion as daemon_wait_for_local_shadow_rollout_completion,
    wait_for_main_daemon_health as daemon_wait_for_main_daemon_health,
};
#[cfg(test)]
use sinomo_runtime::sandbox::handle_daemon_sandbox_http_fetch as daemon_handle_daemon_sandbox_http_fetch;
#[cfg(all(test, feature = "sandbox"))]
use sinomo_runtime::sandbox::{
    handle_daemon_sandbox_http_fetch_to_file as daemon_handle_daemon_sandbox_http_fetch_to_file,
    handle_daemon_sandbox_import_busybox as daemon_handle_daemon_sandbox_import_busybox,
    handle_daemon_sandbox_read as daemon_handle_daemon_sandbox_read,
    handle_daemon_sandbox_toolchain_add_artifact as daemon_handle_daemon_sandbox_toolchain_add_artifact,
    handle_daemon_sandbox_toolchain_add_command as daemon_handle_daemon_sandbox_toolchain_add_command,
    handle_daemon_sandbox_toolchain_import_package as daemon_handle_daemon_sandbox_toolchain_import_package,
    handle_daemon_sandbox_write as daemon_handle_daemon_sandbox_write,
    open_daemon_sandbox_dfs_service as daemon_open_daemon_sandbox_dfs_service,
};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt;
use std::fs;
use std::io::{self, BufRead, Write};
#[cfg(unix)]
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
#[cfg(all(unix, feature = "dfs-fuse"))]
use std::sync::Weak;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

mod agent_history;
pub use agent_history::register_agent_history_search_tool_with_vfs;
mod chat_activity;
mod error;
mod help;
mod media_transport;
pub mod module_commands;
mod providers;
mod sandbox_host;
#[cfg(test)]
mod tests;
mod tui;
mod voice;

/// GUI-facing surface over the main-agent chat runtime (used by `sinomo-app`).
pub mod app_api;

use chat_activity::StableChatProjection;
use error::CliError;
use help::*;
pub use providers::*;
use sandbox_host::SinomoCliSandboxHost;
use time::format_description::well_known::Rfc3339;
pub use tui::*;
pub(crate) use voice::*;

trait CliDaemonTargetRpc {
    fn object_rpc_to_target_with_deadline_and_affinity(
        &self,
        target_node_id: &NodeId,
        request: &network::ObjectRpcRequest,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
        destination_process_affinity: sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity,
    ) -> Result<network::ObjectRpcResponse, ()>;
}

impl CliDaemonTargetRpc for DaemonApiClient {
    fn object_rpc_to_target_with_deadline_and_affinity(
        &self,
        target_node_id: &NodeId,
        request: &network::ObjectRpcRequest,
        connect_timeout: Duration,
        overall_timeout: Option<Duration>,
        observe_connectivity: bool,
        destination_process_affinity: sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity,
    ) -> Result<network::ObjectRpcResponse, ()> {
        DaemonApiClient::object_rpc_to_target_with_deadline_and_affinity(
            self,
            target_node_id,
            request,
            connect_timeout,
            overall_timeout,
            observe_connectivity,
            destination_process_affinity,
        )
        .map_err(|_| ())
    }
}

struct CliClusterStateAuthorityReachabilityHost {
    daemon: Option<Box<dyn CliDaemonTargetRpc>>,
}

const CLI_STATE_AUTHORITY_PROBE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const CLI_STATE_AUTHORITY_PROBE_RETRY_BACKOFF: Duration = Duration::from_millis(50);

impl CliClusterStateAuthorityReachabilityHost {
    fn new(daemon: Option<&DaemonStatus>) -> Self {
        Self {
            daemon: daemon.map(|daemon| {
                Box::new(DaemonApiClient::at_address(daemon.address.clone()))
                    as Box<dyn CliDaemonTargetRpc>
            }),
        }
    }

    #[cfg(test)]
    fn with_daemon(daemon: impl CliDaemonTargetRpc + 'static) -> Self {
        Self {
            daemon: Some(Box::new(daemon)),
        }
    }
}

impl cluster::ClusterStateAuthorityReachabilityHost for CliClusterStateAuthorityReachabilityHost {
    fn probe_cluster_state_authority_node(
        &mut self,
        _state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> bool {
        let request = match network::ObjectRpcRequest::encode_body(
            sinomo_runtime::daemon::DAEMON_STATUS_ROUTE,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            &(),
        ) {
            Ok(request) => request,
            Err(_) => return false,
        };
        let Some(daemon) = self.daemon.as_ref() else {
            return false;
        };
        // A CLI process does not own cluster transports. Ask the already
        // authenticated local daemon to route the read-only authority probe so
        // admission uses its process-owned routing state and transport
        // lifecycle. The reachability scan supplies a total per-target budget;
        // bounded attempts share that deadline instead of turning any one
        // transport's no-progress limit into the operation deadline.
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let attempt_timeout = remaining.min(CLI_STATE_AUTHORITY_PROBE_ATTEMPT_TIMEOUT);
            if daemon
                .object_rpc_to_target_with_deadline_and_affinity(
                    target_node_id,
                    &request,
                    attempt_timeout,
                    Some(attempt_timeout),
                    false,
                    sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                )
                .and_then(|response| response.error_detail().map_err(|_| ()))
                .is_ok()
            {
                return true;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            // The routed daemon rebuilds its immutable directory and
            // connectivity snapshots for each attempt. A short bounded pause
            // lets a just-materialized KV snapshot and its connectivity
            // generation converge without extending the caller's total
            // foreground deadline.
            std::thread::sleep(remaining.min(CLI_STATE_AUTHORITY_PROBE_RETRY_BACKOFF));
        }
    }
}

fn cli_cluster_state_authority_reachability_options()
-> cluster::ClusterStateAuthorityReachabilityOptions {
    let mut options = cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
        .state_authority_reachability_options();
    // CLI quorum checks are foreground diagnostics, so keep their total
    // operation budget short while sharing the daemon transport policy.
    options.timeout = network::DEFAULT_NETWORK_TIMEOUT;
    options
}

fn cluster_has_local_quorum(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    daemon: Option<&DaemonStatus>,
) -> bool {
    let mut host = CliClusterStateAuthorityReachabilityHost::new(daemon);
    cluster::cluster_has_local_quorum_with_host(
        paths,
        state,
        cli_cluster_state_authority_reachability_options(),
        &mut host,
    )
}

fn cluster_state_authority_quorum_status(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    daemon: Option<&DaemonStatus>,
) -> Result<cluster::ClusterStateAuthorityQuorumStatus, network::NetworkError> {
    let mut host = CliClusterStateAuthorityReachabilityHost::new(daemon);
    cluster::cluster_state_authority_quorum_status_with_host(
        paths,
        state,
        cli_cluster_state_authority_reachability_options(),
        &mut host,
    )
}

struct CliClusterStatusProbe<'a> {
    paths: &'a ClusterPaths,
}

impl cluster::ClusterStatusProbe for CliClusterStatusProbe<'_> {
    fn probe_member_status(
        &self,
        state: &ClusterBootstrapState,
        member: &ClusterMemberRecord,
    ) -> Option<ClusterNodeStatusRecord> {
        cli_probe_member_daemon_status(self.paths, state, member)
    }

    fn peer_health_rows(
        &self,
        state: &ClusterBootstrapState,
        member: &ClusterMemberRecord,
        timeout: Duration,
    ) -> Option<Vec<ClusterHealthRow>> {
        cli_fetch_cluster_health_rows_from_member(self.paths, state, member, timeout)
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
    let trace = std::env::var_os("SINOMO_CLUSTER_HEALTH_TRACE").is_some();
    let endpoints = network::ClusterEndpointSelector::new(paths, state)
        .select(network::ClusterEndpointSelectionPurpose::MemberDaemonStatus)
        .into_iter()
        .filter(|endpoint| endpoint.node_id == member.node_id)
        .collect::<Vec<_>>();
    for endpoint in endpoints
        .into_iter()
        .take(CLUSTER_HEALTH_CLI_MAX_ENDPOINTS_PER_MEMBER)
    {
        if trace {
            eprintln!(
                "cluster health trace: probing {} {} {:?}",
                member.node_id, endpoint.address, endpoint.transport
            );
        }
        let status = match daemon_fetch_daemon_status_from_endpoint(
            &endpoint,
            CLUSTER_HEALTH_CLI_ENDPOINT_PROBE_TIMEOUT,
        ) {
            Ok(status) => status,
            Err(error) => {
                if trace {
                    eprintln!(
                        "cluster health trace: {} {} failed: {error}",
                        member.node_id, endpoint.address
                    );
                }
                continue;
            }
        };
        let Some(cluster) = status.cluster.as_ref() else {
            if trace {
                eprintln!(
                    "cluster health trace: {} {} returned no cluster status",
                    member.node_id, endpoint.address
                );
            }
            continue;
        };
        if cluster.cluster_id != state.cluster_id || cluster.node_id != member.node_id {
            if trace {
                eprintln!(
                    "cluster health trace: {} {} returned cluster={} node={} process={}",
                    member.node_id,
                    endpoint.address,
                    cluster.cluster_id,
                    cluster.node_id,
                    cluster.process
                );
            }
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
            if trace {
                eprintln!(
                    "cluster health trace: {} {} returned unexpected process {}",
                    member.node_id, endpoint.address, status.process
                );
            }
            if cluster_member_expected_daemon_process(member)
                == Some(sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE)
                && status.process == sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE
                && let Some(status) =
                    cli_probe_member_status_via_shadow_endpoint(member, &endpoint, trace)
            {
                return Some(status);
            }
            continue;
        }
        if trace {
            eprintln!(
                "cluster health trace: {} {} accepted",
                member.node_id, endpoint.address
            );
        }
        return Some(status);
    }
    None
}

fn cli_probe_member_status_via_shadow_endpoint(
    member: &ClusterMemberRecord,
    endpoint: &ClusterEndpointRecord,
    trace: bool,
) -> Option<ClusterNodeStatusRecord> {
    let request = cluster::cluster_local_health_rpc_request().ok()?;
    let response = match daemon_send_daemon_rpc_to_endpoint(
        endpoint,
        &request,
        CLUSTER_HEALTH_PEER_ROWS_TIMEOUT,
    ) {
        Ok(response) => response,
        Err(error) => {
            if trace {
                eprintln!(
                    "cluster health trace: {} {} shadow-local health failed: {error}",
                    member.node_id, endpoint.address
                );
            }
            return None;
        }
    };
    let rows = match cluster::decode_cluster_health_rpc_response(&response) {
        Ok(rows) => rows,
        Err(error) => {
            if trace {
                eprintln!(
                    "cluster health trace: {} {} shadow-local health decode failed: {error}",
                    member.node_id, endpoint.address
                );
            }
            return None;
        }
    };
    rows.into_iter().find_map(|row| {
        let expected_process = cluster_member_expected_daemon_process(member);
        if row.availability == ClusterNodeAvailability::Online
            && row.live_probe_observed
            && row.status.node_id == member.node_id
            && expected_process
                .map(|process| row.status.process == process)
                .unwrap_or(true)
        {
            if trace {
                eprintln!(
                    "cluster health trace: {} {} accepted via shadow-local health",
                    member.node_id, endpoint.address
                );
            }
            Some(row.status)
        } else {
            None
        }
    })
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

fn cli_fetch_cluster_health_rows_from_member(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    timeout: Duration,
) -> Option<Vec<ClusterHealthRow>> {
    let deadline = Instant::now() + timeout;
    for endpoint in network::ClusterEndpointSelector::new(paths, state)
        .select(network::ClusterEndpointSelectionPurpose::MemberDaemonStatus)
        .into_iter()
        .filter(|endpoint| endpoint.node_id == member.node_id)
    {
        let Ok(request) = cluster::cluster_health_rpc_request_without_recursive_peer_views() else {
            return None;
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let Ok(response) = daemon_send_daemon_rpc_to_endpoint(&endpoint, &request, remaining)
        else {
            continue;
        };
        let Ok(rows) = cluster::decode_cluster_health_rpc_response(&response) else {
            continue;
        };
        return Some(rows);
    }
    None
}

mod agent_management_service;
mod chat_service;
mod commands;
mod completion;
mod dfs_mount_supervisor;
mod file_manager;
mod sifs_file_manager;
mod skill_service;
mod sobj_file_preview;
pub use agent_management_service::register_hosted_agent_management_service;
pub use chat_service::register_hosted_chat_service;
pub(crate) use commands::*;
pub(crate) use completion::*;
pub use skill_service::register_hosted_skill_service;

/// Application-host integration for one node-local supervised SIFS mount.
///
/// Standalone and embedded main daemons register the same application-ring
/// service so UI-written local consent has identical lifecycle semantics.
pub fn sifs_mount_supervisor_component_factory(
    data_dir: sinomo_runtime::local::SinomoDataDir,
    daemon_paths: sinomo_runtime::daemon::DaemonPaths,
) -> sinomo_runtime::daemon::DaemonComponentFactory {
    dfs_mount_supervisor::sifs_mount_supervisor_component_factory(data_dir, daemon_paths)
}

/// Full daemon-hosted agent and interactive-chat service component shared by standalone and
/// app-owned daemon products.
pub fn hosted_agent_chat_component_factory(
    daemon_address: impl Into<String>,
) -> Result<sinomo_runtime::daemon::DaemonComponentFactory, String> {
    let daemon_address = daemon_address.into();
    let factory = sinomo_runtime::module::StaticComponentFactory::new(
        sinomo_runtime::module::ComponentDescriptor::new("sinomo-agent", env!("CARGO_PKG_VERSION")),
        move |_, scope| {
            let runtime_control = sinomo_agent::AgentRuntimeControlRegistry::new();
            let control_host: Arc<dyn sinomo_agent::AgentRuntimeControlHost> =
                Arc::new(runtime_control.clone());
            let services = sinomo_agent::register_services_with_runtime_control(
                sinomo_runtime::service::ServiceRegistry::builder(),
                control_host,
            );
            let services =
                register_hosted_chat_service(services, daemon_address.clone(), runtime_control);
            let services = register_hosted_skill_service(services, daemon_address.clone());
            let services =
                register_hosted_agent_management_service(services, daemon_address.clone());
            scope.set_services(services.build());
            scope.on_host_start(sinomo_agent::register_observe_metric_exporters);
            Ok(())
        },
    )
    .map_err(|error| error.to_string())?
    .with_config_definitions(register_hosted_agent_chat_config_definitions);
    Ok(Arc::new(factory))
}

fn register_hosted_agent_chat_config_definitions(
    builder: sinomo_runtime::config::ConfigRegistryBuilder,
) -> Result<sinomo_runtime::config::ConfigRegistryBuilder, String> {
    let builder =
        sinomo_lm::register_config_definitions(builder).map_err(|error| error.to_string())?;
    register_cli_runtime_config_definitions(builder)
}

thread_local! {
    static CLI_FORCE_LOCAL_MODE: Cell<bool> = const { Cell::new(false) };
    static CLI_OUTPUT_CAPTURE: RefCell<Option<CliOutputBuffer>> = const { RefCell::new(None) };
}

static CLI_RUNTIME_CONFIG: OnceLock<CliRuntimeConfig> = OnceLock::new();

#[cfg(unix)]
static PROCESS_STDIO_CAPTURE_LOCK: Mutex<()> = Mutex::new(());
#[cfg(all(unix, feature = "dfs-fuse"))]
static SIMPLE_SANDBOX_SIFS_MOUNTS: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<Mutex<sinomo_dfs_fuse::DfsFuseMountHandle>>>>,
> = OnceLock::new();

#[derive(Clone)]
struct SimpleSandboxSifsMount {
    mount_root: PathBuf,
    #[cfg(all(unix, feature = "dfs-fuse"))]
    handle: Arc<Mutex<sinomo_dfs_fuse::DfsFuseMountHandle>>,
}

#[derive(Clone)]
pub(crate) struct ResolvedWorkspaceRoot {
    root: PathBuf,
    _live_sifs_mount: Option<SimpleSandboxSifsMount>,
}

pub(crate) struct ResolvedWorkspaceBinding {
    binding: WorkspaceRef,
    _live_sifs_mount: Option<SimpleSandboxSifsMount>,
}

impl ResolvedWorkspaceBinding {
    pub(crate) fn binding(&self) -> &WorkspaceRef {
        &self.binding
    }
}

impl ResolvedWorkspaceRoot {
    fn local(root: PathBuf) -> Self {
        Self {
            root,
            _live_sifs_mount: None,
        }
    }

    fn live_sifs(root: PathBuf, mount: SimpleSandboxSifsMount) -> Self {
        Self {
            root,
            _live_sifs_mount: Some(mount),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }
}

impl SimpleSandboxSifsMount {
    fn mount_root(&self) -> &Path {
        &self.mount_root
    }

    fn retain_for_registry(&self, registry: &AgentToolRegistry) {
        #[cfg(all(unix, feature = "dfs-fuse"))]
        registry.retain_resource(Arc::clone(&self.handle));

        #[cfg(not(all(unix, feature = "dfs-fuse")))]
        let _ = registry;
    }
}

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

fn register_cli_runtime_config_definitions(
    mut builder: sinomo_runtime::config::ConfigRegistryBuilder,
) -> Result<sinomo_runtime::config::ConfigRegistryBuilder, String> {
    for hook in &cli_runtime_config().config_registry_hooks {
        builder = hook(builder)?;
    }
    Ok(builder)
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

#[cfg(all(test, unix))]
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

pub(crate) fn cli_command_requires_cluster_state(command: &RegisteredCliCommand) -> bool {
    command.policy().requires_cluster_state
}

pub(crate) fn cli_command_supports_degraded_local_mode(command: &RegisteredCliCommand) -> bool {
    command.policy().supports_degraded_local_mode
}

pub(crate) fn cli_command_is_help(command: &RegisteredCliCommand) -> bool {
    command.is_help()
}

fn cli_command_needs_daemon_probe(command: &RegisteredCliCommand) -> bool {
    cli_command_requires_cluster_state(command) || cli_command_supports_degraded_local_mode(command)
}

fn cli_command_enters_degraded_local_mode(
    command: &RegisteredCliCommand,
    daemon_available: bool,
    local_cluster_quorum: Option<bool>,
) -> bool {
    cli_command_supports_degraded_local_mode(command)
        && (!daemon_available || local_cluster_quorum == Some(false))
}

#[cfg(test)]
pub(crate) fn cli_invocation_force_local_mode(
    options: CliRuntimeOptions,
    command: &RegisteredCliCommand,
    daemon_available: bool,
    local_cluster_quorum: Option<bool>,
) -> bool {
    options.force_local
        || cli_command_enters_degraded_local_mode(command, daemon_available, local_cluster_quorum)
}

fn cli_local_cluster_quorum_if_relevant(
    supports_degraded_local_mode: bool,
    daemon: Option<&DaemonStatus>,
) -> Result<Option<bool>, CliError> {
    if daemon.is_none() || !supports_degraded_local_mode {
        return Ok(None);
    }
    let paths = cluster_paths_from_defaults()?;
    match read_cluster_state(&paths) {
        Ok(state) => Ok(Some(
            daemon.is_some_and(|daemon| local_daemon_matches_cluster_state(daemon, &state))
                && cluster_has_local_quorum(&paths, &state, daemon),
        )),
        Err(CliError::ClusterOrphan) => Ok(None),
        Err(error) => Err(error),
    }
}

fn dfs_command_requires_cluster_state(command: &DfsCliCommand) -> bool {
    match command {
        DfsCliCommand::Status(options) => options.cluster,
        DfsCliCommand::Services { .. }
        | DfsCliCommand::MetadataPublish { .. }
        | DfsCliCommand::MetadataShardTransferPrimary(_) => true,
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct CliRuntimeOptions {
    pub(crate) force_local: bool,
}

#[derive(Debug)]
pub(crate) struct CliInvocation {
    pub(crate) command: Option<RegisteredCliCommand>,
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

thread_local! {
    static CLI_DEFAULT_MAIN_DAEMON_STATUS: std::cell::RefCell<Option<DaemonStatus>> =
        const { std::cell::RefCell::new(None) };
}

struct CliDefaultMainDaemonStatusGuard {
    previous: Option<DaemonStatus>,
}

impl Drop for CliDefaultMainDaemonStatusGuard {
    fn drop(&mut self) {
        CLI_DEFAULT_MAIN_DAEMON_STATUS.with(|status| {
            status.replace(self.previous.take());
        });
    }
}

fn enter_cli_default_main_daemon_status(status: DaemonStatus) -> CliDefaultMainDaemonStatusGuard {
    let previous = CLI_DEFAULT_MAIN_DAEMON_STATUS.with(|cached| cached.replace(Some(status)));
    CliDefaultMainDaemonStatusGuard { previous }
}

pub(crate) fn cli_default_main_daemon_status() -> Option<DaemonStatus> {
    CLI_DEFAULT_MAIN_DAEMON_STATUS.with(|status| status.borrow().clone())
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

fn default_module_cli_command_config(
    local_data_dir: Option<&SinomoDataDir>,
) -> sinomo_runtime::module::ModuleCliCommandConfig {
    let module_config = sinomo_runtime::module::ModuleCliCommandConfig::new()
        .with_hot_unload_client(module_hot_unload_cli)
        .with_toggleable_native_rust_module_name(
            sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID,
            "distributed filesystem provider module",
        )
        .expect("DFS module id is valid")
        .with_toggleable_native_rust_module_name(
            sinomo_runtime::dfs_nfs::SINOMO_DFS_NFS_MODULE_LEGACY_ID,
            "direct SIFS NFS mount module",
        )
        .expect("NFS module id is valid")
        .with_toggleable_native_rust_module_name(
            sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID,
            "operational observation module",
        )
        .expect("observe module id is valid");

    local_data_dir
        .and_then(|data_dir| {
            module_config
                .clone()
                .with_installed_native_dylib_modules_from_data_dir(&data_dir.root())
                .ok()
        })
        .unwrap_or(module_config)
}

fn module_hot_unload_cli(
    module_id: &sinomo_runtime::module::ModuleId,
    drain_timeout: Duration,
    reason: &str,
) -> Result<sinomo_runtime::module::ModuleHotUnloadReport, String> {
    let status = daemon_default_main_daemon_status(Duration::from_secs(5))
        .map_err(|source| source.to_string())?
        .ok_or_else(|| "module hot unload requires the local daemon to be running".to_string())?;
    let request = network::ObjectRpcRequest::encode_body(
        sinomo_runtime::module::MODULE_HOT_UNLOAD_ROUTE,
        sinomo_runtime::module::MODULE_RPC_CATEGORY,
        &sinomo_runtime::module::ModuleHotUnloadRequest {
            module_id: module_id.to_string(),
            drain_timeout_ms: drain_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            reason: reason.to_string(),
        },
    )
    .map_err(|source| source.to_string())?;
    let timeout = drain_timeout.saturating_add(Duration::from_secs(10));
    daemon_send_daemon_rpc(&status.address, &request, timeout)
        .map_err(|source| source.to_string())?
        .decode_body()
        .map_err(|source| source.to_string())
}

pub fn default_cli_command_registry() -> CliCommandRegistry {
    let local_data_dir = SinomoDataDir::from_env_or_home().ok();
    let module_config = default_module_cli_command_config(local_data_dir.as_ref());
    let mut registry = CliCommandRegistry::new()
        .register(help_cli_command_handler())
        .register(cluster_cli_command_handler())
        .register(install_cli_command_handler())
        .register(uninstall_cli_command_handler())
        .register(daemon_cli_command_handler())
        .register(debug_cli_command_handler())
        .register(rollout_cli_command_handler())
        .register(dfs_cli_command_handler())
        .register(memory_cli_command_handler())
        .register(voice_cli_command_handler())
        .register(test_cli_command_handler())
        .register(agent_cli_command_handler())
        .register(skill_cli_command_handler())
        .register(models_cli_command_handler())
        .register(sinomo_runtime::module::module_cli_command_handler(
            module_config,
        ))
        .register(
            sinomo_runtime::observe::observe_cli_command_handler_with_cluster_rpc(
                observe_cli_rpc,
                observe_cli_cluster_targets,
            ),
        )
        .register(
            sinomo_runtime::remote_management::remote_cli_command_handler(
                remote_management_cli_rpc,
            ),
        )
        .register(sinomo_runtime::sandbox::sandbox_cli_command_handler(
            Arc::new(SinomoCliSandboxHost),
        ));
    registry = sinomo_lm::cli::register_cli_commands(registry, run_provider_command, usage)
        .expect("module-owned model commands must not conflict");
    if let Some(data_dir) = local_data_dir {
        registry = registry.register(
            sinomo_runtime::remote_management::node_permissions_cli_command_handler(data_dir),
        );
    }
    registry
        .validate_reserved_names(&[])
        .expect("registered CLI commands must not conflict with built-in commands");
    registry
}

fn remote_management_cli_rpc(
    target_node_id: Option<String>,
    call: network::ObjectRpcCall,
) -> sinomo_runtime::remote_management::RemoteManagementCliRpcFuture {
    Box::pin(async move {
        // Shell capture may run for five minutes and file sessions may be
        // arbitrarily large. Connect is short; the idle timeout is a
        // no-progress policy and is reset by every transferred frame.
        const REMOTE_MANAGEMENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
        const REMOTE_MANAGEMENT_IDLE_TIMEOUT: Duration = Duration::from_secs(310);
        let data_dir = SinomoDataDir::from_env_or_home().map_err(|source| source.to_string())?;
        data_dir
            .ensure_root_layout()
            .map_err(|source| source.to_string())?;
        let paths = DaemonPaths::new(&data_dir);
        let runtime = tokio::runtime::Handle::current();
        // This closure already runs inside the remote session runtime. Probe on
        // that runtime instead of recursively entering the synchronous client.
        let status =
            match probe_daemon_status_async(&paths, REMOTE_MANAGEMENT_CONNECT_TIMEOUT, &runtime)
                .await
            {
                DaemonProbeStatus::Running(status) => status,
                DaemonProbeStatus::Locked { .. } | DaemonProbeStatus::NotRunning => {
                    return Err(
                        "remote management requires the local daemon to be running".to_string()
                    );
                }
            };
        let target_node_id = match target_node_id {
            Some(target_node_id) => NodeId::new(target_node_id.clone()).map_err(|source| {
                format!("invalid remote-management target node id `{target_node_id}`: {source}")
            })?,
            None => status
                .cluster
                .as_ref()
                .map(|cluster| cluster.node_id.clone())
                .ok_or_else(|| {
                    "remote management requires the local daemon to have cluster identity"
                        .to_string()
                })?,
        };
        let timeouts = network::DaemonRpcTimeouts::connect_only(REMOTE_MANAGEMENT_CONNECT_TIMEOUT)
            .with_idle_timeout(Some(REMOTE_MANAGEMENT_IDLE_TIMEOUT));
        let options = network::ClusterObjectRpcOptions::new(timeouts);
        if status
            .cluster
            .as_ref()
            .is_some_and(|cluster| cluster.node_id == target_node_id)
        {
            daemon_send_daemon_rpc_call_async(&status.address, call, options, &runtime)
                .await
                .map_err(|source| source.to_string())
        } else {
            // The running daemon owns the cluster transports and their module
            // lifecycle. A short-lived CLI process has no active Iroh module,
            // so forwarding must cross the authenticated loopback daemon
            // boundary instead of constructing a routed client here.
            DaemonApiClient::at_address(status.address)
                .object_rpc_call_to_target_with_transport_timeouts_and_affinity_async(
                    &target_node_id,
                    call,
                    timeouts,
                    None,
                    true,
                    sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                )
                .await
                .map_err(|source| source.to_string())
        }
    })
}

fn cli_command_registry() -> CliCommandRegistry {
    let local = cli_runtime_config().command_registry.clone();
    #[cfg(test)]
    {
        local
    }
    #[cfg(not(test))]
    {
        module_commands::discover(local.clone()).unwrap_or(local)
    }
}

fn observe_cli_rpc(
    target_node_id: Option<&str>,
    request: network::ObjectRpcRequest,
) -> Result<network::ObjectRpcResponse, String> {
    const OBSERVE_RPC_TIMEOUT: Duration = Duration::from_secs(5);
    let status = daemon_default_main_daemon_status(OBSERVE_RPC_TIMEOUT)
        .map_err(|source| source.to_string())?
        .ok_or_else(|| "observe live queries require the local daemon to be running".to_string())?;
    observe_cli_rpc_via_daemon(
        &status,
        target_node_id,
        &request,
        |address, request| {
            daemon_send_daemon_rpc(address, request, OBSERVE_RPC_TIMEOUT)
                .map_err(|source| source.to_string())
        },
        |address, target_node_id, request| {
            DaemonApiClient::at_address(address.to_string())
                .object_rpc_to_target_with_deadline(
                    target_node_id,
                    request,
                    OBSERVE_RPC_TIMEOUT,
                    Some(OBSERVE_RPC_TIMEOUT),
                    true,
                )
                .map_err(|source| source.to_string())
        },
    )
}

fn observe_cli_rpc_via_daemon<DirectRpc, ForwardRpc>(
    status: &DaemonStatus,
    target_node_id: Option<&str>,
    request: &network::ObjectRpcRequest,
    direct_rpc: DirectRpc,
    forward_rpc: ForwardRpc,
) -> Result<network::ObjectRpcResponse, String>
where
    DirectRpc:
        FnOnce(&str, &network::ObjectRpcRequest) -> Result<network::ObjectRpcResponse, String>,
    ForwardRpc: FnOnce(
        &str,
        &NodeId,
        &network::ObjectRpcRequest,
    ) -> Result<network::ObjectRpcResponse, String>,
{
    if let Some(target_node_id) = target_node_id {
        let target_node_id = NodeId::new(target_node_id.to_string()).map_err(|source| {
            format!("invalid observe target node id `{target_node_id}`: {source}")
        })?;
        if status
            .cluster
            .as_ref()
            .is_some_and(|cluster| cluster.node_id == target_node_id)
        {
            return direct_rpc(&status.address, request);
        }
        return forward_rpc(&status.address, &target_node_id, request);
    }
    direct_rpc(&status.address, request)
}

fn observe_cli_cluster_targets() -> Result<Vec<String>, String> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(|source| source.to_string())?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = cluster::read_cluster_state_without_kv(&cluster_paths)
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
    if args.first().is_some_and(|arg| arg == "--local") {
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

pub(crate) fn parse_cli_command(args: &[String]) -> Result<Option<RegisteredCliCommand>, CliError> {
    if args.is_empty() {
        return Ok(None);
    }
    if let Some(command) = cli_runtime_config().command_registry.parse(args)? {
        return Ok(Some(command));
    }
    module_commands::discover(cli_runtime_config().command_registry.clone())
        .map_err(|detail| CliError::Parse { detail })?
        .parse(args)?
        .ok_or(CliError::Usage)
        .map(Some)
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum AgentCliCommand {
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
    Inspect(AgentSessionInspectOptions),
    Progress(AgentSessionProgressOptions),
    TerminalResults(AgentSessionTerminalResultsOptions),
    Steer(AgentSessionMessageOptions),
    FollowUp(AgentSessionMessageOptions),
    Interrupt(AgentSessionMessageOptions),
    Resume(AgentSessionResumeOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum SkillCliCommand {
    List(SkillListOptions),
    Show(SkillShowOptions),
    Import(SkillImportOptions),
    Export(SkillExportOptions),
    Activate(SkillActivateOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SkillListOptions {
    pub all: bool,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SkillShowOptions {
    pub skill_id: String,
    pub version: Option<String>,
    pub text: bool,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SkillImportOptions {
    pub path: PathBuf,
    pub skill_id: Option<String>,
    pub version: String,
    pub source: SkillCatalogSourceKind,
    pub source_id: String,
    pub activate: bool,
    pub scope: String,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SkillExportOptions {
    pub skill_id: String,
    pub version: Option<String>,
    pub output: Option<PathBuf>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SkillActivateOptions {
    pub skill_id: String,
    pub version: Option<String>,
    pub scope: String,
    pub reason: String,
    pub json: bool,
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
    pub agent: String,
    pub agent_explicit: bool,
    pub provider: String,
    pub provider_explicit: bool,
    pub model: String,
    pub model_explicit: bool,
    pub account: Option<String>,
    pub session_id: Option<SessionId>,
    pub compact_after: bool,
    pub json: bool,
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

pub(crate) fn agent_cli_command_handler() -> DeclarativeCommandHandler<AgentCliCommand> {
    DeclarativeCommandHandler::new(
        "agent",
        CliCommandPolicy::local_capable(),
        agent_cli_spec,
        parse_agent_matches,
        run_agent_command,
    )
    .with_parsed_policy(agent_cli_policy)
}

pub(crate) fn skill_cli_command_handler() -> DeclarativeCommandHandler<SkillCliCommand> {
    DeclarativeCommandHandler::new(
        "skill",
        CliCommandPolicy::local_capable(),
        skill_cli_spec,
        parse_skill_matches,
        run_skill_command,
    )
    .with_parsed_policy(skill_cli_policy)
}

fn agent_cli_policy(command: &AgentCliCommand) -> CliCommandPolicy {
    match command {
        AgentCliCommand::Inspect(_)
        | AgentCliCommand::Export(_)
        | AgentCliCommand::Run(_)
        | AgentCliCommand::Validate(AgentValidateOptions { file: None, .. }) => {
            CliCommandPolicy::default()
        }
        AgentCliCommand::Import(_)
        | AgentCliCommand::Readiness(_)
        | AgentCliCommand::Session(_)
        | AgentCliCommand::Validate(AgentValidateOptions { file: Some(_), .. }) => {
            CliCommandPolicy::local_capable()
        }
    }
}

fn skill_cli_policy(command: &SkillCliCommand) -> CliCommandPolicy {
    match command {
        SkillCliCommand::List(_) | SkillCliCommand::Show(_) | SkillCliCommand::Export(_) => {
            CliCommandPolicy::default()
        }
        SkillCliCommand::Import(_) | SkillCliCommand::Activate(_) => {
            CliCommandPolicy::local_capable()
        }
    }
}

fn agent_cli_spec() -> CliCommandSpec {
    let json = || CliOptionSpec::flag("--json", "print JSON output");
    let session = || {
        CliOptionSpec::value(
            "--session",
            "agent session id",
            CliValueSpec::new("session-id", "agent session id"),
        )
        .with_aliases(["--session-id"])
    };
    let optional_agent =
        || CliPositionalSpec::optional(CliValueSpec::new("agent-id", "agent package id"));
    let session_positional =
        || CliPositionalSpec::optional(CliValueSpec::new("session-id", "agent session id"));
    let message_command = |name: &'static str, about: &'static str| {
        CliCommandSpec::new(name, about)
            .with_options([
                session(),
                CliOptionSpec::value(
                    "--message",
                    "message or reason text",
                    CliValueSpec::new("text", "message text"),
                )
                .with_aliases(["--reason"])
                .repeatable(),
                json(),
            ])
            .with_positionals([
                session_positional(),
                CliPositionalSpec::optional(CliValueSpec::new("message", "message text"))
                    .repeatable(),
            ])
    };
    let sessions = CliCommandSpec::new("session", "inspect and control agent sessions")
        .with_aliases(["sessions"])
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("inspect", "inspect persisted agent session state")
                .with_options([session(), json()])
                .with_positionals([session_positional()]),
            CliCommandSpec::new("progress", "read agent session control-plane events")
                .with_options([
                    session(),
                    CliOptionSpec::value(
                        "--cursor",
                        "event cursor offset",
                        CliValueSpec::new("offset", "event cursor offset"),
                    )
                    .with_aliases(["--offset"]),
                    CliOptionSpec::value(
                        "--last-event",
                        "last observed event id",
                        CliValueSpec::new("event-id", "event id"),
                    )
                    .with_aliases(["--last-event-id"]),
                    json(),
                ])
                .with_positionals([session_positional()]),
            CliCommandSpec::new("terminal-results", "read terminal agent task results")
                .with_aliases(["results"])
                .with_options([
                    session(),
                    CliOptionSpec::value(
                        "--task",
                        "filter by task id",
                        CliValueSpec::new("task-id", "agent task id"),
                    )
                    .with_aliases(["--task-id"]),
                    json(),
                ])
                .with_positionals([session_positional()]),
            message_command("steer", "steer a live agent session"),
            message_command("follow-up", "queue a follow-up for a live agent session")
                .with_aliases(["followup"]),
            message_command("interrupt", "interrupt a live agent session"),
            CliCommandSpec::new("resume", "reattach to a live agent session")
                .with_options([session(), json()])
                .with_positionals([session_positional()]),
        ]);
    CliCommandSpec::new("agent", "run and inspect Sinomo agents")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("export", "export an agent package")
                .with_options([CliOptionSpec::value(
                    "--output",
                    "output JSON path",
                    CliValueSpec::new("path", "output path"),
                )
                .with_aliases(["-o"])])
                .with_positionals([optional_agent()]),
            CliCommandSpec::new("import", "import an agent package")
                .with_options([
                    CliOptionSpec::value(
                        "--file",
                        "agent package JSON path",
                        CliValueSpec::new("path", "package path"),
                    ),
                    json(),
                ])
                .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                    "path",
                    "package path",
                ))]),
            CliCommandSpec::new("inspect", "inspect an agent package")
                .with_options([
                    CliOptionSpec::value(
                        "--agent",
                        "agent package id",
                        CliValueSpec::new("agent-id", "agent package id"),
                    ),
                    json(),
                ])
                .with_positionals([optional_agent()]),
            CliCommandSpec::new("readiness", "run deterministic local agent readiness gates")
                .with_aliases(["ready"])
                .with_options([json()]),
            CliCommandSpec::new(
                "run",
                "run one agent turn with durable session resume and persisted context projection",
            )
            .with_options([
                CliOptionSpec::value(
                    "--prompt",
                    "agent prompt",
                    CliValueSpec::new("text", "prompt text"),
                ),
                CliOptionSpec::value(
                    "--agent",
                    "agent package id",
                    CliValueSpec::new("agent-id", "agent package id"),
                ),
                CliOptionSpec::value(
                    "--provider",
                    "model provider",
                    CliValueSpec::new("provider", "provider id"),
                ),
                CliOptionSpec::value(
                    "--model",
                    "model id",
                    CliValueSpec::new("model", "model id"),
                ),
                CliOptionSpec::value(
                    "--account",
                    "provider account",
                    CliValueSpec::new("account", "provider account"),
                ),
                session(),
                CliOptionSpec::flag("--compact-after", "compact the session after the turn"),
                json(),
            ])
            .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                "prompt",
                "agent prompt",
            ))]),
            sessions,
            CliCommandSpec::new("validate", "validate an agent package")
                .with_options([
                    CliOptionSpec::value(
                        "--file",
                        "agent package JSON path",
                        CliValueSpec::new("path", "package path"),
                    ),
                    json(),
                ])
                .with_positionals([optional_agent()]),
        ])
}

fn skill_cli_spec() -> CliCommandSpec {
    let json = || CliOptionSpec::flag("--json", "print JSON output");
    let version = || {
        CliOptionSpec::value(
            "--version",
            "skill version",
            CliValueSpec::new("version", "skill version"),
        )
    };
    let skill = || {
        CliOptionSpec::value(
            "--skill",
            "skill id",
            CliValueSpec::new("skill-id", "skill id"),
        )
        .with_aliases(["--id"])
    };
    let optional_skill = || CliPositionalSpec::optional(CliValueSpec::new("skill-id", "skill id"));
    CliCommandSpec::new("skill", "manage standard Agent Skills packages in SIFS")
        .with_aliases(["skills"])
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("list", "list catalog skills")
                .with_aliases(["ls"])
                .with_options([
                    CliOptionSpec::flag("--all", "include inactive versions").with_aliases(["-a"]),
                    json(),
                ]),
            CliCommandSpec::new("show", "show a skill descriptor")
                .with_aliases(["inspect", "describe"])
                .with_options([
                    skill(),
                    version(),
                    CliOptionSpec::flag("--text", "include stored SKILL.md text")
                        .with_aliases(["--markdown"]),
                    json(),
                ])
                .with_positionals([optional_skill()]),
            CliCommandSpec::new(
                "import",
                "validate and import an Agent Skills package directory",
            )
            .with_options([
                CliOptionSpec::value(
                    "--file",
                    "skill directory or its SKILL.md file",
                    CliValueSpec::new("path", "skill path"),
                )
                .with_aliases(["--path"]),
                skill(),
                version(),
                CliOptionSpec::value(
                    "--source",
                    "catalog source kind",
                    CliValueSpec::new("kind", "source kind")
                        .with_choices([
                            CliAutocompleteChoice::new("sinomo", "built-in Sinomo skill"),
                            CliAutocompleteChoice::new("module", "module-provided skill"),
                            CliAutocompleteChoice::new("user", "user-provided skill"),
                            CliAutocompleteChoice::new("agent", "agent-provided skill"),
                            CliAutocompleteChoice::new("evolution", "evolution-provided skill"),
                        ])
                        .restricted(),
                ),
                CliOptionSpec::value(
                    "--source-id",
                    "source identity",
                    CliValueSpec::new("id", "source identity"),
                ),
                CliOptionSpec::flag("--activate", "activate the imported skill"),
                CliOptionSpec::flag("--no-activate", "leave the imported skill inactive"),
                CliOptionSpec::value(
                    "--scope",
                    "activation scope",
                    CliValueSpec::new("scope", "activation scope"),
                ),
                json(),
            ])
            .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                "path",
                "skill path",
            ))]),
            CliCommandSpec::new(
                "export",
                "export a catalog skill as an Agent Skills directory",
            )
            .with_options([
                skill(),
                version(),
                CliOptionSpec::value(
                    "--output",
                    "output directory",
                    CliValueSpec::new("path", "output directory"),
                )
                .with_aliases(["-o"]),
                json(),
            ])
            .with_positionals([optional_skill()]),
            CliCommandSpec::new("activate", "activate a catalog skill")
                .with_aliases(["enable"])
                .with_options([
                    skill(),
                    version(),
                    CliOptionSpec::value(
                        "--scope",
                        "activation scope",
                        CliValueSpec::new("scope", "activation scope"),
                    ),
                    CliOptionSpec::value(
                        "--reason",
                        "activation reason",
                        CliValueSpec::new("text", "activation reason"),
                    ),
                    json(),
                ])
                .with_positionals([optional_skill()]),
        ])
}

fn parse_agent_matches(matches: &CliCommandMatches) -> Result<AgentCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
        ["agent", "export"] => Ok(AgentCliCommand::Export(AgentExportOptions {
            agent: matches
                .positionals()
                .first()
                .cloned()
                .unwrap_or_else(|| "main-agent".to_string()),
            output: matches.value("--output").map(PathBuf::from),
        })),
        ["agent", "import"] => {
            let path = matches
                .value("--file")
                .or_else(|| matches.positionals().first().map(String::as_str))
                .ok_or_else(|| CliParseError::Parse {
                    detail: "agent import requires a package JSON path".to_string(),
                })?;
            Ok(AgentCliCommand::Import(AgentImportOptions {
                path: PathBuf::from(path),
                json: matches.contains("--json"),
            }))
        }
        ["agent", "inspect"] => Ok(AgentCliCommand::Inspect(AgentInspectOptions {
            agent: matches
                .value("--agent")
                .or_else(|| matches.positionals().first().map(String::as_str))
                .unwrap_or("main-agent")
                .to_string(),
            json: matches.contains("--json"),
        })),
        ["agent", "readiness"] => Ok(AgentCliCommand::Readiness(AgentReadinessOptions {
            json: matches.contains("--json"),
        })),
        ["agent", "run"] => parse_agent_run_matches(matches).map(AgentCliCommand::Run),
        ["agent", "session", command] => {
            parse_agent_session_matches(command, matches).map(AgentCliCommand::Session)
        }
        ["agent", "validate"] => Ok(AgentCliCommand::Validate(AgentValidateOptions {
            agent: matches
                .positionals()
                .first()
                .cloned()
                .unwrap_or_else(|| "main-agent".to_string()),
            file: matches.value("--file").map(PathBuf::from),
            json: matches.contains("--json"),
        })),
        _ => Err(CliParseError::Parse {
            detail: "agent requires a subcommand".to_string(),
        }),
    }
}

fn parse_agent_run_matches(matches: &CliCommandMatches) -> Result<AgentRunOptions, CliParseError> {
    let prompt = matches
        .value("--prompt")
        .or_else(|| matches.positionals().first().map(String::as_str))
        .ok_or_else(|| CliParseError::Parse {
            detail: "agent run requires --prompt <text>".to_string(),
        })?;
    let agent_explicit = matches.contains("--agent");
    let provider_explicit = matches.contains("--provider");
    let model_explicit = matches.contains("--model");
    let session_id = matches
        .value("--session")
        .map(parse_agent_session_id)
        .transpose()
        .map_err(semantic_parse_error)?;
    Ok(AgentRunOptions {
        prompt: prompt.to_string(),
        agent: matches.value("--agent").unwrap_or("main-agent").to_string(),
        agent_explicit,
        provider: matches
            .value("--provider")
            .unwrap_or(CODEX_PROVIDER_ID)
            .to_string(),
        provider_explicit,
        model: matches
            .value("--model")
            .unwrap_or(CODEX_DEFAULT_MODEL)
            .to_string(),
        model_explicit,
        account: matches.value("--account").map(str::to_string),
        session_id,
        compact_after: matches.contains("--compact-after"),
        json: matches.contains("--json"),
    })
}

fn agent_session_id_from_matches(matches: &CliCommandMatches) -> Result<SessionId, CliParseError> {
    let value = matches
        .value("--session")
        .or_else(|| matches.positionals().first().map(String::as_str))
        .ok_or_else(|| CliParseError::Parse {
            detail: format!(
                "agent session {} requires a session id",
                matches.command_name()
            ),
        })?;
    parse_agent_session_id(value).map_err(semantic_parse_error)
}

fn parse_agent_session_matches(
    command: &str,
    matches: &CliCommandMatches,
) -> Result<AgentSessionCliCommand, CliParseError> {
    let session_id = agent_session_id_from_matches(matches)?;
    let json = matches.contains("--json");
    match command {
        "inspect" => Ok(AgentSessionCliCommand::Inspect(
            AgentSessionInspectOptions { session_id, json },
        )),
        "progress" => {
            let offset = matches
                .value("--cursor")
                .map(|value| {
                    value
                        .parse::<usize>()
                        .map_err(|source| CliParseError::Parse {
                            detail: format!("invalid --cursor value `{value}`: {source}"),
                        })
                })
                .transpose()?;
            let last_event_id = matches
                .value("--last-event")
                .map(|value| {
                    TaskId::from_str(value).map_err(|source| CliParseError::Parse {
                        detail: format!("invalid agent task id `{value}`: {source}"),
                    })
                })
                .transpose()?;
            let cursor = (offset.is_some() || last_event_id.is_some()).then(|| {
                AgentSessionControlEventCursor {
                    offset: offset.unwrap_or_default(),
                    last_event_id,
                }
            });
            Ok(AgentSessionCliCommand::Progress(
                AgentSessionProgressOptions {
                    session_id,
                    cursor,
                    json,
                },
            ))
        }
        "terminal-results" => {
            let task_id = matches
                .value("--task")
                .map(|value| {
                    TaskId::from_str(value).map_err(|source| CliParseError::Parse {
                        detail: format!("invalid agent task id `{value}`: {source}"),
                    })
                })
                .transpose()?;
            Ok(AgentSessionCliCommand::TerminalResults(
                AgentSessionTerminalResultsOptions {
                    session_id,
                    task_id,
                    json,
                },
            ))
        }
        "steer" | "follow-up" | "interrupt" => {
            let positional_start = usize::from(!matches.contains("--session"));
            let message = matches
                .values("--message")
                .chain(
                    matches
                        .positionals()
                        .iter()
                        .skip(positional_start)
                        .map(String::as_str),
                )
                .collect::<Vec<_>>()
                .join(" ");
            if message.trim().is_empty() {
                return Err(CliParseError::Parse {
                    detail: format!("agent session {command} requires a message"),
                });
            }
            let options = AgentSessionMessageOptions {
                session_id,
                message,
                json,
            };
            match command {
                "steer" => Ok(AgentSessionCliCommand::Steer(options)),
                "follow-up" => Ok(AgentSessionCliCommand::FollowUp(options)),
                "interrupt" => Ok(AgentSessionCliCommand::Interrupt(options)),
                _ => unreachable!(),
            }
        }
        "resume" => Ok(AgentSessionCliCommand::Resume(AgentSessionResumeOptions {
            session_id,
            json,
        })),
        _ => Err(CliParseError::Parse {
            detail: format!("unknown agent session command `{command}`"),
        }),
    }
}

fn parse_skill_matches(matches: &CliCommandMatches) -> Result<SkillCliCommand, CliParseError> {
    let skill_id = || {
        matches
            .value("--skill")
            .or_else(|| matches.positionals().first().map(String::as_str))
            .map(str::to_string)
    };
    match matches.command_name() {
        "list" => Ok(SkillCliCommand::List(SkillListOptions {
            all: matches.contains("--all"),
            json: matches.contains("--json"),
        })),
        "show" => Ok(SkillCliCommand::Show(SkillShowOptions {
            skill_id: skill_id().ok_or_else(|| CliParseError::Parse {
                detail: "skill show requires a skill id".to_string(),
            })?,
            version: matches.value("--version").map(str::to_string),
            text: matches.contains("--text"),
            json: matches.contains("--json"),
        })),
        "import" => {
            if matches.contains("--activate") && matches.contains("--no-activate") {
                return Err(CliParseError::Parse {
                    detail: "skill import cannot combine --activate and --no-activate".to_string(),
                });
            }
            let path = matches
                .value("--file")
                .or_else(|| matches.positionals().first().map(String::as_str))
                .ok_or_else(|| CliParseError::Parse {
                    detail: "skill import requires an Agent Skills package directory".to_string(),
                })?;
            let source = matches
                .value("--source")
                .map(parse_skill_source_kind)
                .transpose()
                .map_err(semantic_parse_error)?
                .unwrap_or(SkillCatalogSourceKind::User);
            Ok(SkillCliCommand::Import(SkillImportOptions {
                path: PathBuf::from(path),
                skill_id: matches.value("--skill").map(str::to_string),
                version: matches.value("--version").unwrap_or("1.0.0").to_string(),
                source,
                source_id: matches
                    .value("--source-id")
                    .unwrap_or("local-user")
                    .to_string(),
                activate: !matches.contains("--no-activate"),
                scope: matches
                    .value("--scope")
                    .unwrap_or(SkillActivationScope::Cluster.as_str())
                    .to_string(),
                json: matches.contains("--json"),
            }))
        }
        "export" => Ok(SkillCliCommand::Export(SkillExportOptions {
            skill_id: skill_id().ok_or_else(|| CliParseError::Parse {
                detail: "skill export requires a skill id".to_string(),
            })?,
            version: matches.value("--version").map(str::to_string),
            output: matches.value("--output").map(PathBuf::from),
            json: matches.contains("--json"),
        })),
        "activate" => Ok(SkillCliCommand::Activate(SkillActivateOptions {
            skill_id: skill_id().ok_or_else(|| CliParseError::Parse {
                detail: "skill activate requires a skill id".to_string(),
            })?,
            version: matches.value("--version").map(str::to_string),
            scope: matches
                .value("--scope")
                .unwrap_or(SkillActivationScope::Cluster.as_str())
                .to_string(),
            reason: matches
                .value("--reason")
                .unwrap_or("manual skill activation")
                .to_string(),
            json: matches.contains("--json"),
        })),
        _ => Err(CliParseError::Parse {
            detail: "skill requires a subcommand".to_string(),
        }),
    }
}

#[cfg(test)]
fn prefixed_schema_args(prefix: &[&str], args: &[String]) -> Vec<String> {
    prefix
        .iter()
        .map(|value| (*value).to_string())
        .chain(args.iter().cloned())
        .collect()
}

#[cfg(test)]
pub(crate) fn parse_agent_schema_for_test(args: &[String]) -> Result<AgentCliCommand, CliError> {
    parse_schema_command_for_test(agent_cli_spec(), args, parse_agent_matches)
}

#[cfg(test)]
pub(crate) fn parse_agent_session_schema_for_test(
    args: &[String],
) -> Result<AgentSessionCliCommand, CliError> {
    match parse_agent_schema_for_test(&prefixed_schema_args(&["session"], args))? {
        AgentCliCommand::Session(command) => Ok(command),
        _ => unreachable!("session schema returned a non-session command"),
    }
}

#[cfg(test)]
macro_rules! agent_schema_options_for_test {
    ($function:ident, $command:literal, $variant:path, $output:ty) => {
        pub(crate) fn $function(args: &[String]) -> Result<$output, CliError> {
            match parse_agent_schema_for_test(&prefixed_schema_args(&[$command], args))? {
                $variant(options) => Ok(options),
                _ => unreachable!("agent schema returned the wrong command"),
            }
        }
    };
}

#[cfg(test)]
agent_schema_options_for_test!(
    parse_agent_run_schema_for_test,
    "run",
    AgentCliCommand::Run,
    AgentRunOptions
);
#[cfg(test)]
agent_schema_options_for_test!(
    parse_agent_inspect_schema_for_test,
    "inspect",
    AgentCliCommand::Inspect,
    AgentInspectOptions
);
#[cfg(test)]
agent_schema_options_for_test!(
    parse_agent_export_schema_for_test,
    "export",
    AgentCliCommand::Export,
    AgentExportOptions
);
#[cfg(test)]
agent_schema_options_for_test!(
    parse_agent_import_schema_for_test,
    "import",
    AgentCliCommand::Import,
    AgentImportOptions
);
#[cfg(test)]
agent_schema_options_for_test!(
    parse_agent_readiness_schema_for_test,
    "readiness",
    AgentCliCommand::Readiness,
    AgentReadinessOptions
);
#[cfg(test)]
agent_schema_options_for_test!(
    parse_agent_validate_schema_for_test,
    "validate",
    AgentCliCommand::Validate,
    AgentValidateOptions
);

#[cfg(test)]
fn parse_skill_schema_for_test(args: &[String]) -> Result<SkillCliCommand, CliError> {
    parse_schema_command_for_test(skill_cli_spec(), args, parse_skill_matches)
}

#[cfg(test)]
macro_rules! skill_schema_options_for_test {
    ($function:ident, $command:literal, $variant:path, $output:ty) => {
        pub(crate) fn $function(args: &[String]) -> Result<$output, CliError> {
            match parse_skill_schema_for_test(&prefixed_schema_args(&[$command], args))? {
                $variant(options) => Ok(options),
                _ => unreachable!("skill schema returned the wrong command"),
            }
        }
    };
}

#[cfg(test)]
skill_schema_options_for_test!(
    parse_skill_list_schema_for_test,
    "list",
    SkillCliCommand::List,
    SkillListOptions
);
#[cfg(test)]
skill_schema_options_for_test!(
    parse_skill_show_schema_for_test,
    "show",
    SkillCliCommand::Show,
    SkillShowOptions
);
#[cfg(test)]
skill_schema_options_for_test!(
    parse_skill_import_schema_for_test,
    "import",
    SkillCliCommand::Import,
    SkillImportOptions
);
#[cfg(test)]
skill_schema_options_for_test!(
    parse_skill_export_schema_for_test,
    "export",
    SkillCliCommand::Export,
    SkillExportOptions
);
#[cfg(test)]
skill_schema_options_for_test!(
    parse_skill_activate_schema_for_test,
    "activate",
    SkillCliCommand::Activate,
    SkillActivateOptions
);

fn parse_skill_source_kind(value: &str) -> Result<SkillCatalogSourceKind, CliError> {
    match value {
        "sinomo" => Ok(SkillCatalogSourceKind::Sinomo),
        "module" => Ok(SkillCatalogSourceKind::Module),
        "user" => Ok(SkillCatalogSourceKind::User),
        "agent" => Ok(SkillCatalogSourceKind::Agent),
        "evolution" => Ok(SkillCatalogSourceKind::Evolution),
        _ => Err(CliError::Parse {
            detail: format!(
                "invalid skill source `{value}`; expected sinomo, module, user, agent, or evolution"
            ),
        }),
    }
}

fn parse_agent_session_id(value: &str) -> Result<SessionId, CliError> {
    SessionId::from_str(value)
        .or_else(|_| SessionId::from_legacy_named(value))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid agent session id `{value}`: {source}"),
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
        self.complete_streaming_events(request, &mut |event| {
            if let LanguageModelStreamEvent::TextDelta { delta } = event {
                on_delta(&delta);
            }
        })
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
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
            on_event(LanguageModelStreamEvent::Started {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: model.clone(),
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
                    &mut |delta| {
                        on_event(LanguageModelStreamEvent::TextDelta {
                            delta: delta.to_string(),
                        });
                    },
                )
                .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                    provider: MINICPM_PROVIDER_ID.to_string(),
                    model: self.model.clone(),
                    detail: source.to_string(),
                })?;
            if let Some(usage) = &response.usage {
                on_event(LanguageModelStreamEvent::Usage {
                    usage: usage.clone(),
                });
            }
            on_event(LanguageModelStreamEvent::Finished {
                finish_reason: None,
            });
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
            on_event(LanguageModelStreamEvent::Started {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: model.clone(),
            });
            let response =
                self.complete_streaming_with_managed_pytorch(request, &model, &mut |delta| {
                    on_event(LanguageModelStreamEvent::TextDelta {
                        delta: delta.to_string(),
                    });
                })?;
            if let Some(usage) = &response.usage {
                on_event(LanguageModelStreamEvent::Usage {
                    usage: usage.clone(),
                });
            }
            on_event(LanguageModelStreamEvent::Finished {
                finish_reason: None,
            });
            return Ok(response);
        }
        complete_configured_model_request_streaming_events_with_account(
            &self.data_dir,
            &self.provider,
            &self.model,
            self.account.as_ref(),
            None,
            request,
            on_event,
        )
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation_token: &AgentCancellationToken,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if self.provider != MINICPM_PROVIDER_ID {
            let token = cancellation_token.clone();
            let cancellation = LanguageModelCancellation::new(move || token.is_cancelled());
            return complete_configured_model_request_streaming_events_with_account_and_cancellation(
                &self.data_dir,
                &self.provider,
                &self.model,
                self.account.as_ref(),
                None,
                request,
                &cancellation,
                on_event,
            );
        }

        if cancellation_token.is_cancelled() {
            return Err(configured_model_cancelled_error(request, "before request"));
        }
        let response = self.complete_streaming_events(request, &mut |event| {
            if !cancellation_token.is_cancelled() {
                on_event(event);
            }
        })?;
        if cancellation_token.is_cancelled() {
            return Err(configured_model_cancelled_error(request, "during response"));
        }
        Ok(response)
    }
}

fn configured_model_cancelled_error(
    request: &LanguageModelRequest,
    phase: &'static str,
) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: request.provider.clone(),
        model: request.model.clone(),
        detail: format!("model invocation cancelled {phase}"),
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
        ManagedPytorchServer::new_for_data_dir(
            ManagedPytorchServerConfig {
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
            },
            self.data_dir.root(),
        )
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

pub(crate) fn run_skill_command(command: SkillCliCommand) -> Result<(), CliError> {
    match command {
        SkillCliCommand::List(options) => list_skills(options),
        SkillCliCommand::Show(options) => show_skill(options),
        SkillCliCommand::Import(options) => import_skill(options),
        SkillCliCommand::Export(options) => export_skill(options),
        SkillCliCommand::Activate(options) => activate_skill(options),
    }
}

fn list_skills(options: SkillListOptions) -> Result<(), CliError> {
    let store = skill_read_store_for_cli()?;
    let catalog = store.load_catalog().map_err(skill_catalog_cli_error)?;
    let active_keys = catalog
        .activations
        .iter()
        .map(|activation| (activation.skill_id.clone(), activation.version.clone()))
        .collect::<BTreeSet<_>>();
    let entries = if options.all {
        catalog.entries.iter().collect::<Vec<_>>()
    } else {
        catalog.active_entries_for_scopes([SkillActivationScope::Cluster.as_str()])
    };
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "count": entries.len(),
                "skills": entries
                    .iter()
                    .map(|entry| skill_entry_json(entry, active_keys.contains(&(entry.skill_id.clone(), entry.version.clone()))))
                    .collect::<Vec<_>>(),
            }))
            .map_err(|source| CliError::Json {
                path: "<skill list>".to_string(),
                detail: source.to_string(),
            })?
        );
        return Ok(());
    }
    println!(
        "Skills: {}{}",
        entries.len(),
        if options.all {
            " known version(s)"
        } else {
            " active"
        }
    );
    for entry in entries {
        let active = if active_keys.contains(&(entry.skill_id.clone(), entry.version.clone())) {
            " active"
        } else {
            ""
        };
        let description = if entry.description.is_empty() {
            ""
        } else {
            entry.description.as_str()
        };
        println!(
            "  {}@{} [{}{}] {}",
            entry.skill_id,
            entry.version,
            entry.source.kind.as_str(),
            active,
            description
        );
    }
    Ok(())
}

fn show_skill(options: SkillShowOptions) -> Result<(), CliError> {
    let store = skill_read_store_for_cli()?;
    let catalog = store.load_catalog().map_err(skill_catalog_cli_error)?;
    let entry = select_skill_entry(&catalog, &options.skill_id, options.version.as_deref())?;
    let text = required_skill_text_for_cli(&store, &entry)?;
    if options.text {
        print!("{text}");
        return Ok(());
    }
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "skill": skill_entry_json(&entry, skill_entry_is_active(&catalog, &entry)),
                "text": text,
            }))
            .map_err(|source| CliError::Json {
                path: "<skill show>".to_string(),
                detail: source.to_string(),
            })?
        );
        return Ok(());
    }
    println!("Skill: {}@{}", entry.skill_id, entry.version);
    println!("  source: {}", skill_source_label(&entry));
    println!("  status: {}", entry.status.as_str());
    println!("  active: {}", skill_entry_is_active(&catalog, &entry));
    println!("  path: {}", entry.sifs_path);
    println!("  manifest: {}", entry.manifest_path);
    println!("  content hash: {}", entry.content_hash);
    if let Some(base_hash) = entry.base_hash.as_deref() {
        println!("  base hash: {base_hash}");
    }
    if !entry.description.is_empty() {
        println!("  description: {}", entry.description);
    }
    println!(
        "  target agents: {}",
        format_string_list(&entry.target_agents)
    );
    println!(
        "  model families: {}",
        format_string_list(&entry.model_families)
    );
    println!(
        "  activation tags: {}",
        format_string_list(&entry.activation_tags)
    );
    println!(
        "  required capabilities: {}",
        format_string_list(&entry.required_capabilities)
    );
    Ok(())
}

fn required_skill_text_for_cli(
    store: &SkillCatalogSifsStore,
    entry: &SkillCatalogEntry,
) -> Result<String, CliError> {
    store
        .read_skill_text(entry)
        .map_err(skill_catalog_cli_error)?
        .ok_or_else(|| CliError::Io {
            path: entry.sifs_path.clone(),
            detail: format!(
                "skill `{}@{}` catalog entry references missing content",
                entry.skill_id, entry.version
            ),
        })
}

fn import_skill(options: SkillImportOptions) -> Result<(), CliError> {
    let store = skill_store_for_cli()?;
    let package = read_agent_skill_package(&options.path).map_err(skill_catalog_cli_error)?;
    let source =
        skill_source_from_import_options(&options.source, &options.source_id, &options.version);
    let mut write = SkillCatalogWrite::from_agent_skill_package(
        options.skill_id.clone(),
        options.version.clone(),
        source,
        package,
    )
    .map_err(skill_catalog_cli_error)?;
    if options.activate {
        write = write.activate_for_scope(options.scope.clone(), "cli", "skill import");
    }
    let entry = write.entry.clone();
    store
        .write_skill_version(write)
        .map_err(skill_catalog_cli_error)?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "imported",
                "skill": skill_entry_json(&entry, options.activate),
                "activated": options.activate,
                "scope": options.scope,
            }))
            .map_err(|source| CliError::Json {
                path: "<skill import>".to_string(),
                detail: source.to_string(),
            })?
        );
    } else {
        println!(
            "imported skill {}@{}{}",
            entry.skill_id,
            entry.version,
            if options.activate {
                " and activated"
            } else {
                ""
            }
        );
    }
    Ok(())
}

fn export_skill(options: SkillExportOptions) -> Result<(), CliError> {
    let store = skill_read_store_for_cli()?;
    let catalog = store.load_catalog().map_err(skill_catalog_cli_error)?;
    let entry = select_skill_entry(&catalog, &options.skill_id, options.version.as_deref())?;
    let package = store
        .read_skill_package(&entry)
        .map_err(skill_catalog_cli_error)?
        .ok_or_else(|| CliError::Parse {
            detail: format!("skill `{}` has no readable package", entry.skill_id),
        })?;
    let rendered =
        render_common_skill_markdown(&entry, &package.markdown).map_err(skill_catalog_cli_error)?;
    if let Some(output) = options.output.as_deref() {
        let path = skill_export_output_directory(output, &entry.skill_id)?;
        let package = sinomo_agent::AgentSkillPackage {
            markdown: rendered,
            files: package.files,
        };
        write_agent_skill_package(&path, &package).map_err(skill_catalog_cli_error)?;
        if options.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": "exported",
                    "path": path,
                    "skill": skill_entry_json(&entry, skill_entry_is_active(&catalog, &entry)),
                }))
                .map_err(|source| CliError::Json {
                    path: "<skill export>".to_string(),
                    detail: source.to_string(),
                })?
            );
        } else {
            println!("exported skill: {}", path.display());
        }
    } else if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "exported",
                "skill": skill_entry_json(&entry, skill_entry_is_active(&catalog, &entry)),
                "text": rendered,
            }))
            .map_err(|source| CliError::Json {
                path: "<skill export>".to_string(),
                detail: source.to_string(),
            })?
        );
    } else {
        print!("{rendered}");
    }
    Ok(())
}

fn activate_skill(options: SkillActivateOptions) -> Result<(), CliError> {
    let store = skill_store_for_cli()?;
    let catalog = store.load_catalog().map_err(skill_catalog_cli_error)?;
    let entry = select_skill_entry(&catalog, &options.skill_id, options.version.as_deref())?;
    store
        .activate_skill_version(&entry, options.scope.clone(), "cli", options.reason.clone())
        .map_err(skill_catalog_cli_error)?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "activated",
                "scope": options.scope,
                "skill": skill_entry_json(&entry, true),
            }))
            .map_err(|source| CliError::Json {
                path: "<skill activate>".to_string(),
                detail: source.to_string(),
            })?
        );
    } else {
        println!(
            "activated skill {}@{} for {}",
            entry.skill_id, entry.version, options.scope
        );
    }
    Ok(())
}

fn skill_store_for_cli() -> Result<SkillCatalogSifsStore, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    SkillCatalogSifsStore::open(Some(data_dir.root())).map_err(skill_catalog_cli_error)
}

/// Open a read-only catalog through the resident daemon's SIFS service.
///
/// A short-lived CLI process must not construct a second local DFS service:
/// even lazy mutation-lane hydration opens durable shard tables, namespace
/// guards, journal cursors, and content reservation writer state before the
/// first catalog byte is read. The daemon already owns that hydrated state and
/// exposes the same SIFS contract through its browser RPCs.
fn read_only_sifs_vfs_for_cli() -> Result<sinomo_runtime::vfs::VfsHandle, CliError> {
    let daemon = default_main_daemon_status()?.ok_or_else(|| CliError::DaemonNotRunning {
        detail: "SIFS catalog reads require the resident main daemon".to_string(),
    })?;
    sinomo_runtime::vfs::daemon_sifs(daemon.address).map_err(|source| CliError::Io {
        path: "/".to_string(),
        detail: source.to_string(),
    })
}

fn skill_read_store_for_cli() -> Result<SkillCatalogSifsStore, CliError> {
    Ok(SkillCatalogSifsStore::from_vfs(
        read_only_sifs_vfs_for_cli()?
    ))
}

fn skill_export_output_directory(path: &Path, skill_id: &str) -> Result<PathBuf, CliError> {
    if path.is_file() {
        return Err(CliError::Parse {
            detail: "Agent Skills exports require a directory path".to_string(),
        });
    }
    if path.file_name().and_then(|name| name.to_str()) == Some(skill_id) {
        Ok(path.to_path_buf())
    } else {
        Ok(path.join(skill_id))
    }
}

fn skill_source_from_import_options(
    kind: &SkillCatalogSourceKind,
    source_id: &str,
    version: &str,
) -> SkillCatalogSource {
    match kind {
        SkillCatalogSourceKind::Sinomo => SkillCatalogSource::sinomo(version),
        SkillCatalogSourceKind::Module => SkillCatalogSource::module(source_id, version),
        SkillCatalogSourceKind::User => SkillCatalogSource::user(source_id),
        SkillCatalogSourceKind::Agent => SkillCatalogSource::agent(source_id),
        SkillCatalogSourceKind::Evolution => SkillCatalogSource::evolution(source_id),
    }
}

fn select_skill_entry(
    catalog: &sinomo_agent::SkillCatalog,
    skill_id: &str,
    version: Option<&str>,
) -> Result<SkillCatalogEntry, CliError> {
    if let Some(version) = version {
        return catalog
            .entry(skill_id, version)
            .cloned()
            .ok_or_else(|| CliError::Parse {
                detail: format!("unknown skill version {skill_id}@{version}"),
            });
    }
    if let Some(active) =
        catalog.active_entry_for_scope(SkillActivationScope::Cluster.as_str(), skill_id)
    {
        return Ok(active.clone());
    }
    let matches = catalog
        .entries
        .iter()
        .filter(|entry| entry.skill_id == skill_id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [entry] => Ok((*entry).clone()),
        [] => Err(CliError::Parse {
            detail: format!("unknown skill `{skill_id}`"),
        }),
        _ => Err(CliError::Parse {
            detail: format!("skill `{skill_id}` has multiple versions; pass --version"),
        }),
    }
}

fn skill_entry_is_active(catalog: &sinomo_agent::SkillCatalog, entry: &SkillCatalogEntry) -> bool {
    catalog.activations.iter().any(|activation| {
        activation.skill_id == entry.skill_id
            && activation.version == entry.version
            && activation.content_hash == entry.content_hash
    })
}

fn skill_entry_json(entry: &SkillCatalogEntry, active: bool) -> serde_json::Value {
    serde_json::json!({
        "id": &entry.skill_id,
        "version": &entry.version,
        "description": &entry.description,
        "license": &entry.license,
        "compatibility": &entry.compatibility,
        "allowed_tools": &entry.allowed_tools,
        "metadata": &entry.metadata,
        "resources": &entry.resources,
        "source": {
            "kind": entry.source.kind.as_str(),
            "id": &entry.source.source_id,
            "version": &entry.source.source_version,
        },
        "active": active,
        "status": entry.status.as_str(),
        "path": &entry.sifs_path,
        "manifest_path": &entry.manifest_path,
        "content_hash": &entry.content_hash,
        "base_hash": &entry.base_hash,
        "target_agents": &entry.target_agents,
        "model_families": &entry.model_families,
        "activation_tags": &entry.activation_tags,
        "required_capabilities": &entry.required_capabilities,
    })
}

fn skill_source_label(entry: &SkillCatalogEntry) -> String {
    match (
        entry.source.kind,
        entry.source.source_id.as_deref(),
        entry.source.source_version.as_deref(),
    ) {
        (SkillCatalogSourceKind::Module, Some(id), Some(version)) => {
            format!("module:{id}@{version}")
        }
        (_, Some(id), Some(version)) => format!("{}:{id}@{version}", entry.source.kind.as_str()),
        (_, Some(id), None) => format!("{}:{id}", entry.source.kind.as_str()),
        _ => entry.source.kind.as_str().to_string(),
    }
}

fn skill_catalog_cli_error(error: sinomo_agent::SkillCatalogError) -> CliError {
    match error {
        sinomo_agent::SkillCatalogError::Store { path, detail }
        | sinomo_agent::SkillCatalogError::Object { path, detail }
        | sinomo_agent::SkillCatalogError::InvalidSkillText { path, detail } => {
            CliError::Io { path, detail }
        }
        other => CliError::Parse {
            detail: other.to_string(),
        },
    }
}

pub(crate) fn run_agent_command(command: AgentCliCommand) -> Result<(), CliError> {
    match command {
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
    let definition = resolve_agent_definition_for_cli(&options.agent)?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&agent_package_inspection_json(&definition)).map_err(
                |source| CliError::Json {
                    path: "<agent inspect>".to_string(),
                    detail: source.to_string(),
                },
            )?
        );
    } else {
        print!("{}", agent_package_inspection_text(&definition));
    }
    Ok(())
}

pub(crate) fn export_agent_package(options: AgentExportOptions) -> Result<(), CliError> {
    let definition = resolve_agent_definition_for_cli(&options.agent)?;
    let bytes = serde_json::to_vec_pretty(&definition).map_err(|source| CliError::Json {
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
    let store = AgentDefinitionSifsStore::open(Some(data_dir.root()))
        .map_err(agent_definition_store_cli_error)?;
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
            saved_path
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
        let definition = resolve_agent_definition_for_cli(&options.agent)?;
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

fn resolve_agent_definition_for_cli(agent: &str) -> Result<AgentDefinition, CliError> {
    app_api::resolve_agent_definition_with_catalog_vfs(agent, read_only_sifs_vfs_for_cli)
        .map_err(|detail| CliError::Parse { detail })?
        .ok_or_else(|| CliError::Parse {
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
        "tool_runtime": definition.tool_runtime,
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
    output.push_str("  tool runtime:\n");
    output.push_str(&format!(
        "    sandbox node: {}\n",
        format_optional_value(definition.tool_runtime.sandbox_node_id.as_deref())
    ));
    if definition.tool_runtime.mcp_servers.is_empty() {
        output.push_str("    MCP servers: none\n");
    } else {
        output.push_str("    MCP servers:\n");
        for server in &definition.tool_runtime.mcp_servers {
            let mut facets = vec![
                if server.enabled {
                    "enabled"
                } else {
                    "disabled"
                }
                .to_string(),
                if server.required {
                    "required"
                } else {
                    "optional"
                }
                .to_string(),
            ];
            if !server.protocol_era.is_default() {
                facets.push(format!("{} protocol", server.protocol_era.as_str()));
            }
            output.push_str(&format!(
                "      {}: {} ({})\n",
                server.name,
                server.url,
                facets.join(", ")
            ));
        }
    }
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

#[cfg(test)]
pub(crate) fn best_effort_reconciled_skill_catalog(
    data_dir: &SinomoDataDir,
) -> sinomo_agent::SkillCatalog {
    sinomo_agent::load_reconciled_skill_catalog_from_sifs(data_dir).unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn best_effort_skill_catalog_texts(
    data_dir: &SinomoDataDir,
    catalog: &sinomo_agent::SkillCatalog,
) -> BTreeMap<String, String> {
    sinomo_agent::load_skill_catalog_texts_from_sifs(data_dir, catalog).unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn best_effort_skill_catalog_resources(
    data_dir: &SinomoDataDir,
    catalog: &sinomo_agent::SkillCatalog,
) -> BTreeMap<String, Vec<sinomo_agent::AgentSkillPackageFile>> {
    sinomo_agent::load_skill_catalog_resources_from_sifs(data_dir, catalog).unwrap_or_default()
}

type RuntimeSkillCatalog = (
    sinomo_agent::SkillCatalog,
    BTreeMap<String, String>,
    BTreeMap<String, Vec<sinomo_agent::AgentSkillPackageFile>>,
);

fn load_runtime_skill_catalog_from_vfs(
    vfs: sinomo_runtime::vfs::VfsHandle,
) -> Result<RuntimeSkillCatalog, sinomo_agent::SkillCatalogError> {
    let store = SkillCatalogSifsStore::from_vfs(vfs);
    let catalog = store.load_catalog()?;
    let mut texts = BTreeMap::new();
    let mut resources = BTreeMap::new();
    for entry in &catalog.entries {
        let package = store.read_skill_package(entry)?.ok_or_else(|| {
            sinomo_agent::SkillCatalogError::Object {
                path: entry.sifs_path.clone(),
                detail: format!(
                    "skill `{}@{}` catalog entry references missing package content",
                    entry.skill_id, entry.version
                ),
            }
        })?;
        let key = sinomo_agent::skill_catalog_entry_text_key(entry);
        texts.insert(key.clone(), package.markdown);
        if !package.files.is_empty() {
            resources.insert(key, package.files);
        }
    }
    Ok((catalog, texts, resources))
}

/// Keep the TUI's first frame independent of unbounded DFS recovery while
/// preserving catalog availability semantics. Intentional daemonless mode has
/// no cluster skill view; once connected, an unavailable or malformed daemon
/// SIFS catalog is an error rather than a semantically false empty catalog.
/// The outer deadline bounds daemon handlers whose internal DFS timeout exceeds
/// the client idle timeout.
pub(crate) fn runtime_skill_catalog(
    daemon: Option<&DaemonStatus>,
) -> Result<RuntimeSkillCatalog, CliError> {
    let Some(daemon) = daemon else {
        return Ok(Default::default());
    };
    let vfs = sinomo_runtime::vfs::daemon_sifs_with_timeout(
        daemon.address.clone(),
        Duration::from_millis(250),
    )
    .map_err(|source| CliError::Io {
        path: "/services/sinomo/skills".to_string(),
        detail: source.to_string(),
    })?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    if std::thread::Builder::new()
        .name("sinomo-tui-skills".to_string())
        .spawn(move || {
            let _ = sender.send(load_runtime_skill_catalog_from_vfs(vfs));
        })
        .is_err()
    {
        return Err(CliError::Parse {
            detail: "failed to start the resident daemon skill-catalog reader".to_string(),
        });
    }
    match receiver.recv_timeout(Duration::from_millis(250)) {
        Ok(loaded) => loaded.map_err(skill_catalog_cli_error),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(CliError::Parse {
            detail: "resident daemon skill-catalog read timed out after 250ms".to_string(),
        }),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(CliError::Parse {
            detail: "resident daemon skill-catalog reader stopped without a result".to_string(),
        }),
    }
}

pub(crate) fn run_agent_once(options: AgentRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let session_id = options
        .session_id
        .clone()
        .unwrap_or_else(SessionId::generate);
    let session_store = FileAgentSessionStore::for_data_dir(&data_dir);
    let persisted_session = if options.session_id.is_some() {
        session_store
            .load_session(&session_id)
            .map_err(|source| CliError::Parse {
                detail: format!("load agent session `{session_id}` failed: {source}"),
            })?
    } else {
        None
    };
    let agent_name = match persisted_session
        .as_ref()
        .and_then(|session| session.agent_definition_key.as_deref())
    {
        Some(persisted_key) if options.agent_explicit && persisted_key != options.agent => {
            return Err(CliError::Parse {
                detail: format!(
                    "agent session `{session_id}` belongs to `{persisted_key}` and cannot be resumed as `{}`",
                    options.agent
                ),
            });
        }
        Some(persisted_key) => persisted_key.to_string(),
        None if persisted_session.is_some() && !options.agent_explicit => {
            return Err(CliError::Parse {
                detail: format!(
                    "legacy agent session `{session_id}` has no persisted agent definition; resume it once with an explicit `--agent <name>`"
                ),
            });
        }
        None => options.agent.clone(),
    };
    let definition = resolve_agent_definition_for_cli(&agent_name)?;
    let (provider, model) = resolve_agent_run_model_selection(&options, &definition);
    let current_root = std::env::current_dir().map_err(|source| CliError::Io {
        path: "current directory".to_string(),
        detail: source.to_string(),
    })?;
    let workspace_mutation_authority =
        agent_definition_has_workspace_mutation_authority(&definition);
    let mut _resolved_workspace_root = None;
    let mut _resolved_workspace_binding = None;
    let (workspace_root, workspace_binding) = if workspace_mutation_authority {
        match persisted_session.as_ref() {
            Some(session) => {
                let binding = session.workspace_binding.as_ref().ok_or_else(|| CliError::Parse {
                    detail: format!(
                        "mutation-capable agent session `{session_id}` has no workspace binding; refusing resume because its original workspace cannot be verified"
                    ),
                })?;
                let resolved =
                    workspace_binding_root_for_data_dir(&data_dir, &session_id, binding)?;
                let root = resolved.root().to_path_buf();
                _resolved_workspace_root = Some(resolved);
                (root, Some(binding.clone()))
            }
            None => {
                let resolved = mutating_agent_workspace_binding(&data_dir, &current_root)?;
                let binding = resolved.binding().clone();
                _resolved_workspace_binding = Some(resolved);
                (current_root.clone(), Some(binding))
            }
        }
    } else {
        (current_root, None)
    };
    let account = if let Some(account_name) = options.account.as_deref() {
        let config = load_model_config(&ModelPaths::new(&data_dir))?;
        provider_account_config(&config, &provider, Some(account_name)).cloned()
    } else {
        None
    };
    let (mut agent_session, resumed) = load_or_create_agent_run_session(
        &session_store,
        session_id.clone(),
        definition.role.clone(),
        definition.key(),
        workspace_binding.as_ref(),
    )?;
    let node_id = match read_cluster_state(&ClusterPaths::new(&data_dir)) {
        Ok(state) => state.local_node_id,
        Err(CliError::ClusterOrphan) => {
            NodeId::from_legacy_named("node_agent_cli").expect("legacy node id")
        }
        Err(error) => return Err(error),
    };
    validate_agent_local_placement(&definition, &node_id)?;
    if agent_session.sandbox_attachment.is_none() {
        agent_session
            .attach_sandbox(sinomo_agent::AgentSessionSandboxAttachment {
                sandbox_id: sinomo_agent::local_workspace::agent_sandbox_id_for_session(
                    &session_id,
                )
                .to_string(),
                server_id: node_id.to_string(),
                attachment_kind: sinomo_agent::AgentSessionSandboxAttachmentKind::Automatic,
            })
            .map_err(|source| CliError::Parse {
                detail: format!("attach default sandbox to agent session: {source}"),
            })?;
        session_store
            .save_session(&agent_session)
            .map_err(|source| CliError::Parse {
                detail: format!("persist agent session sandbox attachment: {source}"),
            })?;
    }
    let session = InteractionSession::new(
        session_id.clone(),
        node_id,
        InteractionSurface::new("cli.agent", InteractionSurfaceKind::CommandLine),
    );
    let input_event = user_interaction_input_event(&session, &options.prompt);
    // `session.session_id` is the stable Sinomo interaction/activity identity.
    // Provider-native session handles are neither required nor projected, so
    // they may be ephemeral or unrelated without changing chat history.
    let chat_projection = StableChatProjection::open(&data_dir, &session)?;
    let input_message_id = input_event.event_id.clone();
    let input_recorded_at = input_event.time;
    let input = AgentLoopInput {
        session,
        latest_event: input_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    };
    let (runtime_config, _) = load_model_runtime_config_for_node(&data_dir)?;
    let sandbox_node_id = definition
        .tool_runtime
        .sandbox_node_id
        .as_deref()
        .map(parse_agent_package_node_id)
        .transpose()?;
    let tool_registry = agent_tool_registry_for_workspace(
        &data_dir,
        &workspace_root,
        &definition,
        Some(&session_id),
        sandbox_node_id,
        true,
    )?;
    let daemon = default_main_daemon_status()?;
    let (skill_catalog, skill_catalog_texts, skill_catalog_resources) =
        runtime_skill_catalog(daemon.as_ref())?;
    let loop_model = ConfiguredInteractionModel {
        data_dir: data_dir.clone(),
        provider: provider.clone(),
        model: model.clone(),
        account,
        runtime_config,
        managed_pytorch: Mutex::new(None),
    };
    let agent_config = sinomo_agent::AgentConfig::new(provider.clone(), model.clone())
        .with_agent_definition(definition.clone())
        .with_module_tool_projection(agent_module_tool_projection(&data_dir))
        .with_workspace_root(&workspace_root)
        .with_skill_catalog(skill_catalog)
        .with_skill_catalog_texts(skill_catalog_texts)
        .with_skill_catalog_resources(skill_catalog_resources);
    let agent = Agent::new(loop_model, provider, model)
        .with_config(agent_config)
        .with_tool_registry(tool_registry)
        .with_session_store(session_store)
        .with_trace_store(FileAgentTraceStore::for_data_dir_session(
            &data_dir,
            &session_id,
        ));
    let runtime = AgentSessionRuntime::with_data_dir_storage(agent, agent_session, &data_dir);
    let op = AgentOp::start_turn(input);
    let op_id = op.op_id.clone();
    // Persist the typed user input only after all local setup succeeds, but
    // before dispatch. From this point every terminal dispatch path below is
    // paired with either an agent message or a system-error message.
    chat_projection.record_user_message(
        definition.key(),
        input_message_id,
        input_recorded_at,
        &options.prompt,
    )?;
    let submit = match runtime.submit_op(op) {
        Ok(submit) => submit,
        Err(source) => {
            return Err(agent_run_error_with_projection(
                &chat_projection,
                definition.key(),
                op_id,
                source.into(),
            ));
        }
    };
    if !submit.accepted {
        let reason = submit
            .events
            .iter()
            .find_map(|event| match &event.payload {
                AgentEventPayload::OperationRejected { reason, .. } => Some(reason.clone()),
                _ => None,
            })
            .unwrap_or_else(|| format!("agent `{}` turn was rejected", definition.key()));
        return Err(agent_run_error_with_projection(
            &chat_projection,
            definition.key(),
            op_id,
            CliError::Parse { detail: reason },
        ));
    }
    if let Err(source) = runtime.join_active_turn() {
        return Err(agent_run_error_with_projection(
            &chat_projection,
            definition.key(),
            op_id,
            source.into(),
        ));
    }
    let final_recorded_at = OffsetDateTime::now_utc();
    let turn_result = match runtime.turn_result(&op_id) {
        Ok(result) => result,
        Err(source) => {
            return Err(agent_run_error_with_projection(
                &chat_projection,
                definition.key(),
                op_id,
                source.into(),
            ));
        }
    };
    let decision = turn_result
        .and_then(|outcome| outcome.run)
        .map(|run| run.final_decision)
        .ok_or_else(|| {
            agent_run_error_with_projection(
                &chat_projection,
                definition.key(),
                op_id.clone(),
                CliError::Parse {
                    detail: format!(
                        "agent `{}` runtime turn `{op_id}` completed without a decision",
                        definition.key()
                    ),
                },
            )
        })?;
    match agent_run_activity_output(&decision)? {
        AgentRunActivityOutput::Agent(text) => chat_projection.record_agent_message(
            definition.key(),
            op_id.clone(),
            final_recorded_at,
            text,
        )?,
        AgentRunActivityOutput::System(text) => chat_projection.record_system_message(
            definition.key(),
            op_id.clone(),
            final_recorded_at,
            &text,
            false,
        )?,
    }
    if let Some(handoff) = decision.background_handoff.as_ref() {
        chat_projection.record_background_handoff_request(
            definition.key(),
            &op_id,
            final_recorded_at,
            handoff,
        )?;
    }
    let compaction_evidence = if options.compact_after {
        let compact_op = AgentOp::compact(format!(
            "CLI --compact-after following completed turn {op_id}"
        ));
        let compact_op_id = compact_op.op_id.clone();
        let compact_submit = runtime.submit_op(compact_op)?;
        if !compact_submit.accepted {
            let reason = compact_submit
                .events
                .iter()
                .find_map(|event| match &event.payload {
                    AgentEventPayload::OperationRejected { reason, .. } => Some(reason.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| format!("agent `{}` compaction was rejected", definition.key()));
            return Err(CliError::Parse { detail: reason });
        }
        let persisted_session = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&session_id)
            .map_err(|source| CliError::Parse {
                detail: format!(
                    "reload compacted agent session `{session_id}` failed: {source}"
                ),
            })?
            .ok_or_else(|| CliError::Parse {
                detail: format!(
                    "compacted agent session `{session_id}` was not persisted after operation `{compact_op_id}`"
                ),
            })?;
        Some(agent_run_compaction_evidence(
            &compact_op_id,
            &compact_submit.events,
            &persisted_session,
        )?)
    } else {
        None
    };
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "session": session_id,
                "op": op_id,
                "agent": definition.key(),
                "provider": options.provider,
                "model": options.model,
                "decision": decision,
                "resumed": resumed,
                "compaction": compaction_evidence.as_ref(),
            }))
            .map_err(|source| CliError::Json {
                path: "<agent run>".to_string(),
                detail: source.to_string(),
            })?
        );
    } else if decision.text.trim().is_empty() {
        println!("{}", agent_decision_json(&decision)?);
    } else {
        println!("{}", decision.text);
        if !decision.tool_calls.is_empty() || decision.background_handoff.is_some() {
            println!("{}", agent_decision_json(&decision)?);
        }
    }
    if !options.json {
        if let Some(evidence) = compaction_evidence.as_ref() {
            eprintln!(
                "compaction: persisted projection {}",
                evidence["projection"]["projection_id"]
                    .as_str()
                    .unwrap_or("<unknown>")
            );
        }
        eprintln!(
            "session: {session_id} (resume with `sinomo agent run --session {session_id} --prompt <text>`)"
        );
    }
    Ok(())
}

fn resolve_agent_run_model_selection(
    options: &AgentRunOptions,
    definition: &AgentDefinition,
) -> (String, String) {
    let provider = if options.provider_explicit {
        options.provider.clone()
    } else {
        definition
            .model_policy
            .provider
            .clone()
            .unwrap_or_else(|| options.provider.clone())
    };
    let model = if options.model_explicit {
        options.model.clone()
    } else if let Some(model) = definition.model_policy.model.clone() {
        model
    } else if provider != options.provider {
        sinomo_lm::PROVIDER_DEFAULT_MODEL.to_string()
    } else {
        options.model.clone()
    };
    (provider, model)
}

fn parse_agent_package_node_id(value: &str) -> Result<NodeId, CliError> {
    NodeId::new(value.to_string())
        .or_else(|_| NodeId::from_legacy_named(value.to_string()))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid agent package node id `{value}`: {source}"),
        })
}

fn validate_agent_local_placement(
    definition: &AgentDefinition,
    local_node_id: &NodeId,
) -> Result<(), CliError> {
    for (field, configured) in [
        (
            "home server",
            definition.placement.home_server_node_id.as_deref(),
        ),
        (
            "sandbox runtime",
            definition.tool_runtime.sandbox_node_id.as_deref(),
        ),
    ] {
        let Some(configured) = configured else {
            continue;
        };
        let configured_node_id = parse_agent_package_node_id(configured)?;
        if &configured_node_id != local_node_id {
            return Err(CliError::Parse {
                detail: format!(
                    "agent `{}` requests {field} node `{configured_node_id}`, but this execution path is local to `{local_node_id}` and remote agent routing is unavailable",
                    definition.key()
                ),
            });
        }
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum AgentRunActivityOutput<'a> {
    Agent(&'a str),
    System(String),
}

/// Render only the stable, human-visible terminal chat output. Provider and
/// model-session metadata deliberately stay out of the activity projection.
fn agent_run_activity_output(
    decision: &AgentLoopDecision,
) -> Result<AgentRunActivityOutput<'_>, CliError> {
    if decision.text.trim().is_empty() {
        Ok(AgentRunActivityOutput::System(agent_decision_json(
            decision,
        )?))
    } else {
        Ok(AgentRunActivityOutput::Agent(&decision.text))
    }
}

fn agent_run_error_with_projection(
    projection: &StableChatProjection,
    agent_key: &str,
    message_id: TaskId,
    error: CliError,
) -> CliError {
    let visible_error = error.to_string();
    match projection.record_system_message(
        agent_key,
        message_id,
        OffsetDateTime::now_utc(),
        &visible_error,
        true,
    ) {
        Ok(()) => error,
        Err(projection_error) => CliError::Parse {
            detail: format!(
                "{visible_error}; recording the stable chat error also failed: {projection_error}"
            ),
        },
    }
}

fn agent_run_compaction_evidence(
    compact_op_id: &TaskId,
    events: &[sinomo_agent::AgentEvent],
    persisted_session: &AgentSession,
) -> Result<serde_json::Value, CliError> {
    let (summary, item_count_after) = events
        .iter()
        .find_map(|event| match &event.payload {
            AgentEventPayload::Compacted {
                summary,
                item_count,
            } => Some((summary, *item_count)),
            _ => None,
        })
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "agent compaction operation `{compact_op_id}` completed without compaction evidence"
            ),
        })?;
    let projection = persisted_session
        .projection
        .as_ref()
        .filter(|projection| projection.projection_id == summary.projection.projection_id)
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "agent compaction operation `{compact_op_id}` did not persist projection `{}`",
                summary.projection.projection_id
            ),
        })?;
    Ok(serde_json::json!({
        "op": compact_op_id,
        "compaction_id": summary.compaction_id,
        "reason": summary.reason,
        "strategy_id": summary.strategy_id,
        "trigger": summary.trigger,
        "summary": summary.summary,
        "summary_model": summary.summary_model,
        "fallback_reason": summary.fallback_reason,
        "token_estimate_before": summary.token_estimate_before,
        "retained_tail_count": summary.retained_tail_count,
        "item_count_after": item_count_after,
        "transcript_item_count_after": persisted_session.transcript.items.len(),
        "raw_item_count_after": persisted_session.raw_transcript.items.len(),
        "projection": projection,
        "persisted": true,
    }))
}

fn load_or_create_agent_run_session(
    store: &FileAgentSessionStore,
    session_id: SessionId,
    role: AgentRole,
    definition_key: &str,
    workspace_binding: Option<&WorkspaceRef>,
) -> Result<(AgentSession, bool), CliError> {
    let loaded = store
        .load_session(&session_id)
        .map_err(|source| CliError::Parse {
            detail: format!("load agent session `{session_id}` failed: {source}"),
        })?;
    match loaded {
        Some(mut session) => {
            if let Some(existing_key) = session.agent_definition_key.as_deref()
                && existing_key != definition_key
            {
                return Err(CliError::Parse {
                    detail: format!(
                        "agent session `{}` belongs to `{existing_key}` and cannot be resumed as `{definition_key}`",
                        session.session_id
                    ),
                });
            }
            session.role = role;
            if let Some(requested_binding) = workspace_binding {
                match session.workspace_binding.as_ref() {
                    Some(existing_binding) => validate_agent_session_workspace_binding(
                        &session.session_id,
                        existing_binding,
                        requested_binding,
                    )?,
                    None => {
                        return Err(CliError::Parse {
                            detail: format!(
                                "mutation-capable agent session `{}` has no workspace binding; refusing resume because its original workspace cannot be verified; start a new session without `--session`",
                                session.session_id
                            ),
                        });
                    }
                }
            }
            let needs_identity_migration = session.agent_definition_key.is_none();
            session.agent_definition_key = Some(definition_key.to_string());
            if needs_identity_migration {
                store
                    .save_session(&session)
                    .map_err(|source| CliError::Parse {
                        detail: format!(
                            "persist agent identity for session `{}` failed: {source}",
                            session.session_id
                        ),
                    })?;
            }
            Ok((session, true))
        }
        None => {
            let mut session = AgentSession::new(session_id, role);
            session.agent_definition_key = Some(definition_key.to_string());
            session.workspace_binding = workspace_binding.cloned();
            store
                .save_session(&session)
                .map_err(|source| CliError::Parse {
                    detail: format!(
                        "create agent session `{}` failed: {source}",
                        session.session_id
                    ),
                })?;
            Ok((session, false))
        }
    }
}

pub(crate) fn mutating_agent_workspace_binding(
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
) -> Result<ResolvedWorkspaceBinding, CliError> {
    let verified_sifs_mount = live_sifs_mount_for_selected_root(data_dir, workspace_root)?;
    validate_mutating_workspace_root_with_live_mount(
        data_dir,
        workspace_root,
        verified_sifs_mount
            .as_ref()
            .map(SimpleSandboxSifsMount::mount_root),
    )?;
    let binding = workspace_ref_for_root_with_verified_sifs_mount(
        workspace_root,
        verified_sifs_mount
            .as_ref()
            .map(SimpleSandboxSifsMount::mount_root),
    )?;
    Ok(ResolvedWorkspaceBinding {
        binding,
        _live_sifs_mount: verified_sifs_mount,
    })
}

const LIVE_POSIX_SIFS_WRITE_POLICY: &str = "live-posix";

pub(crate) fn workspace_ref_for_selected_root(
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
) -> Result<ResolvedWorkspaceBinding, CliError> {
    let verified_sifs_mount = live_sifs_mount_for_selected_root(data_dir, workspace_root)?;
    let binding = workspace_ref_for_root_with_verified_sifs_mount(
        workspace_root,
        verified_sifs_mount
            .as_ref()
            .map(SimpleSandboxSifsMount::mount_root),
    )?;
    Ok(ResolvedWorkspaceBinding {
        binding,
        _live_sifs_mount: verified_sifs_mount,
    })
}

pub(crate) fn workspace_ref_for_root_with_verified_sifs_mount(
    workspace_root: &Path,
    verified_sifs_mount: Option<&Path>,
) -> Result<WorkspaceRef, CliError> {
    let workspace_root = absolute_normalized_path(workspace_root)?;
    let Some(mount_root) = verified_sifs_mount else {
        return Ok(WorkspaceRef::local_path(workspace_root));
    };
    let mount_root = absolute_normalized_path(mount_root)?;
    let relative = workspace_root
        .strip_prefix(&mount_root)
        .map_err(|_| CliError::Parse {
            detail: format!(
                "workspace `{}` is not beneath verified live SIFS mount `{}`",
                workspace_root.display(),
                mount_root.display()
            ),
        })?;
    let mut segments = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(segment) => {
                let segment = segment.to_str().ok_or_else(|| CliError::Parse {
                    detail: format!(
                        "SIFS workspace path contains non-Unicode component: {}",
                        workspace_root.display()
                    ),
                })?;
                segments.push(segment);
            }
            std::path::Component::CurDir => {}
            _ => {
                return Err(CliError::Parse {
                    detail: format!("invalid SIFS workspace path: {}", workspace_root.display()),
                });
            }
        }
    }
    let sifs_root = if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", segments.join("/"))
    };
    Ok(WorkspaceRef::sifs(format!("sifs:{sifs_root}"), sifs_root)
        .with_write_policy(LIVE_POSIX_SIFS_WRITE_POLICY))
}

pub(crate) fn validate_agent_session_workspace_binding(
    session_id: &SessionId,
    existing_binding: &WorkspaceRef,
    requested_binding: &WorkspaceRef,
) -> Result<(), CliError> {
    match (
        existing_binding.sifs_root.as_deref(),
        requested_binding.sifs_root.as_deref(),
    ) {
        (Some(existing), Some(requested)) => {
            if normalize_logical_sifs_root(existing)? == normalize_logical_sifs_root(requested)? {
                return Ok(());
            }
        }
        (None, None) => {
            let existing_root = local_workspace_binding_root(session_id, existing_binding)?;
            let requested_root = local_workspace_binding_root(session_id, requested_binding)?;
            if absolute_normalized_path(existing_root)? == absolute_normalized_path(requested_root)?
            {
                return Ok(());
            }
        }
        _ => {}
    }
    Err(CliError::Parse {
        detail: format!(
            "agent session `{session_id}` is bound to workspace `{}` and cannot be resumed from `{}`; choose the original workspace or start a new session",
            workspace_binding_display(existing_binding),
            workspace_binding_display(requested_binding)
        ),
    })
}

fn workspace_binding_display(binding: &WorkspaceRef) -> String {
    binding
        .sifs_root
        .as_ref()
        .map(|root| format!("sifs:{root}"))
        .or_else(|| {
            binding
                .local_root
                .as_ref()
                .map(|root| root.display().to_string())
        })
        .unwrap_or_else(|| binding.id.clone())
}

fn normalize_logical_sifs_root(root: &str) -> Result<String, CliError> {
    if !root.starts_with('/') {
        return Err(CliError::Parse {
            detail: format!("SIFS workspace root must be absolute: {root}"),
        });
    }
    let mut segments = Vec::new();
    for segment in root.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                return Err(CliError::Parse {
                    detail: format!("SIFS workspace root escapes its mount: {root}"),
                });
            }
            segment => segments.push(segment),
        }
    }
    Ok(if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", segments.join("/"))
    })
}

pub(crate) fn local_workspace_binding_root<'a>(
    session_id: &SessionId,
    binding: &'a WorkspaceRef,
) -> Result<&'a Path, CliError> {
    binding.local_root.as_deref().ok_or_else(|| CliError::Parse {
        detail: format!(
            "agent session `{session_id}` has a non-local workspace binding and cannot be resumed by the local CLI"
        ),
    })
}

pub(crate) fn workspace_binding_root_for_data_dir(
    data_dir: &SinomoDataDir,
    session_id: &SessionId,
    binding: &WorkspaceRef,
) -> Result<ResolvedWorkspaceRoot, CliError> {
    if binding.sifs_root.is_none() {
        let root = absolute_normalized_path(local_workspace_binding_root(session_id, binding)?)?;
        let metadata = fs::metadata(&root).map_err(|source| CliError::Io {
            path: root.display().to_string(),
            detail: format!("persisted workspace is unavailable: {source}"),
        })?;
        if !metadata.is_dir() {
            return Err(CliError::Parse {
                detail: format!("persisted workspace is not a directory: {}", root.display()),
            });
        }
        return Ok(ResolvedWorkspaceRoot::local(root));
    }
    let mount = ensure_simple_sandbox_sifs_mount(data_dir)?.ok_or_else(|| CliError::Parse {
        detail: format!(
            "agent session `{session_id}` uses SIFS workspace `{}`, but this device has no active live SIFS mount",
            workspace_binding_display(binding)
        ),
    })?;
    let root =
        workspace_binding_root_with_verified_sifs_mount(session_id, binding, mount.mount_root())?;
    Ok(ResolvedWorkspaceRoot::live_sifs(root, mount))
}

pub(crate) fn workspace_binding_root_with_verified_sifs_mount(
    session_id: &SessionId,
    binding: &WorkspaceRef,
    verified_sifs_mount: &Path,
) -> Result<PathBuf, CliError> {
    let Some(sifs_root) = binding.sifs_root.as_deref() else {
        return absolute_normalized_path(local_workspace_binding_root(session_id, binding)?);
    };
    let sifs_root = normalize_logical_sifs_root(sifs_root)?;
    let mut local_root = absolute_normalized_path(verified_sifs_mount)?;
    for segment in sifs_root.trim_start_matches('/').split('/') {
        if !segment.is_empty() {
            local_root.push(segment);
        }
    }
    Ok(local_root)
}

pub(crate) fn main_agent_config_tool_registry(
    data_dir: &SinomoDataDir,
) -> Result<AgentToolRegistry, CliError> {
    let workspace_root = std::env::current_dir().map_err(|source| CliError::Io {
        path: "current directory".to_string(),
        detail: source.to_string(),
    })?;
    main_agent_tool_registry_for_workspace(data_dir, &workspace_root)
}

pub(crate) fn main_agent_tool_registry_for_workspace(
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
) -> Result<AgentToolRegistry, CliError> {
    agent_tool_registry_for_workspace(
        data_dir,
        workspace_root,
        &AgentDefinition::main(),
        None,
        None,
        false,
    )
}

pub(crate) fn agent_tool_registry_for_workspace(
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
    definition: &AgentDefinition,
    session_id: Option<&SessionId>,
    sandbox_node_id: Option<NodeId>,
    allow_workspace_mutation: bool,
) -> Result<AgentToolRegistry, CliError> {
    let registry = AgentToolRegistry::new();
    configure_agent_tool_registry_for_workspace(
        &registry,
        data_dir,
        workspace_root,
        definition,
        session_id,
        sandbox_node_id,
        allow_workspace_mutation,
    )?;
    Ok(registry)
}

#[derive(Clone)]
struct CliClusterStatusAgentTool {
    data_dir: SinomoDataDir,
}

impl CliClusterStatusAgentTool {
    fn new(data_dir: SinomoDataDir) -> Self {
        Self { data_dir }
    }
}

impl AgentTool for CliClusterStatusAgentTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ClusterStatus,
            "cluster_status",
            "Read live health for every known node in the local Sinomo cluster.",
            false,
        ))
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "enum": ["cluster", "local"],
                    "description": "Optional compatibility hint; both values inspect the cluster reached through this local node."
                }
            },
            "additionalProperties": false
        }))
        .with_output_schema(serde_json::json!({
            "type": "object",
            "required": ["cluster_id", "local_node_id", "summary", "nodes"],
            "properties": {
                "cluster_id": { "type": "string" },
                "local_node_id": { "type": "string" },
                "summary": {
                    "type": "object",
                    "required": ["online", "offline", "unknown", "total"]
                },
                "nodes": { "type": "array" }
            }
        }))
    }

    fn execute(&self, _request: AgentToolRequest<'_>) -> AgentToolResult {
        let (state, rows) = match cluster_health_rows_for_data_dir(&self.data_dir) {
            Ok(result) => result,
            Err(error) => {
                return AgentToolResult::failed(
                    format!("cluster health is unavailable: {error}"),
                    serde_json::json!({
                        "error": "cluster_health_unavailable",
                        "detail": error.to_string(),
                    }),
                );
            }
        };
        let count = |availability: ClusterNodeAvailability| {
            rows.iter()
                .filter(|row| row.availability == availability)
                .count()
        };
        let online = count(ClusterNodeAvailability::Online);
        let offline = count(ClusterNodeAvailability::Offline);
        let unknown = count(ClusterNodeAvailability::Unknown);
        let output = serde_json::json!({
            "cluster_id": state.cluster_id,
            "local_node_id": state.local_node_id,
            "summary": {
                "online": online,
                "offline": offline,
                "unknown": unknown,
                "total": rows.len(),
            },
            "nodes": rows,
        });
        AgentToolResult::succeeded(
            format!("cluster health: {online} online, {offline} offline, {unknown} unknown"),
            output.clone(),
        )
        .with_evidence(
            AgentToolEvidence::availability_only(
                true,
                "read cluster health through the local Sinomo control plane",
            )
            .with_output_value(&output, false),
        )
    }
}

pub(crate) fn configure_agent_tool_registry_for_workspace(
    registry: &AgentToolRegistry,
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
    definition: &AgentDefinition,
    session_id: Option<&SessionId>,
    sandbox_node_id: Option<NodeId>,
    allow_workspace_mutation: bool,
) -> Result<(), CliError> {
    let workspace = workspace_ref_for_selected_root(data_dir, workspace_root)?
        .binding()
        .clone();
    configure_agent_tool_registry_for_workspace_projection(
        registry,
        data_dir,
        workspace_root,
        &[workspace],
        &[workspace_root.to_path_buf()],
        definition,
        session_id,
        sandbox_node_id,
        allow_workspace_mutation,
    )
}

pub(crate) fn configure_agent_tool_registry_for_workspace_projection(
    registry: &AgentToolRegistry,
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
    workspace_grants: &[WorkspaceRef],
    resolved_workspace_roots: &[PathBuf],
    definition: &AgentDefinition,
    session_id: Option<&SessionId>,
    sandbox_node_id: Option<NodeId>,
    allow_workspace_mutation: bool,
) -> Result<(), CliError> {
    let store = main_agent_config_store()?;
    let mut config = LocalWorkspaceAgentConfig::new(workspace_root)
        .with_additional_workspace_roots(resolved_workspace_roots.iter().skip(1).cloned());
    let requested_sandbox_node_id = sandbox_node_id.clone();
    let workspace_mutation_authority =
        agent_definition_has_workspace_mutation_authority(definition);
    let workspace_uses_live_mount =
        workspace_root_uses_simple_sandbox_sifs_mount(data_dir, workspace_root)?;
    let needs_live_sifs_mount = session_id.is_some()
        || workspace_uses_live_mount
        || (allow_workspace_mutation && workspace_mutation_authority);
    let live_sifs_mount = if needs_live_sifs_mount {
        match ensure_simple_sandbox_sifs_mount(data_dir) {
            Ok(mount) => mount,
            Err(error) if !workspace_uses_live_mount => {
                // The workspace can remain usable without a live SIFS projection,
                // but this runtime cannot claim that the agent's canonical home is
                // available in that case.
                eprintln!(
                    "warning: live SIFS integration for the agent runtime is unavailable; using the local workspace without the agent home: {error}"
                );
                None
            }
            Err(error) => return Err(error),
        }
    } else {
        None
    };
    if allow_workspace_mutation && workspace_mutation_authority {
        for root in resolved_workspace_roots {
            validate_mutating_workspace_root_with_live_mount(
                data_dir,
                root,
                live_sifs_mount
                    .as_ref()
                    .map(SimpleSandboxSifsMount::mount_root),
            )?;
        }
        if !resolved_workspace_roots.is_empty() {
            config = config
                .allowing_shell_commands(true)
                .allowing_file_writes(true);
        }
    }
    let mut provision_default_sandbox = session_id.is_some();
    if let Some(session_id) = session_id {
        config = config.with_data_dir_evidence(data_dir, session_id);
        if let Some(attachment) = FileAgentSessionStore::for_data_dir(data_dir)
            .load_session(session_id)
            .map_err(|source| CliError::Parse {
                detail: format!("load agent session sandbox attachment: {source}"),
            })?
            .and_then(|session| session.sandbox_attachment)
        {
            let sandbox_id = sinomo_runtime::sandbox::SandboxId::new(
                attachment.sandbox_id.clone(),
            )
            .or_else(|_| {
                sinomo_runtime::sandbox::SandboxId::from_legacy_named(
                    attachment.sandbox_id.clone(),
                )
            })
            .map_err(|source| CliError::Parse {
                detail: format!(
                    "agent session `{session_id}` has invalid sandbox attachment `{}`: {source}",
                    attachment.sandbox_id
                ),
            })?;
            config = config.with_sandbox_instance_id(sandbox_id);
            provision_default_sandbox = attachment.attachment_kind
                == sinomo_agent::AgentSessionSandboxAttachmentKind::Automatic;
        }
    }
    match live_sifs_mount.as_ref() {
        Some(mount) => {
            config = config.with_sandbox_sifs_mount_root(mount.mount_root());
            match prepare_simple_sandbox_agent_home(data_dir, definition.key()) {
                Ok(home_sifs_path) => {
                    config = config.with_agent_home_sifs_path(home_sifs_path);
                }
                Err(detail) => {
                    // A permissions, ownership, or cluster availability problem
                    // must not silently substitute a session- or host-scoped
                    // directory for the canonical agent home.
                    eprintln!(
                        "warning: live SIFS home for agent `{}` is unavailable: {detail}",
                        definition.key()
                    );
                    config = config.without_agent_home_sifs_path();
                }
            }
        }
        None => {
            // A SIFS path label without a live filesystem would falsely imply
            // synchronization.
            config = config.without_agent_home_sifs_path();
        }
    }
    if let Some(sandbox_node_id) = sandbox_node_id {
        config = config.with_sandbox_runtime_node_id(sandbox_node_id);
    }
    if provision_default_sandbox {
        if let Some(requested_node_id) = requested_sandbox_node_id.as_ref()
            && let Ok(cluster) = read_cluster_state(&ClusterPaths::new(data_dir))
            && requested_node_id != &cluster.local_node_id
        {
            return Err(CliError::Parse {
                detail: format!(
                    "automatic agent sandbox provisioning on remote node `{requested_node_id}` must be routed through the sandbox service; local node is `{}`",
                    cluster.local_node_id
                ),
            });
        }
        let mut mounts = sandbox_mounts_for_workspace_grants(
            workspace_grants,
            allow_workspace_mutation && workspace_mutation_authority,
        )?;
        if let Some(home_sifs_path) = config.agent_home_sifs_path.as_ref() {
            mounts.push(sinomo_runtime::sandbox::SandboxMount {
                source_sifs_path: home_sifs_path.clone(),
                mount_path: sinomo_runtime::sandbox::SandboxPath::new("/home/agent").map_err(
                    |source| CliError::Parse {
                        detail: format!("invalid default sandbox home mount: {source}"),
                    },
                )?,
                mode: sinomo_runtime::sandbox::SandboxMountMode::ReadWrite,
            });
        }
        sinomo_agent::local_workspace::provision_default_agent_session_sandbox(&config, mounts)
            .map_err(|detail| CliError::Parse { detail })?;
    }
    // Stage every fallible principal-bound executor before touching the live
    // registry. A failed SIFS/history setup must leave the previous agent's
    // complete registry intact rather than exposing a partial rebuild.
    let recall_tools = agent_recall_tool_registry(data_dir, definition)?;
    let mcp_tools = agent_mcp_tool_registry(data_dir, definition)?;
    registry.clear();
    // Effect-boundary permissions are host-local state. Configure the
    // authoritative store after rebuilding the registry so unrestricted tools
    // fail closed while ordinary sandboxed tools remain unaffected.
    registry.set_local_permission_data_dir(data_dir.root());
    if let Some(mount) = live_sifs_mount.as_ref() {
        mount.retain_for_registry(registry);
    }
    // An authoritative empty conversation projection is a complete revocation,
    // not permission to keep using the handler's previous compatibility root.
    // The root is still needed above to reconcile the persistent sandbox and
    // model configuration, but no workspace tool may retain it as authority.
    if !resolved_workspace_roots.is_empty() {
        sinomo_agent::local_workspace::register_tools(registry, config);
    }
    sinomo_agent::config::register_tools(
        registry,
        ConfigAgentToolConfig::new(data_dir.clone(), store),
    );
    sinomo_agent::register_skill_tools(
        registry,
        sinomo_agent::SkillAgentToolConfig::new(data_dir.clone()),
    );
    register_hosted_model_tools_for_workspace(registry, data_dir, resolved_workspace_roots);
    registry.replace_registered_tools_from(&mcp_tools);
    registry.replace_registered_tools_from(&recall_tools);
    let remote_transport: Arc<dyn sinomo_agent::AgentRemoteObjectRpcTransport> = Arc::new(
        sinomo_agent::TargetAwareAgentRemoteObjectRpcTransport::new(
            |target_node_id: Option<String>,
             route_key: Option<String>,
             call: network::ObjectRpcCall|
             -> sinomo_agent::AgentRemoteObjectRpcFuture<'static> {
                if route_key.is_some() {
                    return Box::pin(async {
                        Err(
                            "this runtime does not resolve remote-management route_key values; use target_node_id"
                                .to_string(),
                        )
                    });
                }
                remote_management_cli_rpc(target_node_id, call)
            },
        ),
    );
    let module_projection = agent_module_tool_projection(data_dir);
    sinomo_agent::register_module_agent_tools(
        registry,
        &module_projection,
        Arc::clone(&remote_transport),
    );
    sinomo_agent::register_remote_management_agent_tools(registry, remote_transport);
    registry.register(CliClusterStatusAgentTool::new(data_dir.clone()));
    Ok(())
}

pub(crate) fn agent_module_tool_projection(
    data_dir: &SinomoDataDir,
) -> sinomo_agent::AgentModuleToolProjection {
    match sinomo_agent::product_module_tool_projection(data_dir) {
        Ok(projection) => projection,
        Err(error) => {
            eprintln!(
                "warning: active module tools are unavailable to this agent runtime: {error}"
            );
            sinomo_agent::AgentModuleToolProjection::default()
        }
    }
}

fn agent_mcp_tool_registry(
    data_dir: &SinomoDataDir,
    definition: &AgentDefinition,
) -> Result<AgentToolRegistry, CliError> {
    let registry = AgentToolRegistry::new();
    let servers = definition
        .tool_runtime
        .mcp_servers
        .iter()
        .filter(|server| server.enabled)
        .collect::<Vec<_>>();
    if servers.is_empty() {
        return Ok(registry);
    }

    let required = servers.iter().any(|server| server.required);
    let permission = LocalNodePermissionStore::new(data_dir.root())
        .try_permission_granted(LocalNodePermission::ExternalIntegrations);
    match permission {
        Ok(true) => {}
        Ok(false) if required => {
            return Err(CliError::Parse {
                detail: "required MCP integrations need the node-local `external-integrations` permission; grant it with `sinomo node permissions allow external-integrations`"
                    .to_string(),
            });
        }
        Ok(false) => {
            eprintln!(
                "warning: configured MCP integrations are disabled until `sinomo node permissions allow external-integrations` is run locally"
            );
            return Ok(registry);
        }
        Err(source) if required => {
            return Err(CliError::Parse {
                detail: format!(
                    "cannot verify node-local authority for required MCP integrations: {source}"
                ),
            });
        }
        Err(source) => {
            eprintln!(
                "warning: configured MCP integrations fail closed because node-local permission state is unreadable: {source}"
            );
            return Ok(registry);
        }
    }

    for server in servers {
        match sinomo_agent::register_mcp_server_tools(&registry, server) {
            Ok(_) => {}
            Err(source) if server.required => {
                return Err(CliError::Parse {
                    detail: format!(
                        "configure required MCP server `{}` for agent `{}`: {source}",
                        server.name,
                        definition.key()
                    ),
                });
            }
            Err(source) => {
                eprintln!(
                    "warning: optional MCP server `{}` is unavailable for agent `{}`: {source}",
                    server.name,
                    definition.key()
                );
            }
        }
    }
    Ok(registry)
}

/// Registers the hosted model tools backed by whichever supported provider credential is active.
///
/// An OpenAI API key is preferred because it serves both hosted web search and image generation. A
/// Codex ChatGPT sign-in reaches the same image model and is used when no OpenAI key is present, so
/// device-authenticated installs still get image generation. With neither credential the hosted
/// tools stay unregistered rather than failing at call time.
fn register_hosted_model_tools_for_workspace(
    registry: &AgentToolRegistry,
    data_dir: &SinomoDataDir,
    workspace_roots: &[PathBuf],
) {
    let Some(backend) = active_hosted_model_backend(data_dir) else {
        return;
    };
    let artifact_root = data_dir
        .root()
        .join("events")
        .join("agent")
        .join("evidence")
        .join("generated-images");
    register_hosted_model_tools(
        registry,
        HostedModelAgentToolConfig::new(backend, artifact_root)
            .with_allowed_image_roots(workspace_roots.iter().cloned()),
    );
}

fn active_hosted_model_backend(
    data_dir: &SinomoDataDir,
) -> Option<Arc<dyn HostedModelToolBackend>> {
    if let Some(api_key) = provider_api_key_from_env(OpenAiProvider::PROVIDER_ID)
        .map(|(api_key, _source)| api_key)
        .or_else(|| {
            stored_provider_api_key_in_data_dir(data_dir, OpenAiProvider::PROVIDER_ID)
                .ok()
                .flatten()
        })
    {
        let defaults = OpenAiConfig::default();
        return Some(Arc::new(OpenAiProvider::new(OpenAiConfig {
            api_base_url: defaults.api_base_url,
            default_model: defaults.default_model,
            api_key: Some(api_key),
            credentials_path: None,
            options: defaults.options,
        })));
    }

    let client = OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
        credentials_path: Some(codex_credentials_file_in_data_dir(data_dir)),
        ..OpenAiResponsesCodexConfig::default()
    });
    // A stored sign-in is what makes the image tool available, so resolve it now rather than
    // advertising a tool that every call would reject.
    client.chatgpt_tokens().ok()?;
    Some(Arc::new(client))
}

fn agent_recall_tool_registry(
    data_dir: &SinomoDataDir,
    definition: &AgentDefinition,
) -> Result<AgentToolRegistry, CliError> {
    let registry = AgentToolRegistry::new();
    let memory_backend = sinomo_agent::SifsMemoryNoteBackend::new(data_dir, definition.key())
        .map_err(|source| CliError::Parse {
            detail: format!("configure agent Markdown memory notes: {source}"),
        })?;
    sinomo_agent::memory_notes::register_tools(
        &registry,
        sinomo_agent::MemoryNoteAgentToolConfig::from_backend(memory_backend),
    );
    agent_history::register_agent_history_search_tool(&registry, data_dir, definition.key())?;
    Ok(registry)
}

fn sandbox_mounts_for_workspace_grants(
    workspace_grants: &[WorkspaceRef],
    read_write: bool,
) -> Result<Vec<sinomo_runtime::sandbox::SandboxMount>, CliError> {
    workspace_grants
        .iter()
        .enumerate()
        .filter_map(|(index, workspace)| {
            workspace
                .sifs_root
                .as_ref()
                .map(|sifs_root| (index, sifs_root))
        })
        .map(|(index, sifs_root)| {
            let mount_path = if index == 0 {
                "/work".to_string()
            } else {
                format!("/workspaces/{}", index + 1)
            };
            Ok(sinomo_runtime::sandbox::SandboxMount {
                source_sifs_path: sifs_root.clone(),
                mount_path: sinomo_runtime::sandbox::SandboxPath::new(&mount_path).map_err(
                    |source| CliError::Parse {
                        detail: format!("invalid sandbox workspace mount `{mount_path}`: {source}"),
                    },
                )?,
                mode: if read_write {
                    sinomo_runtime::sandbox::SandboxMountMode::ReadWrite
                } else {
                    sinomo_runtime::sandbox::SandboxMountMode::ReadOnly
                },
            })
        })
        .collect()
}

#[cfg(unix)]
fn prepare_simple_sandbox_agent_home(
    data_dir: &SinomoDataDir,
    agent_id: &str,
) -> Result<String, String> {
    let path = sinomo_agent::local_workspace::agent_home_sifs_path_for_id(agent_id);
    // These libc accessors have no preconditions and only read the effective
    // credentials of the current process.
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    sifs_file_manager::SifsFileManagerBackend::open(data_dir)?
        .ensure_posix_directory_with_attrs(&path, uid, gid, 0o700)?;
    Ok(path)
}

#[cfg(not(unix))]
fn prepare_simple_sandbox_agent_home(
    _data_dir: &SinomoDataDir,
    _agent_id: &str,
) -> Result<String, String> {
    Err("POSIX SIFS agent homes are unavailable on this platform".to_string())
}

#[cfg(all(unix, feature = "dfs-fuse"))]
fn ensure_simple_sandbox_sifs_mount(
    data_dir: &SinomoDataDir,
) -> Result<Option<SimpleSandboxSifsMount>, CliError> {
    if read_cluster_state(&ClusterPaths::new(data_dir)).is_err() {
        return Ok(None);
    }
    let data_root =
        fs::canonicalize(data_dir.root()).unwrap_or_else(|_| data_dir.root().to_path_buf());
    let mounts = SIMPLE_SANDBOX_SIFS_MOUNTS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut mounts = mounts.lock().map_err(|_| CliError::Parse {
        detail: "simple-sandbox SIFS mount registry is unavailable".to_string(),
    })?;
    if let Some(handle) = mounts.get(&data_root).and_then(Weak::upgrade) {
        let existing = handle.lock().map_err(|_| CliError::Parse {
            detail: "simple-sandbox SIFS mount handle is unavailable".to_string(),
        })?;
        if existing.is_active() {
            let mount_root = existing.mount_root().to_path_buf();
            drop(existing);
            return Ok(Some(SimpleSandboxSifsMount { mount_root, handle }));
        }
    }
    mounts.remove(&data_root);
    let mount_point = simple_sandbox_sifs_mount_point(data_dir);
    let mut options = sinomo_dfs_fuse::DfsFuseMountOptions::new(mount_point)
        .with_data_dir(data_dir.root())
        .with_root_path("/");
    // Live sandbox paths must not wait for a command/checkpoint boundary or a
    // metadata cache interval before observing cluster changes.
    options.attr_ttl = Duration::ZERO;
    options.entry_ttl = Duration::ZERO;
    let handle =
        sinomo_dfs_fuse::mount_sifs_background(options).map_err(|source| CliError::Parse {
            detail: format!("mount live SIFS for simple sandbox: {source}"),
        })?;
    let mount_root = handle.mount_root().to_path_buf();
    let handle = Arc::new(Mutex::new(handle));
    mounts.insert(data_root, Arc::downgrade(&handle));
    Ok(Some(SimpleSandboxSifsMount { mount_root, handle }))
}

#[cfg(not(all(unix, feature = "dfs-fuse")))]
fn ensure_simple_sandbox_sifs_mount(
    _data_dir: &SinomoDataDir,
) -> Result<Option<SimpleSandboxSifsMount>, CliError> {
    Ok(None)
}

fn simple_sandbox_sifs_mount_point(data_dir: &SinomoDataDir) -> PathBuf {
    simple_sandbox_sifs_mount_point_for_process(data_dir, std::process::id())
}

fn simple_sandbox_sifs_mount_point_for_process(
    data_dir: &SinomoDataDir,
    process_id: u32,
) -> PathBuf {
    data_dir
        .root()
        .join("mounts")
        .join("sifs")
        .join("by-process")
        .join(process_id.to_string())
}

fn live_sifs_mount_for_selected_root(
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
) -> Result<Option<SimpleSandboxSifsMount>, CliError> {
    if !workspace_root_uses_simple_sandbox_sifs_mount(data_dir, workspace_root)? {
        return Ok(None);
    }
    let workspace_root = absolute_normalized_path(workspace_root)?;
    let Some(active_mount) = ensure_simple_sandbox_sifs_mount(data_dir)? else {
        return Ok(None);
    };
    let active_mount_root = absolute_normalized_path(active_mount.mount_root())?;
    if workspace_root.starts_with(&active_mount_root) {
        Ok(Some(active_mount))
    } else {
        Ok(None)
    }
}

fn workspace_root_uses_simple_sandbox_sifs_mount(
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
) -> Result<bool, CliError> {
    let workspace_root = absolute_normalized_path(workspace_root)?;
    let expected_mount_root = absolute_normalized_path(&simple_sandbox_sifs_mount_point(data_dir))?;
    Ok(workspace_root.starts_with(expected_mount_root))
}

pub(crate) fn agent_definition_has_workspace_mutation_authority(
    definition: &AgentDefinition,
) -> bool {
    definition.package.provenance.source == AgentPackageSource::BuiltIn
        && matches!(definition.key(), "main-agent" | "coding-agent")
}

fn validate_mutating_workspace_root_with_live_mount(
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
    verified_sifs_mount: Option<&Path>,
) -> Result<(), CliError> {
    let workspace_root = absolute_normalized_path(workspace_root)?;
    if workspace_root.parent().is_none() {
        return Err(unsafe_mutating_workspace_error(&workspace_root));
    }
    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        let home = absolute_normalized_path(Path::new(&home))?;
        if workspace_root == home {
            return Err(unsafe_mutating_workspace_error(&workspace_root));
        }
    }
    let data_root = absolute_normalized_path(&data_dir.root())?;
    let is_live_sifs_workspace = verified_sifs_mount
        .map(absolute_normalized_path)
        .transpose()?
        .is_some_and(|mount_root| workspace_root.starts_with(mount_root));
    if (workspace_root == data_root || workspace_root.starts_with(&data_root))
        && !is_live_sifs_workspace
    {
        return Err(unsafe_mutating_workspace_error(&workspace_root));
    }
    Ok(())
}

pub(crate) fn absolute_normalized_path(path: &Path) -> Result<PathBuf, CliError> {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return Ok(canonical);
    }
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|current| current.join(path))
        .map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

fn unsafe_mutating_workspace_error(workspace_root: &Path) -> CliError {
    CliError::Parse {
        detail: format!(
            "agent workspace mutation is disabled for unsafe workspace root `{}`; select an explicit workspace directory",
            workspace_root.display()
        ),
    }
}

pub(crate) fn main_agent_config_store() -> Result<sinomo_runtime::config::ConfigStore, CliError> {
    let builder = sinomo_runtime::config::ConfigRegistry::builder();
    let builder = sinomo_runtime::module::register_config_definitions(builder)
        .map_err(|detail| CliError::Parse { detail })?;
    let builder =
        sinomo_runtime::rollout::register_config_definitions(builder).map_err(CliError::from)?;
    let builder = sinomo_runtime::sandbox::register_config_definitions(builder)?;
    let builder =
        sinomo_lm::register_config_definitions(builder).map_err(CliError::LanguageModel)?;
    let builder = register_cli_runtime_config_definitions(builder)
        .map_err(|detail| CliError::Parse { detail })?;
    Ok(sinomo_runtime::config::ConfigStore::new(builder.build()))
}

fn agent_decision_json(decision: &sinomo_agent::AgentLoopDecision) -> Result<String, CliError> {
    serde_json::to_string_pretty(decision).map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })
}

/// Installs the KV-backed cluster directory provider once per CLI process so
/// direct/debug commands and local fallback reads see cluster state through
/// the routing layer's provider seam (the daemon does the same at startup).
pub(crate) fn ensure_cluster_directory_installed() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(sinomo_runtime::cluster::install_kv_cluster_directory);
}

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    init_explicit_cli_dfs_perf_tracing(&args);
    run_parsed_cli_invocation(parse_cli_invocation(&args)?)
}

/// Host the full main-daemon product catalog in a foreground process whose external wrapper owns
/// its lifetime. This is intentionally separate from CLI parsing so the graphical app's sibling
/// daemon executable does not masquerade as a general command-line surface.
pub fn run_app_owned_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    ensure_cluster_directory_installed();
    commands::run_app_owned_daemon_host(options)
}

fn init_explicit_cli_dfs_perf_tracing(args: &[String]) {
    if !cli_invocation_supports_explicit_dfs_perf_tracing(args) {
        return;
    }
    let requested = std::env::var("SINOMO_DFS_PERF_TRACE")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        });
    if requested {
        let _ = sinomo_runtime::observe::init_observe_runtime_with_stderr(
            sinomo_runtime::observe::ObserveRuntimeConfig::process("cli", None),
        );
    }
}

fn cli_invocation_supports_explicit_dfs_perf_tracing(args: &[String]) -> bool {
    matches!(args, [test, run, ..] if test == "test" && run == "run")
}

fn run_cli_command(args: &[String]) -> Result<(), CliError> {
    run_parsed_cli_invocation(parse_cli_invocation(args)?)
}

pub(crate) fn with_rollout_activation_admission<T>(
    rollout_paths: &SelfRolloutPaths,
    operation: impl FnOnce() -> Result<T, CliError>,
) -> Result<T, CliError> {
    let _admission = acquire_rollout_activation_guard(rollout_paths)?;
    operation()
}

fn run_registered_cli_command(
    command: RegisteredCliCommand,
    force_local: bool,
) -> Result<(), CliError> {
    let command_name = command.name().to_string();
    let mut stdout = CliStdoutWriter;
    let mut stderr = CliStderrWriter;
    let mut stdin = std::io::stdin();
    let mut context =
        CliCommandContext::new(&mut stdout, &mut stderr).with_force_local(force_local);
    if !std::io::IsTerminal::is_terminal(&stdin) {
        context = context.with_stdin(&mut stdin);
    }
    command.run(&mut context).map_err(|error| {
        if error.exit_code() == 1 {
            CliError::ExternalCommand {
                command: command_name,
                detail: error.to_string(),
            }
        } else {
            CliError::ModuleCommandExit {
                command: command_name,
                detail: error.to_string(),
                exit_code: error.exit_code(),
            }
        }
    })
}

fn run_parsed_cli_invocation(invocation: CliInvocation) -> Result<(), CliError> {
    ensure_cluster_directory_installed();
    let command = invocation.command;
    let command_is_help = command.as_ref().is_some_and(cli_command_is_help);
    let supports_degraded_local_mode = command
        .as_ref()
        .is_none_or(cli_command_supports_degraded_local_mode);
    let needs_daemon_probe = command.as_ref().is_none_or(cli_command_needs_daemon_probe);
    let daemon = if invocation.options.force_local || command_is_help || !needs_daemon_probe {
        None
    } else {
        default_main_daemon_status()?
    };
    // Cache only a successful probe. Daemon absence is transient (for example,
    // while a busy local control endpoint exceeds the short preflight timeout),
    // so downstream operations must retain the opportunity to probe again.
    let _daemon_status_guard = daemon.clone().map(enter_cli_default_main_daemon_status);
    let local_cluster_quorum = if invocation.options.force_local || command_is_help {
        None
    } else {
        cli_local_cluster_quorum_if_relevant(supports_degraded_local_mode, daemon.as_ref())?
    };
    let force_local = invocation.options.force_local
        || match command.as_ref() {
            Some(command) => cli_command_enters_degraded_local_mode(
                command,
                daemon.is_some(),
                local_cluster_quorum,
            ),
            None => daemon.is_none() || local_cluster_quorum == Some(false),
        };
    let _guard = enter_cli_force_local_mode(force_local);

    if !cli_force_local_mode()
        && !command_is_help
        && command
            .as_ref()
            .is_some_and(cli_command_requires_cluster_state)
    {
        let paths = cluster_paths_from_defaults()?;
        require_cluster_runtime_quorum_with_paths_and_daemon(&paths, daemon.as_ref())?;
    }

    match command {
        None => run_tui(),
        Some(command) if command.requires_rollout_admission() => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
            with_rollout_activation_admission(&rollout_paths, || {
                run_registered_cli_command(command, force_local)
            })
        }
        Some(command) => run_registered_cli_command(command, force_local),
    }
}

const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
const DAEMON_LOCKED_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const DAEMON_LOCKED_REPROBE_INTERVAL: Duration = Duration::from_millis(50);
// Candidate startup includes enabled-module recovery and native-module loading.
// Cold, resource-constrained nodes can legitimately need more than one minute;
// keep the controller and shadow supervisor on the same generous deadline.
const DAEMON_ROLLOUT_TIMEOUT: Duration = Duration::from_secs(180);
const DAEMON_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const DAEMON_CLUSTER_KV_TXN_TIMEOUT: Duration = Duration::from_secs(8);
const IROH_DAEMON_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const CLUSTER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const CLUSTER_HEALTH_DAEMON_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const CLUSTER_HEALTH_CLI_ENDPOINT_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const CLUSTER_HEALTH_CLI_MAX_ENDPOINTS_PER_MEMBER: usize = 2;
const CLUSTER_HEALTH_PEER_ROWS_TIMEOUT: Duration = network::DEFAULT_NETWORK_TIMEOUT;
const DAEMON_AGENT_SESSION_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const SINOMO_JOIN_URI_PREFIX: &str = cluster::SINOMO_JOIN_URI_PREFIX;
const CLUSTER_INVITE_DEFAULT_TTL_SECONDS: i64 = 24 * 60 * 60;
const INSTALL_DEFAULT_HOST: &str = "0.0.0.0";

fn daemon_fetch_cluster_kv_local_snapshot_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<KvLocalRead<KvSnapshot>, sinomo_runtime::daemon::DaemonError> {
    let request = cluster_kv_local_snapshot_rpc_request()?;
    let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &request, timeout)?;
    decode_cluster_kv_local_snapshot_rpc_response(&response).map_err(Into::into)
}

fn daemon_fetch_cluster_kv_authoritative_snapshot(
    address: &str,
    timeout: Duration,
) -> Result<KvAuthoritativeRead<KvSnapshot>, sinomo_runtime::daemon::DaemonError> {
    let request = cluster_kv_authoritative_snapshot_rpc_request()?;
    let response = daemon_send_daemon_rpc(address, &request, timeout)?;
    decode_cluster_kv_authoritative_snapshot_rpc_response(&response).map_err(Into::into)
}

fn daemon_fetch_cluster_kv_authoritative_snapshot_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<KvAuthoritativeRead<KvSnapshot>, sinomo_runtime::daemon::DaemonError> {
    let request = cluster_kv_authoritative_snapshot_rpc_request()?;
    let response = daemon_send_daemon_rpc_to_endpoint(endpoint, &request, timeout)?;
    decode_cluster_kv_authoritative_snapshot_rpc_response(&response).map_err(Into::into)
}

fn cluster_kv_local_snapshot_rpc_request() -> Result<network::ObjectRpcRequest, KvError> {
    let payload = sinomo_runtime::kv::cluster_kv_local_snapshot_rpc_payload()?;
    Ok(network::ObjectRpcRequest::from_encoded_body(
        payload.route,
        payload.category,
        payload.body,
    ))
}

fn decode_cluster_kv_local_snapshot_rpc_response(
    response: &network::ObjectRpcResponse,
) -> Result<KvLocalRead<KvSnapshot>, network::NetworkError> {
    response.error_detail()?;
    let read = sinomo_runtime::kv::decode_cluster_kv_local_snapshot_rpc_body(&response.body)
        .map_err(sinomo_runtime::cluster::kv_err)?;
    if read.revision != read.value.revision {
        return Err(network::NetworkError::Serialization {
            detail: format!(
                "cluster KV local-read metadata revision {:?} does not match snapshot revision {:?}",
                read.revision, read.value.revision
            ),
        });
    }
    Ok(read)
}

fn cluster_kv_authoritative_snapshot_rpc_request() -> Result<network::ObjectRpcRequest, KvError> {
    let payload = sinomo_runtime::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
    Ok(network::ObjectRpcRequest::from_encoded_body(
        payload.route,
        payload.category,
        payload.body,
    ))
}

fn decode_cluster_kv_authoritative_snapshot_rpc_response(
    response: &network::ObjectRpcResponse,
) -> Result<KvAuthoritativeRead<KvSnapshot>, network::NetworkError> {
    response.error_detail()?;
    let read =
        sinomo_runtime::kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
            .map_err(sinomo_runtime::cluster::kv_err)?;
    if read.authority.term == 0
        || !matches!(
            read.authority.basis,
            sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks,
                required_quorum,
            } if required_quorum > 0 && voter_acks >= required_quorum
        )
    {
        return Err(network::NetworkError::ClusterReadOnly {
            detail: "cluster KV authoritative snapshot lacks a fresh current-term quorum proof"
                .to_string(),
        });
    }
    if read.authority.revision != read.value.revision {
        return Err(network::NetworkError::Serialization {
            detail: format!(
                "cluster KV authority revision {:?} does not match snapshot revision {:?}",
                read.authority.revision, read.value.revision
            ),
        });
    }
    Ok(read)
}
