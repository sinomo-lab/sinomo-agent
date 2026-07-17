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
    AgentCatalog, AgentDefinition, AgentDefinitionSifsStore, AgentDefinitionStore,
    AgentEventPayload, AgentLoopDecision, AgentLoopInput, AgentModelEvent, AgentOp,
    AgentPackageProvenance, AgentPackageSource, AgentReadinessCheckStatus, AgentReadinessReport,
    AgentRole, AgentRuntimeEvent, AgentRuntimeEventCursor, AgentRuntimeEventPayload,
    AgentRuntimeStorage, AgentSession, AgentSessionControlEventCursor, AgentSessionControlResponse,
    AgentSessionFollowUpRequest, AgentSessionInspectRequest, AgentSessionInspectResponse,
    AgentSessionInterruptRequest, AgentSessionProgressRequest, AgentSessionProgressResponse,
    AgentSessionResumeRequest, AgentSessionResumeResponse, AgentSessionRuntime,
    AgentSessionSteerRequest, AgentSessionStore, AgentSessionTerminalResultsRequest,
    AgentSessionTerminalResultsResponse, AgentTaskTerminalResult, AgentTool, AgentToolDescriptor,
    AgentToolEvidence, AgentToolKind, AgentToolProgressEvent, AgentToolRegistry, AgentToolRequest,
    AgentToolResult, AgentToolSpec, ConfigAgentToolConfig, EventEnvelopeExt, FileAgentSessionStore,
    FileAgentTraceStore, InteractionModel, InteractionOutput, InteractionSession,
    InteractionSurface, InteractionSurfaceKind, LocalWorkspaceAgentConfig, SkillActivationScope,
    SkillCatalogEntry, SkillCatalogSifsStore, SkillCatalogSource, SkillCatalogSourceKind,
    SkillCatalogStore, SkillCatalogWrite, TaskId, WorkspaceRef, interaction_event,
    render_common_skill_markdown, run_local_agent_readiness_gate, user_interaction_input_event,
};
use sinomo_lm::{
    AnthropicProvider, CODEX_DEFAULT_MODEL, CODEX_PROVIDER_ID, ClaudeProvider, CurrentDefaultModel,
    DEFAULT_PROVIDER_ACCOUNT, DeepSeekProvider, GeminiProvider, GitHubCopilotProvider,
    LM_PROVIDER_CONFIG_KEY, LM_PROVIDER_CONFIG_KV_KEY, LM_RUNTIME_CONFIG_KEY,
    LM_RUNTIME_CONFIG_KV_KEY, LanguageModelCancellation, LanguageModelError,
    LanguageModelModelOptions, LanguageModelProviderOptions, LanguageModelProviderUsage,
    LanguageModelRequest, LanguageModelResponse, LanguageModelStreamEvent, ModelConfig, ModelPaths,
    ModelRuntimeConfig, NvidiaProvider, OpenAiProvider, ProviderAccountConfig,
    ProviderReasoningEffort, ProviderTextVerbosity, claude_provider, codex_provider,
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
    subscription_usage_provider_ids, test_provider_account_credentials, test_provider_credentials,
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
    CliCommandContext, CliCommandRegistry, CliParseError, RegisteredCliCommand,
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
use sinomo_runtime::daemon::{
    AuthenticatedDaemonCommand, DaemonError, DaemonSignerAuthority,
    authenticate_daemon_control_request, authorize_authenticated_daemon_command,
    sign_daemon_rpc_request_from_data_dir,
};
use sinomo_runtime::daemon::{
    DaemonClusterProcess, DaemonClusterRegistration, DaemonStopBeforeInstallStart,
    DaemonStopBeforeInstallStatus, DaemonStopIfRunningStatus, InstallLayout,
    InstalledDaemonInstallPlanRequest, InstalledDaemonStartStatus, ManagedSourceRepoInstallStatus,
    activate_boot_entries as daemon_activate_boot_entries, current_shadow_daemon_options,
    daemon_advertised_endpoints_for_state_with_manual_config, daemon_cluster_registration,
    daemon_cluster_status, daemon_install_start_installed_sinomo_daemon,
    daemon_install_stop_daemon_before_install, daemon_install_stop_daemon_if_running,
    daemon_main_binary_for_shadow_start, deactivate_boot_entries as daemon_deactivate_boot_entries,
    default_boot_dir as daemon_default_boot_dir,
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
    read_daemon_endpoint_advertisement_config, remove_boot_entry as daemon_remove_boot_entry,
    run_daemon_control_loop_with_services as daemon_run_daemon_control_loop_with_services,
    run_shadow_daemon_supervision_loop as daemon_run_shadow_daemon_supervision_loop,
    send_daemon_cluster_maintenance_refresh as daemon_send_daemon_cluster_maintenance_refresh,
    send_daemon_cluster_maintenance_refresh_to_endpoint as daemon_send_daemon_cluster_maintenance_refresh_to_endpoint,
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
    send_daemon_rpc as daemon_send_daemon_rpc,
    send_daemon_rpc_session_async as daemon_send_daemon_rpc_session_async,
    send_daemon_rpc_to_endpoint as daemon_send_daemon_rpc_to_endpoint,
    send_daemon_rpc_with_timeouts as daemon_send_daemon_rpc_with_timeouts,
    send_daemon_shutdown_request,
    send_object_rpc_to_target_with_timeouts as daemon_send_object_rpc_to_target_with_timeouts,
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
#[cfg(test)]
use sinomo_runtime::daemon::{
    daemon_advertised_endpoints_for_state, daemon_endpoint_advertisement_config_file,
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
    DfsConflictRecord, DfsFileSystemId, DfsMetadataBackend, DfsMetadataShardConfig, DfsObjectId,
    DfsOperatorConflictVersionView, DfsOperatorConflictVersionsReport, DfsOperatorInspectionCursor,
    DfsOperatorInspector, DfsOperatorRepairView, DfsVersion, LocalShardedMetadataBackend,
    LocalSobjMetadataBackend,
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
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::{
    ClusterId, DAEMON_MAIN_CLUSTER_ROLE, NodeId, cluster_member_expected_daemon_process,
    cluster_member_is_local_process, load_or_create_main_node_identity,
    load_or_create_shadow_node_identity, shadow_daemon_node_id,
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
    prepare_local_rollout_artifacts_with_options, prepare_rollout_dev_artifacts,
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
        daemon_send_object_rpc_to_target_with_timeouts(
            target_node_id,
            &request,
            network::DaemonRpcTimeouts::connect_and_idle(timeout),
            false,
        )
        .and_then(|response| response.error_detail().map_err(Into::into))
        .is_ok()
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

fn cluster_has_local_quorum(paths: &ClusterPaths, state: &ClusterBootstrapState) -> bool {
    cluster::cluster_has_local_quorum_with_host(
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
    cluster::cluster_reachable_state_authority_count_with_host(
        paths,
        state,
        cli_cluster_state_authority_reachability_options(),
        &mut CliClusterStateAuthorityReachabilityHost,
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
        CliCommand::Registered(command) | CliCommand::RegisteredRolloutMutation(command) => {
            command.policy().requires_cluster_state
        }
        _ => false,
    }
}

pub(crate) fn cli_command_supports_degraded_local_mode(command: &CliCommand) -> bool {
    match command {
        CliCommand::Tui
        | CliCommand::Models(_)
        | CliCommand::Providers(_)
        | CliCommand::Agent(_)
        | CliCommand::Skill(_) => true,
        CliCommand::Test(command) => !test_command_requires_cluster_state(command),
        CliCommand::Dfs(command) => !dfs_command_requires_cluster_state(command),
        CliCommand::Registered(command) | CliCommand::RegisteredRolloutMutation(command) => {
            command.policy().supports_degraded_local_mode
        }
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
        | CliCommand::Skill(SkillCliCommand::Help)
        | CliCommand::Agent(AgentCliCommand::Session(AgentSessionCliCommand::Help)) => true,
        CliCommand::Registered(command) | CliCommand::RegisteredRolloutMutation(command) => {
            command.is_help()
        }
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
    Skill(SkillCliCommand),
    Registered(RegisteredCliCommand),
    RegisteredRolloutMutation(RegisteredCliCommand),
    Usage(UsageOptions),
}

pub fn default_cli_command_registry() -> CliCommandRegistry {
    let local_data_dir = SinomoDataDir::from_env_or_home().ok();
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
    let module_config = local_data_dir
        .as_ref()
        .and_then(|data_dir| {
            module_config
                .clone()
                .with_installed_native_dylib_modules_from_data_dir(&data_dir.root())
                .ok()
        })
        .unwrap_or(module_config);
    let mut registry = CliCommandRegistry::new()
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
    if let Some(data_dir) = local_data_dir {
        registry = registry.register(
            sinomo_runtime::remote_management::node_permissions_cli_command_handler(data_dir),
        );
    }
    registry
}

fn remote_management_cli_rpc(
    target_node_id: Option<String>,
    call: network::ObjectRpcSessionCall,
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
            daemon_send_daemon_rpc_session_async(&status.address, call, options, &runtime)
                .await
                .map_err(|source| source.to_string())
        } else {
            // The running daemon owns the cluster transports and their module
            // lifecycle. A short-lived CLI process has no active Iroh module,
            // so forwarding must cross the authenticated loopback daemon
            // boundary instead of constructing a routed client here.
            DaemonApiClient::at_address(status.address)
                .object_rpc_session_to_target_with_transport_timeouts_async(
                    &target_node_id,
                    call,
                    timeouts,
                    None,
                    true,
                )
                .await
                .map_err(|source| source.to_string())
        }
    })
}

fn cli_command_registry() -> &'static CliCommandRegistry {
    &cli_runtime_config().command_registry
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
        Some((command, rest)) if matches!(command.as_str(), "skill" | "skills") => {
            Ok(CliCommand::Skill(parse_skill_command(rest)?))
        }
        Some((command, rest)) if command == "usage" => {
            Ok(CliCommand::Usage(parse_usage_options(rest)?))
        }
        _ => match cli_command_registry().parse(args)? {
            Some(command) if registered_command_requires_rollout_admission(args) => {
                Ok(CliCommand::RegisteredRolloutMutation(command))
            }
            Some(command) => Ok(CliCommand::Registered(command)),
            None => Err(CliError::Usage),
        },
    }
}

fn registered_command_requires_rollout_admission(args: &[String]) -> bool {
    matches!(
        args,
        [module, native_dylib, mutation, ..]
            if module == "module"
                && native_dylib == "native-dylib"
                && matches!(mutation.as_str(), "activate" | "unload")
    )
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
pub(crate) enum SkillCliCommand {
    Help,
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
    pub model: String,
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

pub(crate) fn parse_skill_command(args: &[String]) -> Result<SkillCliCommand, CliError> {
    match args.split_first() {
        None => Ok(SkillCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(SkillCliCommand::Help)
        }
        Some((command, rest)) if matches!(command.as_str(), "list" | "ls") => {
            Ok(SkillCliCommand::List(parse_skill_list_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "show" | "inspect" | "describe") => {
            Ok(SkillCliCommand::Show(parse_skill_show_options(rest)?))
        }
        Some((command, rest)) if command == "import" => {
            Ok(SkillCliCommand::Import(parse_skill_import_options(rest)?))
        }
        Some((command, rest)) if command == "export" => {
            Ok(SkillCliCommand::Export(parse_skill_export_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "activate" | "enable") => Ok(
            SkillCliCommand::Activate(parse_skill_activate_options(rest)?),
        ),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_skill_list_options(args: &[String]) -> Result<SkillListOptions, CliError> {
    let mut all = false;
    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--all" | "-a" => all = true,
            "--json" => json = true,
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected skill list argument `{value}`"),
                });
            }
        }
    }
    Ok(SkillListOptions { all, json })
}

pub(crate) fn parse_skill_show_options(args: &[String]) -> Result<SkillShowOptions, CliError> {
    let mut skill_id = None;
    let mut version = None;
    let mut text = false;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--text" | "--markdown" => text = true,
            "--version" => {
                index += 1;
                version = Some(skill_option_value(args.get(index), "--version")?);
            }
            "--skill" | "--id" => {
                index += 1;
                skill_id = Some(skill_option_value(args.get(index), "--skill")?);
            }
            value if skill_id.is_none() => skill_id = Some(value.to_string()),
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected skill show argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(SkillShowOptions {
        skill_id: skill_id.ok_or_else(|| CliError::Parse {
            detail: "skill show requires a skill id".to_string(),
        })?,
        version,
        text,
        json,
    })
}

pub(crate) fn parse_skill_import_options(args: &[String]) -> Result<SkillImportOptions, CliError> {
    let mut path = None;
    let mut skill_id = None;
    let mut version = "1.0.0".to_string();
    let mut source = SkillCatalogSourceKind::User;
    let mut source_id = "local-user".to_string();
    let mut activate = true;
    let mut scope = SkillActivationScope::Cluster.as_str().to_string();
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--no-activate" => activate = false,
            "--activate" => activate = true,
            "--file" | "--path" => {
                index += 1;
                path = Some(PathBuf::from(skill_option_value(
                    args.get(index),
                    "--file",
                )?));
            }
            "--id" | "--skill" => {
                index += 1;
                skill_id = Some(skill_option_value(args.get(index), "--id")?);
            }
            "--version" => {
                index += 1;
                version = skill_option_value(args.get(index), "--version")?;
            }
            "--source" => {
                index += 1;
                source =
                    parse_skill_source_kind(&skill_option_value(args.get(index), "--source")?)?;
            }
            "--source-id" => {
                index += 1;
                source_id = skill_option_value(args.get(index), "--source-id")?;
            }
            "--scope" => {
                index += 1;
                scope = skill_option_value(args.get(index), "--scope")?;
            }
            value if path.is_none() => path = Some(PathBuf::from(value)),
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected skill import argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(SkillImportOptions {
        path: path.ok_or_else(|| CliError::Parse {
            detail: "skill import requires a SKILL.md path or skill directory".to_string(),
        })?,
        skill_id,
        version,
        source,
        source_id,
        activate,
        scope,
        json,
    })
}

pub(crate) fn parse_skill_export_options(args: &[String]) -> Result<SkillExportOptions, CliError> {
    let mut skill_id = None;
    let mut version = None;
    let mut output = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--version" => {
                index += 1;
                version = Some(skill_option_value(args.get(index), "--version")?);
            }
            "--output" | "-o" => {
                index += 1;
                output = Some(PathBuf::from(skill_option_value(
                    args.get(index),
                    "--output",
                )?));
            }
            "--skill" | "--id" => {
                index += 1;
                skill_id = Some(skill_option_value(args.get(index), "--skill")?);
            }
            value if skill_id.is_none() => skill_id = Some(value.to_string()),
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected skill export argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(SkillExportOptions {
        skill_id: skill_id.ok_or_else(|| CliError::Parse {
            detail: "skill export requires a skill id".to_string(),
        })?,
        version,
        output,
        json,
    })
}

pub(crate) fn parse_skill_activate_options(
    args: &[String],
) -> Result<SkillActivateOptions, CliError> {
    let mut skill_id = None;
    let mut version = None;
    let mut scope = SkillActivationScope::Cluster.as_str().to_string();
    let mut reason = "manual skill activation".to_string();
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--version" => {
                index += 1;
                version = Some(skill_option_value(args.get(index), "--version")?);
            }
            "--scope" => {
                index += 1;
                scope = skill_option_value(args.get(index), "--scope")?;
            }
            "--reason" => {
                index += 1;
                reason = skill_option_value(args.get(index), "--reason")?;
            }
            "--skill" | "--id" => {
                index += 1;
                skill_id = Some(skill_option_value(args.get(index), "--skill")?);
            }
            value if skill_id.is_none() => skill_id = Some(value.to_string()),
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected skill activate argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(SkillActivateOptions {
        skill_id: skill_id.ok_or_else(|| CliError::Parse {
            detail: "skill activate requires a skill id".to_string(),
        })?,
        version,
        scope,
        reason,
        json,
    })
}

fn skill_option_value(value: Option<&String>, option: &str) -> Result<String, CliError> {
    value.cloned().ok_or_else(|| CliError::Parse {
        detail: format!("{option} requires a value"),
    })
}

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
    let mut agent = "main-agent".to_string();
    let mut agent_explicit = false;
    let mut provider = CODEX_PROVIDER_ID.to_string();
    let mut model = CODEX_DEFAULT_MODEL.to_string();
    let mut account = None;
    let mut session_id = None;
    let mut compact_after = false;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--prompt" => {
                index += 1;
                prompt = Some(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--prompt requires a value".to_string(),
                })?);
            }
            "--agent" => {
                index += 1;
                agent = args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--agent requires a value".to_string(),
                })?;
                agent_explicit = true;
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
            "--session" | "--session-id" => {
                index += 1;
                session_id = Some(parse_agent_session_id_arg(args.get(index), "--session")?);
            }
            "--compact-after" => compact_after = true,
            "--json" => json = true,
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
        agent,
        agent_explicit,
        provider,
        model,
        account,
        session_id,
        compact_after,
        json,
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
        SkillCliCommand::Help => {
            print_skill_help();
            Ok(())
        }
        SkillCliCommand::List(options) => list_skills(options),
        SkillCliCommand::Show(options) => show_skill(options),
        SkillCliCommand::Import(options) => import_skill(options),
        SkillCliCommand::Export(options) => export_skill(options),
        SkillCliCommand::Activate(options) => activate_skill(options),
    }
}

fn list_skills(options: SkillListOptions) -> Result<(), CliError> {
    let store = skill_store_for_cli()?;
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
    let store = skill_store_for_cli()?;
    let catalog = store.load_catalog().map_err(skill_catalog_cli_error)?;
    let entry = select_skill_entry(&catalog, &options.skill_id, options.version.as_deref())?;
    let text = store
        .read_skill_text(&entry)
        .map_err(skill_catalog_cli_error)?
        .unwrap_or_default();
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

fn import_skill(options: SkillImportOptions) -> Result<(), CliError> {
    let store = skill_store_for_cli()?;
    let text = read_skill_markdown_import(&options.path)?;
    let source =
        skill_source_from_import_options(&options.source, &options.source_id, &options.version);
    let mut write = SkillCatalogWrite::from_markdown_skill(
        options.skill_id.clone(),
        options.version.clone(),
        source,
        text,
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
    let store = skill_store_for_cli()?;
    let catalog = store.load_catalog().map_err(skill_catalog_cli_error)?;
    let entry = select_skill_entry(&catalog, &options.skill_id, options.version.as_deref())?;
    let text = store
        .read_skill_text(&entry)
        .map_err(skill_catalog_cli_error)?
        .ok_or_else(|| CliError::Parse {
            detail: format!("skill `{}` has no readable SKILL.md body", entry.skill_id),
        })?;
    let rendered = render_common_skill_markdown(&entry, &text);
    if let Some(output) = options.output.as_deref() {
        let path = skill_export_output_path(output);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| CliError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&path, rendered.as_bytes()).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
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

fn read_skill_markdown_import(path: &Path) -> Result<String, CliError> {
    let path = if path.is_dir() {
        path.join(sinomo_agent::SKILL_FILE_NAME)
    } else {
        path.to_path_buf()
    };
    fs::read_to_string(&path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn skill_export_output_path(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join(sinomo_agent::SKILL_FILE_NAME)
    } else {
        path.to_path_buf()
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

pub(crate) fn best_effort_reconciled_skill_catalog(
    data_dir: &SinomoDataDir,
) -> sinomo_agent::SkillCatalog {
    sinomo_agent::load_reconciled_skill_catalog_from_sifs(data_dir).unwrap_or_default()
}

pub(crate) fn best_effort_skill_catalog_texts(
    data_dir: &SinomoDataDir,
    catalog: &sinomo_agent::SkillCatalog,
) -> BTreeMap<String, String> {
    sinomo_agent::load_skill_catalog_texts_from_sifs(data_dir, catalog).unwrap_or_default()
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
    let catalog = load_agent_catalog_for_cli()?;
    let definition = agent_definition_from_catalog(&catalog, &agent_name)?.clone();
    let current_root = std::env::current_dir().map_err(|source| CliError::Io {
        path: "current directory".to_string(),
        detail: source.to_string(),
    })?;
    let coding_authority = agent_definition_has_coding_authority(&definition);
    let mut _resolved_workspace_root = None;
    let mut _resolved_workspace_binding = None;
    let (workspace_root, workspace_binding) = if coding_authority {
        match persisted_session.as_ref() {
            Some(session) => {
                let binding = session.workspace_binding.as_ref().ok_or_else(|| CliError::Parse {
                    detail: format!(
                        "coding-agent session `{session_id}` has no workspace binding; refusing a mutation-capable resume because its original workspace cannot be verified"
                    ),
                })?;
                let resolved =
                    workspace_binding_root_for_data_dir(&data_dir, &session_id, binding)?;
                let root = resolved.root().to_path_buf();
                _resolved_workspace_root = Some(resolved);
                (root, Some(binding.clone()))
            }
            None => {
                let resolved = coding_agent_workspace_binding(&data_dir, &current_root)?;
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
        provider_account_config(&config, &options.provider, Some(account_name)).cloned()
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
    let input = AgentLoopInput {
        session,
        latest_event: input_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    };
    let provider = options.provider.clone();
    let model = options.model.clone();
    let (runtime_config, _) = load_model_runtime_config_for_node(&data_dir)?;
    let tool_registry = agent_tool_registry_for_workspace(
        &data_dir,
        &workspace_root,
        &definition,
        Some(&session_id),
        None,
        true,
    )?;
    let skill_catalog = best_effort_reconciled_skill_catalog(&data_dir);
    let skill_catalog_texts = best_effort_skill_catalog_texts(&data_dir, &skill_catalog);
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
        .with_workspace_root(&workspace_root)
        .with_skill_catalog(skill_catalog)
        .with_skill_catalog_texts(skill_catalog_texts);
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
    let submit = runtime.submit_op(op)?;
    if !submit.accepted {
        let reason = submit
            .events
            .iter()
            .find_map(|event| match &event.payload {
                AgentEventPayload::OperationRejected { reason, .. } => Some(reason.clone()),
                _ => None,
            })
            .unwrap_or_else(|| format!("agent `{}` turn was rejected", definition.key()));
        return Err(CliError::Parse { detail: reason });
    }
    runtime.join_active_turn()?;
    let decision = runtime
        .turn_result(&op_id)?
        .and_then(|outcome| outcome.run)
        .map(|run| run.final_decision)
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "agent `{}` runtime turn `{op_id}` completed without a decision",
                definition.key()
            ),
        })?;
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
                                "coding-agent session `{}` has no workspace binding; refusing a mutation-capable resume because its original workspace cannot be verified; start a new session without `--session`",
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

pub(crate) fn coding_agent_workspace_binding(
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
            "coding-agent session `{session_id}` has a non-local workspace binding and cannot be resumed by the local CLI"
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
    allow_coding_mutation: bool,
) -> Result<AgentToolRegistry, CliError> {
    let registry = AgentToolRegistry::new();
    configure_agent_tool_registry_for_workspace(
        &registry,
        data_dir,
        workspace_root,
        definition,
        session_id,
        sandbox_node_id,
        allow_coding_mutation,
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
    allow_coding_mutation: bool,
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
        allow_coding_mutation,
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
    allow_coding_mutation: bool,
) -> Result<(), CliError> {
    let store = main_agent_config_store()?;
    let mut config = LocalWorkspaceAgentConfig::new(workspace_root)
        .with_additional_workspace_roots(resolved_workspace_roots.iter().skip(1).cloned());
    let requested_sandbox_node_id = sandbox_node_id.clone();
    let coding_authority = agent_definition_has_coding_authority(definition);
    let live_sifs_mount = match ensure_simple_sandbox_sifs_mount(data_dir) {
        Ok(mount) => mount,
        Err(error) if !workspace_root_uses_simple_sandbox_sifs_mount(data_dir, workspace_root)? => {
            // The workspace can remain usable without a live SIFS projection,
            // but this runtime cannot claim that the agent's canonical home is
            // available in that case.
            eprintln!(
                "warning: live SIFS integration for the agent runtime is unavailable; using the local workspace without the agent home: {error}"
            );
            None
        }
        Err(error) => return Err(error),
    };
    if allow_coding_mutation && coding_authority {
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
            allow_coding_mutation && coding_authority,
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
    let remote_transport: Arc<dyn sinomo_agent::AgentRemoteObjectRpcTransport> = Arc::new(
        sinomo_agent::TargetAwareAgentRemoteObjectRpcTransport::new(
            |target_node_id: Option<String>,
             route_key: Option<String>,
             call: network::ObjectRpcSessionCall|
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
    sinomo_agent::register_remote_management_agent_tools(registry, remote_transport);
    registry.register(CliClusterStatusAgentTool::new(data_dir.clone()));
    Ok(())
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

pub(crate) fn agent_definition_has_coding_authority(definition: &AgentDefinition) -> bool {
    definition.package.provenance.source == AgentPackageSource::BuiltIn
        && definition.key() == "coding-agent"
}

fn validate_mutating_workspace_root_with_live_mount(
    data_dir: &SinomoDataDir,
    workspace_root: &Path,
    verified_sifs_mount: Option<&Path>,
) -> Result<(), CliError> {
    let workspace_root = absolute_normalized_path(workspace_root)?;
    if workspace_root.parent().is_none() {
        return Err(unsafe_coding_workspace_error(&workspace_root));
    }
    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        let home = absolute_normalized_path(Path::new(&home))?;
        if workspace_root == home {
            return Err(unsafe_coding_workspace_error(&workspace_root));
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
        return Err(unsafe_coding_workspace_error(&workspace_root));
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

fn unsafe_coding_workspace_error(workspace_root: &Path) -> CliError {
    CliError::Parse {
        detail: format!(
            "coding-agent mutation is disabled for unsafe workspace root `{}`; run the command from an explicit workspace directory",
            workspace_root.display()
        ),
    }
}

pub(crate) fn main_agent_config_store() -> Result<sinomo_runtime::config::ConfigStore, CliError> {
    let builder = sinomo_runtime::config::ConfigRegistry::builder();
    let builder = sinomo_runtime::dfs::register_dfs_config_definitions(builder)?;
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

pub(crate) fn is_help_args(args: &[String]) -> bool {
    matches!(args, [arg] if matches!(arg.as_str(), "help" | "--help" | "-h"))
}

/// Installs the KV-backed cluster directory provider once per CLI process so
/// direct/debug commands and local fallback reads see cluster state through
/// the routing layer's provider seam (the daemon does the same at startup).
pub(crate) fn ensure_cluster_directory_installed() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| sinomo_runtime::cluster::install_kv_cluster_directory());
}

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    run_parsed_cli_invocation(parse_cli_invocation(&args)?)
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
    let mut context =
        CliCommandContext::new(&mut stdout, &mut stderr).with_force_local(force_local);
    command
        .run(&mut context)
        .map_err(|error| CliError::ExternalCommand {
            command: command_name,
            detail: error.to_string(),
        })
}

fn run_parsed_cli_invocation(invocation: CliInvocation) -> Result<(), CliError> {
    ensure_cluster_directory_installed();
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
        CliCommand::Skill(command) => run_skill_command(command),
        CliCommand::Registered(command) => run_registered_cli_command(command, force_local),
        CliCommand::RegisteredRolloutMutation(command) => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
            with_rollout_activation_admission(&rollout_paths, || {
                run_registered_cli_command(command, force_local)
            })
        }
        CliCommand::Usage(options) => usage(options),
    }
}

const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
// Candidate startup includes mandatory DFS recovery and native-module loading.
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
    sinomo_runtime::kv::decode_cluster_kv_snapshot_rpc_body(&response.body)
        .map_err(sinomo_runtime::network::NetworkError::directory)
}
