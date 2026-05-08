//! Shared Sinomo command-line handlers.

#[cfg(test)]
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;
use sinomo_agent::{
    EventEnvelopeExt, InteractionModel, InteractionOutput, InteractionSession, InteractionSurface,
    InteractionSurfaceKind, interaction_event, interaction_model_request,
    user_interaction_input_event,
};
#[cfg(test)]
use sinomo_evolution::is_terminal_status;
use sinomo_evolution::{
    BackgroundSchedulerPaths, BackgroundSchedulingPlan, BackgroundSchedulingRequest,
    CampaignBundlePaths, CampaignProposalInput, EvolutionCampaign, EvolutionFailureClass,
    EvolutionJobSelectionMode, EvolutionLanguageModelMutationModel, EvolutionOrchestrator,
    EvolutionPaths, EvolutionReport, EvolutionRequestInput, GitWorkspaceAdapter,
    LocalCandidateBundleStore, ResourceSchedulerDryRun, ResourceSchedulerDryRunRequest,
    ResourceUsageEstimate, ScheduledIterationPlan, ScheduledIterationRequest,
    StageEvaluationRequest, StageEvaluationUpdate, StoredEvolutionJob, StoredJobArtifactKind,
    decide_stored_evolution_job, decision_outcome_label, dry_run_resource_scheduler,
    ensure_stored_job_awaiting_approval, evaluate_campaign_stage, evolution_report_from_record,
    job_status_label, list_job_records, parse_background_tick_estimates,
    parse_background_tick_external_workloads, parse_background_tick_resources, parse_failure_class,
    parse_schedule_estimates, parse_schedule_resources, parse_stage_resource_usage,
    plan_background_scheduling_tick, prepare_campaign_proposal_request, prepare_evolution_request,
    prepare_scheduled_iteration, propose_evolution_campaign, read_background_tick_campaigns,
    read_campaign, read_config, read_stored_job, read_stored_job_artifact, select_stored_job,
    verify_stored_evolution_job, write_background_scheduling_plan, write_campaign_bundle,
    write_campaign_iteration_dry_run, write_campaign_schedule_dry_run,
    write_campaign_stage_evaluation, write_report_record,
};
use sinomo_lm::{
    AnthropicProvider, CODEX_PROVIDER_ID, CurrentDefaultModel, GeminiProvider, GitHubCopilotConfig,
    GitHubCopilotProvider, LM_PROVIDER_CONFIG_KV_KEY, LanguageModelError,
    LanguageModelProviderUsage, LanguageModelRequest, LanguageModelResponse, ModelConfig,
    ModelPaths, NvidiaProvider, OpenAiProvider, codex_provider, collect_provider_model_catalogs,
    collect_provider_usage, complete_configured_model_request_with_account,
    current_default_model as resolve_current_default_model, default_model_provider_ids,
    github_copilot_provider, load_model_config, model_config_cluster_kv_txn,
    model_config_from_cluster_kv_snapshot, normalize_model_id_for_provider,
    normalize_model_provider, normalize_provider_account_name, provider_account_config,
    provider_api_key_from_env, resolve_default_model, save_model_config, save_provider_api_key,
    save_provider_api_key_in_model_config, subscription_usage_provider_ids,
    test_provider_account_credentials, test_provider_credentials, write_default_model_config,
};
#[cfg(test)]
use sinomo_runtime::core::EvolutionJobStatus;
use sinomo_runtime::core::{
    CandidateDecisionOutcome, ClusterBootstrapState, ClusterEndpointRecord, ClusterId,
    ClusterJoinInvite, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
    ClusterNodeAvailability, ClusterNodeRole, ClusterNodeRolloutStatus, DfsFileSystemId,
    DfsObjectId, EvaluationOutcome, EventEnvelope, EvolutionJobId, NodeId, SessionId, TrustRing,
};
#[cfg(test)]
use sinomo_runtime::daemon::read_cluster_kv_snapshot as daemon_read_cluster_kv_snapshot;
#[cfg(test)]
use sinomo_runtime::daemon::send_cluster_join_registration_to_endpoint as daemon_send_cluster_join_registration_to_endpoint;
#[cfg(test)]
use sinomo_runtime::daemon::sync_cluster_state_to_kv as daemon_sync_cluster_state_to_kv;
#[cfg(test)]
use sinomo_runtime::daemon::{
    AuthenticatedDaemonCommand, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    DAEMON_MAIN_CLUSTER_ROLE, DaemonError, DaemonSignerAuthority, SignedDaemonCommand,
    authenticate_daemon_control_request, authorize_authenticated_daemon_command,
    daemon_advertised_endpoints_for_state, daemon_package_version, elect_cluster_raft_leader,
    refresh_daemon_node_status_in_kv, sign_daemon_rpc_request_from_data_dir, write_daemon_status,
};
use sinomo_runtime::daemon::{
    DaemonClusterProcess, DaemonClusterRegistration, DaemonClusterStatus, DaemonPaths,
    DaemonProbeStatus, DaemonRunOptions, DaemonStatus, DaemonStopBeforeInstallStart,
    DaemonStopBeforeInstallStatus, DaemonStopIfRunningStatus, InstallLayout,
    InstalledDaemonInstallPlanRequest, InstalledDaemonStartStatus, ManagedSourceRepoInstallStatus,
    SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV, SHADOW_MAIN_PORT_ENV, SINOMO_DAEMON_DEFAULT_HOST,
    SINOMO_DAEMON_DEFAULT_PORT, SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
    activate_boot_entries as daemon_activate_boot_entries,
    build_cluster_join_invite as daemon_build_cluster_join_invite, cluster_doctor_findings,
    cluster_has_local_quorum, cluster_health_rows,
    cluster_invite_publish_targets as daemon_cluster_invite_publish_targets,
    cluster_peer_members as daemon_cluster_peer_members, cluster_reachable_voter_count,
    cluster_rollout_should_use_remote_build as daemon_cluster_rollout_should_use_remote_build,
    daemon_bind_address, daemon_cluster_registration,
    daemon_cluster_registration_if_initialized as daemon_registration_if_initialized,
    daemon_connect_address, daemon_install_start_installed_sinomo_daemon,
    daemon_install_stop_daemon_before_install, daemon_install_stop_daemon_if_running,
    deactivate_boot_entries as daemon_deactivate_boot_entries,
    default_boot_dir as daemon_default_boot_dir,
    default_cluster_invite_introducers as daemon_default_cluster_invite_introducers,
    default_main_daemon_status as daemon_default_main_daemon_status,
    ensure_install_layout_dirs as daemon_ensure_install_layout_dirs,
    ensure_rollout_target_platform_matches_local as daemon_ensure_rollout_target_platform_matches_local,
    fetch_cluster_kv_snapshot as daemon_fetch_cluster_kv_snapshot,
    fetch_daemon_status_from_address as daemon_fetch_daemon_status_from_address,
    git_is_available as daemon_git_is_available,
    handle_daemon_cluster_kv_txn_request as daemon_handle_daemon_cluster_kv_txn_request,
    handle_daemon_connection as daemon_handle_daemon_connection,
    initialize_local_cluster as daemon_initialize_local_cluster,
    install_managed_source_repo as daemon_install_managed_source_repo,
    installed_daemon_paths as daemon_installed_daemon_paths,
    local_daemon_cluster_options as daemon_local_daemon_cluster_options,
    managed_source_git_status_lines as daemon_managed_source_git_status_lines,
    prepare_cluster_join_retry_state as daemon_prepare_cluster_join_retry_state,
    prepare_installed_daemon_install_plan as daemon_prepare_installed_daemon_install_plan,
    prepare_local_rollout_main_activation as daemon_prepare_local_rollout_main_activation,
    prepare_local_shadow_rollout_restart as daemon_prepare_local_shadow_rollout_restart,
    prepare_pending_cluster_join_state as daemon_prepare_pending_cluster_join_state,
    probe_daemon_status, publish_cluster_invitation_to_quorum,
    read_cluster_connectivity_graph_from_kv as daemon_read_cluster_connectivity_graph_from_kv,
    read_local_cluster_state as daemon_read_local_cluster_state,
    read_local_cluster_state_without_kv as daemon_read_local_cluster_state_without_kv,
    reconcile_cluster_join_with_introducers as daemon_reconcile_cluster_join_with_introducers,
    register_daemon_cluster_node as daemon_register_daemon_cluster_node,
    remove_boot_entry as daemon_remove_boot_entry,
    remove_cluster_members as daemon_remove_cluster_members,
    remove_local_cluster_state as daemon_remove_local_cluster_state,
    rollout_target_platform_matches_local as daemon_rollout_target_platform_matches_local,
    run_daemon_control_loop as daemon_run_daemon_control_loop,
    run_shadow_daemon_supervision_loop as daemon_run_shadow_daemon_supervision_loop,
    send_daemon_rollout_binary as daemon_send_daemon_rollout_binary,
    send_daemon_rollout_binary_to_endpoint as daemon_send_daemon_rollout_binary_to_endpoint,
    send_daemon_rollout_build as daemon_send_daemon_rollout_build,
    send_daemon_rollout_build_to_endpoint as daemon_send_daemon_rollout_build_to_endpoint,
    send_daemon_rollout_diagnostics as daemon_send_daemon_rollout_diagnostics,
    send_daemon_shutdown_request,
    shadow_main_binary_from_env as daemon_shadow_main_binary_from_env,
    shadow_main_options_from_env as daemon_shadow_main_options_from_env,
    sleep_for_shadow_start_delay as daemon_sleep_for_shadow_start_delay,
    stage_rollout_main_activation as daemon_stage_rollout_main_activation,
    start_daemon_command_process as daemon_start_daemon_command_process,
    update_cluster_node_role as daemon_update_cluster_node_role,
    update_cluster_raft_role as daemon_update_cluster_raft_role,
    wait_for_local_shadow_rollout_completion as daemon_wait_for_local_shadow_rollout_completion,
    wait_for_main_daemon_health as daemon_wait_for_main_daemon_health,
    write_cluster_kv_snapshot_if_changed as daemon_write_cluster_kv_snapshot_if_changed,
    write_install_boot_entries as daemon_write_install_boot_entries,
    write_local_cluster_state as daemon_write_local_cluster_state,
};
#[cfg(test)]
use sinomo_runtime::daemon::{
    DaemonLock, daemon_rpc_command,
    handle_daemon_cluster_join_request as daemon_handle_daemon_cluster_join_request,
    handle_daemon_cluster_kv_range_prefix, handle_daemon_cluster_kv_snapshot,
    handle_daemon_cluster_kv_watch_drain_request, handle_daemon_cluster_kv_watch_events_request,
    handle_daemon_cluster_kv_watch_prefix_request, handle_daemon_cluster_raft_append_request,
    handle_daemon_cluster_raft_vote_request,
    handle_daemon_rollout_build_request as daemon_handle_daemon_rollout_build_request,
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
use sinomo_runtime::dfs::{
    DfsExposeAddRequest, DfsPinAddRequest, DfsPinMode, add_dfs_expose_entry, add_local_dfs_pin,
    dfs_expose_config_path, ensure_default_dfs_expose_config, fallback_dfs_node_id,
    inspect_dfs_conflicts, inspect_dfs_file_system, inspect_dfs_object, inspect_dfs_repairs,
    inspect_dfs_status, list_local_dfs_pins, load_dfs_expose_config, remove_dfs_expose_entry,
    remove_local_dfs_pin, resolve_dfs_metadata_root, resolve_dfs_pin_root, sifs_file_system_id,
};
use sinomo_runtime::eval::{ExecutingEvalHarness, LocalCommandRunner};
use sinomo_runtime::events::{EventStore, InMemoryEventStore};
use sinomo_runtime::kv::{KvError, KvSnapshot};
#[cfg(test)]
use sinomo_runtime::kv::{KvTxn, KvTxnResponse};
use sinomo_runtime::local::SinomoDataDir;
#[cfg(test)]
use sinomo_runtime::network::ClusterJoinRegistrationRequest;
#[cfg(test)]
use sinomo_runtime::network::ClusterJoinRegistrationResponse;
use sinomo_runtime::network::{self, ClusterPaths};
#[cfg(test)]
use sinomo_runtime::node::load_or_create_main_node_identity;
#[cfg(test)]
use sinomo_runtime::node::shadow_daemon_node_id;
#[cfg(test)]
use sinomo_runtime::rollout::read_rollout_status;
#[cfg(test)]
use sinomo_runtime::rollout::rollout_main_activation_path;
#[cfg(test)]
use sinomo_runtime::rollout::write_rollout_status;
use sinomo_runtime::rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ManagedSourceStatus, RolloutError, RolloutMainActivation, SelfRolloutPaths,
    ensure_rollout_layout, ensure_rollout_source_repo, install_binary_atomically,
    install_current_executable, managed_source_status, prepare_cluster_rollout_binary_request,
    prepare_cluster_rollout_build_request, prepare_local_rollout_artifacts,
    prepare_rollout_cluster_targets, prepare_rollout_dev_binary,
    prepare_rollout_source_bundle_base64, rollout_status_view, source_heads_match,
    sync_external_repo_from_managed, unique_rollout_targets,
};
use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

mod cui;
mod error;
mod help;
mod providers;
#[cfg(test)]
mod tests;

pub use cui::*;
use error::CliError;
use help::*;
pub use providers::*;
use time::format_description::well_known::Rfc3339;

mod commands;
pub(crate) use commands::*;

pub(crate) fn cli_command_requires_cluster_state(command: &CliCommand) -> bool {
    matches!(command, CliCommand::Evolve(_))
}

#[derive(Debug, Clone)]
pub(crate) enum CliCommand {
    Cui,
    Help,
    Cluster(ClusterCliCommand),
    InstallHelp,
    Install(InstallOptions),
    Uninstall(UninstallOptions),
    Evolve(EvolveCliCommand),
    Daemon(DaemonCliCommand),
    Rollout(RolloutCliCommand),
    Dfs(DfsCliCommand),
    Models(ModelsCliCommand),
    Providers(ProviderCliCommand),
    Usage(UsageOptions),
}

pub(crate) fn parse_cli_command(args: &[String]) -> Result<CliCommand, CliError> {
    match args.split_first() {
        None => Ok(CliCommand::Cui),
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
        Some((command, rest)) if command == "evolve" => {
            Ok(CliCommand::Evolve(parse_evolve_command(rest)?))
        }
        Some((command, rest)) if command == "daemon" => {
            Ok(CliCommand::Daemon(parse_daemon_command(rest)?))
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
        Some((command, rest)) if command == "usage" => {
            Ok(CliCommand::Usage(parse_usage_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn is_help_args(args: &[String]) -> bool {
    matches!(args, [arg] if matches!(arg.as_str(), "help" | "--help" | "-h"))
}

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    run_parsed_cli_command(parse_cli_command(&args)?)
}

fn run_cli_command(args: &[String]) -> Result<(), CliError> {
    run_parsed_cli_command(parse_cli_command(args)?)
}

fn run_parsed_cli_command(command: CliCommand) -> Result<(), CliError> {
    if cli_command_requires_cluster_state(&command) {
        require_cluster_runtime_quorum()?;
    }

    match command {
        CliCommand::Cui => run_cui(),
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
        CliCommand::Evolve(command) => run_evolve_cli_command(command),
        CliCommand::Daemon(command) => run_daemon_cli_command(command),
        CliCommand::Rollout(command) => run_rollout_cli_command(command),
        CliCommand::Dfs(command) => run_dfs_cli_command(command),
        CliCommand::Models(command) => run_models_command(command),
        CliCommand::Providers(command) => run_provider_command(command),
        CliCommand::Usage(options) => usage(options),
    }
}

const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
const DAEMON_ROLLOUT_TIMEOUT: Duration = Duration::from_secs(60);
const DAEMON_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const IROH_DAEMON_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const CLUSTER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(test)]
const SINOMO_JOIN_URI_PREFIX: &str = network::SINOMO_JOIN_URI_PREFIX;
const CLUSTER_INVITE_DEFAULT_TTL_SECONDS: i64 = 24 * 60 * 60;
const INSTALL_DEFAULT_HOST: &str = "0.0.0.0";
