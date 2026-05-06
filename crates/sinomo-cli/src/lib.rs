//! Shared Sinomo command-line handlers.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_evolution::{
    BackgroundSchedulerPaths, BackgroundSchedulingPlan, BackgroundSchedulingRequest,
    CampaignBundlePaths, CampaignProposalRequest, CandidateWorkspace, ClusterLongHorizonWorkload,
    ClusterLongHorizonWorkloadStatus, EvolutionCampaign, EvolutionCampaignStatus,
    EvolutionFailureClass, EvolutionOrchestrator, EvolutionPaths, EvolutionReport,
    EvolutionRequest, GitWorkspaceAdapter, LanguageModelMutationBridge, LocalCandidateBundleStore,
    MutationModel, MutationModelError, ReserveFloor, ResourceClassDefinition,
    ResourceSchedulerDryRun, ResourceSchedulerDryRunRequest, ResourceUsageEstimate,
    ScheduledIterationPlan, ScheduledIterationRequest, StageEvaluationRequest,
    StageEvaluationUpdate, StoredEvolutionJob, decision_outcome_label, dry_run_resource_scheduler,
    evaluate_campaign_stage, evolution_report_from_record, is_terminal_status, list_job_records,
    plan_background_scheduling_tick, prepare_scheduled_iteration, propose_evolution_campaign,
    read_background_tick_campaigns, read_campaign, read_config, read_stored_job, safe_file_stem,
    write_background_scheduling_plan, write_bundle_manifest, write_campaign_bundle,
    write_campaign_iteration_dry_run, write_campaign_schedule_dry_run,
    write_campaign_stage_evaluation, write_report_record, write_stored_job,
};
use sinomo_lm::{
    AnthropicConfig, AnthropicProvider, CLAUDE_CODE_DEFAULT_MODEL, CODEX_CLI_DEFAULT_MODEL,
    CODEX_DEFAULT_MODEL, CODEX_PROVIDER_ID, ClaudeCodeConfig, ClaudeCodeProvider, CodexCliConfig,
    CodexCliProvider, CodexConfig, CodexProvider, CodexToolRegistry, DefaultCodexContextManager,
    DefaultCodexPromptBuilder, GeminiConfig, GeminiProvider, GitHubCopilotCliConfig,
    GitHubCopilotCliProvider, GitHubCopilotConfig, GitHubCopilotProvider, LanguageModelError,
    LanguageModelMessage, LanguageModelProvider, LanguageModelProviderUsage, LanguageModelRequest,
    LanguageModelResponse, LanguageModelResponseFormat, LanguageModelRole, NvidiaConfig,
    NvidiaProvider, OpenAiConfig, OpenAiProvider, OpenAiResponsesCodexClient,
    OpenAiResponsesCodexConfig, PROVIDER_DEFAULT_MODEL, normalize_nvidia_model_id,
};
use sinomo_runtime::core::{
    CandidateDecision, CandidateDecisionOutcome, CapabilityName, ClusterBootstrapState,
    ClusterEndpointId, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource,
    ClusterId, ClusterInvitationChannel, ClusterJoinInvite, ClusterMemberRecord, ClusterMemberRole,
    ClusterMemberState, ClusterNodeAvailability, ClusterNodeRolloutStatus, ClusterNodeStatusRecord,
    ClusterTransportKind, Confidence, DfsFileSystemId, DfsObjectId, EvaluationGate,
    EvaluationOutcome, EventEnvelope, EventId, EventKind, EvolutionJobId, EvolutionJobStatus,
    LatencyClass, MutationPacket, NodeId, PrivacyClass, RoutingMetadata, RoutingScope, Salience,
    SessionId, Source, TrustDomain, TrustRing, VerificationCommand,
};
use sinomo_runtime::daemon::{
    AuthenticatedDaemonCommand, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    DaemonClusterStatus, DaemonLock, DaemonPaths, DaemonProbeStatus, DaemonRunOptions,
    DaemonSignerAuthority, DaemonStatus, daemon_bind_address, daemon_connect_address,
    handle_authenticated_daemon_connection, probe_daemon_status, read_daemon_status,
    send_daemon_command, sign_daemon_command_for_local_node, verify_cluster_invite_challenge_proof,
    write_daemon_status,
};
#[cfg(test)]
use sinomo_runtime::daemon::{
    DaemonError, authenticate_daemon_command, authorize_authenticated_daemon_command,
    daemon_package_version, send_daemon_command_raw, sign_daemon_command_from_data_dir,
};
use sinomo_runtime::dfs::{
    DfsExposeEntry, DfsExposeStore, DfsFileSystemConfig, DfsMetadataBackend,
    DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardId, DfsNodeAdvertisement,
    DfsOperatorInspector, DfsPinMode, DfsPinRecord, LocalDfsPinStore, LocalJsonMetadataBackend,
    content_service_key, distributed_sifs, distributed_sifs_bootstrap_objects,
    file_system_config_key, metadata_service_key, metadata_shard_key,
};
use sinomo_runtime::eval::{ExecutingEvalHarness, LocalCommandRunner};
use sinomo_runtime::events::{EventStore, InMemoryEventStore, ReplayDirection, ReplayQuery};
use sinomo_runtime::kv::{
    KvCommand, KvCompare, KvEntry, KvError, KvOp, KvSnapshot, KvTxn, KvTxnResponse,
    RaftAppendEntriesRequest, RaftRequestVoteRequest, RaftRequestVoteResponse, RaftSnapshot,
    cluster_member_key,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{
    self, ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse,
    ClusterKvWatchDrainRequest, ClusterKvWatchDrainResponse, ClusterKvWatchEventsRequest,
    ClusterKvWatchEventsResponse, ClusterKvWatchPrefixRequest, ClusterKvWatchRegistration,
    ClusterPaths, ClusterRaftAppendRequest, ClusterRaftAppendResponse, ClusterRaftVoteRequest,
    ClusterRaftVoteResponse, MdnsDiscoveryAdvertisement, MdnsDiscoveryCache, MdnsDiscoverySocket,
};
use sinomo_runtime::node::{
    NodeIdentity, daemon_lan_endpoint_id, daemon_loopback_endpoint_id,
    load_or_create_main_node_identity, load_or_create_shadow_node_identity,
    main_daemon_capabilities, shadow_daemon_capabilities, shadow_daemon_node_id,
};
use sinomo_runtime::rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    MainExitKind, MainExitObservation, PendingShadowRolloutContext, RolloutError,
    RolloutFailureDiagnostics, RolloutMainActivation, SelfRolloutPaths,
    activate_staged_rollout_binary, classify_main_exit, create_source_repo_bundle,
    deploy_managed_source_bundle, deploy_managed_source_repo, discover_current_git_repo,
    ensure_rollout_layout, managed_source_status, read_pending_shadow_rollout,
    read_rollout_main_activation, record_activation_failure, record_main_activation_staged,
    record_main_rollout_failure_with_diagnostics, record_shadow_rollout_finalized,
    remove_shadow_finalize, restore_rollout_binary_from_backup, rollout_main_activation_path,
    rollout_shadow_finalize_path, rollout_staged_main_binary, rollout_staged_shadow_binary,
    stage_pending_shadow_rollout, stage_rollout_bytes, stage_rollout_file,
    sync_external_repo_from_managed, write_main_activation,
};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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
    matches!(command, CliCommand::Evolve(_) | CliCommand::Rollout(_))
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

const SINOMO_DAEMON_DEFAULT_HOST: &str = "127.0.0.1";
const SINOMO_DAEMON_DEFAULT_PORT: u16 = 21_353;
const SINOMO_SHADOW_DAEMON_DEFAULT_PORT: u16 = 21_354;
const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
const SHADOW_MAIN_HOST_ENV: &str = "SINOMO_SHADOW_MAIN_HOST";
const SHADOW_MAIN_PORT_ENV: &str = "SINOMO_SHADOW_MAIN_PORT";
const SHADOW_MAIN_BINARY_ENV: &str = "SINOMO_SHADOW_MAIN_BINARY";
const SHADOW_START_DELAY_MS_ENV: &str = "SINOMO_SHADOW_START_DELAY_MS";
const DAEMON_MAIN_CLUSTER_ROLE: &str = "daemon-main";
const DAEMON_SHADOW_CLUSTER_ROLE: &str = "daemon-shadow";
const DAEMON_ROLLOUT_TIMEOUT: Duration = Duration::from_secs(60);
const CLUSTER_INVITE_AUTH_PROTOCOL: &str = "sinomo.cluster-invite.v1";
const DAEMON_JOIN_TIMEOUT: Duration = Duration::from_millis(750);
const CLUSTER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const DAEMON_CLUSTER_TICK_INTERVAL: Duration = Duration::from_secs(5);
const DAEMON_CLUSTER_PUBLISH_INTERVAL: Duration = Duration::from_secs(60);
const DAEMON_CLUSTER_DISCOVERY_INTERVAL: Duration = Duration::from_secs(10);
const DAEMON_CLUSTER_RAFT_INTERVAL: Duration = Duration::from_secs(30);
const DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL: Duration = Duration::from_secs(60);
const CLUSTER_NETWORK_INFO_TTL_SECONDS: i64 = 30;
const CLUSTER_CONNECTIVITY_TTL_SECONDS: i64 = 60;
const CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD: u16 = 3;
const CLUSTER_NODE_STATUS_STALE_AFTER_SECONDS: i64 = 120;
const ROLLOUT_DIAGNOSTIC_TAIL_BYTES: usize = 8 * 1024;
const SINOMO_JOIN_URI_PREFIX: &str = "sinomo-join://v1/";
const CLUSTER_INVITE_DEFAULT_TTL_SECONDS: i64 = 24 * 60 * 60;
const INSTALL_DEFAULT_HOST: &str = "0.0.0.0";
const INSTALL_MAIN_BOOT_NAME: &str = "Sinomo Daemon";
const INSTALL_SHADOW_BOOT_NAME: &str = "Sinomo Shadow Daemon";
