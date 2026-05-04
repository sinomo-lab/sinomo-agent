//! Shared Sinomo command-line handlers.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fs2::FileExt;
use ring::rand::SystemRandom;
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_evolution::{
    BackgroundSchedulingPlan, BackgroundSchedulingRequest,
    CampaignDecision as EvolutionCampaignDecision, CampaignDecisionKind, CampaignProposalRequest,
    CampaignScheduleDecision, CandidateWorkspace, ClusterLongHorizonWorkload,
    ClusterLongHorizonWorkloadStatus, EvolutionCampaign, EvolutionCampaignStatus,
    EvolutionFailureClass, EvolutionOrchestrator, EvolutionReport, EvolutionRequest,
    GitWorkspaceAdapter, LanguageModelMutationBridge, LocalCandidateBundleStore, MutationModel,
    MutationModelError, ReserveFloor, ResourceClassDefinition, ResourceSchedulerDryRun,
    ResourceSchedulerDryRunRequest, ResourceUsageEstimate, ScheduledIterationPlan,
    ScheduledIterationRequest, StageEvaluationRequest, StageEvaluationUpdate,
    dry_run_resource_scheduler, evaluate_campaign_stage, plan_background_scheduling_tick,
    prepare_scheduled_iteration, propose_evolution_campaign,
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
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    MainExitKind, MainExitObservation, PendingShadowRolloutContext, RolloutError,
    RolloutMainActivation, SelfRolloutPaths, activate_staged_rollout_binary, classify_main_exit,
    deploy_managed_source_repo, discover_current_git_repo, ensure_rollout_layout,
    managed_source_status, read_pending_shadow_rollout, read_rollout_main_activation,
    record_activation_failure, record_main_activation_staged, record_main_rollout_failure,
    record_shadow_rollout_finalized, remove_shadow_finalize, restore_rollout_binary_from_backup,
    rollout_main_activation_path, rollout_staged_main_binary, rollout_staged_shadow_binary,
    stage_pending_shadow_rollout, stage_rollout_bytes, stage_rollout_file,
    sync_external_repo_from_managed, write_main_activation,
};
use sinomo_runtime::core::{
    CandidateBundleManifest, CandidateDecision, CandidateDecisionOutcome, CapabilityName,
    ClusterBootstrap, ClusterBootstrapState, ClusterEndpointId, ClusterEndpointPrivacy,
    ClusterEndpointRecord, ClusterEndpointSource, ClusterId, ClusterJoinInvite,
    ClusterMemberRecord, ClusterMemberRole, ClusterMemberState, ClusterTransportKind, Confidence,
    DfsFileSystemId, DfsObjectId, EvaluationGate, EvaluationOutcome, EvaluationScorecard,
    EventEnvelope, EventId, EventKind, EvolutionJob, EvolutionJobId, EvolutionJobStatus,
    LatencyClass, MutationPacket, NodeId, PrivacyClass, RoutingMetadata, RoutingScope, Salience,
    SessionId, Source, TrustDomain, TrustRing, VerificationCommand,
};
use sinomo_runtime::dfs::{
    DfsMetadataShardId, DfsOperatorInspector, DfsPinMode, DfsPinRecord, LocalDfsPinStore,
    LocalJsonMetadataBackend,
};
use sinomo_runtime::eval::{ExecutingEvalHarness, LocalCommandRunner};
use sinomo_runtime::events::{EventStore, InMemoryEventStore, ReplayDirection, ReplayQuery};
use sinomo_runtime::kv::{InProcessKvStore, KvEntry, KvError, KvSnapshot, KvTxn, KvTxnResponse};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{
    MdnsDiscoveryAdvertisement, MdnsDiscoveryCache, MdnsDiscoverySocket,
};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Read, Write};
use std::net::{IpAddr, Shutdown, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    run_parsed_cli_command(parse_cli_command(&args)?)
}

fn run_cli_command(args: &[String]) -> Result<(), CliError> {
    run_parsed_cli_command(parse_cli_command(args)?)
}

fn run_parsed_cli_command(command: CliCommand) -> Result<(), CliError> {
    if cli_command_requires_cluster_state(&command) {
        require_cluster_runtime_state()?;
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

fn cli_command_requires_cluster_state(command: &CliCommand) -> bool {
    matches!(
        command,
        CliCommand::Cui
            | CliCommand::Evolve(_)
            | CliCommand::Daemon(_)
            | CliCommand::Rollout(_)
            | CliCommand::Models(_)
            | CliCommand::Providers(_)
            | CliCommand::Usage(_)
    )
}

#[derive(Debug, Clone)]
enum CliCommand {
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

fn parse_cli_command(args: &[String]) -> Result<CliCommand, CliError> {
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
        Some((command, rest)) if matches!(command.as_str(), "models" | "model") => Ok(
            CliCommand::Models(parse_models_command(rest, ModelDefaultRepoMode::Reject)?),
        ),
        Some((command, rest)) if command == "providers" => {
            Ok(CliCommand::Providers(parse_provider_command(rest)?))
        }
        Some((command, rest)) if command == "usage" => {
            Ok(CliCommand::Usage(parse_usage_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

fn is_help_args(args: &[String]) -> bool {
    matches!(args, [arg] if matches!(arg.as_str(), "help" | "--help" | "-h"))
}

const SINOMO_DAEMON_DEFAULT_HOST: &str = "127.0.0.1";
const SINOMO_DAEMON_DEFAULT_PORT: u16 = 21_353;
const SINOMO_SHADOW_DAEMON_DEFAULT_PORT: u16 = 21_354;
const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(400);
const SHADOW_MAIN_HOST_ENV: &str = "SINOMO_SHADOW_MAIN_HOST";
const SHADOW_MAIN_PORT_ENV: &str = "SINOMO_SHADOW_MAIN_PORT";
const SHADOW_MAIN_BINARY_ENV: &str = "SINOMO_SHADOW_MAIN_BINARY";
const SHADOW_START_DELAY_MS_ENV: &str = "SINOMO_SHADOW_START_DELAY_MS";
const DAEMON_MAIN_CLUSTER_ROLE: &str = "daemon-main";
const DAEMON_SHADOW_CLUSTER_ROLE: &str = "daemon-shadow";
const DAEMON_ROLLOUT_TIMEOUT: Duration = Duration::from_secs(60);
const DAEMON_AUTH_PROTOCOL: &str = "sinomo.daemon-auth.v1";
const CLUSTER_INVITE_AUTH_PROTOCOL: &str = "sinomo.cluster-invite.v1";
const DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS: i64 = 300;
const DAEMON_JOIN_TIMEOUT: Duration = Duration::from_millis(750);
const DAEMON_CLUSTER_TICK_INTERVAL: Duration = Duration::from_millis(500);
const DAEMON_CLUSTER_DISCOVERY_INTERVAL: Duration = Duration::from_secs(2);
const DAEMON_CLUSTER_SYNC_INTERVAL: Duration = Duration::from_secs(2);
const SINOMO_JOIN_URI_PREFIX: &str = "sinomo-join://v1/";
const CLUSTER_INVITE_DEFAULT_TTL_SECONDS: i64 = 24 * 60 * 60;
const INSTALL_DEFAULT_HOST: &str = "0.0.0.0";
const INSTALL_MAIN_BOOT_NAME: &str = "Sinomo Daemon";
const INSTALL_SHADOW_BOOT_NAME: &str = "Sinomo Shadow Daemon";

#[derive(Clone, Debug, Eq, PartialEq)]
struct InstallOptions {
    host: String,
    port: u16,
    shadow_port: u16,
    start: bool,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            host: INSTALL_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
            shadow_port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
            start: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UninstallOptions {
    stop: bool,
}

impl Default for UninstallOptions {
    fn default() -> Self {
        Self { stop: true }
    }
}

fn parse_install_options(args: &[String]) -> Result<InstallOptions, CliError> {
    let mut options = InstallOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "help" | "--help" | "-h" if args.len() == 1 => return Err(CliError::Usage),
            "--host" => {
                index += 1;
                let Some(host) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.host = host.clone();
            }
            "--port" => {
                index += 1;
                let Some(port) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.port = parse_port("install port", port)?;
            }
            "--shadow-port" => {
                index += 1;
                let Some(port) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.shadow_port = parse_port("install shadow port", port)?;
            }
            "--no-start" => options.start = false,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_uninstall_options(args: &[String]) -> Result<UninstallOptions, CliError> {
    let mut options = UninstallOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "help" | "--help" | "-h" if args.len() == 1 => return Err(CliError::Usage),
            "--no-stop" => options.stop = false,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_port(label: &str, value: &str) -> Result<u16, CliError> {
    value.parse::<u16>().map_err(|source| CliError::Parse {
        detail: format!("invalid {label} `{value}`: {source}"),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ClusterCliCommand {
    Help,
    Init(ClusterInitOptions),
    Invite(ClusterInviteOptions),
    Join(ClusterJoinOptions),
    Status,
    Peers,
    Leave,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterInitOptions {
    cluster_id: Option<ClusterId>,
    node_id: Option<NodeId>,
    name: String,
}

impl Default for ClusterInitOptions {
    fn default() -> Self {
        Self {
            cluster_id: None,
            node_id: None,
            name: "sinomo-local".to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterJoinOptions {
    invite: String,
    cluster_id: Option<ClusterId>,
    node_id: Option<NodeId>,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterInviteOptions {
    ttl_seconds: i64,
    introducers: Vec<String>,
    output: Option<PathBuf>,
}

impl Default for ClusterInviteOptions {
    fn default() -> Self {
        Self {
            ttl_seconds: CLUSTER_INVITE_DEFAULT_TTL_SECONDS,
            introducers: Vec::new(),
            output: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterPaths {
    root: PathBuf,
    state: PathBuf,
    invites: PathBuf,
    kv_snapshot: PathBuf,
}

impl ClusterPaths {
    fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("cluster");
        Self {
            state: root.join("state.json"),
            invites: root.join("invites"),
            kv_snapshot: root.join("kv-snapshot.json"),
            root,
        }
    }

    fn ensure(&self) -> Result<(), CliError> {
        fs::create_dir_all(&self.root).map_err(|source| CliError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }

    fn invite_path(&self, invite_id: &sinomo_runtime::core::ClusterInviteId) -> PathBuf {
        self.invites.join(format!("{}.json", invite_id.as_str()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NodeIdentityPaths {
    main: PathBuf,
    shadow: PathBuf,
}

impl NodeIdentityPaths {
    fn new(data_dir: &Path) -> Self {
        let root = data_dir.join("identity");
        Self {
            main: root.join("main-ed25519.json"),
            shadow: root.join("shadow-ed25519.json"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct NodeIdentity {
    public_key: String,
    private_key_pkcs8: String,
    created_at: String,
}

impl NodeIdentity {
    fn key_pair(&self) -> Result<Ed25519KeyPair, CliError> {
        let pkcs8 = URL_SAFE_NO_PAD
            .decode(self.private_key_pkcs8.as_bytes())
            .map_err(|source| CliError::Parse {
                detail: format!("invalid node identity private key encoding: {source}"),
            })?;
        Ed25519KeyPair::from_pkcs8(&pkcs8).map_err(|source| CliError::Parse {
            detail: format!("invalid node identity private key: {source}"),
        })
    }
}

fn load_or_create_main_node_identity(data_dir: &Path) -> Result<NodeIdentity, CliError> {
    load_or_create_node_identity(&NodeIdentityPaths::new(data_dir).main)
}

fn load_or_create_shadow_node_identity(data_dir: &Path) -> Result<NodeIdentity, CliError> {
    load_or_create_node_identity(&NodeIdentityPaths::new(data_dir).shadow)
}

fn load_or_create_node_identity(path: &Path) -> Result<NodeIdentity, CliError> {
    if path.exists() {
        let content = fs::read_to_string(path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        return serde_json::from_str(&content).map_err(|source| CliError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        });
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).map_err(|source| CliError::Parse {
        detail: format!("failed to generate node identity key: {source}"),
    })?;
    let key_pair =
        Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).map_err(|source| CliError::Parse {
            detail: format!("failed to load generated node identity key: {source}"),
        })?;
    let identity = NodeIdentity {
        public_key: URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref()),
        private_key_pkcs8: URL_SAFE_NO_PAD.encode(pkcs8.as_ref()),
        created_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string()),
    };
    write_json(path, &identity)?;
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    }
    Ok(identity)
}

fn parse_cluster_command(args: &[String]) -> Result<ClusterCliCommand, CliError> {
    match args.split_first() {
        None => Ok(ClusterCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(ClusterCliCommand::Help)
        }
        Some((command, rest)) if command == "init" => {
            Ok(ClusterCliCommand::Init(parse_cluster_init_options(rest)?))
        }
        Some((command, rest)) if command == "invite" => Ok(ClusterCliCommand::Invite(
            parse_cluster_invite_options(rest)?,
        )),
        Some((command, rest)) if command == "join" => {
            Ok(ClusterCliCommand::Join(parse_cluster_join_options(rest)?))
        }
        Some((command, [])) if command == "status" => Ok(ClusterCliCommand::Status),
        Some((command, [])) if command == "peers" => Ok(ClusterCliCommand::Peers),
        Some((command, [])) if command == "leave" => Ok(ClusterCliCommand::Leave),
        _ => Err(CliError::Usage),
    }
}

fn parse_cluster_init_options(args: &[String]) -> Result<ClusterInitOptions, CliError> {
    let mut options = ClusterInitOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--cluster-id" => {
                index += 1;
                let Some(cluster_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.cluster_id = Some(parse_cluster_id(cluster_id)?);
            }
            "--node-id" => {
                index += 1;
                let Some(node_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.node_id =
                    Some(
                        NodeId::new(node_id.clone()).map_err(|source| CliError::Parse {
                            detail: format!("invalid node id `{node_id}`: {source}"),
                        })?,
                    );
            }
            "--name" => {
                index += 1;
                let Some(name) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.name = non_empty_cluster_name(name)?;
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_cluster_invite_options(args: &[String]) -> Result<ClusterInviteOptions, CliError> {
    let mut options = ClusterInviteOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--ttl-seconds" => {
                index += 1;
                let Some(ttl) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.ttl_seconds = ttl.parse::<i64>().map_err(|source| CliError::Parse {
                    detail: format!("invalid invite ttl `{ttl}`: {source}"),
                })?;
                if options.ttl_seconds <= 0 {
                    return Err(CliError::Parse {
                        detail: "invite ttl must be greater than zero seconds".to_string(),
                    });
                }
            }
            "--introducer" => {
                index += 1;
                let Some(introducer) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.introducers.push(non_empty_address(introducer)?);
            }
            "--output" => {
                index += 1;
                let Some(output) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.output = Some(PathBuf::from(output));
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_cluster_join_options(args: &[String]) -> Result<ClusterJoinOptions, CliError> {
    let Some((invite, rest)) = args.split_first() else {
        return Err(CliError::Usage);
    };
    if invite.starts_with("--") {
        return Err(CliError::Usage);
    }

    let mut options = ClusterJoinOptions {
        invite: invite.clone(),
        cluster_id: None,
        node_id: None,
        name: "sinomo-node".to_string(),
    };
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--cluster-id" => {
                index += 1;
                let Some(cluster_id) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.cluster_id = Some(parse_cluster_id(cluster_id)?);
            }
            "--node-id" => {
                index += 1;
                let Some(node_id) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.node_id =
                    Some(
                        NodeId::new(node_id.clone()).map_err(|source| CliError::Parse {
                            detail: format!("invalid node id `{node_id}`: {source}"),
                        })?,
                    );
            }
            "--name" => {
                index += 1;
                let Some(name) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.name = non_empty_cluster_name(name)?;
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_cluster_id(cluster_id: &str) -> Result<ClusterId, CliError> {
    ClusterId::new(cluster_id.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid cluster id `{cluster_id}`: {source}"),
    })
}

fn non_empty_cluster_name(name: &str) -> Result<String, CliError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(CliError::Parse {
            detail: "cluster node name cannot be empty".to_string(),
        });
    }
    Ok(name.to_string())
}

fn non_empty_address(address: &str) -> Result<String, CliError> {
    let address = address.trim();
    if address.is_empty() {
        return Err(CliError::Parse {
            detail: "cluster introducer address cannot be empty".to_string(),
        });
    }
    Ok(address.to_string())
}

fn format_rfc3339(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InstallLayout {
    data_dir: PathBuf,
    bin_dir: PathBuf,
    binary: PathBuf,
    shadow_binary: PathBuf,
    boot_dir: PathBuf,
    main_boot_entry: PathBuf,
    shadow_boot_entry: PathBuf,
}

impl InstallLayout {
    fn new(data_dir: &SinomoDataDir, boot_dir: PathBuf) -> Self {
        let data_root = data_dir.root();
        let bin_dir = data_root.join("bin");
        let binary = bin_dir.join(format!("sinomo{}", env::consts::EXE_SUFFIX));
        let shadow_binary = bin_dir.join(format!("sinomo-shadow{}", env::consts::EXE_SUFFIX));
        let main_boot_entry = boot_entry_path(&boot_dir, INSTALL_MAIN_BOOT_NAME);
        let shadow_boot_entry = boot_entry_path(&boot_dir, INSTALL_SHADOW_BOOT_NAME);
        Self {
            data_dir: data_root,
            bin_dir,
            binary,
            shadow_binary,
            boot_dir,
            main_boot_entry,
            shadow_boot_entry,
        }
    }
}

fn install(options: InstallOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let boot_dir = default_boot_dir(&data_dir)?;
    let layout = InstallLayout::new(&data_dir, boot_dir);
    install_with_layout(options, &layout)
}

fn install_with_layout(options: InstallOptions, layout: &InstallLayout) -> Result<(), CliError> {
    fs::create_dir_all(&layout.bin_dir).map_err(|source| CliError::Io {
        path: layout.bin_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::create_dir_all(&layout.boot_dir).map_err(|source| CliError::Io {
        path: layout.boot_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    let main_paths = DaemonPaths::with_root(layout.data_dir.join("daemon"));
    let shadow_paths = DaemonPaths::with_root(layout.data_dir.join("shadow-daemon"));
    let updating_binary = install_should_update_binary(&layout.binary)?
        || install_should_update_binary(&layout.shadow_binary)?;
    let restarting_for_options =
        options.start && install_running_daemons_need_restart(&options, &main_paths, &shadow_paths);
    if updating_binary || restarting_for_options {
        let reason = if updating_binary {
            "binary update"
        } else {
            "install option update"
        };
        stop_installed_daemons_before_install(layout, reason)?;
    }
    install_current_binary(&layout.binary)?;
    install_current_binary(&layout.shadow_binary)?;
    install_managed_source_repo(layout)?;
    write_boot_entries(layout, &options)?;
    activate_boot_entries(layout);
    println!("installed binary: {}", layout.binary.display());
    println!(
        "installed shadow binary: {}",
        layout.shadow_binary.display()
    );
    println!(
        "managed source repo: {}",
        SelfRolloutPaths::new(&layout.data_dir)
            .source_repo
            .display()
    );
    println!("boot entry: {}", layout.main_boot_entry.display());
    println!("boot entry: {}", layout.shadow_boot_entry.display());
    println!("data directory retained at: {}", layout.data_dir.display());

    if options.start {
        let main_args = vec![
            "daemon".to_string(),
            "run".to_string(),
            "--host".to_string(),
            options.host.clone(),
            "--port".to_string(),
            options.port.to_string(),
        ];
        ensure_installed_daemon_started(
            layout,
            &layout.binary,
            &main_args,
            &main_paths,
            "daemon",
            &[],
        )?;
        let shadow_args = vec![
            "daemon".to_string(),
            "shadow".to_string(),
            "run".to_string(),
            "--host".to_string(),
            options.host.clone(),
            "--port".to_string(),
            options.shadow_port.to_string(),
        ];
        let shadow_env = vec![
            (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
            (SHADOW_MAIN_HOST_ENV, options.host.clone()),
            (SHADOW_MAIN_PORT_ENV, options.port.to_string()),
        ];
        ensure_installed_daemon_started(
            layout,
            &layout.shadow_binary,
            &shadow_args,
            &shadow_paths,
            "shadow daemon",
            &shadow_env,
        )?;
    }
    Ok(())
}

fn uninstall(options: UninstallOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let boot_dir = default_boot_dir(&data_dir)?;
    let layout = InstallLayout::new(&data_dir, boot_dir);
    uninstall_with_layout(options, &layout)
}

fn uninstall_with_layout(
    options: UninstallOptions,
    layout: &InstallLayout,
) -> Result<(), CliError> {
    if options.stop {
        stop_daemon_if_running(
            &DaemonPaths::with_root(layout.data_dir.join("shadow-daemon")),
            "shadow daemon",
        )?;
        stop_daemon_if_running(
            &DaemonPaths::with_root(layout.data_dir.join("daemon")),
            "daemon",
        )?;
    }
    deactivate_boot_entries(layout);
    remove_boot_entry(&layout.main_boot_entry)?;
    remove_boot_entry(&layout.shadow_boot_entry)?;
    println!("removed boot entry: {}", layout.main_boot_entry.display());
    println!("removed boot entry: {}", layout.shadow_boot_entry.display());
    println!("data directory retained at: {}", layout.data_dir.display());
    Ok(())
}

fn install_current_binary(destination: &Path) -> Result<(), CliError> {
    let current = current_executable_path()?;
    if same_path_if_exists(&current, destination) {
        return Ok(());
    }
    install_binary_atomically(&current, destination)
}

fn install_should_update_binary(destination: &Path) -> Result<bool, CliError> {
    if !destination.exists() {
        return Ok(false);
    }
    let current = current_executable_path()?;
    if same_path_if_exists(&current, destination) {
        return Ok(false);
    }
    let current_bytes = fs::read(&current).map_err(|source| CliError::Io {
        path: current.display().to_string(),
        detail: source.to_string(),
    })?;
    let destination_bytes = fs::read(destination).map_err(|source| CliError::Io {
        path: destination.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(current_bytes != destination_bytes)
}

fn install_managed_source_repo(layout: &InstallLayout) -> Result<(), CliError> {
    let rollout_paths = SelfRolloutPaths::new(&layout.data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let Some(source_repo) = discover_current_git_repo()? else {
        println!(
            "warning: managed source repo was not updated because the current directory is not inside a git checkout"
        );
        return Ok(());
    };
    Ok(deploy_managed_source_repo(&rollout_paths, &source_repo)?)
}

fn install_running_daemons_need_restart(
    options: &InstallOptions,
    main_paths: &DaemonPaths,
    shadow_paths: &DaemonPaths,
) -> bool {
    daemon_probe_status_needs_install_restart(
        probe_daemon_status(main_paths, DAEMON_PROBE_TIMEOUT),
        &options.host,
        options.port,
    ) || daemon_probe_status_needs_install_restart(
        probe_daemon_status(shadow_paths, DAEMON_PROBE_TIMEOUT),
        &options.host,
        options.shadow_port,
    )
}

fn daemon_probe_status_needs_install_restart(
    status: DaemonProbeStatus,
    host: &str,
    port: u16,
) -> bool {
    match status {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => !daemon_status_matches_options(&status, host, port),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => false,
    }
}

fn daemon_status_matches_options(status: &DaemonStatus, host: &str, port: u16) -> bool {
    status.host == host && status.port == port
}

fn current_executable_path() -> Result<PathBuf, CliError> {
    env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })
}

fn same_path_if_exists(left: &Path, right: &Path) -> bool {
    let Ok(left) = fs::canonicalize(left) else {
        return false;
    };
    let Ok(right) = fs::canonicalize(right) else {
        return false;
    };
    left == right
}

fn default_boot_dir(data_dir: &SinomoDataDir) -> Result<PathBuf, CliError> {
    #[cfg(windows)]
    {
        if let Ok(appdata) = env::var("APPDATA") {
            let appdata = appdata.trim();
            if !appdata.is_empty() {
                return Ok(PathBuf::from(appdata)
                    .join("Microsoft")
                    .join("Windows")
                    .join("Start Menu")
                    .join("Programs")
                    .join("Startup"));
            }
        }
        Ok(data_dir.root().join("startup"))
    }
    #[cfg(not(windows))]
    {
        if let Ok(config_home) = env::var("XDG_CONFIG_HOME") {
            let config_home = config_home.trim();
            if !config_home.is_empty() {
                return Ok(PathBuf::from(config_home).join("systemd").join("user"));
            }
        }
        if let Ok(home) = env::var("HOME") {
            let home = home.trim();
            if !home.is_empty() {
                return Ok(PathBuf::from(home)
                    .join(".config")
                    .join("systemd")
                    .join("user"));
            }
        }
        Ok(data_dir.root().join("startup"))
    }
}

fn boot_entry_path(boot_dir: &Path, name: &str) -> PathBuf {
    #[cfg(windows)]
    {
        boot_dir.join(format!("{name}.cmd"))
    }
    #[cfg(not(windows))]
    {
        let file_name = name.to_ascii_lowercase().replace(' ', "-");
        boot_dir.join(format!("{file_name}.service"))
    }
}

fn write_boot_entries(layout: &InstallLayout, options: &InstallOptions) -> Result<(), CliError> {
    #[cfg(windows)]
    let main_command_prefix = ["daemon", "start"];
    #[cfg(not(windows))]
    let main_command_prefix = ["daemon", "run"];
    #[cfg(windows)]
    let shadow_command_prefix = ["daemon", "shadow", "start"];
    #[cfg(not(windows))]
    let shadow_command_prefix = ["daemon", "shadow", "run"];

    write_boot_entry(
        &layout.main_boot_entry,
        &layout.data_dir,
        &layout.binary,
        &main_command_prefix,
        &options.host,
        options.port,
        "Sinomo daemon",
        &[],
    )?;
    let shadow_env = vec![
        (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, options.host.clone()),
        (SHADOW_MAIN_PORT_ENV, options.port.to_string()),
    ];
    write_boot_entry(
        &layout.shadow_boot_entry,
        &layout.data_dir,
        &layout.shadow_binary,
        &shadow_command_prefix,
        &options.host,
        options.shadow_port,
        "Sinomo shadow daemon",
        &shadow_env,
    )
}

fn write_boot_entry(
    path: &Path,
    data_dir: &Path,
    binary: &Path,
    command_prefix: &[&str],
    host: &str,
    port: u16,
    description: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = boot_entry_content(
        data_dir,
        binary,
        command_prefix,
        host,
        port,
        description,
        extra_env,
    );
    fs::write(path, content).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn boot_entry_content(
    data_dir: &Path,
    binary: &Path,
    command_prefix: &[&str],
    host: &str,
    port: u16,
    description: &str,
    extra_env: &[(&str, String)],
) -> String {
    let command = command_prefix.join(" ");
    #[cfg(windows)]
    {
        let _ = description;
        let extra_env = extra_env
            .iter()
            .map(|(key, value)| format!("set \"{key}={value}\"\r\n"))
            .collect::<String>();
        format!(
            "@echo off\r\nset \"SINOMO_DATA_DIR={}\"\r\n{}\"{}\" {} --host {} --port {}\r\n",
            data_dir.display(),
            extra_env,
            binary.display(),
            command,
            host,
            port
        )
    }
    #[cfg(not(windows))]
    {
        let extra_env = extra_env
            .iter()
            .map(|(key, value)| format!("Environment={key}={value}\n"))
            .collect::<String>();
        format!(
            "[Unit]\nDescription={description}\n\n[Service]\nEnvironment=SINOMO_DATA_DIR={}\n{}ExecStart={} {} --host {} --port {}\nRestart=on-failure\n\n[Install]\nWantedBy=default.target\n",
            data_dir.display(),
            extra_env,
            binary.display(),
            command,
            host,
            port
        )
    }
}

fn remove_boot_entry(path: &Path) -> Result<(), CliError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

#[cfg(windows)]
fn activate_boot_entries(_layout: &InstallLayout) {}

#[cfg(not(windows))]
fn activate_boot_entries(layout: &InstallLayout) {
    systemctl_user_best_effort(&["daemon-reload"]);
    for entry in [&layout.main_boot_entry, &layout.shadow_boot_entry] {
        let Some(unit) = entry.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        systemctl_user_best_effort(&["enable", unit]);
    }
}

#[cfg(windows)]
fn deactivate_boot_entries(_layout: &InstallLayout) {}

#[cfg(not(windows))]
fn deactivate_boot_entries(layout: &InstallLayout) {
    for entry in [&layout.main_boot_entry, &layout.shadow_boot_entry] {
        let Some(unit) = entry.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        systemctl_user_best_effort(&["disable", unit]);
    }
    systemctl_user_best_effort(&["daemon-reload"]);
}

#[cfg(not(windows))]
fn systemctl_user_best_effort(args: &[&str]) {
    let output = Command::new("systemctl").arg("--user").args(args).output();
    match output {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            println!(
                "warning: systemctl --user {} failed: {}",
                args.join(" "),
                stderr.trim()
            );
        }
        Err(source) => {
            println!(
                "warning: systemctl --user {} could not run: {}",
                args.join(" "),
                source
            );
        }
    }
}

fn ensure_installed_daemon_started(
    layout: &InstallLayout,
    binary: &Path,
    args: &[String],
    paths: &DaemonPaths,
    label: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    if matches!(
        probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT),
        DaemonProbeStatus::Running(_)
    ) {
        println!("sinomo {label} already running");
        return Ok(());
    }
    start_installed_sinomo_daemon(binary, &layout.data_dir, args, paths, label, extra_env)?;
    Ok(())
}

fn stop_installed_daemons_before_install(
    layout: &InstallLayout,
    reason: &str,
) -> Result<(), CliError> {
    stop_daemon_before_install(
        &DaemonPaths::with_root(layout.data_dir.join("shadow-daemon")),
        "shadow daemon",
        reason,
    )?;
    stop_daemon_before_install(
        &DaemonPaths::with_root(layout.data_dir.join("daemon")),
        "daemon",
        reason,
    )?;
    Ok(())
}

fn stop_daemon_before_install(
    paths: &DaemonPaths,
    label: &str,
    reason: &str,
) -> Result<(), CliError> {
    match probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            match send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT) {
                Ok(_) => println!("sinomo {label} stop requested for {reason}"),
                Err(error) => {
                    if matches!(
                        probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT),
                        DaemonProbeStatus::NotRunning
                    ) {
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
        DaemonProbeStatus::Locked { .. } => {
            println!("sinomo {label} lock is held; waiting before {reason}");
        }
        DaemonProbeStatus::NotRunning => return Ok(()),
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if matches!(
            probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT),
            DaemonProbeStatus::NotRunning
        ) {
            println!("sinomo {label} stopped");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }

    Err(CliError::Network {
        address: paths.status.display().to_string(),
        detail: format!("{label} did not stop before {reason}"),
    })
}

fn start_installed_sinomo_daemon(
    binary: &Path,
    data_dir: &Path,
    args: &[String],
    paths: &DaemonPaths,
    label: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    let mut command = Command::new(binary);
    command.args(args).env("SINOMO_DATA_DIR", data_dir);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })?;

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) = probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT)
        {
            println!(
                "sinomo {label} started: pid={} bind={} control={}",
                status.pid,
                daemon_bind_address(&status),
                status.address
            );
            return Ok(());
        }
        if let Some(status) = child.try_wait().map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })? {
            return Err(CliError::Process {
                command: format!("{} {}", binary.display(), args.join(" ")),
                status: status.code().unwrap_or(-1),
                stderr: format!("{label} process exited before becoming ready"),
            });
        }
        thread::sleep(Duration::from_millis(100));
    }

    Err(CliError::Network {
        address: paths.status.display().to_string(),
        detail: format!(
            "{label} process {} did not become ready after install",
            child.id()
        ),
    })
}

fn stop_daemon_if_running(paths: &DaemonPaths, label: &str) -> Result<(), CliError> {
    match probe_daemon_status(paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT)?;
            println!("sinomo {label} stop requested");
            Ok(())
        }
        DaemonProbeStatus::Locked { .. } => {
            println!("sinomo {label} lock is held; boot entry removal will continue");
            Ok(())
        }
        DaemonProbeStatus::NotRunning => {
            println!("sinomo {label} is not running");
            Ok(())
        }
    }
}

fn run_cluster_cli_command(command: ClusterCliCommand) -> Result<(), CliError> {
    match command {
        ClusterCliCommand::Help => {
            print_cluster_help();
            Ok(())
        }
        ClusterCliCommand::Init(options) => cluster_init(options),
        ClusterCliCommand::Invite(options) => cluster_invite(options),
        ClusterCliCommand::Join(options) => cluster_join(options),
        ClusterCliCommand::Status => cluster_status(),
        ClusterCliCommand::Peers => cluster_peers(),
        ClusterCliCommand::Leave => cluster_leave(),
    }
}

fn cluster_paths_from_defaults() -> Result<ClusterPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(ClusterPaths::new(&data_dir))
}

fn require_cluster_bootstrap_state() -> Result<ClusterBootstrapState, CliError> {
    let paths = cluster_paths_from_defaults()?;
    read_cluster_state(&paths)
}

fn require_cluster_runtime_state() -> Result<(), CliError> {
    if let Some(status) = default_main_daemon_status()? {
        if status.cluster.is_some() {
            return Ok(());
        }
    }
    require_cluster_bootstrap_state().map(|_| ())
}

fn cluster_init(options: ClusterInitOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    cluster_init_with_paths(options, &paths)
}

fn cluster_init_with_paths(
    options: ClusterInitOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    paths.ensure()?;
    if paths.state.exists() {
        return Err(CliError::Parse {
            detail: format!(
                "cluster is already initialized at {}; use `sinomo cluster status`",
                paths.state.display()
            ),
        });
    }

    let local_node_id = options.node_id.unwrap_or_else(NodeId::generate);
    let timestamp = OffsetDateTime::now_utc();
    let cluster_id = options.cluster_id.unwrap_or_else(|| {
        ClusterId::new(format!(
            "cluster_{}",
            local_node_id.as_str().trim_start_matches("node_")
        ))
        .expect("generated cluster id")
    });
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        options.name.clone(),
        local_node_id.clone(),
        options.name,
        main_identity.public_key.clone(),
        timestamp,
    );
    ensure_daemon_cluster_members(
        &mut state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        timestamp,
    );
    assign_local_node_public_keys(paths, &mut state)?;
    write_json(&paths.state, &state)?;
    sync_cluster_state_to_kv(paths, &state)?;
    println!("cluster initialized: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("state: {}", paths.state.display());
    Ok(())
}

fn cluster_invite(options: ClusterInviteOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let rendered = cluster_invite_with_paths(options, &paths)?;
    println!("invite: {}", rendered.uri);
    println!("join command: {}", rendered.join_command);
    println!("cluster: {}", rendered.invite.cluster_id);
    println!("expires: {}", format_rfc3339(rendered.invite.expires_at));
    println!("saved: {}", rendered.persisted_path.display());
    if let Some(output) = rendered.output_path {
        println!("output: {}", output.display());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RenderedClusterInvite {
    invite: ClusterJoinInvite,
    uri: String,
    join_command: String,
    persisted_path: PathBuf,
    output_path: Option<PathBuf>,
}

fn cluster_invite_with_paths(
    options: ClusterInviteOptions,
    paths: &ClusterPaths,
) -> Result<RenderedClusterInvite, CliError> {
    paths.ensure()?;
    fs::create_dir_all(&paths.invites).map_err(|source| CliError::Io {
        path: paths.invites.display().to_string(),
        detail: source.to_string(),
    })?;
    let state = read_cluster_state(paths)?;
    let now = OffsetDateTime::now_utc();
    let invite = build_cluster_invite(&state, &options, paths, now)?;
    if invite
        .introducers
        .iter()
        .all(|endpoint| !is_remote_usable_invite_endpoint(endpoint))
    {
        println!(
            "warning: invite only contains local-only introducers; remote machines should use `sinomo cluster invite --introducer <host-lan-ip>:21353`"
        );
    }
    let uri = encode_cluster_join_invite(&invite)?;
    let join_command = cluster_join_command(&uri);
    let persisted_path = paths.invite_path(&invite.invite_id);
    write_json(&persisted_path, &invite)?;
    if let Some(output) = &options.output {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent).map_err(|source| CliError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(output, format!("{join_command}\n")).map_err(|source| CliError::Io {
            path: output.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(RenderedClusterInvite {
        invite,
        uri,
        join_command,
        persisted_path,
        output_path: options.output,
    })
}

fn cluster_join_command(uri: &str) -> String {
    format!("sinomo cluster join \"{uri}\"")
}

fn sign_cluster_join_invite(
    invite: &ClusterJoinInvite,
    identity: &NodeIdentity,
) -> Result<String, CliError> {
    let key_pair = identity.key_pair()?;
    let payload = cluster_invite_signing_payload(invite)?;
    Ok(URL_SAFE_NO_PAD.encode(key_pair.sign(payload.as_bytes()).as_ref()))
}

fn verify_cluster_join_invite_signature(invite: &ClusterJoinInvite) -> Result<(), CliError> {
    if invite.signature.trim().is_empty()
        || invite.issuer_public_key.trim().is_empty()
        || invite.issuer_node_id.as_str().trim().is_empty()
    {
        return Err(CliError::AuthDenied);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(invite.issuer_public_key.as_bytes())
        .map_err(|_| CliError::AuthDenied)?;
    let signature = URL_SAFE_NO_PAD
        .decode(invite.signature.as_bytes())
        .map_err(|_| CliError::AuthDenied)?;
    let payload = cluster_invite_signing_payload(invite)?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(payload.as_bytes(), &signature)
        .map_err(|_| CliError::AuthDenied)
}

fn cluster_invite_signing_payload(invite: &ClusterJoinInvite) -> Result<String, CliError> {
    let expires_at = invite
        .expires_at
        .format(&Rfc3339)
        .map_err(|source| CliError::Parse {
            detail: format!("invalid invite expiry timestamp: {source}"),
        })?;
    let introducers =
        serde_json::to_string(&invite.introducers).map_err(|source| CliError::Json {
            path: "cluster invite introducers".to_string(),
            detail: source.to_string(),
        })?;
    let allowed_roles =
        serde_json::to_string(&invite.allowed_member_roles).map_err(|source| CliError::Json {
            path: "cluster invite allowed roles".to_string(),
            detail: source.to_string(),
        })?;
    Ok(format!(
        "{CLUSTER_INVITE_AUTH_PROTOCOL}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        invite.invite_id,
        invite.cluster_id,
        invite.cluster_name.clone().unwrap_or_default(),
        introducers,
        invite.token_id,
        expires_at,
        invite.trust_domain,
        allowed_roles,
        invite.issuer_node_id,
        invite.issuer_public_key
    ))
}

fn build_cluster_invite(
    state: &ClusterBootstrapState,
    options: &ClusterInviteOptions,
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<ClusterJoinInvite, CliError> {
    let introducers = if options.introducers.is_empty() {
        default_invite_introducers(state, paths)
    } else {
        options
            .introducers
            .iter()
            .map(|address| invite_introducer_endpoint(&state.local_node_id, address.clone()))
            .collect()
    };
    let invite_id = sinomo_runtime::core::ClusterInviteId::generate();
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let issuer_identity = load_or_create_main_node_identity(data_dir)?;
    let mut invite = ClusterJoinInvite {
        invite_id: invite_id.clone(),
        cluster_id: state.cluster_id.clone(),
        cluster_name: Some(state.cluster_name.clone()),
        introducers,
        token_id: format!("token_{}", invite_id.as_str().trim_start_matches("invite_")),
        expires_at: now + time::Duration::seconds(options.ttl_seconds),
        trust_domain: TrustDomain::new("trust.local").expect("static trust domain"),
        allowed_member_roles: vec![ClusterMemberRole::Learner, ClusterMemberRole::NonVoting],
        issuer_node_id: state.local_node_id.clone(),
        issuer_public_key: issuer_identity.public_key.clone(),
        signature: String::new(),
    };
    invite.signature = sign_cluster_join_invite(&invite, &issuer_identity)?;
    Ok(invite)
}

fn default_invite_introducers(
    state: &ClusterBootstrapState,
    paths: &ClusterPaths,
) -> Vec<ClusterEndpointRecord> {
    let mut introducers = Vec::new();
    if let Some(data_dir) = paths.root.parent() {
        let daemon_paths = DaemonPaths::with_root(data_dir.join("daemon"));
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&daemon_paths, DAEMON_PROBE_TIMEOUT)
        {
            for endpoint in invite_introducers_from_daemon_status(&state.local_node_id, &status) {
                push_unique_introducer(&mut introducers, endpoint);
            }
        }
    }

    for endpoint in state
        .members
        .iter()
        .filter(|member| member.node_id == state.local_node_id)
        .flat_map(|member| member.endpoints.iter().cloned())
    {
        push_unique_introducer(&mut introducers, endpoint);
    }
    sort_default_invite_introducers(&mut introducers);
    introducers
}

fn invite_introducers_from_daemon_status(
    node_id: &NodeId,
    status: &DaemonStatus,
) -> Vec<ClusterEndpointRecord> {
    if matches!(status.host.as_str(), "0.0.0.0" | "::") {
        let mut endpoints = Vec::new();
        for ip in discover_lan_ip_candidates() {
            endpoints.push(invite_introducer_endpoint(
                node_id,
                format_ip_port(ip, status.port),
            ));
        }
        endpoints.push(invite_introducer_endpoint(node_id, status.address.clone()));
        endpoints
    } else {
        vec![invite_introducer_endpoint(node_id, status.address.clone())]
    }
}

fn discover_lan_ip_candidates() -> Vec<IpAddr> {
    let mut candidates = Vec::new();
    for target in ["8.8.8.8:80", "1.1.1.1:80", "192.0.2.1:80"] {
        let Ok(socket) = UdpSocket::bind("0.0.0.0:0") else {
            continue;
        };
        if socket.connect(target).is_err() {
            continue;
        }
        let Ok(address) = socket.local_addr() else {
            continue;
        };
        let ip = address.ip();
        if is_invitable_ip(ip) && !candidates.contains(&ip) {
            candidates.push(ip);
        }
    }
    candidates
}

fn is_invitable_ip(ip: IpAddr) -> bool {
    !ip.is_loopback() && !ip.is_unspecified() && !ip.is_multicast()
}

fn format_ip_port(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(ip) => format!("{ip}:{port}"),
        IpAddr::V6(ip) => format!("[{ip}]:{port}"),
    }
}

fn push_unique_introducer(
    introducers: &mut Vec<ClusterEndpointRecord>,
    endpoint: ClusterEndpointRecord,
) {
    if !introducers
        .iter()
        .any(|candidate| candidate.address == endpoint.address)
    {
        introducers.push(endpoint);
    }
}

fn sort_default_invite_introducers(introducers: &mut [ClusterEndpointRecord]) {
    introducers.sort_by_key(|endpoint| {
        (
            !is_remote_usable_invite_endpoint(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
}

fn is_remote_usable_invite_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly)
        && !is_loopback_connect_address(&endpoint.address)
}

fn is_loopback_connect_address(address: &str) -> bool {
    address.starts_with("127.")
        || address.starts_with("localhost:")
        || address.starts_with("[::1]:")
        || address.starts_with("::1:")
}

fn invite_introducer_endpoint(node_id: &NodeId, address: String) -> ClusterEndpointRecord {
    let is_loopback = is_loopback_connect_address(&address);
    ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::generate(),
        node_id: node_id.clone(),
        transport: if is_loopback {
            ClusterTransportKind::Loopback
        } else {
            ClusterTransportKind::LanTcp
        },
        address,
        source: ClusterEndpointSource::Static,
        privacy: if is_loopback {
            ClusterEndpointPrivacy::LocalOnly
        } else {
            ClusterEndpointPrivacy::Lan
        },
        priority: if is_loopback { 100 } else { 20 },
        last_observed_latency_ms: None,
    }
}

fn cluster_join(options: ClusterJoinOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    cluster_join_with_paths(options, &paths)
}

fn cluster_join_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    paths.ensure()?;
    if paths.state.exists() {
        let existing = read_cluster_state(paths)?;
        if matches!(existing.bootstrap, ClusterBootstrap::Joined { .. }) {
            return cluster_join_retry_with_paths(options, paths, existing);
        }
        return Err(CliError::Parse {
            detail: format!(
                "cluster is already initialized at {}; leave before joining another cluster",
                paths.state.display()
            ),
        });
    }

    let local_node_id = options.node_id.unwrap_or_else(NodeId::generate);
    let timestamp = OffsetDateTime::now_utc();
    let invite = parse_cluster_join_invite_argument(&options.invite)?;
    verify_cluster_join_invite_signature(&invite)?;
    if invite.expires_at <= timestamp {
        return Err(CliError::Parse {
            detail: format!(
                "cluster invite expired at {}",
                format_rfc3339(invite.expires_at)
            ),
        });
    }
    if let Some(cluster_id) = &options.cluster_id
        && cluster_id != &invite.cluster_id
    {
        return Err(CliError::Parse {
            detail: format!(
                "cluster id override `{cluster_id}` does not match invite cluster `{}`",
                invite.cluster_id
            ),
        });
    }
    let cluster_id = invite.cluster_id.clone();
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let mut state = ClusterBootstrapState::pending_join(
        cluster_id,
        invite
            .cluster_name
            .clone()
            .unwrap_or_else(|| options.name.clone()),
        local_node_id.clone(),
        options.name,
        main_identity.public_key.clone(),
        invite,
        timestamp,
    );
    let (main_options, shadow_options) = local_daemon_cluster_options()?;
    ensure_daemon_cluster_members(&mut state, &main_options, &shadow_options, timestamp);
    assign_local_node_public_keys(paths, &mut state)?;
    reconcile_cluster_join_with_introducers(&mut state, Some(paths))?;
    write_json(&paths.state, &state)?;
    sync_cluster_state_to_kv(paths, &state)?;
    println!("cluster join recorded: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("invite: {}", options.invite);
    println!("state: {}", paths.state.display());
    Ok(())
}

fn cluster_join_retry_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
) -> Result<(), CliError> {
    let timestamp = OffsetDateTime::now_utc();
    let invite = parse_cluster_join_invite_argument(&options.invite)?;
    verify_cluster_join_invite_signature(&invite)?;
    if invite.cluster_id != state.cluster_id {
        return Err(CliError::Parse {
            detail: format!(
                "join retry invite cluster `{}` does not match local cluster `{}`",
                invite.cluster_id, state.cluster_id
            ),
        });
    }
    state.bootstrap = ClusterBootstrap::Joined { invite };
    let (main_options, shadow_options) = local_daemon_cluster_options()?;
    ensure_daemon_cluster_members(&mut state, &main_options, &shadow_options, timestamp);
    assign_local_node_public_keys(paths, &mut state)?;
    reconcile_cluster_join_with_introducers(&mut state, Some(paths))?;
    state.updated_at = timestamp;
    write_json(&paths.state, &state)?;
    sync_cluster_state_to_kv(paths, &state)?;
    println!("cluster join reconciled: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("invite: {}", options.invite);
    println!("state: {}", paths.state.display());
    Ok(())
}

fn reconcile_cluster_join_with_introducers(
    state: &mut ClusterBootstrapState,
    paths: Option<&ClusterPaths>,
) -> Result<(), CliError> {
    let invite = match &state.bootstrap {
        ClusterBootstrap::Joined { invite } => invite.clone(),
        ClusterBootstrap::InitializedLocally => return Ok(()),
    };
    if invite.introducers.is_empty() {
        return Ok(());
    }
    let request = ClusterJoinRegistrationRequest {
        invite,
        members: state.members.clone(),
    };
    let mut last_error = None;
    for introducer in &request.invite.introducers {
        match send_cluster_join_registration(&introducer.address, &request) {
            Ok(response) => {
                if response.accepted {
                    if let Some(snapshot) = response.kv_snapshot.clone()
                        && let Some(paths) = paths
                    {
                        write_cluster_kv_snapshot(paths, &snapshot)?;
                    }
                    merge_cluster_join_registration(state, response.state);
                    println!("introducer accepted join: {}", introducer.address);
                    return Ok(());
                }
                last_error = Some(response.detail);
            }
            Err(error) => last_error = Some(error.to_string()),
        }
    }
    if let Some(error) = last_error {
        return Err(CliError::Network {
            address: "cluster introducers".to_string(),
            detail: format!(
                "join registration failed for all introducers: {error}. If the invite contains 127.0.0.1 or localhost, create a new invite with `sinomo cluster invite --introducer <host-lan-ip>:21353`."
            ),
        });
    }
    Ok(())
}

fn merge_cluster_join_registration(
    local: &mut ClusterBootstrapState,
    accepted: ClusterBootstrapState,
) {
    let bootstrap = local.bootstrap.clone();
    let local_node_id = local.local_node_id.clone();
    let local_node_display_name = local.local_node_display_name.clone();
    let initialized_at = local.initialized_at;
    *local = accepted;
    local.bootstrap = bootstrap;
    local.local_node_id = local_node_id;
    local.local_node_display_name = local_node_display_name;
    local.initialized_at = initialized_at;
}

fn send_cluster_join_registration(
    address: &str,
    request: &ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    let payload = serde_json::to_string(request).map_err(|source| CliError::Json {
        path: "cluster join registration".to_string(),
        detail: source.to_string(),
    })?;
    let response = send_daemon_command(
        address,
        &format!("CLUSTER_JOIN {payload}"),
        DAEMON_JOIN_TIMEOUT,
    )?;
    serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
        path: format!("cluster join response from {address}"),
        detail: source.to_string(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ClusterJoinRegistrationRequest {
    invite: ClusterJoinInvite,
    members: Vec<ClusterMemberRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ClusterJoinRegistrationResponse {
    accepted: bool,
    detail: String,
    state: ClusterBootstrapState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kv_snapshot: Option<KvSnapshot>,
}

fn parse_cluster_join_invite_argument(raw: &str) -> Result<ClusterJoinInvite, CliError> {
    let raw = raw.trim();
    let invite = if let Some(path) = raw.strip_prefix('@') {
        fs::read_to_string(path).map_err(|source| CliError::Io {
            path: path.to_string(),
            detail: source.to_string(),
        })?
    } else if Path::new(raw).exists() {
        fs::read_to_string(raw).map_err(|source| CliError::Io {
            path: raw.to_string(),
            detail: source.to_string(),
        })?
    } else {
        raw.to_string()
    };
    decode_cluster_join_invite(extract_invite_from_join_command(invite.trim()))
}

fn extract_invite_from_join_command(raw: &str) -> &str {
    let first_line = raw
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(raw);
    let trimmed = first_line.trim();
    let Some(rest) = trimmed.strip_prefix("sinomo cluster join ") else {
        return raw.trim();
    };
    let rest = rest.trim();
    if let Some(after_quote) = rest.strip_prefix('"')
        && let Some(end) = after_quote.find('"')
    {
        return &after_quote[..end];
    }
    rest.split_whitespace().next().unwrap_or(rest)
}

fn encode_cluster_join_invite(invite: &ClusterJoinInvite) -> Result<String, CliError> {
    let bytes = serde_json::to_vec(invite).map_err(|source| CliError::Json {
        path: "cluster invite".to_string(),
        detail: source.to_string(),
    })?;
    Ok(format!(
        "{SINOMO_JOIN_URI_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(bytes)
    ))
}

fn decode_cluster_join_invite(raw: &str) -> Result<ClusterJoinInvite, CliError> {
    if let Some(payload) = raw.strip_prefix(SINOMO_JOIN_URI_PREFIX) {
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|source| CliError::Parse {
                detail: format!("invalid cluster invite encoding: {source}"),
            })?;
        serde_json::from_slice(&bytes).map_err(|source| CliError::Json {
            path: "cluster invite".to_string(),
            detail: source.to_string(),
        })
    } else if raw.trim_start().starts_with('{') {
        serde_json::from_str(raw).map_err(|source| CliError::Json {
            path: "cluster invite".to_string(),
            detail: source.to_string(),
        })
    } else {
        Err(CliError::Parse {
            detail:
                "cluster join requires a sinomo-join://v1 invite, invite JSON, or invite file path"
                    .to_string(),
        })
    }
}

fn cluster_status() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    match read_cluster_state(&paths) {
        Ok(state) => {
            let kv_revision = read_cluster_kv_snapshot(&paths)
                .map(|snapshot| snapshot.revision.0)
                .unwrap_or(state.kv_revision);
            println!("cluster: {}", state.cluster_id);
            println!("name: {}", state.cluster_name);
            println!("local node: {}", state.local_node_id);
            println!("bootstrap: {}", cluster_bootstrap_label(&state.bootstrap));
            println!("term: {}", state.term);
            println!("kv revision: {}", kv_revision);
            println!("kv snapshot: {}", paths.kv_snapshot.display());
            println!("members: {}", state.members.len());
            println!("state: {}", paths.state.display());
            Ok(())
        }
        Err(CliError::ClusterOrphan) => {
            println!("cluster: orphan");
            println!("state: not initialized");
            println!("next: run `sinomo cluster init` or `sinomo cluster join <invite>`");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn cluster_peers() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let mut state = read_cluster_state(&paths)?;
    if matches!(state.bootstrap, ClusterBootstrap::Joined { .. }) {
        reconcile_cluster_join_with_introducers(&mut state, Some(&paths))?;
        let (main_options, shadow_options) = local_daemon_cluster_options()?;
        ensure_daemon_cluster_members(
            &mut state,
            &main_options,
            &shadow_options,
            OffsetDateTime::now_utc(),
        );
        write_json(&paths.state, &state)?;
        sync_cluster_state_to_kv(&paths, &state)?;
    }
    for member in state.members {
        println!(
            "{}\t{}\t{:?}\t{:?}",
            member.node_id, member.display_name, member.role, member.state
        );
    }
    Ok(())
}

fn cluster_leave() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    if !paths.state.exists() {
        return Err(CliError::ClusterOrphan);
    }
    fs::remove_file(&paths.state).map_err(|source| CliError::Io {
        path: paths.state.display().to_string(),
        detail: source.to_string(),
    })?;
    if paths.kv_snapshot.exists() {
        fs::remove_file(&paths.kv_snapshot).map_err(|source| CliError::Io {
            path: paths.kv_snapshot.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    println!("cluster state removed: {}", paths.state.display());
    Ok(())
}

fn read_cluster_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, CliError> {
    if !paths.state.exists() {
        return Err(CliError::ClusterOrphan);
    }
    let content = fs::read_to_string(&paths.state).map_err(|source| CliError::Io {
        path: paths.state.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut state: ClusterBootstrapState =
        serde_json::from_str(&content).map_err(|source| CliError::Json {
            path: paths.state.display().to_string(),
            detail: source.to_string(),
        })?;
    let kv_members = read_cluster_members_from_kv(paths)?;
    if !kv_members.is_empty() {
        state.members = kv_members;
    }
    ensure_daemon_cluster_members(
        &mut state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        OffsetDateTime::now_utc(),
    );
    assign_local_node_public_keys(paths, &mut state)?;
    Ok(state)
}

fn refresh_cluster_kv_from_local_daemon(paths: &ClusterPaths) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Ok(());
    };
    let Ok(response) =
        send_daemon_command(&status.address, "CLUSTER_KV_SNAPSHOT", DAEMON_PROBE_TIMEOUT)
    else {
        return Ok(());
    };
    let snapshot: KvSnapshot =
        serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
            path: "cluster kv snapshot response".to_string(),
            detail: source.to_string(),
        })?;
    write_cluster_kv_snapshot(paths, &snapshot)
}

fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    let content = fs::read_to_string(&paths.kv_snapshot).map_err(|source| CliError::Io {
        path: paths.kv_snapshot.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: paths.kv_snapshot.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_cluster_kv_snapshot(paths: &ClusterPaths, snapshot: &KvSnapshot) -> Result<(), CliError> {
    write_json(&paths.kv_snapshot, snapshot)
}

fn read_cluster_kv_store(paths: &ClusterPaths) -> Result<InProcessKvStore, CliError> {
    if !paths.kv_snapshot.exists() {
        return Ok(InProcessKvStore::new());
    }
    read_cluster_kv_snapshot(paths).map(InProcessKvStore::restore)
}

fn read_cluster_members_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, CliError> {
    if !paths.kv_snapshot.exists() {
        return Ok(Vec::new());
    }
    let store = read_cluster_kv_store(paths)?;
    let mut members = Vec::new();
    for entry in store.range_prefix("/cluster/members/") {
        let member: ClusterMemberRecord =
            serde_json::from_slice(&entry.value).map_err(|source| CliError::Json {
                path: entry.key,
                detail: source.to_string(),
            })?;
        members.push(member);
    }
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(members)
}

fn sync_cluster_state_to_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, CliError> {
    let mut store = read_cluster_kv_store(paths)?;
    let mut keyed_state = state.clone();
    assign_local_node_public_keys(paths, &mut keyed_state)?;
    for member in &keyed_state.members {
        store.put_cluster_member(member.clone())?;
    }
    let snapshot = store.snapshot();
    write_cluster_kv_snapshot(paths, &snapshot)?;
    Ok(snapshot)
}

fn assign_local_node_public_keys(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<(), CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    for member in &mut state.members {
        if member.node_id == state.local_node_id {
            member.node_public_key = main_identity.public_key.clone();
        } else if member.node_id == shadow_node_id {
            member.node_public_key = shadow_identity.public_key.clone();
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ClusterSyncRequest {
    state: ClusterBootstrapState,
    kv_snapshot: KvSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ClusterSyncResponse {
    accepted: bool,
    detail: String,
    state: ClusterBootstrapState,
    kv_snapshot: KvSnapshot,
}

struct ClusterNetworkMaintenance {
    discovery: Option<MdnsDiscoverySocket>,
    discovery_cache: MdnsDiscoveryCache,
    tick: u64,
    last_discovery: Instant,
    last_sync: Instant,
}

impl ClusterNetworkMaintenance {
    fn new() -> Self {
        Self {
            discovery: MdnsDiscoverySocket::bind_default().ok(),
            discovery_cache: MdnsDiscoveryCache::new(),
            tick: 0,
            last_discovery: Instant::now() - DAEMON_CLUSTER_DISCOVERY_INTERVAL,
            last_sync: Instant::now() - DAEMON_CLUSTER_SYNC_INTERVAL,
        }
    }

    fn tick(&mut self, paths: &DaemonPaths, options: &DaemonRunOptions) -> Result<(), CliError> {
        self.tick = self.tick.saturating_add(1);
        let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon paths have no data directory parent".to_string(),
        })?;
        let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        if !cluster_paths.state.exists() {
            return Ok(());
        }

        let mut state = read_cluster_state(&cluster_paths)?;
        let shadow_options = current_shadow_daemon_options(data_dir);
        ensure_daemon_cluster_members(
            &mut state,
            options,
            &shadow_options,
            OffsetDateTime::now_utc(),
        );
        assign_local_node_public_keys(&cluster_paths, &mut state)?;

        self.process_discovery(&mut state);
        let mut snapshot = sync_cluster_state_to_kv(&cluster_paths, &state)?;

        let now = Instant::now();
        if now.duration_since(self.last_discovery) >= DAEMON_CLUSTER_DISCOVERY_INTERVAL {
            self.last_discovery = now;
            self.announce(&state);
        }

        if now.duration_since(self.last_sync) >= DAEMON_CLUSTER_SYNC_INTERVAL {
            self.last_sync = now;
            sync_cluster_with_reachable_peers(&cluster_paths, &mut state, &mut snapshot)?;
        }

        write_json(&cluster_paths.state, &state)?;
        write_cluster_kv_snapshot(&cluster_paths, &snapshot)?;
        Ok(())
    }

    fn process_discovery(&mut self, state: &mut ClusterBootstrapState) {
        let Some(discovery) = &self.discovery else {
            return;
        };
        let Ok(advertisements) = discovery.drain() else {
            return;
        };
        for advertisement in advertisements {
            if advertisement.cluster_id != state.cluster_id
                || advertisement.node_id == state.local_node_id
            {
                continue;
            }
            for record in advertisement.records() {
                self.discovery_cache.upsert(record.clone());
                if let Some(member) = state
                    .members
                    .iter_mut()
                    .find(|member| member.node_id == record.endpoint.node_id)
                {
                    upsert_member_endpoint(member, record.endpoint);
                }
            }
        }
        self.discovery_cache.expire_after_tick(self.tick);
    }

    fn announce(&self, state: &ClusterBootstrapState) {
        let Some(discovery) = &self.discovery else {
            return;
        };
        let Some(member) = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
        else {
            return;
        };
        let endpoints: Vec<_> = member
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.privacy == ClusterEndpointPrivacy::Lan
                    && matches!(
                        endpoint.transport,
                        ClusterTransportKind::LanTcp | ClusterTransportKind::LanQuic
                    )
            })
            .cloned()
            .collect();
        if endpoints.is_empty() {
            return;
        }
        let advertisement = MdnsDiscoveryAdvertisement::new(
            state.cluster_id.clone(),
            state.local_node_id.clone(),
            endpoints,
            self.tick,
        );
        let _ = discovery.announce(&advertisement);
    }
}

fn current_shadow_daemon_options(data_dir: &Path) -> DaemonRunOptions {
    match probe_daemon_status(
        &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
        DAEMON_PROBE_TIMEOUT,
    ) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => daemon_options_from_status(&status),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            DaemonRunOptions::shadow_default()
        }
    }
}

fn sync_cluster_with_reachable_peers(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    snapshot: &mut KvSnapshot,
) -> Result<(), CliError> {
    let request = ClusterSyncRequest {
        state: state.clone(),
        kv_snapshot: snapshot.clone(),
    };
    for endpoint in cluster_sync_targets(state) {
        let Ok(response) = send_cluster_sync(&endpoint.address, &request) else {
            continue;
        };
        if !response.accepted {
            continue;
        }
        merge_cluster_state_from_peer(state, response.state);
        merge_cluster_kv_snapshot(paths, &response.kv_snapshot)?;
        *snapshot = read_cluster_kv_snapshot(paths)?;
    }
    Ok(())
}

fn cluster_sync_targets(state: &ClusterBootstrapState) -> Vec<ClusterEndpointRecord> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        if !member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "daemon.main.run")
        {
            continue;
        }
        let mut endpoints = member.endpoints.clone();
        endpoints.sort_by_key(|endpoint| {
            (
                endpoint.privacy != ClusterEndpointPrivacy::Lan,
                endpoint.priority,
                endpoint.address.clone(),
            )
        });
        targets.extend(endpoints.into_iter().filter(|endpoint| {
            matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
            )
        }));
    }
    targets
}

fn send_cluster_sync(
    address: &str,
    request: &ClusterSyncRequest,
) -> Result<ClusterSyncResponse, CliError> {
    let payload = serde_json::to_string(request).map_err(|source| CliError::Json {
        path: "cluster sync request".to_string(),
        detail: source.to_string(),
    })?;
    let response = send_daemon_command(
        address,
        &format!("CLUSTER_SYNC {payload}"),
        DAEMON_JOIN_TIMEOUT,
    )?;
    serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
        path: format!("cluster sync response from {address}"),
        detail: source.to_string(),
    })
}

fn handle_daemon_cluster_sync(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<ClusterSyncResponse, CliError> {
    let request: ClusterSyncRequest =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "cluster sync request".to_string(),
            detail: source.to_string(),
        })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut state = read_cluster_state(&cluster_paths)?;
    if request.state.cluster_id != state.cluster_id {
        let snapshot = read_cluster_kv_store(&cluster_paths)?.snapshot();
        return Ok(ClusterSyncResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: snapshot,
        });
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_json(&cluster_paths.state, &state)?;
    merge_cluster_kv_snapshot(&cluster_paths, &request.kv_snapshot)?;
    let snapshot = sync_cluster_state_to_kv(&cluster_paths, &state)?;
    Ok(ClusterSyncResponse {
        accepted: true,
        detail: "cluster sync applied".to_string(),
        state,
        kv_snapshot: snapshot,
    })
}

fn merge_cluster_state_from_peer(
    local: &mut ClusterBootstrapState,
    peer: ClusterBootstrapState,
) -> bool {
    if peer.cluster_id != local.cluster_id {
        return false;
    }
    let mut changed = false;
    for member in peer.members {
        changed |= merge_member_record(local, member);
    }
    if peer.kv_revision > local.kv_revision {
        local.kv_revision = peer.kv_revision;
    }
    if peer.updated_at > local.updated_at {
        local.updated_at = peer.updated_at;
    }
    changed
}

fn merge_member_record(local: &mut ClusterBootstrapState, incoming: ClusterMemberRecord) -> bool {
    if incoming.cluster_id != local.cluster_id {
        return false;
    }
    if let Some(existing) = local
        .members
        .iter_mut()
        .find(|member| member.node_id == incoming.node_id)
    {
        let mut changed = false;
        if incoming.updated_at > existing.updated_at {
            *existing = incoming;
            return true;
        }
        for endpoint in incoming.endpoints {
            changed |= upsert_member_endpoint(existing, endpoint);
        }
        changed
    } else {
        local.members.push(incoming);
        true
    }
}

fn upsert_member_endpoint(
    member: &mut ClusterMemberRecord,
    endpoint: ClusterEndpointRecord,
) -> bool {
    if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
    {
        if *existing == endpoint {
            false
        } else {
            *existing = endpoint;
            true
        }
    } else if member.endpoints.iter().any(|candidate| {
        candidate.transport == endpoint.transport && candidate.address == endpoint.address
    }) {
        false
    } else {
        member.endpoints.push(endpoint);
        true
    }
}

fn merge_cluster_kv_snapshot(
    paths: &ClusterPaths,
    incoming: &KvSnapshot,
) -> Result<KvSnapshot, CliError> {
    let mut store = read_cluster_kv_store(paths)?;
    for lease in &incoming.leases {
        let _ = store.apply_command(sinomo_runtime::kv::KvCommand::Op(
            sinomo_runtime::kv::KvOp::LeaseGrant {
                lease_id: lease.lease_id,
                ttl_ticks: lease.ttl_ticks.max(1),
            },
        ));
    }
    for entry in &incoming.entries {
        let should_put = store
            .get(&entry.key)
            .map(|existing| {
                entry.modify_revision > existing.modify_revision || entry.value != existing.value
            })
            .unwrap_or(true);
        if should_put {
            store.put(entry.key.clone(), entry.value.clone(), entry.lease_id)?;
        }
    }
    let snapshot = store.snapshot();
    write_cluster_kv_snapshot(paths, &snapshot)?;
    Ok(snapshot)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonClusterProcess {
    Main,
    Shadow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonClusterRegistration {
    process: DaemonClusterProcess,
    main_options: DaemonRunOptions,
    shadow_options: DaemonRunOptions,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DaemonClusterStatus {
    cluster_id: ClusterId,
    node_id: NodeId,
    process: String,
    member_role: ClusterMemberRole,
    member_state: ClusterMemberState,
}

fn ensure_daemon_cluster_members(
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) {
    ensure_main_daemon_cluster_member(state, main_options, timestamp);
    ensure_shadow_daemon_cluster_member(state, shadow_options, timestamp);
    state.updated_at = timestamp;
}

fn local_daemon_cluster_options() -> Result<(DaemonRunOptions, DaemonRunOptions), CliError> {
    let main = match default_main_daemon_status()? {
        Some(status) => daemon_options_from_status(&status),
        None => DaemonRunOptions::default(),
    };
    let shadow =
        match probe_daemon_status(&shadow_daemon_paths_from_defaults()?, DAEMON_PROBE_TIMEOUT) {
            DaemonProbeStatus::Running(status)
            | DaemonProbeStatus::Locked {
                status: Some(status),
            } => daemon_options_from_status(&status),
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                DaemonRunOptions::shadow_default()
            }
        };
    Ok((main, shadow))
}

fn daemon_options_from_status(status: &DaemonStatus) -> DaemonRunOptions {
    DaemonRunOptions {
        host: status.host.clone(),
        port: status.port,
    }
}

fn ensure_main_daemon_cluster_member(
    state: &mut ClusterBootstrapState,
    options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) {
    let role = match &state.bootstrap {
        ClusterBootstrap::InitializedLocally => ClusterMemberRole::Voter,
        ClusterBootstrap::Joined { .. } => ClusterMemberRole::Learner,
    };
    let member_state = match &state.bootstrap {
        ClusterBootstrap::InitializedLocally => ClusterMemberState::Active,
        ClusterBootstrap::Joined { .. } => state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
            .map(|member| member.state.clone())
            .filter(|state| *state != ClusterMemberState::Pending)
            .unwrap_or(ClusterMemberState::Pending),
    };
    let local_node_id = state.local_node_id.clone();
    let node_public_key = existing_member_public_key(state, &local_node_id).unwrap_or_default();
    let mut member = ClusterMemberRecord::local_voter(
        state.cluster_id.clone(),
        local_node_id.clone(),
        state.local_node_display_name.clone(),
        node_public_key,
        timestamp,
    );
    let capabilities = main_daemon_capabilities(&role);
    member.role = role;
    member.state = member_state;
    upsert_daemon_member(
        state,
        member,
        capabilities,
        daemon_advertised_endpoints(&local_node_id, options, 0),
        timestamp,
    );
}

fn ensure_shadow_daemon_cluster_member(
    state: &mut ClusterBootstrapState,
    options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) {
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let existing_shadow_state = state
        .members
        .iter()
        .find(|member| member.node_id == shadow_node_id)
        .map(|member| member.state.clone())
        .filter(|state| *state != ClusterMemberState::Pending);
    let node_public_key = existing_member_public_key(state, &shadow_node_id).unwrap_or_default();
    let mut member = match &state.bootstrap {
        ClusterBootstrap::InitializedLocally => ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            shadow_node_id.clone(),
            format!("{} shadow", state.local_node_display_name),
            node_public_key,
            timestamp,
        ),
        ClusterBootstrap::Joined { .. } => ClusterMemberRecord::pending_non_voting(
            state.cluster_id.clone(),
            shadow_node_id.clone(),
            format!("{} shadow", state.local_node_display_name),
            node_public_key,
            timestamp,
        ),
    };
    if let Some(existing_state) = existing_shadow_state {
        member.state = existing_state;
    }
    upsert_daemon_member(
        state,
        member,
        shadow_daemon_capabilities(),
        daemon_advertised_endpoints(&shadow_node_id, options, 10),
        timestamp,
    );
}

fn existing_member_public_key(state: &ClusterBootstrapState, node_id: &NodeId) -> Option<String> {
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .map(|member| member.node_public_key.clone())
        .filter(|key| !key.trim().is_empty())
}

fn upsert_daemon_member(
    state: &mut ClusterBootstrapState,
    replacement: ClusterMemberRecord,
    capabilities: Vec<CapabilityName>,
    endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) {
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == replacement.node_id)
    {
        member.display_name = replacement.display_name;
        member.role = replacement.role;
        member.state = replacement.state;
        member.node_public_key = replacement.node_public_key;
        member.capabilities = capabilities.clone();
        member.allowed_capabilities = capabilities;
        for endpoint in endpoints {
            member
                .endpoints
                .retain(|candidate| candidate.endpoint_id != endpoint.endpoint_id);
            member.endpoints.push(endpoint);
        }
        member.updated_at = timestamp;
    } else {
        let mut member = replacement;
        member.capabilities = capabilities.clone();
        member.allowed_capabilities = capabilities;
        member.endpoints = endpoints;
        state.members.push(member);
    }
}

fn daemon_advertised_endpoints(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = vec![daemon_loopback_endpoint(node_id, options, priority)];
    let mut lan_addresses = Vec::new();
    if matches!(options.host.as_str(), "0.0.0.0" | "::") {
        for ip in discover_lan_ip_candidates() {
            lan_addresses.push(format_ip_port(ip, options.port));
        }
    } else if options.host.parse::<IpAddr>().is_ok_and(is_invitable_ip)
        || !is_loopback_connect_address(&daemon_connect_address(&options.host, options.port))
    {
        lan_addresses.push(format!("{}:{}", options.host, options.port));
    }

    for address in lan_addresses {
        endpoints.push(daemon_lan_endpoint(
            node_id,
            address,
            priority.saturating_add(20),
        ));
    }
    endpoints
}

fn daemon_loopback_endpoint(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: daemon_loopback_endpoint_id(node_id),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Loopback,
        address: daemon_connect_address(&options.host, options.port),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::LocalOnly,
        priority,
        last_observed_latency_ms: None,
    }
}

fn daemon_lan_endpoint(node_id: &NodeId, address: String, priority: u16) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: daemon_lan_endpoint_id(node_id, &address),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address,
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority,
        last_observed_latency_ms: None,
    }
}

fn daemon_loopback_endpoint_id(node_id: &NodeId) -> ClusterEndpointId {
    let body = node_id.as_str().trim_start_matches(NodeId::PREFIX);
    ClusterEndpointId::new(format!("endpoint_{body}_daemon_loopback"))
        .expect("derived daemon endpoint id")
}

fn daemon_lan_endpoint_id(node_id: &NodeId, address: &str) -> ClusterEndpointId {
    let node = node_id.as_str().trim_start_matches(NodeId::PREFIX);
    let address = sanitize_endpoint_id_body(address);
    ClusterEndpointId::new(format!("endpoint_{node}_daemon_lan_{address}"))
        .expect("derived daemon lan endpoint id")
}

fn sanitize_endpoint_id_body(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn shadow_daemon_node_id(main_node_id: &NodeId) -> NodeId {
    NodeId::new(format!("{}-shadow", main_node_id.as_str())).expect("derived shadow daemon node id")
}

fn main_daemon_capabilities(role: &ClusterMemberRole) -> Vec<CapabilityName> {
    let mut capabilities = vec![
        CapabilityName::new("cluster.message.receive").expect("static capability"),
        CapabilityName::new("cluster.message.send").expect("static capability"),
        CapabilityName::new("daemon.main.run").expect("static capability"),
    ];
    if matches!(role, ClusterMemberRole::Voter) {
        capabilities.push(CapabilityName::new("cluster.kv.vote").expect("static capability"));
    }
    capabilities
}

fn shadow_daemon_capabilities() -> Vec<CapabilityName> {
    vec![
        CapabilityName::new("cluster.message.receive").expect("static capability"),
        CapabilityName::new("cluster.message.send").expect("static capability"),
        CapabilityName::new("daemon.shadow.supervise").expect("static capability"),
    ]
}

fn daemon_cluster_status(
    state: &ClusterBootstrapState,
    process: DaemonClusterProcess,
) -> Option<DaemonClusterStatus> {
    let node_id = match process {
        DaemonClusterProcess::Main => state.local_node_id.clone(),
        DaemonClusterProcess::Shadow => shadow_daemon_node_id(&state.local_node_id),
    };
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == node_id)?;
    Some(DaemonClusterStatus {
        cluster_id: state.cluster_id.clone(),
        node_id,
        process: match process {
            DaemonClusterProcess::Main => DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            DaemonClusterProcess::Shadow => DAEMON_SHADOW_CLUSTER_ROLE.to_string(),
        },
        member_role: member.role.clone(),
        member_state: member.state.clone(),
    })
}

fn register_daemon_cluster_node(
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, CliError> {
    let paths = cluster_paths_from_defaults()?;
    let mut state = read_cluster_state(&paths)?;
    ensure_daemon_cluster_members(
        &mut state,
        &registration.main_options,
        &registration.shadow_options,
        OffsetDateTime::now_utc(),
    );
    write_json(&paths.state, &state)?;
    sync_cluster_state_to_kv(&paths, &state)?;
    daemon_cluster_status(&state, registration.process).ok_or_else(|| CliError::Parse {
        detail: "daemon cluster member could not be registered".to_string(),
    })
}

fn daemon_cluster_registration(
    process: DaemonClusterProcess,
    main_options: DaemonRunOptions,
    shadow_options: DaemonRunOptions,
) -> DaemonClusterRegistration {
    DaemonClusterRegistration {
        process,
        main_options,
        shadow_options,
    }
}

fn format_member_role(role: &ClusterMemberRole) -> &'static str {
    match role {
        ClusterMemberRole::Voter => "voter",
        ClusterMemberRole::Learner => "learner",
        ClusterMemberRole::NonVoting => "non-voting",
    }
}

fn format_member_state(state: &ClusterMemberState) -> &'static str {
    match state {
        ClusterMemberState::Pending => "pending",
        ClusterMemberState::Active => "active",
        ClusterMemberState::Learner => "learner",
        ClusterMemberState::Voter => "voter",
        ClusterMemberState::NonVoting => "non-voting",
        ClusterMemberState::Suspect => "suspect",
        ClusterMemberState::Offline => "offline",
        ClusterMemberState::Removed => "removed",
    }
}

fn cluster_bootstrap_label(bootstrap: &ClusterBootstrap) -> &'static str {
    match bootstrap {
        ClusterBootstrap::InitializedLocally => "initialized-locally",
        ClusterBootstrap::Joined { .. } => "joined",
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DaemonCliCommand {
    Help,
    Run(DaemonRunOptions),
    Start(DaemonRunOptions),
    Status(DaemonRunOptions),
    Stop(DaemonRunOptions),
    ShadowRun(DaemonRunOptions),
    ShadowStart(DaemonRunOptions),
    ShadowStatus(DaemonRunOptions),
    ShadowStop(DaemonRunOptions),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DaemonRunOptions {
    host: String,
    port: u16,
}

impl Default for DaemonRunOptions {
    fn default() -> Self {
        Self {
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
        }
    }
}

impl DaemonRunOptions {
    fn shadow_default() -> Self {
        Self {
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DaemonProbeStatus {
    Running(DaemonStatus),
    Locked { status: Option<DaemonStatus> },
    NotRunning,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DaemonStatus {
    pid: u32,
    host: String,
    port: u16,
    address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_os: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_arch: Option<String>,
    started_at: String,
    data_dir: PathBuf,
    lock_file: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cluster: Option<DaemonClusterStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonPaths {
    root: PathBuf,
    lock: PathBuf,
    status: PathBuf,
}

impl DaemonPaths {
    fn new(data_dir: &SinomoDataDir) -> Self {
        Self::with_root(data_dir.root().join("daemon"))
    }

    fn for_shadow(data_dir: &SinomoDataDir) -> Self {
        Self::with_root(data_dir.root().join("shadow-daemon"))
    }

    fn with_root(root: PathBuf) -> Self {
        Self {
            lock: root.join("daemon.lock"),
            status: root.join("status.json"),
            root,
        }
    }

    fn ensure(&self) -> Result<(), CliError> {
        fs::create_dir_all(&self.root).map_err(|source| CliError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }
}

#[derive(Debug)]
struct DaemonLock {
    file: File,
    paths: DaemonPaths,
}

impl DaemonLock {
    fn acquire(
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        registration: Option<&DaemonClusterRegistration>,
    ) -> Result<Self, CliError> {
        paths.ensure()?;
        let cluster = registration.map(register_daemon_cluster_node).transpose()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&paths.lock)
            .map_err(|source| CliError::Io {
                path: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;
        file.try_lock_exclusive()
            .map_err(|source| CliError::DaemonAlreadyRunning {
                lock_file: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;

        let status = DaemonStatus::new(options, paths, cluster);
        let status_json = serde_json::to_vec_pretty(&status).map_err(|source| CliError::Json {
            path: paths.status.display().to_string(),
            detail: source.to_string(),
        })?;
        fs::write(&paths.status, status_json).map_err(|source| CliError::Io {
            path: paths.status.display().to_string(),
            detail: source.to_string(),
        })?;
        file.set_len(0).map_err(|source| CliError::Io {
            path: paths.lock.display().to_string(),
            detail: source.to_string(),
        })?;
        serde_json::to_writer_pretty(&file, &status).map_err(|source| CliError::Json {
            path: paths.lock.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(Self {
            file,
            paths: paths.clone(),
        })
    }
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.paths.status);
        let _ = self.file.unlock();
        let _ = fs::remove_file(&self.paths.lock);
    }
}

impl DaemonStatus {
    fn new(
        options: &DaemonRunOptions,
        paths: &DaemonPaths,
        cluster: Option<DaemonClusterStatus>,
    ) -> Self {
        Self {
            pid: std::process::id(),
            host: options.host.clone(),
            port: options.port,
            address: daemon_connect_address(&options.host, options.port),
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            started_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
            data_dir: paths.root.parent().unwrap_or(&paths.root).to_path_buf(),
            lock_file: paths.lock.clone(),
            cluster,
        }
    }
}

fn daemon_connect_address(host: &str, port: u16) -> String {
    let connect_host = match host {
        "0.0.0.0" | "::" => SINOMO_DAEMON_DEFAULT_HOST,
        value => value,
    };
    format!("{connect_host}:{port}")
}

fn daemon_bind_address(status: &DaemonStatus) -> String {
    format!("{}:{}", status.host, status.port)
}

fn print_daemon_platform(status: &DaemonStatus) {
    if let (Some(target_os), Some(target_arch)) = (&status.target_os, &status.target_arch) {
        println!("  platform: {target_os}/{target_arch}");
    }
}

fn parse_daemon_command(args: &[String]) -> Result<DaemonCliCommand, CliError> {
    match args.split_first() {
        None => Ok(DaemonCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(DaemonCliCommand::Help)
        }
        Some((command, rest)) if command == "shadow" => parse_shadow_daemon_command(rest),
        Some((command, rest)) if command == "run" => {
            Ok(DaemonCliCommand::Run(parse_daemon_run_options(rest)?))
        }
        Some((command, rest)) if command == "start" => {
            Ok(DaemonCliCommand::Start(parse_daemon_run_options(rest)?))
        }
        Some((command, rest)) if command == "status" => {
            Ok(DaemonCliCommand::Status(parse_daemon_run_options(rest)?))
        }
        Some((command, rest)) if command == "stop" => {
            Ok(DaemonCliCommand::Stop(parse_daemon_run_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_shadow_daemon_command(args: &[String]) -> Result<DaemonCliCommand, CliError> {
    match args.split_first() {
        None => Ok(DaemonCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(DaemonCliCommand::Help)
        }
        Some((command, rest)) if command == "run" => Ok(DaemonCliCommand::ShadowRun(
            parse_daemon_options_with_default(rest, DaemonRunOptions::shadow_default())?,
        )),
        Some((command, rest)) if command == "start" => Ok(DaemonCliCommand::ShadowStart(
            parse_daemon_options_with_default(rest, DaemonRunOptions::shadow_default())?,
        )),
        Some((command, rest)) if command == "status" => Ok(DaemonCliCommand::ShadowStatus(
            parse_daemon_options_with_default(rest, DaemonRunOptions::shadow_default())?,
        )),
        Some((command, rest)) if command == "stop" => Ok(DaemonCliCommand::ShadowStop(
            parse_daemon_options_with_default(rest, DaemonRunOptions::shadow_default())?,
        )),
        _ => Err(CliError::Usage),
    }
}

fn parse_daemon_run_options(args: &[String]) -> Result<DaemonRunOptions, CliError> {
    parse_daemon_options_with_default(args, DaemonRunOptions::default())
}

fn parse_daemon_options_with_default(
    args: &[String],
    mut options: DaemonRunOptions,
) -> Result<DaemonRunOptions, CliError> {
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--host" => {
                index += 1;
                let Some(host) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.host = host.clone();
            }
            "--port" => {
                index += 1;
                let Some(port) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.port = port.parse::<u16>().map_err(|source| CliError::Parse {
                    detail: format!("invalid daemon port `{port}`: {source}"),
                })?;
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn run_daemon_cli_command(command: DaemonCliCommand) -> Result<(), CliError> {
    match command {
        DaemonCliCommand::Help => {
            print_daemon_help();
            Ok(())
        }
        DaemonCliCommand::Run(options) => run_daemon(options),
        DaemonCliCommand::Start(options) => start_daemon(options),
        DaemonCliCommand::Status(options) => print_daemon_status(options),
        DaemonCliCommand::Stop(options) => stop_daemon(options),
        DaemonCliCommand::ShadowRun(options) => run_shadow_daemon(options),
        DaemonCliCommand::ShadowStart(options) => start_shadow_daemon(options),
        DaemonCliCommand::ShadowStatus(options) => print_shadow_daemon_status(options),
        DaemonCliCommand::ShadowStop(options) => stop_shadow_daemon(options),
    }
}

fn daemon_paths_from_defaults() -> Result<DaemonPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(DaemonPaths::new(&data_dir))
}

fn default_main_daemon_status() -> Result<Option<DaemonStatus>, CliError> {
    let paths = daemon_paths_from_defaults()?;
    Ok(match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { .. } | DaemonProbeStatus::NotRunning => None,
    })
}

fn shadow_daemon_paths_from_defaults() -> Result<DaemonPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(DaemonPaths::for_shadow(&data_dir))
}

fn run_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    let registration = daemon_cluster_registration(
        DaemonClusterProcess::Main,
        options.clone(),
        DaemonRunOptions::shadow_default(),
    );
    run_daemon_with_paths(
        options,
        daemon_paths_from_defaults()?,
        "daemon",
        registration,
    )
}

fn run_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    sleep_for_shadow_start_delay()?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let shadow_paths = DaemonPaths::for_shadow(&data_dir);
    let main_paths = DaemonPaths::new(&data_dir);
    let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
    let main_options = shadow_main_options_from_env()?;
    let main_binary = shadow_main_binary_from_env(&rollout_paths);
    run_shadow_daemon_with_paths(
        options,
        shadow_paths,
        main_paths,
        rollout_paths,
        main_options,
        main_binary,
    )
}

fn sleep_for_shadow_start_delay() -> Result<(), CliError> {
    let Ok(delay) = env::var(SHADOW_START_DELAY_MS_ENV) else {
        return Ok(());
    };
    let delay = delay.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid {SHADOW_START_DELAY_MS_ENV} `{delay}`: {source}"),
    })?;
    if delay > 0 {
        thread::sleep(Duration::from_millis(delay));
    }
    Ok(())
}

fn run_shadow_daemon_with_paths(
    options: DaemonRunOptions,
    shadow_paths: DaemonPaths,
    main_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    mut main_options: DaemonRunOptions,
    main_binary: PathBuf,
) -> Result<(), CliError> {
    let registration = daemon_cluster_registration(
        DaemonClusterProcess::Shadow,
        main_options.clone(),
        options.clone(),
    );
    let _lock = DaemonLock::acquire(&shadow_paths, &options, Some(&registration))?;
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = TcpListener::bind(&bind_address).map_err(|source| CliError::Network {
        address: bind_address.clone(),
        detail: source.to_string(),
    })?;
    listener
        .set_nonblocking(true)
        .map_err(|source| CliError::Network {
            address: bind_address.clone(),
            detail: source.to_string(),
        })?;
    println!("sinomo shadow daemon listening on {bind_address}");

    let mut main_binary = main_binary;
    let mut supervised_main: Option<Child> = None;
    let mut main_restart_suppressed = false;
    let mut pending_rollout: Option<ShadowMainRolloutMonitor> = None;
    loop {
        match listener.accept() {
            Ok((mut stream, _address)) => {
                if handle_daemon_connection(&mut stream, &shadow_paths)? {
                    break;
                }
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => {
                return Err(CliError::Network {
                    address: bind_address.clone(),
                    detail: source.to_string(),
                });
            }
        }

        if let Some(pending) = pending_rollout.as_ref() {
            if matches!(
                probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::Running(_)
            ) {
                stage_pending_shadow_rollout(&rollout_paths, pending, &options.host, options.port)?;
                let main_address =
                    daemon_connect_address(&pending.main_options.host, pending.main_options.port);
                let promoted_main_options = pending.main_options.clone();
                thread::spawn(move || {
                    let _ = send_daemon_command(
                        &main_address,
                        "FINALIZE_SHADOW_ROLLOUT",
                        DAEMON_ROLLOUT_TIMEOUT,
                    );
                });
                let _ = fs::remove_file(rollout_main_activation_path(&rollout_paths));
                pending_rollout = None;
                main_options = promoted_main_options;
            } else if Instant::now() >= pending.deadline {
                if let Some(child) = supervised_main.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                record_main_rollout_failure(
                    &rollout_paths,
                    &pending.activation,
                    &pending.previous_main_binary,
                    &pending.previous_shadow_binary,
                    "candidate main did not become healthy before rollout timeout",
                )?;
                main_binary = pending.previous_main_binary.clone();
                if let Some(backup) = pending.previous_main_backup.as_ref() {
                    let _ = restore_rollout_binary_from_backup(backup, &main_binary);
                }
                main_options = pending.main_options.clone();
                supervised_main = spawn_main_for_shadow(&main_binary, &main_options).ok();
                pending_rollout = None;
                let _ = fs::remove_file(rollout_main_activation_path(&rollout_paths));
            }
        }

        if !main_restart_suppressed
            && supervised_main.is_none()
            && matches!(
                probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            )
        {
            if let Some(activation) = read_rollout_main_activation(&rollout_paths)? {
                let previous_main_binary = activation
                    .previous_main_binary
                    .clone()
                    .unwrap_or_else(|| main_binary.clone());
                let activation_main_options = DaemonRunOptions {
                    host: activation.main_host.clone(),
                    port: activation.main_port,
                };
                let previous_main_backup = activation
                    .staged_main_binary
                    .as_ref()
                    .map(|staged| activate_staged_rollout_binary(staged, &activation.main_binary))
                    .transpose()?
                    .flatten();
                match spawn_main_for_shadow(&activation.main_binary, &activation_main_options) {
                    Ok(child) => {
                        main_binary = activation.main_binary.clone();
                        supervised_main = Some(child);
                        pending_rollout = Some(ShadowMainRolloutMonitor {
                            activation,
                            main_options: activation_main_options,
                            previous_main_binary,
                            previous_main_backup,
                            previous_shadow_binary: env::current_exe().unwrap_or_default(),
                            deadline: Instant::now() + Duration::from_secs(10),
                        });
                    }
                    Err(error) => {
                        record_activation_failure(
                            &rollout_paths,
                            &activation,
                            &previous_main_binary,
                            &format!("candidate main could not execute: {error}"),
                        )?;
                        main_binary = previous_main_binary;
                        if let Some(backup) = previous_main_backup {
                            let _ = restore_rollout_binary_from_backup(&backup, &main_binary);
                        }
                        main_options = activation_main_options;
                        supervised_main = spawn_main_for_shadow(&main_binary, &main_options).ok();
                        let _ = fs::remove_file(rollout_main_activation_path(&rollout_paths));
                    }
                }
            } else if main_binary.exists() {
                supervised_main = Some(spawn_main_for_shadow(&main_binary, &main_options)?);
            }
        }

        if let Some(child) = supervised_main.as_mut() {
            if let Some(status) = child.try_wait().map_err(|source| CliError::Io {
                path: main_binary.display().to_string(),
                detail: source.to_string(),
            })? {
                if let Some(pending) = pending_rollout.take() {
                    let observation = main_exit_observation_from_status(status);
                    record_main_rollout_failure(
                        &rollout_paths,
                        &pending.activation,
                        &pending.previous_main_binary,
                        &pending.previous_shadow_binary,
                        &format!(
                            "candidate main exited before health check: exit_code={:?} signal={:?}",
                            observation.exit_code, observation.signal
                        ),
                    )?;
                    main_binary = pending.previous_main_binary;
                    if let Some(backup) = pending.previous_main_backup {
                        let _ = restore_rollout_binary_from_backup(&backup, &main_binary);
                    }
                    main_options = pending.main_options;
                    supervised_main = spawn_main_for_shadow(&main_binary, &main_options).ok();
                    let _ = fs::remove_file(rollout_main_activation_path(&rollout_paths));
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
                if read_rollout_main_activation(&rollout_paths)?.is_some() {
                    supervised_main = None;
                    main_restart_suppressed = false;
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
                let observation = main_exit_observation_from_status(status);
                let kind = classify_main_exit(observation);
                write_json(
                    &rollout_paths.root.join("last-main-exit.json"),
                    &serde_json::json!({
                        "exit_code": observation.exit_code,
                        "signal": observation.signal,
                        "requested_shutdown": observation.requested_shutdown,
                        "kind": kind,
                        "observed_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_else(|_| "unknown".to_string())
                    }),
                )?;
                supervised_main = None;
                if kind == MainExitKind::Crash {
                    supervised_main = Some(spawn_main_for_shadow(&main_binary, &main_options)?);
                } else {
                    main_restart_suppressed = true;
                }
            }
        }

        thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}

fn spawn_main_for_shadow(binary: &Path, options: &DaemonRunOptions) -> Result<Child, CliError> {
    Command::new(binary)
        .arg("daemon")
        .arg("run")
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(options.port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })
}

#[derive(Clone, Debug)]
struct ShadowMainRolloutMonitor {
    activation: RolloutMainActivation,
    main_options: DaemonRunOptions,
    previous_main_binary: PathBuf,
    previous_main_backup: Option<PathBuf>,
    previous_shadow_binary: PathBuf,
    deadline: Instant,
}

impl PendingShadowRolloutContext for ShadowMainRolloutMonitor {
    fn activation(&self) -> &RolloutMainActivation {
        &self.activation
    }

    fn main_binary(&self) -> &Path {
        &self.activation.main_binary
    }

    fn main_host(&self) -> &str {
        &self.main_options.host
    }

    fn main_port(&self) -> u16 {
        self.main_options.port
    }

    fn previous_shadow_binary(&self) -> &Path {
        &self.previous_shadow_binary
    }
}

fn main_exit_observation_from_status(status: ExitStatus) -> MainExitObservation {
    #[cfg(unix)]
    {
        if let Some(signal) = status.signal() {
            return MainExitObservation::signal(signal);
        }
    }
    MainExitObservation::exit_code(status.code().unwrap_or(1))
}

fn shadow_main_options_from_env() -> Result<DaemonRunOptions, CliError> {
    let mut options = DaemonRunOptions::default();
    if let Ok(host) = env::var(SHADOW_MAIN_HOST_ENV) {
        options.host = host;
    }
    if let Ok(port) = env::var(SHADOW_MAIN_PORT_ENV) {
        options.port = port.parse::<u16>().map_err(|source| CliError::Parse {
            detail: format!("invalid {SHADOW_MAIN_PORT_ENV} `{port}`: {source}"),
        })?;
    }
    Ok(options)
}

fn shadow_main_binary_from_env(rollout_paths: &SelfRolloutPaths) -> PathBuf {
    env::var(SHADOW_MAIN_BINARY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| rollout_paths.main_binary.clone())
}

fn run_daemon_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
    registration: DaemonClusterRegistration,
) -> Result<(), CliError> {
    let _lock = DaemonLock::acquire(&paths, &options, Some(&registration))?;
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = TcpListener::bind(&bind_address).map_err(|source| CliError::Network {
        address: bind_address.clone(),
        detail: source.to_string(),
    })?;
    listener
        .set_nonblocking(true)
        .map_err(|source| CliError::Network {
            address: bind_address.clone(),
            detail: source.to_string(),
        })?;
    println!("sinomo {label} listening on {bind_address}");

    let mut cluster_network = ClusterNetworkMaintenance::new();
    let mut next_cluster_tick = Instant::now();
    loop {
        match listener.accept() {
            Ok((mut stream, _address)) => {
                if handle_daemon_connection(&mut stream, &paths)? {
                    break;
                }
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => {
                return Err(CliError::Network {
                    address: bind_address.clone(),
                    detail: source.to_string(),
                });
            }
        }

        if Instant::now() >= next_cluster_tick {
            if let Err(error) = cluster_network.tick(&paths, &options) {
                eprintln!("sinomo {label} cluster network tick failed: {error}");
            }
            next_cluster_tick = Instant::now() + DAEMON_CLUSTER_TICK_INTERVAL;
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

fn start_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    start_daemon_with_paths(
        options,
        daemon_paths_from_defaults()?,
        &["daemon", "run"],
        "daemon",
    )
}

fn start_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    let main_binary = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    let extra_env = vec![
        (SHADOW_MAIN_BINARY_ENV, main_binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, options.host.clone()),
        (SHADOW_MAIN_PORT_ENV, SINOMO_DAEMON_DEFAULT_PORT.to_string()),
    ];
    start_daemon_with_paths_with_env(
        options,
        shadow_daemon_paths_from_defaults()?,
        &["daemon", "shadow", "run"],
        "shadow daemon",
        &extra_env,
    )
}

fn start_daemon_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    command_prefix: &[&str],
    label: &str,
) -> Result<(), CliError> {
    start_daemon_with_paths_with_env(options, paths, command_prefix, label, &[])
}

fn start_daemon_with_paths_with_env(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    command_prefix: &[&str],
    label: &str,
    extra_env: &[(&str, String)],
) -> Result<(), CliError> {
    if matches!(
        probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT),
        DaemonProbeStatus::Running(_) | DaemonProbeStatus::Locked { .. }
    ) {
        return Err(CliError::DaemonAlreadyRunning {
            lock_file: paths.lock.display().to_string(),
            detail: "daemon lock is already held".to_string(),
        });
    }

    let exe = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    let mut command = Command::new(exe);
    command.args(command_prefix);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(options.port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: format!("sinomo {label} start"),
            detail: source.to_string(),
        })?;

    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT)
        {
            println!(
                "sinomo {label} started: pid={} bind={} control={}",
                status.pid,
                daemon_bind_address(&status),
                status.address
            );
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }

    Err(CliError::Network {
        address: daemon_connect_address(&options.host, options.port),
        detail: format!("daemon process {} did not become ready", child.id()),
    })
}

fn print_daemon_status(options: DaemonRunOptions) -> Result<(), CliError> {
    print_daemon_status_with_paths(options, daemon_paths_from_defaults()?, "daemon")
}

fn print_shadow_daemon_status(options: DaemonRunOptions) -> Result<(), CliError> {
    print_daemon_status_with_paths(
        options,
        shadow_daemon_paths_from_defaults()?,
        "shadow daemon",
    )
}

fn print_daemon_status_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
) -> Result<(), CliError> {
    match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            println!("sinomo {label} is running");
            println!("  pid: {}", status.pid);
            println!("  bind: {}", daemon_bind_address(&status));
            println!("  control address: {}", status.address);
            print_daemon_platform(&status);
            if let Some(cluster) = &status.cluster {
                println!("  cluster: {}", cluster.cluster_id);
                println!("  node: {}", cluster.node_id);
                println!("  cluster process: {}", cluster.process);
                println!(
                    "  member: {} {}",
                    format_member_role(&cluster.member_role),
                    format_member_state(&cluster.member_state)
                );
            }
            println!("  lock: {}", status.lock_file.display());
            println!("  status: {}", paths.status.display());
            Ok(())
        }
        DaemonProbeStatus::Locked { status } => {
            println!("sinomo {label} lock is held but the listener did not answer");
            if let Some(status) = status {
                println!("  pid: {}", status.pid);
                println!("  bind: {}", daemon_bind_address(&status));
                println!("  control address: {}", status.address);
                print_daemon_platform(&status);
                if let Some(cluster) = &status.cluster {
                    println!("  cluster: {}", cluster.cluster_id);
                    println!("  node: {}", cluster.node_id);
                    println!("  cluster process: {}", cluster.process);
                    println!(
                        "  member: {} {}",
                        format_member_role(&cluster.member_role),
                        format_member_state(&cluster.member_state)
                    );
                }
            } else {
                println!("  default bind: {}:{}", options.host, options.port);
                println!(
                    "  default control address: {}",
                    daemon_connect_address(&options.host, options.port)
                );
            }
            println!("  lock: {}", paths.lock.display());
            Ok(())
        }
        DaemonProbeStatus::NotRunning => {
            println!("sinomo {label} is not running");
            println!("  default bind: {}:{}", options.host, options.port);
            println!(
                "  default control address: {}",
                daemon_connect_address(&options.host, options.port)
            );
            Ok(())
        }
    }
}

fn stop_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    stop_daemon_with_paths(daemon_paths_from_defaults()?, options, "daemon")
}

fn stop_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    stop_daemon_with_paths(
        shadow_daemon_paths_from_defaults()?,
        options,
        "shadow daemon",
    )
}

fn stop_daemon_with_paths(
    paths: DaemonPaths,
    _options: DaemonRunOptions,
    label: &str,
) -> Result<(), CliError> {
    match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            let response = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT)?;
            println!("{}", response.trim());
            Ok(())
        }
        DaemonProbeStatus::Locked { .. } => Err(CliError::DaemonNotRunning {
            detail: format!("{label} lock is held but the listener did not answer"),
        }),
        DaemonProbeStatus::NotRunning => Err(CliError::DaemonNotRunning {
            detail: "no daemon lock is held".to_string(),
        }),
    }
}

fn probe_daemon_status(paths: &DaemonPaths, timeout: Duration) -> DaemonProbeStatus {
    let status = read_daemon_status(paths).ok();
    if let Some(status) = &status {
        if send_daemon_command(&status.address, "STATUS", timeout)
            .ok()
            .and_then(|response| serde_json::from_str::<DaemonStatus>(&response).ok())
            .is_some_and(|remote_status| daemon_status_matches_probe(status, &remote_status))
        {
            return DaemonProbeStatus::Running(status.clone());
        }
    }

    if !paths.lock.exists() {
        let _ = fs::remove_file(&paths.status);
        return DaemonProbeStatus::NotRunning;
    }

    let Ok(file) = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&paths.lock)
    else {
        return DaemonProbeStatus::Locked { status };
    };

    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = file.unlock();
            let _ = fs::remove_file(&paths.lock);
            let _ = fs::remove_file(&paths.status);
            DaemonProbeStatus::NotRunning
        }
        Err(_) => DaemonProbeStatus::Locked { status },
    }
}

fn daemon_status_matches_probe(expected: &DaemonStatus, actual: &DaemonStatus) -> bool {
    expected.pid == actual.pid
        && expected.data_dir == actual.data_dir
        && expected.lock_file == actual.lock_file
        && expected.port == actual.port
}

fn read_daemon_status(paths: &DaemonPaths) -> Result<DaemonStatus, CliError> {
    let content = fs::read_to_string(&paths.status).map_err(|source| CliError::Io {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })
}

fn send_daemon_command(
    address: &str,
    command: &str,
    timeout: Duration,
) -> Result<String, CliError> {
    let signed = sign_daemon_command_for_local_node(command)?;
    let response = send_daemon_command_raw(address, &signed, timeout)?;
    if response.trim() == "unknown daemon command" {
        return send_daemon_command_raw(address, command, timeout);
    }
    Ok(response)
}

fn send_daemon_command_raw(
    address: &str,
    command: &str,
    timeout: Duration,
) -> Result<String, CliError> {
    let socket_address = address
        .to_socket_addrs()
        .map_err(|source| CliError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?
        .next()
        .ok_or_else(|| CliError::Network {
            address: address.to_string(),
            detail: "no socket addresses resolved".to_string(),
        })?;
    let mut stream = TcpStream::connect_timeout(&socket_address, timeout).map_err(|source| {
        CliError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        }
    })?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|source| CliError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|source| CliError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    stream
        .write_all(format!("{command}\n").as_bytes())
        .map_err(|source| CliError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    let _ = stream.shutdown(Shutdown::Write);
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|source| CliError::Network {
            address: address.to_string(),
            detail: source.to_string(),
        })?;
    Ok(response)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SignedDaemonCommand {
    protocol: String,
    node_id: String,
    public_key: String,
    timestamp: String,
    nonce: String,
    command: String,
    signature: String,
}

fn sign_daemon_command_for_local_node(command: &str) -> Result<String, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    sign_daemon_command_from_data_dir(&data_dir, command)
}

fn sign_daemon_command_from_data_dir(
    data_dir: &SinomoDataDir,
    command: &str,
) -> Result<String, CliError> {
    let identity = load_or_create_main_node_identity(&data_dir.root())?;
    let node_id = read_cluster_state(&ClusterPaths::new(&data_dir))
        .map(|state| state.local_node_id.to_string())
        .unwrap_or_else(|_| "node_local".to_string());
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let nonce = format!("{}-{}", std::process::id(), rollout_artifact_suffix());
    let key_pair = identity.key_pair()?;
    let signing_payload =
        daemon_auth_signing_payload(&node_id, &identity.public_key, &timestamp, &nonce, command);
    let signature = key_pair.sign(signing_payload.as_bytes());
    let envelope = SignedDaemonCommand {
        protocol: DAEMON_AUTH_PROTOCOL.to_string(),
        node_id,
        public_key: identity.public_key,
        timestamp,
        nonce,
        command: command.to_string(),
        signature: URL_SAFE_NO_PAD.encode(signature.as_ref()),
    };
    let payload = serde_json::to_string(&envelope).map_err(|source| CliError::Json {
        path: "daemon auth envelope".to_string(),
        detail: source.to_string(),
    })?;
    Ok(format!("AUTH {payload}"))
}

fn authenticate_daemon_command(paths: &DaemonPaths, raw: &str) -> Result<String, CliError> {
    let payload = raw
        .strip_prefix("AUTH ")
        .ok_or_else(|| CliError::AuthDenied)?;
    let envelope: SignedDaemonCommand =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "daemon auth envelope".to_string(),
            detail: source.to_string(),
        })?;
    verify_signed_daemon_command(paths, &envelope)?;
    Ok(envelope.command)
}

fn verify_signed_daemon_command(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
) -> Result<(), CliError> {
    if envelope.protocol != DAEMON_AUTH_PROTOCOL {
        return Err(CliError::AuthDenied);
    }
    let signed_at =
        OffsetDateTime::parse(&envelope.timestamp, &Rfc3339).map_err(|_| CliError::AuthDenied)?;
    let now = OffsetDateTime::now_utc();
    let skew = (now - signed_at).whole_seconds().abs();
    if skew > DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS {
        return Err(CliError::AuthExpired);
    }
    if envelope.nonce.trim().is_empty() || envelope.command.trim().is_empty() {
        return Err(CliError::AuthDenied);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(envelope.public_key.as_bytes())
        .map_err(|_| CliError::AuthDenied)?;
    let signature = URL_SAFE_NO_PAD
        .decode(envelope.signature.as_bytes())
        .map_err(|_| CliError::AuthDenied)?;
    let signing_payload = daemon_auth_signing_payload(
        &envelope.node_id,
        &envelope.public_key,
        &envelope.timestamp,
        &envelope.nonce,
        &envelope.command,
    );
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(signing_payload.as_bytes(), &signature)
        .map_err(|_| CliError::AuthDenied)?;
    authorize_daemon_signer(paths, envelope)
}

fn daemon_auth_signing_payload(
    node_id: &str,
    public_key: &str,
    timestamp: &str,
    nonce: &str,
    command: &str,
) -> String {
    format!("{DAEMON_AUTH_PROTOCOL}\n{node_id}\n{public_key}\n{timestamp}\n{nonce}\n{command}")
}

fn authorize_daemon_signer(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
) -> Result<(), CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    if envelope.public_key == main_identity.public_key
        || envelope.public_key == shadow_identity.public_key
    {
        return Ok(());
    }

    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    if cluster_paths.state.exists() {
        let state = read_cluster_state(&cluster_paths)?;
        if state.members.iter().any(|member| {
            member.node_public_key == envelope.public_key && member.state.is_authorized()
        }) {
            return Ok(());
        }
    }

    if envelope.command.starts_with("CLUSTER_JOIN ") {
        return authorize_cluster_join_signer(&envelope.command, &envelope.public_key);
    }
    Err(CliError::AuthDenied)
}

fn authorize_cluster_join_signer(command: &str, public_key: &str) -> Result<(), CliError> {
    let payload = command
        .strip_prefix("CLUSTER_JOIN ")
        .unwrap_or_default()
        .trim();
    let request: ClusterJoinRegistrationRequest =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "cluster join registration".to_string(),
            detail: source.to_string(),
        })?;
    if request
        .members
        .iter()
        .any(|member| member.node_public_key == public_key)
    {
        return Ok(());
    }
    Err(CliError::AuthDenied)
}

fn handle_daemon_connection(stream: &mut TcpStream, paths: &DaemonPaths) -> Result<bool, CliError> {
    let mut reader =
        io::BufReader::new(stream.try_clone().map_err(|source| CliError::Network {
            address: "daemon client".to_string(),
            detail: source.to_string(),
        })?);
    let mut request = String::new();
    reader
        .read_line(&mut request)
        .map_err(|source| CliError::Network {
            address: "daemon client".to_string(),
            detail: source.to_string(),
        })?;
    let command = request.lines().next().unwrap_or_default().trim();
    let command = match authenticate_daemon_command(paths, command) {
        Ok(command) => command,
        Err(error) => {
            stream
                .write_all(format!("auth failed: {error}\n").as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            return Ok(false);
        }
    };
    let command = command.as_str();
    match command {
        "STATUS" => {
            let status = read_daemon_status(paths)?;
            let body = serde_json::to_string_pretty(&status).map_err(|source| CliError::Json {
                path: paths.status.display().to_string(),
                detail: source.to_string(),
            })?;
            stream
                .write_all(body.as_bytes())
                .map_err(|source| CliError::Network {
                    address: status.address.clone(),
                    detail: source.to_string(),
                })?;
            Ok(false)
        }
        "SHUTDOWN" => {
            stream
                .write_all(b"sinomo daemon stopping\n")
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(true)
        }
        "FINALIZE_SHADOW_ROLLOUT" => {
            let response = handle_daemon_finalize_shadow_rollout(paths)?;
            stream
                .write_all(response.as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(false)
        }
        command if command.starts_with("CLUSTER_JOIN ") => {
            let payload = command
                .strip_prefix("CLUSTER_JOIN ")
                .unwrap_or_default()
                .trim();
            let response = handle_daemon_cluster_join(paths, payload)?;
            let body =
                serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                    path: "cluster join response".to_string(),
                    detail: source.to_string(),
                })?;
            stream
                .write_all(body.as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(false)
        }
        "CLUSTER_KV_SNAPSHOT" => {
            let snapshot = handle_daemon_cluster_kv_snapshot(paths)?;
            let body =
                serde_json::to_string_pretty(&snapshot).map_err(|source| CliError::Json {
                    path: "cluster kv snapshot response".to_string(),
                    detail: source.to_string(),
                })?;
            stream
                .write_all(body.as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(false)
        }
        command if command.starts_with("CLUSTER_KV_TXN ") => {
            let payload = command
                .strip_prefix("CLUSTER_KV_TXN ")
                .unwrap_or_default()
                .trim();
            let response = handle_daemon_cluster_kv_txn(paths, payload)?;
            let body =
                serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                    path: "cluster kv txn response".to_string(),
                    detail: source.to_string(),
                })?;
            stream
                .write_all(body.as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(false)
        }
        command if command.starts_with("CLUSTER_KV_RANGE_PREFIX ") => {
            let prefix = command
                .strip_prefix("CLUSTER_KV_RANGE_PREFIX ")
                .unwrap_or_default()
                .trim();
            let response = handle_daemon_cluster_kv_range_prefix(paths, prefix)?;
            let body =
                serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                    path: "cluster kv range response".to_string(),
                    detail: source.to_string(),
                })?;
            stream
                .write_all(body.as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(false)
        }
        command if command.starts_with("CLUSTER_SYNC ") => {
            let payload = command
                .strip_prefix("CLUSTER_SYNC ")
                .unwrap_or_default()
                .trim();
            let response = handle_daemon_cluster_sync(paths, payload)?;
            let body =
                serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                    path: "cluster sync response".to_string(),
                    detail: source.to_string(),
                })?;
            stream
                .write_all(body.as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(false)
        }
        command if command.starts_with("ROLLOUT_BINARY ") => {
            let payload = command
                .strip_prefix("ROLLOUT_BINARY ")
                .unwrap_or_default()
                .trim();
            let response = handle_daemon_rollout_binary(paths, payload)?;
            let body =
                serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                    path: "rollout response".to_string(),
                    detail: source.to_string(),
                })?;
            stream
                .write_all(body.as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(true)
        }
        command if command.starts_with("ROLLOUT_BUILD ") => {
            let payload = command
                .strip_prefix("ROLLOUT_BUILD ")
                .unwrap_or_default()
                .trim();
            let response = handle_daemon_rollout_build(paths, payload)?;
            let body =
                serde_json::to_string_pretty(&response).map_err(|source| CliError::Json {
                    path: "rollout response".to_string(),
                    detail: source.to_string(),
                })?;
            stream
                .write_all(body.as_bytes())
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(response.status == "accepted")
        }
        _ => {
            stream
                .write_all(b"unknown daemon command\n")
                .map_err(|source| CliError::Network {
                    address: "daemon client".to_string(),
                    detail: source.to_string(),
                })?;
            Ok(false)
        }
    }
}

fn handle_daemon_cluster_kv_snapshot(paths: &DaemonPaths) -> Result<KvSnapshot, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    read_cluster_kv_store(&cluster_paths).map(|store| store.snapshot())
}

fn handle_daemon_cluster_kv_txn(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<KvTxnResponse, CliError> {
    let txn: KvTxn = serde_json::from_str(payload).map_err(|source| CliError::Json {
        path: "cluster kv txn".to_string(),
        detail: source.to_string(),
    })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut store = read_cluster_kv_store(&cluster_paths)?;
    let response = store.txn(txn)?;
    write_cluster_kv_snapshot(&cluster_paths, &store.snapshot())?;
    Ok(response)
}

fn handle_daemon_cluster_kv_range_prefix(
    paths: &DaemonPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let store = read_cluster_kv_store(&cluster_paths)?;
    Ok(store.range_prefix(prefix))
}

fn handle_daemon_cluster_join(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    let request: ClusterJoinRegistrationRequest =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "cluster join registration".to_string(),
            detail: source.to_string(),
        })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut state = read_cluster_state(&cluster_paths)?;
    if let Err(error) = verify_cluster_join_invite_signature(&request.invite) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
        });
    }
    if !state.members.iter().any(|member| {
        member.node_id == request.invite.issuer_node_id
            && member.node_public_key == request.invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite issuer {} is not an authorized cluster member",
                request.invite.issuer_node_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if let Ok(main_status) = read_daemon_status(paths) {
        let main_options = daemon_options_from_status(&main_status);
        let shadow_options = match probe_daemon_status(
            &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
            DAEMON_PROBE_TIMEOUT,
        ) {
            DaemonProbeStatus::Running(status)
            | DaemonProbeStatus::Locked {
                status: Some(status),
            } => daemon_options_from_status(&status),
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                DaemonRunOptions::shadow_default()
            }
        };
        ensure_daemon_cluster_members(
            &mut state,
            &main_options,
            &shadow_options,
            OffsetDateTime::now_utc(),
        );
    }
    if request.invite.cluster_id != state.cluster_id {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite cluster `{}` does not match introducer cluster `{}`",
                request.invite.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if request.invite.expires_at != OffsetDateTime::UNIX_EPOCH
        && request.invite.expires_at <= OffsetDateTime::now_utc()
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite expired at {}",
                format_rfc3339(request.invite.expires_at)
            ),
            state,
            kv_snapshot: None,
        });
    }
    let timestamp = OffsetDateTime::now_utc();
    for mut member in request.members {
        member.cluster_id = state.cluster_id.clone();
        if matches!(member.state, ClusterMemberState::Pending) {
            member.state = match member.role {
                ClusterMemberRole::Learner => ClusterMemberState::Learner,
                ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
                ClusterMemberRole::Voter => ClusterMemberState::Voter,
            };
        }
        member.updated_at = timestamp;
        upsert_member_record(&mut state, member);
    }
    state.kv_revision = state.kv_revision.saturating_add(1);
    state.updated_at = timestamp;
    write_json(&cluster_paths.state, &state)?;
    let snapshot = sync_cluster_state_to_kv(&cluster_paths, &state)?;
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state,
        kv_snapshot: Some(snapshot),
    })
}

fn upsert_member_record(state: &mut ClusterBootstrapState, member: ClusterMemberRecord) {
    if let Some(existing) = state
        .members
        .iter_mut()
        .find(|existing| existing.node_id == member.node_id)
    {
        *existing = member;
    } else {
        state.members.push(member);
    }
}

fn handle_daemon_rollout_binary(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request: ClusterRolloutBinaryRequest =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "cluster rollout request".to_string(),
            detail: source.to_string(),
        })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    if (!request.target_os.is_empty() && request.target_os != env::consts::OS)
        || (!request.target_arch.is_empty() && request.target_arch != env::consts::ARCH)
    {
        return Ok(ClusterRolloutBinaryResponse {
            status: "rejected".to_string(),
            detail: format!(
                "artifact platform {}/{} does not match target platform {}/{}",
                request.target_os,
                request.target_arch,
                env::consts::OS,
                env::consts::ARCH
            ),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
        });
    }
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let binary = URL_SAFE_NO_PAD
        .decode(request.binary_base64.as_bytes())
        .map_err(|source| CliError::Parse {
            detail: format!("invalid rollout binary encoding: {source}"),
        })?;
    let main_binary = rollout_paths.main_binary.clone();
    let shadow_binary = rollout_paths.shadow_binary.clone();
    let staged_main_binary = rollout_staged_main_binary(&rollout_paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&rollout_paths);
    stage_rollout_bytes(&binary, &staged_main_binary)?;
    stage_rollout_bytes(&binary, &staged_shadow_binary)?;

    let current = read_daemon_status(paths)?;
    let previous_main_binary = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    stage_rollout_main_activation(
        data_dir,
        &rollout_paths,
        RolloutMainActivation {
            source: request.source,
            main_binary: main_binary.clone(),
            shadow_binary: shadow_binary.clone(),
            staged_main_binary: Some(staged_main_binary),
            staged_shadow_binary: Some(staged_shadow_binary),
            previous_main_binary: Some(previous_main_binary),
            main_host: current.host.clone(),
            main_port: current.port,
            shadow_port: request.shadow_port,
            requested_at: request.requested_at,
            mode: "cluster-binary-push".to_string(),
        },
    )?;
    Ok(ClusterRolloutBinaryResponse {
        status: "accepted".to_string(),
        detail: format!(
            "candidate main staged; existing shadow daemon will restart main, verify health, then update shadow (current main pid {} will stop)",
            current.pid
        ),
        main_binary,
        shadow_binary,
    })
}

fn stage_rollout_main_activation(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    activation: RolloutMainActivation,
) -> Result<(), CliError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    if !matches!(
        probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT),
        DaemonProbeStatus::Running(_)
    ) {
        return Err(CliError::DaemonNotRunning {
            detail: "safe rollout requires the existing shadow daemon to be running".to_string(),
        });
    }
    write_main_activation(rollout_paths, &activation)?;
    record_main_activation_staged(rollout_paths, &activation)?;
    Ok(())
}

fn handle_daemon_finalize_shadow_rollout(paths: &DaemonPaths) -> Result<String, CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let Some(finalize) = read_pending_shadow_rollout(&rollout_paths)? else {
        return Ok("no pending shadow rollout\n".to_string());
    };

    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    if let DaemonProbeStatus::Running(status) =
        probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT)
    {
        let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT);
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if matches!(
                probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            ) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    let mut detail = "shadow rollout finalized".to_string();
    let mut status = "accepted";
    let previous_shadow_backup = finalize
        .staged_shadow_binary
        .as_ref()
        .map(|staged| activate_staged_rollout_binary(staged, &finalize.shadow_binary))
        .transpose()?
        .flatten();
    let candidate_result = spawn_shadow_for_cluster_rollout(
        &finalize.shadow_binary,
        &finalize.main_binary,
        &finalize.main_host,
        finalize.main_port,
        &finalize.shadow_host,
        finalize.shadow_port,
    )
    .and_then(|()| {
        wait_for_shadow_rollout_health(
            data_dir,
            &finalize.shadow_host,
            finalize.shadow_port,
            Duration::from_secs(30),
        )
    });
    if let Err(error) = candidate_result {
        if wait_for_shadow_rollout_health(
            data_dir,
            &finalize.shadow_host,
            finalize.shadow_port,
            Duration::from_secs(15),
        )
        .is_ok()
        {
            detail = format!("shadow rollout finalized after handoff grace period: {error}");
        } else {
            status = "recovered";
            detail = format!("candidate shadow failed: {error}; restarting previous shadow");
            let recovery_shadow_binary = if let Some(backup) = previous_shadow_backup.as_ref() {
                let _ = restore_rollout_binary_from_backup(backup, &finalize.shadow_binary);
                &finalize.shadow_binary
            } else {
                &finalize.previous_shadow_binary
            };
            let _ = spawn_shadow_for_cluster_rollout(
                recovery_shadow_binary,
                &finalize.main_binary,
                &finalize.main_host,
                finalize.main_port,
                &finalize.shadow_host,
                finalize.shadow_port,
            );
        }
    }

    record_shadow_rollout_finalized(&rollout_paths, &finalize, status, &detail)?;
    remove_shadow_finalize(&rollout_paths);
    Ok(format!("{status}: {detail}\n"))
}

fn handle_daemon_rollout_build(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request: ClusterRolloutBuildRequest =
        serde_json::from_str(payload).map_err(|source| CliError::Json {
            path: "cluster rollout build request".to_string(),
            detail: source.to_string(),
        })?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let repo = rollout_remote_repo_path(&rollout_paths, request.remote_repo.as_deref());
    if !repo.join("Cargo.toml").exists() {
        return Ok(ClusterRolloutBinaryResponse {
            status: "rejected".to_string(),
            detail: format!(
                "remote source repo is not initialized at {}; run `sinomo rollout init-source --repo <path>` on this node or pass --remote-repo <path>",
                repo.display()
            ),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
        });
    }

    let built_binary = match rollout_build_dev_binary(&repo, &rollout_paths.build_log) {
        Ok(binary) => binary,
        Err(error) => {
            return Ok(ClusterRolloutBinaryResponse {
                status: "rejected".to_string(),
                detail: format!("remote build failed: {error}"),
                main_binary: PathBuf::new(),
                shadow_binary: PathBuf::new(),
            });
        }
    };
    let main_binary = rollout_paths.main_binary.clone();
    let shadow_binary = rollout_paths.shadow_binary.clone();
    let staged_main_binary = rollout_staged_main_binary(&rollout_paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&rollout_paths);
    stage_rollout_file(&built_binary, &staged_main_binary)?;
    stage_rollout_file(&built_binary, &staged_shadow_binary)?;

    let current = read_daemon_status(paths)?;
    let previous_main_binary = env::current_exe().map_err(|source| CliError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    let source = request.source.clone();
    let requested_at = request.requested_at.clone();
    stage_rollout_main_activation(
        data_dir,
        &rollout_paths,
        RolloutMainActivation {
            source: source.clone(),
            main_binary: main_binary.clone(),
            shadow_binary: shadow_binary.clone(),
            staged_main_binary: Some(staged_main_binary),
            staged_shadow_binary: Some(staged_shadow_binary),
            previous_main_binary: Some(previous_main_binary),
            main_host: current.host.clone(),
            main_port: current.port,
            shadow_port: request.shadow_port,
            requested_at: requested_at.clone(),
            mode: "cluster-remote-build".to_string(),
        },
    )?;
    write_json(
        &rollout_paths.state,
        &serde_json::json!({
            "source": source,
            "source_repo": repo,
            "main_binary": main_binary,
            "shadow_binary": shadow_binary,
            "built_binary": built_binary,
            "build_log": rollout_paths.build_log,
            "requested_at": requested_at,
            "staged_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_else(|_| "unknown".to_string()),
            "status": "main-staged",
            "mode": "cluster-remote-build"
        }),
    )?;
    Ok(ClusterRolloutBinaryResponse {
        status: "accepted".to_string(),
        detail: format!(
            "remote build staged; existing shadow daemon will restart main, verify health, then update shadow (current main pid {} will stop)",
            current.pid
        ),
        main_binary,
        shadow_binary,
    })
}

fn rollout_artifact_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{nanos}", std::process::id())
}

fn spawn_shadow_for_cluster_rollout(
    shadow_binary: &Path,
    main_binary: &Path,
    main_host: &str,
    main_port: u16,
    shadow_host: &str,
    shadow_port: u16,
) -> Result<(), CliError> {
    Command::new(shadow_binary)
        .args(["daemon", "shadow", "run"])
        .arg("--host")
        .arg(shadow_host)
        .arg("--port")
        .arg(shadow_port.to_string())
        .env(SHADOW_MAIN_BINARY_ENV, main_binary)
        .env(SHADOW_MAIN_HOST_ENV, main_host)
        .env(SHADOW_MAIN_PORT_ENV, main_port.to_string())
        .env(SHADOW_START_DELAY_MS_ENV, "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: shadow_binary.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(())
}

fn wait_for_shadow_rollout_health(
    data_dir: &Path,
    shadow_host: &str,
    shadow_port: u16,
    timeout: Duration,
) -> Result<(), CliError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if matches!(
            probe_daemon_status(&shadow_paths, Duration::from_secs(2)),
            DaemonProbeStatus::Running(_)
        ) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Network {
        address: daemon_connect_address(shadow_host, shadow_port),
        detail: "rolled out shadow daemon did not become ready".to_string(),
    })
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum RolloutCliCommand {
    Help,
    InitSource(RolloutOptions),
    ImportSource(RolloutOptions),
    ExportSource(RolloutOptions),
    ApplyExternal(RolloutOptions),
    Apply(RolloutOptions),
    Cluster(RolloutOptions),
    Status,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct RolloutOptions {
    repo: Option<PathBuf>,
    binary: Option<PathBuf>,
    remote_repo: Option<PathBuf>,
    targets: Vec<String>,
    host: String,
    port: u16,
    shadow_port: u16,
    skip_local: bool,
    remote_build: bool,
}

impl Default for RolloutOptions {
    fn default() -> Self {
        Self {
            repo: None,
            binary: None,
            remote_repo: None,
            targets: Vec::new(),
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
            shadow_port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
            skip_local: false,
            remote_build: false,
        }
    }
}

fn parse_rollout_command(args: &[String]) -> Result<RolloutCliCommand, CliError> {
    match args.split_first() {
        None => Ok(RolloutCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(RolloutCliCommand::Help)
        }
        Some((command, rest)) if command == "init-source" => {
            Ok(RolloutCliCommand::InitSource(parse_rollout_options(rest)?))
        }
        Some((command, rest))
            if matches!(command.as_str(), "import-source" | "sync-from-external") =>
        {
            Ok(RolloutCliCommand::ImportSource(parse_rollout_options(
                rest,
            )?))
        }
        Some((command, rest))
            if matches!(command.as_str(), "export-source" | "sync-to-external") =>
        {
            Ok(RolloutCliCommand::ExportSource(parse_rollout_options(
                rest,
            )?))
        }
        Some((command, rest)) if matches!(command.as_str(), "apply-external" | "from-external") => {
            Ok(RolloutCliCommand::ApplyExternal(parse_rollout_options(
                rest,
            )?))
        }
        Some((command, rest)) if command == "apply" => {
            Ok(RolloutCliCommand::Apply(parse_rollout_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "cluster" | "apply-cluster") => {
            Ok(RolloutCliCommand::Cluster(parse_rollout_options(rest)?))
        }
        Some((command, [])) if command == "status" => Ok(RolloutCliCommand::Status),
        _ => Err(CliError::Usage),
    }
}

fn parse_rollout_options(args: &[String]) -> Result<RolloutOptions, CliError> {
    let mut options = RolloutOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--repo" => {
                index += 1;
                let Some(repo) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.repo = Some(PathBuf::from(repo));
            }
            "--binary" => {
                index += 1;
                let Some(binary) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.binary = Some(PathBuf::from(binary));
            }
            "--remote-repo" => {
                index += 1;
                let Some(repo) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.remote_repo = Some(PathBuf::from(repo));
            }
            "--target" => {
                index += 1;
                let Some(target) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.targets.push(non_empty_address(target)?);
            }
            "--host" => {
                index += 1;
                let Some(host) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.host = host.clone();
            }
            "--port" => {
                index += 1;
                let Some(port) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.port = port.parse::<u16>().map_err(|source| CliError::Parse {
                    detail: format!("invalid rollout daemon port `{port}`: {source}"),
                })?;
            }
            "--shadow-port" => {
                index += 1;
                let Some(port) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.shadow_port = port.parse::<u16>().map_err(|source| CliError::Parse {
                    detail: format!("invalid rollout shadow daemon port `{port}`: {source}"),
                })?;
            }
            "--skip-local" => options.skip_local = true,
            "--remote-build" => options.remote_build = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn run_rollout_cli_command(command: RolloutCliCommand) -> Result<(), CliError> {
    match command {
        RolloutCliCommand::Help => {
            print_rollout_help();
            Ok(())
        }
        RolloutCliCommand::InitSource(options) => rollout_init_source(options),
        RolloutCliCommand::ImportSource(options) => rollout_import_source(options),
        RolloutCliCommand::ExportSource(options) => rollout_export_source(options),
        RolloutCliCommand::ApplyExternal(options) => rollout_apply_external(options),
        RolloutCliCommand::Apply(options) => rollout_apply(options),
        RolloutCliCommand::Cluster(options) => rollout_cluster(options),
        RolloutCliCommand::Status => rollout_status(),
    }
}

fn rollout_paths_from_defaults() -> Result<SelfRolloutPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(SelfRolloutPaths::new(&data_dir.root()))
}

fn rollout_init_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_source_repo(&paths, options.repo.as_deref())?;
    println!("source repo: {}", paths.source_repo.display());
    Ok(())
}

fn rollout_import_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_source_repo(&paths, options.repo.as_deref())?;
    println!("imported external source into managed repo");
    println!("  managed source: {}", paths.source_repo.display());
    print_managed_source_status(&paths)?;
    Ok(())
}

fn rollout_export_source(options: RolloutOptions) -> Result<(), CliError> {
    let Some(repo) = options.repo.as_deref() else {
        return Err(CliError::Parse {
            detail: "export-source requires --repo <external-repo>".to_string(),
        });
    };
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    sync_external_repo_from_managed(&paths, repo)?;
    println!("exported managed source changes into external repo");
    println!("  managed source: {}", paths.source_repo.display());
    println!("  external repo: {}", repo.display());
    Ok(())
}

fn rollout_apply_external(options: RolloutOptions) -> Result<(), CliError> {
    rollout_import_source(options.clone())?;
    let mut apply_options = options;
    apply_options.repo = None;
    rollout_apply(apply_options)
}

fn rollout_status() -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    println!("rollout source: {}", paths.source_repo.display());
    println!("main binary: {}", paths.main_binary.display());
    println!("shadow binary: {}", paths.shadow_binary.display());
    println!("state: {}", paths.state.display());
    println!(
        "source repo exists: {}",
        paths.source_repo.join(".git").exists()
    );
    println!("main binary exists: {}", paths.main_binary.exists());
    println!("shadow binary exists: {}", paths.shadow_binary.exists());
    print_managed_source_status(&paths)?;
    Ok(())
}

fn print_managed_source_status(paths: &SelfRolloutPaths) -> Result<(), CliError> {
    let status = managed_source_status(paths)?;
    println!("source git repo: {}", status.is_git_repo);
    if let Some(head) = status.head {
        println!("source head: {head}");
    }
    println!("source external remotes: {}", status.has_external_remotes);
    if !status.remotes.is_empty() {
        println!("source remotes: {}", status.remotes.join(", "));
    }
    Ok(())
}

fn rollout_apply(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_source_repo(&paths, options.repo.as_deref())?;
    let plan = paths.plan();
    let build = Command::new(&plan.build_program)
        .args(&plan.build_args)
        .current_dir(&plan.build_dir)
        .output()
        .map_err(|source| CliError::Io {
            path: format!("{} {}", plan.build_program, plan.build_args.join(" ")),
            detail: source.to_string(),
        })?;
    let build_log = format!(
        "command: {} {}\nstatus: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        plan.build_program,
        plan.build_args.join(" "),
        build.status,
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr)
    );
    fs::write(&plan.build_log, build_log).map_err(|source| CliError::Io {
        path: plan.build_log.display().to_string(),
        detail: source.to_string(),
    })?;
    if !build.status.success() {
        return Err(CliError::Process {
            command: format!("{} {}", plan.build_program, plan.build_args.join(" ")),
            status: build.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&build.stderr).trim().to_string(),
        });
    }
    install_binary_atomically(&plan.built_binary, &plan.install_main_binary)?;
    stop_main_daemon_if_running(&options)?;
    install_binary_atomically(&plan.built_binary, &plan.install_shadow_binary)?;
    restart_shadow_daemon_from_binary(
        &plan.install_shadow_binary,
        &plan.install_main_binary,
        &options,
    )?;
    wait_for_main_daemon_health(&options)?;
    write_json(
        &plan.state,
        &serde_json::json!({
            "source_repo": paths.source_repo,
            "main_binary": plan.install_main_binary,
            "shadow_binary": plan.install_shadow_binary,
            "built_binary": plan.built_binary,
            "build_log": plan.build_log,
            "updated_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_else(|_| "unknown".to_string())
        }),
    )?;
    println!("rollout applied");
    println!("  source: {}", plan.build_dir.display());
    println!("  main binary: {}", plan.install_main_binary.display());
    println!("  shadow binary: {}", plan.install_shadow_binary.display());
    Ok(())
}

fn rollout_cluster(options: RolloutOptions) -> Result<(), CliError> {
    let targets = rollout_cluster_targets(&options)?;
    if targets.is_empty() {
        return Err(CliError::Parse {
            detail: "no rollout targets found; pass --target <host:port> or start a clustered local daemon".to_string(),
        });
    }
    let source = options
        .remote_repo
        .as_ref()
        .or(options.repo.as_ref())
        .or(options.binary.as_ref())
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "remote source repo".to_string());
    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let build_request = ClusterRolloutBuildRequest {
        source: source.clone(),
        remote_repo: options.remote_repo.clone(),
        host: options.host.clone(),
        port: options.port,
        shadow_port: options.shadow_port,
        requested_at: requested_at.clone(),
    };
    let mut binary_request: Option<ClusterRolloutBinaryRequest> = None;

    for target in targets {
        let target_status = match read_rollout_target_status(&target) {
            Ok(status) => status,
            Err(error) => {
                println!("{target}: failed: {error}");
                continue;
            }
        };
        let platform_match = rollout_target_platform_matches_local(&target_status);
        let use_remote_build = options.remote_build
            || (!is_loopback_connect_address(&target)
                && options.binary.is_none()
                && platform_match != Some(true));
        if use_remote_build {
            match send_daemon_rollout_build(&target, &build_request) {
                Ok(response) => {
                    println!("{target}: {}", response.status);
                    if !response.detail.is_empty() {
                        println!("  {}", response.detail);
                    }
                }
                Err(error) => {
                    println!("{target}: failed: {error}");
                }
            }
            continue;
        }

        if let Err(error) = ensure_rollout_target_platform_from_status(&target, &target_status) {
            println!("{target}: skipped: {error}");
            continue;
        }
        if binary_request.is_none() {
            let binary = rollout_prepare_dev_binary(&options)?;
            let binary_bytes = fs::read(&binary).map_err(|source| CliError::Io {
                path: binary.display().to_string(),
                detail: source.to_string(),
            })?;
            println!("cluster rollout binary: {}", binary.display());
            binary_request = Some(ClusterRolloutBinaryRequest {
                source: source.clone(),
                binary_base64: URL_SAFE_NO_PAD.encode(binary_bytes),
                target_os: env::consts::OS.to_string(),
                target_arch: env::consts::ARCH.to_string(),
                host: options.host.clone(),
                port: options.port,
                shadow_port: options.shadow_port,
                requested_at: requested_at.clone(),
            });
        }
        let request = binary_request.as_ref().expect("binary rollout request");
        match send_daemon_rollout_binary(&target, request) {
            Ok(response) => {
                println!("{target}: {}", response.status);
                if !response.detail.is_empty() {
                    println!("  {}", response.detail);
                }
            }
            Err(error) => println!("{target}: failed: {error}"),
        }
    }
    Ok(())
}

fn rollout_prepare_dev_binary(options: &RolloutOptions) -> Result<PathBuf, CliError> {
    if let Some(binary) = &options.binary {
        if !binary.exists() {
            return Err(CliError::MissingArtifact {
                artifact: binary.display().to_string(),
            });
        }
        return Ok(binary.clone());
    }
    let repo = options
        .repo
        .clone()
        .unwrap_or(env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?);
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    let plan = SelfRolloutPaths::new(paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "rollout root has no data directory parent".to_string(),
    })?)
    .plan();
    rollout_build_dev_binary(&repo, &plan.build_log)
}

fn rollout_build_dev_binary(repo: &Path, build_log: &Path) -> Result<PathBuf, CliError> {
    let build = Command::new("cargo")
        .args(["build", "-p", "sinomo"])
        .current_dir(repo)
        .output()
        .map_err(|source| CliError::Io {
            path: "cargo build -p sinomo".to_string(),
            detail: source.to_string(),
        })?;
    let log = format!(
        "command: cargo build -p sinomo\nrepo: {}\nstatus: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        repo.display(),
        build.status,
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr)
    );
    fs::write(build_log, log).map_err(|source| CliError::Io {
        path: build_log.display().to_string(),
        detail: source.to_string(),
    })?;
    if !build.status.success() {
        return Err(CliError::Process {
            command: "cargo build -p sinomo".to_string(),
            status: build.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&build.stderr).trim().to_string(),
        });
    }
    Ok(repo
        .join("target")
        .join("debug")
        .join(format!("sinomo{}", env::consts::EXE_SUFFIX)))
}

fn rollout_remote_repo_path(paths: &SelfRolloutPaths, repo: Option<&Path>) -> PathBuf {
    match repo {
        Some(repo) if repo.is_absolute() => repo.to_path_buf(),
        Some(repo) => paths.root.parent().unwrap_or(&paths.root).join(repo),
        None => paths.source_repo.clone(),
    }
}

fn rollout_cluster_targets(options: &RolloutOptions) -> Result<Vec<String>, CliError> {
    let mut targets = Vec::new();
    if !options.skip_local {
        if let Some(status) = default_main_daemon_status()? {
            targets.push(status.address);
        } else {
            targets.push(daemon_connect_address(&options.host, options.port));
        }
    }
    targets.extend(options.targets.iter().cloned());

    if let Ok(state) = require_cluster_bootstrap_state() {
        for member in state.members {
            let is_local_member = member.node_id == state.local_node_id;
            if options.skip_local && is_local_member {
                continue;
            }
            let is_main_daemon = member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "daemon.main.run");
            if !is_main_daemon {
                continue;
            }
            for endpoint in member.endpoints {
                if !is_local_member && matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly)
                {
                    continue;
                }
                targets.push(endpoint.address);
            }
        }
    }

    let mut unique = Vec::new();
    for target in targets {
        if !unique.contains(&target) {
            unique.push(target);
        }
    }
    Ok(unique)
}

fn read_rollout_target_status(address: &str) -> Result<DaemonStatus, CliError> {
    let response = send_daemon_command(address, "STATUS", DAEMON_JOIN_TIMEOUT)?;
    serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
        path: format!("daemon status from {address}"),
        detail: source.to_string(),
    })
}

fn rollout_target_platform_matches_local(status: &DaemonStatus) -> Option<bool> {
    match (&status.target_os, &status.target_arch) {
        (Some(target_os), Some(target_arch)) => {
            Some(target_os == env::consts::OS && target_arch == env::consts::ARCH)
        }
        _ => None,
    }
}

fn ensure_rollout_target_platform_from_status(
    address: &str,
    status: &DaemonStatus,
) -> Result<(), CliError> {
    let local = is_loopback_connect_address(address);
    match (&status.target_os, &status.target_arch) {
        (Some(target_os), Some(target_arch))
            if target_os == env::consts::OS && target_arch == env::consts::ARCH =>
        {
            Ok(())
        }
        (Some(target_os), Some(target_arch)) => Err(CliError::Parse {
            detail: format!(
                "target platform {target_os}/{target_arch} does not match local artifact platform {}/{}",
                env::consts::OS,
                env::consts::ARCH
            ),
        }),
        _ if local => Ok(()),
        _ => Err(CliError::Parse {
            detail:
                "target daemon does not report platform metadata; update it locally before binary rollout"
                    .to_string(),
        }),
    }
}

fn send_daemon_rollout_binary(
    address: &str,
    request: &ClusterRolloutBinaryRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let payload = serde_json::to_string(request).map_err(|source| CliError::Json {
        path: "cluster rollout request".to_string(),
        detail: source.to_string(),
    })?;
    let response = send_daemon_command(
        address,
        &format!("ROLLOUT_BINARY {payload}"),
        DAEMON_ROLLOUT_TIMEOUT,
    )?;
    serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
        path: format!("rollout response from {address}"),
        detail: source.to_string(),
    })
}

fn send_daemon_rollout_build(
    address: &str,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let payload = serde_json::to_string(request).map_err(|source| CliError::Json {
        path: "cluster rollout build request".to_string(),
        detail: source.to_string(),
    })?;
    let response = send_daemon_command(
        address,
        &format!("ROLLOUT_BUILD {payload}"),
        DAEMON_ROLLOUT_TIMEOUT,
    )?;
    serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
        path: format!("rollout response from {address}"),
        detail: source.to_string(),
    })
}

fn ensure_source_repo(paths: &SelfRolloutPaths, repo: Option<&Path>) -> Result<(), CliError> {
    let source = match repo {
        Some(repo) => repo.to_path_buf(),
        None => env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?,
    };
    Ok(deploy_managed_source_repo(paths, &source)?)
}

fn install_binary_atomically(source: &Path, destination: &Path) -> Result<(), CliError> {
    if !source.exists() {
        return Err(CliError::MissingArtifact {
            artifact: source.display().to_string(),
        });
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let next = destination.with_extension("next");
    fs::copy(source, &next).map_err(|error| CliError::Io {
        path: format!("{} -> {}", source.display(), next.display()),
        detail: error.to_string(),
    })?;
    if destination.exists() {
        let previous = destination.with_extension("previous");
        let _ = fs::remove_file(&previous);
        fs::rename(destination, &previous).map_err(|source| CliError::Io {
            path: format!("{} -> {}", destination.display(), previous.display()),
            detail: source.to_string(),
        })?;
    }
    fs::rename(&next, destination).map_err(|source| CliError::Io {
        path: format!("{} -> {}", next.display(), destination.display()),
        detail: source.to_string(),
    })?;
    Ok(())
}

fn stop_main_daemon_if_running(_options: &RolloutOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = DaemonPaths::new(&data_dir);
    if let DaemonProbeStatus::Running(status) = probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT);
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if matches!(
                probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            ) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    Ok(())
}

fn restart_shadow_daemon_from_binary(
    binary: &Path,
    main_binary: &Path,
    options: &RolloutOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = DaemonPaths::for_shadow(&data_dir);
    if let DaemonProbeStatus::Running(status) = probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        let _ = send_daemon_command(&status.address, "SHUTDOWN", DAEMON_PROBE_TIMEOUT);
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if matches!(
                probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            ) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    start_daemon_binary(
        binary,
        main_binary,
        &["daemon", "shadow", "run"],
        options,
        paths,
        "shadow daemon",
    )
}

fn start_daemon_binary(
    binary: &Path,
    main_binary: &Path,
    command_prefix: &[&str],
    options: &RolloutOptions,
    paths: DaemonPaths,
    label: &str,
) -> Result<(), CliError> {
    let mut command = Command::new(binary);
    command.args(command_prefix);
    command.env(SHADOW_MAIN_BINARY_ENV, main_binary);
    command.env(SHADOW_MAIN_HOST_ENV, &options.host);
    command.env(SHADOW_MAIN_PORT_ENV, options.port.to_string());
    let child = command
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(SINOMO_SHADOW_DAEMON_DEFAULT_PORT.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT)
        {
            println!(
                "{label} healthy: pid={} bind={} control={}",
                status.pid,
                daemon_bind_address(&status),
                status.address
            );
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Network {
        address: daemon_connect_address(&options.host, SINOMO_SHADOW_DAEMON_DEFAULT_PORT),
        detail: format!(
            "rolled out {label} process {} did not become ready",
            child.id()
        ),
    })
}

fn wait_for_main_daemon_health(options: &RolloutOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = DaemonPaths::new(&data_dir);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT)
        {
            println!(
                "main daemon healthy: pid={} bind={} control={}",
                status.pid,
                daemon_bind_address(&status),
                status.address
            );
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Network {
        address: daemon_connect_address(&options.host, options.port),
        detail: "rolled out main daemon did not become ready under shadow supervision".to_string(),
    })
}

pub fn run_cui() -> Result<(), CliError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_cui_with_io(stdin.lock(), stdout.lock())
}

pub fn run_cui_with_io<R: BufRead, W: Write>(mut reader: R, mut writer: W) -> Result<(), CliError> {
    let handler = CliInteractionHandler::from_default_model()?;
    let daemon = default_main_daemon_status()?;
    run_cui_with_handler_io_and_daemon(&mut reader, &mut writer, handler, daemon)
}

pub fn run_cui_with_handler_io<R: BufRead, W: Write, H: InteractionEventHandler>(
    mut reader: R,
    mut writer: W,
    handler: H,
) -> Result<(), CliError> {
    run_cui_with_handler_io_and_daemon(&mut reader, &mut writer, handler, None)
}

fn run_cui_with_handler_io_and_daemon<R: BufRead, W: Write, H: InteractionEventHandler>(
    mut reader: R,
    mut writer: W,
    handler: H,
    daemon: Option<DaemonStatus>,
) -> Result<(), CliError> {
    let node_id = daemon
        .as_ref()
        .and_then(|status| status.cluster.as_ref())
        .map(|cluster| cluster.node_id.clone())
        .unwrap_or_else(|| NodeId::new("node_cli").expect("valid node id"));
    let session = InteractionSession::new(
        SessionId::generate(),
        node_id,
        InteractionSurface::new("surface.cli", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());

    writeln!(writer, "Sinomo CUI").map_err(CliError::stdio)?;
    writeln!(writer, "session: {}", session.session_id).map_err(CliError::stdio)?;
    writeln!(writer, "node: {}", session.node_id).map_err(CliError::stdio)?;
    if let Some(status) = &daemon {
        writeln!(
            writer,
            "daemon: connected pid={} address={}",
            status.pid, status.address
        )
        .map_err(CliError::stdio)?;
        if let Some(cluster) = &status.cluster {
            writeln!(writer, "cluster: {}", cluster.cluster_id).map_err(CliError::stdio)?;
        }
    } else {
        writeln!(writer, "daemon: not connected").map_err(CliError::stdio)?;
    }
    writeln!(writer, "Type /help for commands or /quit to exit.").map_err(CliError::stdio)?;

    loop {
        write!(writer, "sinomo> ").map_err(CliError::stdio)?;
        writer.flush().map_err(CliError::stdio)?;

        let mut line = String::new();
        if reader.read_line(&mut line).map_err(CliError::stdio)? == 0 {
            break;
        }

        let line = line.trim_end_matches(['\r', '\n']);
        if line.trim().is_empty() {
            continue;
        }

        let outputs = runtime.ingest_user_input(line)?;
        for output in outputs {
            match output {
                InteractionOutput::Line(line) => {
                    writeln!(writer, "{line}").map_err(CliError::stdio)?;
                }
                InteractionOutput::Exit => return Ok(()),
            }
        }
    }

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractionSession {
    pub session_id: SessionId,
    pub node_id: NodeId,
    pub surface: InteractionSurface,
}

impl InteractionSession {
    pub fn new(session_id: SessionId, node_id: NodeId, surface: InteractionSurface) -> Self {
        Self {
            session_id,
            node_id,
            surface,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractionSurface {
    pub surface_id: String,
    pub kind: InteractionSurfaceKind,
}

impl InteractionSurface {
    pub fn new(surface_id: impl Into<String>, kind: InteractionSurfaceKind) -> InteractionSurface {
        InteractionSurface {
            surface_id: surface_id.into(),
            kind,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractionSurfaceKind {
    CommandLine,
    App,
    Api,
    Voice,
    Remote,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractionInputKind {
    Message,
    Command,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InteractionOutput {
    Line(String),
    Exit,
}

pub trait InteractionEventHandler {
    fn handle_event<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
    ) -> Result<Vec<InteractionOutput>, CliError>;
}

#[derive(Clone, Debug)]
pub struct InteractionRuntime<S, H> {
    store: S,
    handler: H,
    session: InteractionSession,
}

impl<S: EventStore, H: InteractionEventHandler> InteractionRuntime<S, H> {
    pub fn new(store: S, handler: H, session: InteractionSession) -> Self {
        Self {
            store,
            handler,
            session,
        }
    }

    pub fn ingest_user_input(&mut self, text: &str) -> Result<Vec<InteractionOutput>, CliError> {
        let event = user_input_event(&self.session, text);
        self.store.append(event.clone())?;
        self.handler
            .handle_event(&mut self.store, &self.session, &event)
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn session(&self) -> &InteractionSession {
        &self.session
    }
}

pub trait InteractionModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError>;
}

impl<T: LanguageModelProvider> InteractionModel for T {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete(self, request)
    }
}

#[derive(Clone, Debug)]
pub struct ModelBackedInteractionHandler<M> {
    model: M,
    provider: String,
    model_name: String,
}

pub type CliInteractionHandler = ModelBackedInteractionHandler<CliConversationModel>;

impl<M> ModelBackedInteractionHandler<M> {
    pub fn new(model: M, provider: impl Into<String>, model_name: impl Into<String>) -> Self {
        Self {
            model,
            provider: provider.into(),
            model_name: model_name.into(),
        }
    }
}

impl CliInteractionHandler {
    pub fn from_default_model() -> Result<Self, CliError> {
        let model = CliConversationModel::from_defaults()?;
        Ok(Self::new(
            model.clone(),
            model.provider.clone(),
            model.model.clone(),
        ))
    }
}

impl<M: InteractionModel> InteractionEventHandler for ModelBackedInteractionHandler<M> {
    fn handle_event<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
    ) -> Result<Vec<InteractionOutput>, CliError> {
        let text = event
            .payload
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();

        let Some(args) = parse_slash_command(text)? else {
            let message = interaction_event(
                session,
                "interaction.message.received",
                serde_json::json!({
                    "input_event_id": event.event_id,
                    "text": text,
                    "state": "received",
                    "delivery": "event-stream"
                }),
            )
            .caused_by(&event.event_id);
            store.append(message.clone())?;
            let request = conversation_model_request(
                store,
                session,
                &message.event_id,
                text,
                &self.provider,
                &self.model_name,
            )?;
            let requested = interaction_event(
                session,
                "interaction.model.requested",
                serde_json::json!({
                    "message_event_id": message.event_id,
                    "provider": request.provider,
                    "model": request.model,
                    "message_count": request.messages.len()
                }),
            )
            .caused_by(&message.event_id);
            store.append(requested.clone())?;

            match self.model.complete(&request) {
                Ok(response) => {
                    let response_event = interaction_event(
                        session,
                        "interaction.model.responded",
                        serde_json::json!({
                            "message_event_id": message.event_id,
                            "request_event_id": requested.event_id,
                            "provider": response.provider,
                            "model": response.model,
                            "text": response.content,
                            "usage": response.usage
                        }),
                    )
                    .caused_by(&requested.event_id);
                    store.append(response_event.clone())?;
                    return Ok(vec![InteractionOutput::Line(format!(
                        "sinomo: {}",
                        response_event
                            .payload
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                    ))]);
                }
                Err(error) => {
                    let failure = interaction_event(
                        session,
                        "interaction.model.failed",
                        serde_json::json!({
                            "message_event_id": message.event_id,
                            "request_event_id": requested.event_id,
                            "provider": request.provider,
                            "model": request.model,
                            "error": error.to_string()
                        }),
                    )
                    .caused_by(&requested.event_id);
                    store.append(failure)?;
                    return Err(CliError::LanguageModel(error));
                }
            }
        };

        if matches!(args.first().map(String::as_str), Some("quit" | "exit")) {
            let quit = interaction_event(
                session,
                "interaction.session.ended",
                serde_json::json!({
                    "input_event_id": event.event_id,
                    "reason": "user-requested"
                }),
            )
            .caused_by(&event.event_id);
            store.append(quit)?;
            return Ok(vec![
                InteractionOutput::Line("ending session".to_string()),
                InteractionOutput::Exit,
            ]);
        }

        let command = interaction_event(
            session,
            "interaction.command.requested",
            serde_json::json!({
                "input_event_id": event.event_id,
                "args": args,
                "command": args.join(" ")
            }),
        )
        .caused_by(&event.event_id);
        store.append(command.clone())?;

        let result = run_cli_command(&args);
        let (kind, status, output) = match result {
            Ok(()) => (
                "interaction.command.completed",
                "completed",
                format!("completed command: /{}", args.join(" ")),
            ),
            Err(error) => (
                "interaction.command.failed",
                "failed",
                format!("command failed: {error}"),
            ),
        };
        let outcome = interaction_event(
            session,
            kind,
            serde_json::json!({
                "command_event_id": command.event_id,
                "status": status,
                "args": args
            }),
        )
        .caused_by(&command.event_id);
        store.append(outcome)?;

        Ok(vec![InteractionOutput::Line(output)])
    }
}

#[derive(Clone, Debug)]
pub struct CliConversationModel {
    provider: String,
    model: String,
    data_dir: SinomoDataDir,
}

impl CliConversationModel {
    pub fn from_defaults() -> Result<Self, CliError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        data_dir.ensure_root_layout()?;
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure()?;
        let evolution_config = read_config(&paths).ok();
        let model_config = read_model_config(&ModelPaths::new(&data_dir)).ok();
        let provider = model_config
            .as_ref()
            .and_then(|config| config.default_provider.clone())
            .or_else(|| {
                evolution_config
                    .as_ref()
                    .and_then(|config| config.default_provider.clone())
            })
            .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
        let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
            detail: format!("unknown provider `{provider}`"),
        })?;
        let model = model_config
            .as_ref()
            .and_then(|config| config.default_model.clone())
            .or_else(|| {
                evolution_config
                    .as_ref()
                    .and_then(|config| config.default_model.clone())
            })
            .unwrap_or_else(|| PROVIDER_DEFAULT_MODEL.to_string());
        let model = normalize_model_id_for_provider(&provider, &model);
        Ok(Self {
            provider,
            model,
            data_dir,
        })
    }
}

impl InteractionModel for CliConversationModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let request = LanguageModelRequest {
            provider: self.provider.clone(),
            model: self.model.clone(),
            ..request.clone()
        };
        match self.provider.as_str() {
            GitHubCopilotProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
                let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
                    client_id: env::var("SINOMO_COPILOT_CLIENT_ID")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().client_id),
                    github_host: env::var("SINOMO_COPILOT_GITHUB_HOST")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().github_host),
                    api_base_url: env::var("SINOMO_COPILOT_API_BASE_URL")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().api_base_url),
                    default_model: model_or_default(
                        &self.model,
                        GitHubCopilotConfig::default().default_model,
                    ),
                    token_store_path: Some(credentials_path),
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            GitHubCopilotCliProvider::PROVIDER_ID => {
                let provider = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
                    model: model_or_default(&self.model, GitHubCopilotCliConfig::default().model),
                    ..GitHubCopilotCliConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            CodexCliProvider::PROVIDER_ID => {
                let provider = CodexCliProvider::new(CodexCliConfig {
                    model: model_or_default(&self.model, CODEX_CLI_DEFAULT_MODEL.to_string()),
                    working_dir: env::current_dir().ok(),
                    ..CodexCliConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            CODEX_PROVIDER_ID => {
                let provider = codex_provider_for_model(&self.model);
                LanguageModelProvider::complete(&provider, &request)
            }
            ClaudeCodeProvider::PROVIDER_ID => {
                let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
                    model: model_or_default(&self.model, CLAUDE_CODE_DEFAULT_MODEL.to_string()),
                    working_dir: env::current_dir().ok(),
                    ..ClaudeCodeConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            AnthropicProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(AnthropicProvider::PROVIDER_ID);
                let provider = AnthropicProvider::new(AnthropicConfig {
                    default_model: model_or_default(
                        &self.model,
                        AnthropicConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..AnthropicConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            OpenAiProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(OpenAiProvider::PROVIDER_ID);
                let provider = OpenAiProvider::new(OpenAiConfig {
                    default_model: model_or_default(
                        &self.model,
                        OpenAiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..OpenAiConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            GeminiProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GeminiProvider::PROVIDER_ID);
                let provider = GeminiProvider::new(GeminiConfig {
                    default_model: model_or_default(
                        &self.model,
                        GeminiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..GeminiConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            NvidiaProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(NvidiaProvider::PROVIDER_ID);
                let provider = NvidiaProvider::new(NvidiaConfig {
                    api_base_url: env::var("SINOMO_NVIDIA_API_BASE_URL")
                        .unwrap_or_else(|_| NvidiaConfig::default().api_base_url),
                    default_model: model_or_default(
                        &self.model,
                        NvidiaConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            _ => Err(LanguageModelError::ProviderInvocationFailed {
                provider: self.provider.clone(),
                model: self.model.clone(),
                detail: "unknown interaction model provider".to_string(),
            }),
        }
    }
}

trait EventEnvelopeExt {
    fn caused_by(self, event_id: &EventId) -> Self;
}

impl EventEnvelopeExt for EventEnvelope {
    fn caused_by(mut self, event_id: &EventId) -> Self {
        self.links.caused_by.push(event_id.clone());
        self
    }
}

fn user_input_event(session: &InteractionSession, text: &str) -> EventEnvelope {
    let input_kind = if text.trim_start().starts_with('/') {
        InteractionInputKind::Command
    } else {
        InteractionInputKind::Message
    };
    interaction_event(
        session,
        "interaction.input.received",
        serde_json::json!({
            "text": text,
            "input_kind": input_kind,
            "surface": session.surface,
        }),
    )
}

fn interaction_event(
    session: &InteractionSession,
    kind: &str,
    payload: serde_json::Value,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        session.node_id.clone(),
        Source::new("interaction.surface").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.62).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session.session_id.clone())
    .with_payload(payload)
}

fn conversation_model_request<S: EventStore>(
    store: &S,
    session: &InteractionSession,
    message_event_id: &EventId,
    text: &str,
    provider: &str,
    model: &str,
) -> Result<LanguageModelRequest, CliError> {
    let recent = store.replay(ReplayQuery {
        session_id: Some(session.session_id.clone()),
        kind_prefix: Some("interaction.model.responded".to_string()),
        origin_node_id: None,
        after_sequence: None,
        start_time: None,
        end_time: None,
        direction: ReplayDirection::Reverse,
        limit: Some(6),
    })?;
    let mut recent_turns: Vec<String> = recent
        .into_iter()
        .rev()
        .filter_map(|stored| {
            stored
                .event
                .payload
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect();
    if recent_turns.is_empty() {
        recent_turns.push("No prior assistant turns in this session.".to_string());
    }

    Ok(LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        messages: vec![
            LanguageModelMessage {
                role: LanguageModelRole::System,
                content: INTERACTION_SYSTEM_PROMPT.to_string(),
            },
            LanguageModelMessage {
                role: LanguageModelRole::User,
                content: serde_json::to_string_pretty(&serde_json::json!({
                    "session_id": session.session_id,
                    "surface": session.surface,
                    "input_event_id": message_event_id,
                    "recent_assistant_turns": recent_turns,
                    "user_input": text
                }))
                .expect("interaction request serializes"),
            },
        ],
        temperature: Some(0.4),
        max_output_tokens: Some(800),
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::json!({
            "loop": "interactive",
            "node_id": session.node_id,
            "session_id": session.session_id,
            "input_event_id": message_event_id
        }),
    })
}

const INTERACTION_SYSTEM_PROMPT: &str = r#"You are Sinomo's command-line conversation loop.
Respond directly to the user in plain text.
Use the supplied event ids and session metadata as context, but do not expose internal event JSON unless asked.
Slash commands are handled by Sinomo outside the model; ordinary user input reaches you only after being recorded as an interaction event."#;

pub fn parse_slash_command(input: &str) -> Result<Option<Vec<String>>, CliError> {
    let trimmed = input.trim();
    let Some(command) = trimmed.strip_prefix('/') else {
        return Ok(None);
    };
    let args = split_command_line(command)?;
    if args.is_empty() {
        return Err(CliError::Usage);
    }
    Ok(Some(args))
}

fn split_command_line(input: &str) -> Result<Vec<String>, CliError> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();
    let mut quote: Option<char> = None;
    let mut arg_started = false;

    while let Some(character) = chars.next() {
        match (quote, character) {
            (Some(active_quote), value) if value == active_quote => quote = None,
            (Some(_), '\\') => {
                arg_started = true;
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                } else {
                    current.push('\\');
                }
            }
            (Some(_), value) => {
                arg_started = true;
                current.push(value);
            }
            (None, '"' | '\'') => {
                arg_started = true;
                quote = Some(character);
            }
            (None, value) if value.is_whitespace() => {
                if arg_started {
                    args.push(std::mem::take(&mut current));
                    arg_started = false;
                }
            }
            (None, '\\') => {
                arg_started = true;
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                } else {
                    current.push('\\');
                }
            }
            (None, value) => {
                arg_started = true;
                current.push(value);
            }
        }
    }

    if let Some(quote) = quote {
        return Err(CliError::Parse {
            detail: format!("unterminated {quote} quote"),
        });
    }

    if arg_started {
        args.push(current);
    }

    Ok(args)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct EvolutionConfig {
    repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_model: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct ModelConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_model: Option<String>,
}

#[derive(Clone, Debug)]
struct ModelPaths {
    config: PathBuf,
}

impl ModelPaths {
    fn new(data_dir: &SinomoDataDir) -> Self {
        Self {
            config: data_dir.configs_dir().join("models.json"),
        }
    }

    fn ensure(&self) -> Result<(), CliError> {
        let Some(parent) = self.config.parent() else {
            return Ok(());
        };
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct StoredEvolutionJob {
    job: EvolutionJob,
    candidate_workspace: CandidateWorkspace,
    bundle: CandidateBundleManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verification_outcome: Option<EvaluationOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    decision: Option<CandidateDecision>,
}

#[derive(Clone, Debug)]
struct EvolutionPaths {
    root: PathBuf,
    jobs: PathBuf,
    campaigns: PathBuf,
    logs: PathBuf,
    config: PathBuf,
}

impl EvolutionPaths {
    fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("evolution");
        Self {
            jobs: root.join("jobs"),
            campaigns: root.join("campaigns"),
            logs: root.join("logs"),
            config: root.join("config.json"),
            root,
        }
    }

    fn ensure(&self) -> Result<(), CliError> {
        for path in [&self.root, &self.jobs, &self.campaigns, &self.logs] {
            fs::create_dir_all(path).map_err(|source| CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(())
    }

    fn campaign_dir(&self, campaign_id: &str) -> PathBuf {
        self.campaigns.join(campaign_id)
    }

    fn campaign_path(&self, campaign_id: &str) -> PathBuf {
        self.campaign_dir(campaign_id).join("campaign.json")
    }

    fn job_dir(&self, job_id: &EvolutionJobId) -> PathBuf {
        self.jobs.join(job_id.as_str())
    }

    fn job_path(&self, job_id: &EvolutionJobId) -> PathBuf {
        self.job_dir(job_id).join("record.json")
    }

    fn legacy_job_path(&self, job_id: &EvolutionJobId) -> PathBuf {
        self.jobs.join(format!("{job_id}.json"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BackgroundSchedulerPaths {
    root: PathBuf,
    plan_json: PathBuf,
    status_json: PathBuf,
    events_jsonl: PathBuf,
}

impl BackgroundSchedulerPaths {
    fn new(paths: &EvolutionPaths) -> Self {
        let root = paths.root.join("background-scheduler");
        Self {
            plan_json: root.join("scheduler-plan.json"),
            status_json: root.join("background-status.json"),
            events_jsonl: root.join("scheduler-events.jsonl"),
            root,
        }
    }

    fn ensure(&self) -> Result<(), CliError> {
        fs::create_dir_all(&self.root).map_err(|source| CliError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CampaignBundlePaths {
    root: PathBuf,
    campaign_json: PathBuf,
    objective_card_json: PathBuf,
    stages_json: PathBuf,
    near_term_tasks_json: PathBuf,
    resource_schedule_preview_json: PathBuf,
    resource_ledger_json: PathBuf,
    scheduler_dry_run_json: PathBuf,
    lease_requests_jsonl: PathBuf,
    lease_decisions_jsonl: PathBuf,
    iteration_plan_json: PathBuf,
    iteration_request_json: PathBuf,
    iteration_dispatches_jsonl: PathBuf,
    stage_scorecard_json: PathBuf,
    stage_scorecards_jsonl: PathBuf,
    adaptation_json: PathBuf,
    adaptations_jsonl: PathBuf,
    approval_checkpoint_json: PathBuf,
    decisions_jsonl: PathBuf,
}

impl CampaignBundlePaths {
    fn new(paths: &EvolutionPaths, campaign_id: &str) -> Self {
        let root = paths.campaign_dir(campaign_id);
        Self {
            campaign_json: paths.campaign_path(campaign_id),
            objective_card_json: root.join("objective-card.json"),
            stages_json: root.join("stages.json"),
            near_term_tasks_json: root.join("near-term-tasks.json"),
            resource_schedule_preview_json: root.join("resource-schedule-preview.json"),
            resource_ledger_json: root.join("resource-ledger.json"),
            scheduler_dry_run_json: root.join("scheduler-dry-run.json"),
            lease_requests_jsonl: root.join("lease-requests.jsonl"),
            lease_decisions_jsonl: root.join("lease-decisions.jsonl"),
            iteration_plan_json: root.join("iteration-plan.json"),
            iteration_request_json: root.join("iteration-request.json"),
            iteration_dispatches_jsonl: root.join("iteration-dispatches.jsonl"),
            stage_scorecard_json: root.join("stage-scorecard.json"),
            stage_scorecards_jsonl: root.join("stage-scorecards.jsonl"),
            adaptation_json: root.join("adaptation.json"),
            adaptations_jsonl: root.join("adaptations.jsonl"),
            approval_checkpoint_json: root.join("approval-checkpoint.json"),
            decisions_jsonl: root.join("decisions.jsonl"),
            root,
        }
    }
}

#[derive(Debug, Clone)]
enum EvolveCliCommand {
    Help,
    Propose(Vec<String>),
    Schedule(Vec<String>),
    Iterate(Vec<String>),
    Evaluate(Vec<String>),
    Tick(Vec<String>),
    Decide(CandidateDecisionOutcome, Vec<String>),
    Status(Vec<String>),
    Show(Vec<String>),
    Verify(Vec<String>),
    Packet(Vec<String>),
    Cleanup(Vec<String>),
    Job(Vec<String>),
    Models(ModelsCliCommand),
}

fn parse_evolve_command(args: &[String]) -> Result<EvolveCliCommand, CliError> {
    match args.split_first() {
        None => Ok(EvolveCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(EvolveCliCommand::Help)
        }
        Some((command, rest)) if command == "propose" => {
            Ok(EvolveCliCommand::Propose(rest.to_vec()))
        }
        Some((command, rest)) if command == "schedule" => {
            Ok(EvolveCliCommand::Schedule(rest.to_vec()))
        }
        Some((command, rest)) if command == "iterate" => {
            Ok(EvolveCliCommand::Iterate(rest.to_vec()))
        }
        Some((command, rest)) if command == "evaluate" => {
            Ok(EvolveCliCommand::Evaluate(rest.to_vec()))
        }
        Some((command, rest)) if matches!(command.as_str(), "tick" | "run-background") => {
            Ok(EvolveCliCommand::Tick(rest.to_vec()))
        }
        Some((command, rest)) if command == "approve" => Ok(EvolveCliCommand::Decide(
            CandidateDecisionOutcome::Approved,
            rest.to_vec(),
        )),
        Some((command, rest)) if command == "reject" => Ok(EvolveCliCommand::Decide(
            CandidateDecisionOutcome::Rejected,
            rest.to_vec(),
        )),
        Some((command, rest)) if command == "restore" => Ok(EvolveCliCommand::Decide(
            CandidateDecisionOutcome::Restored,
            rest.to_vec(),
        )),
        Some((command, rest)) if command == "status" => Ok(EvolveCliCommand::Status(rest.to_vec())),
        Some((command, rest)) if command == "show" => Ok(EvolveCliCommand::Show(rest.to_vec())),
        Some((command, rest)) if command == "verify" => Ok(EvolveCliCommand::Verify(rest.to_vec())),
        Some((command, rest)) if command == "packet" => Ok(EvolveCliCommand::Packet(rest.to_vec())),
        Some((command, rest)) if command == "cleanup" => {
            Ok(EvolveCliCommand::Cleanup(rest.to_vec()))
        }
        Some((command, rest)) if command == "job" => Ok(EvolveCliCommand::Job(rest.to_vec())),
        Some((command, rest)) if command == "models" => Ok(EvolveCliCommand::Models(
            parse_models_command(rest, ModelDefaultRepoMode::Allow)?,
        )),
        _ => Err(CliError::Usage),
    }
}

#[cfg(test)]
fn run_evolve_command(args: &[String]) -> Result<(), CliError> {
    run_evolve_cli_command(parse_evolve_command(args)?)
}

fn run_evolve_cli_command(command: EvolveCliCommand) -> Result<(), CliError> {
    match command {
        EvolveCliCommand::Help => {
            print_evolve_help();
            Ok(())
        }
        EvolveCliCommand::Propose(args) => evolve_propose(&args),
        EvolveCliCommand::Schedule(args) => evolve_schedule(&args),
        EvolveCliCommand::Iterate(args) => evolve_iterate(&args),
        EvolveCliCommand::Evaluate(args) => evolve_evaluate(&args),
        EvolveCliCommand::Tick(args) => evolve_tick(&args),
        EvolveCliCommand::Decide(outcome, args) => evolve_decide(&args, outcome),
        EvolveCliCommand::Status(args) => evolve_status(&args),
        EvolveCliCommand::Show(args) => evolve_show(&args),
        EvolveCliCommand::Verify(args) => evolve_verify(&args),
        EvolveCliCommand::Packet(args) => evolve_show_packet(&args),
        EvolveCliCommand::Cleanup(args) => evolve_cleanup(&args),
        EvolveCliCommand::Job(args) => evolve_job(&args),
        EvolveCliCommand::Models(command) => run_models_command(command),
    }
}

fn evolve_propose(args: &[String]) -> Result<(), CliError> {
    let options = parse_propose_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let config = read_config(&paths).ok();
    let model_config = read_model_config(&ModelPaths::new(&data_dir)).ok();
    let repo = if let Some(repo) = options
        .repo
        .clone()
        .or_else(|| config.as_ref().map(|config| PathBuf::from(&config.repo)))
    {
        repo
    } else {
        let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
        ensure_rollout_layout(&rollout_paths)?;
        let current_repo = env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?;
        ensure_source_repo(&rollout_paths, Some(&current_repo))?;
        rollout_paths.source_repo
    };

    if options.dry_run || options.pause_after_proposal {
        let campaign = propose_evolution_campaign(CampaignProposalRequest {
            objective: options.objective.clone(),
            horizon: options
                .horizon
                .clone()
                .unwrap_or_else(|| "30 days".to_string()),
            budget_policy: options
                .budget_policy
                .clone()
                .unwrap_or_else(|| "free-and-prepaid".to_string()),
            reserve_floors: parse_reserve_floors(&options.reserve_floors)?,
            approval_policy: options
                .approval_policy
                .clone()
                .unwrap_or_else(|| "stage-rollout".to_string()),
            max_wall_time: options.max_wall_time.clone(),
            pause_after_proposal: options.pause_after_proposal,
            dry_run: true,
            approved: options.approve_campaign,
            target_ring: options.target_ring.clone(),
            source_repo: Some(repo.display().to_string()),
        });
        let bundle = write_campaign_bundle(&paths, &campaign)?;
        println_campaign_proposal(&campaign, &bundle);
        return Ok(());
    }

    let provider = options
        .provider
        .or_else(|| {
            model_config
                .as_ref()
                .and_then(|config| config.default_provider.clone())
        })
        .or_else(|| {
            config
                .as_ref()
                .and_then(|config| config.default_provider.clone())
        })
        .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
    let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
        detail: format!("unknown provider `{provider}`"),
    })?;
    let model = options.model.or_else(|| {
        model_config
            .as_ref()
            .and_then(|config| config.default_model.clone())
            .or_else(|| {
                config
                    .as_ref()
                    .and_then(|config| config.default_model.clone())
            })
    });
    let model = model.map(|model| normalize_model_id_for_provider(&provider, &model));
    let verification_commands = if options.verify.is_empty() {
        Vec::new()
    } else {
        options
            .verify
            .iter()
            .enumerate()
            .map(|(index, command)| VerificationCommand {
                name: format!("verify-{}", index + 1),
                command: command.clone(),
                expected_exit_code: 0,
                gate: EvaluationGate::StaticChecks,
            })
            .collect()
    };
    let request = EvolutionRequest {
        objective: options.objective,
        target_ring: options.target_ring,
        intended_paths: options.allow,
        verification_commands,
        model_provider: Some(provider.clone()),
        model: model.clone(),
        dry_run: options.dry_run,
    };

    let orchestrator = EvolutionOrchestrator::new(
        GitWorkspaceAdapter::new(&repo, &paths.jobs),
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(&paths.jobs),
    );
    let model_runner = CliMutationModel {
        provider,
        model: model.unwrap_or_else(|| PROVIDER_DEFAULT_MODEL.to_string()),
        data_dir: data_dir.clone(),
    };
    let report = if request.dry_run {
        orchestrator.propose(request, None)?
    } else {
        orchestrator.propose(request, Some(&model_runner))?
    };

    write_report_record(&paths, &report)?;
    println_report(&report);
    Ok(())
}

fn evolve_schedule(args: &[String]) -> Result<(), CliError> {
    let options = parse_schedule_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    if !options.dry_run {
        return Err(CliError::Usage);
    }
    let mut campaign = read_campaign(&paths, &options.campaign_id)?;
    let resources = parse_schedule_resources(&options.resources, &campaign)?;
    let task_estimates = parse_schedule_estimates(&options.estimates, &campaign, &resources)?;
    let dry_run = dry_run_resource_scheduler(ResourceSchedulerDryRunRequest {
        campaign: campaign.clone(),
        preview_horizon: "24h".to_string(),
        resources,
        task_estimates,
    });
    let bundle = write_campaign_schedule_dry_run(&paths, &mut campaign, &dry_run)?;
    println_scheduler_dry_run(&dry_run, &bundle);
    Ok(())
}

fn evolve_iterate(args: &[String]) -> Result<(), CliError> {
    let options = parse_iteration_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    if !options.dry_run {
        return Err(CliError::Usage);
    }
    let mut campaign = read_campaign(&paths, &options.campaign_id)?;
    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign: campaign.clone(),
        task_id: options.task_id.clone(),
        intended_paths: options.intended_paths.clone(),
        extra_verification_commands: options.verification_commands.clone(),
        dry_run: true,
    })
    .map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })?;
    let bundle = write_campaign_iteration_dry_run(&paths, &mut campaign, &plan)?;
    println_iteration_dry_run(&plan, &bundle);
    Ok(())
}

fn evolve_evaluate(args: &[String]) -> Result<(), CliError> {
    let options = parse_stage_evaluation_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let campaign = read_campaign(&paths, &options.campaign_id)?;
    let job_path = paths.job_path(&options.job_id);
    let record_path = if job_path.exists() {
        job_path
    } else {
        paths.legacy_job_path(&options.job_id)
    };
    let record = read_stored_job(&record_path)?;
    let report = evolution_report_from_record(record)?;
    let update = evaluate_campaign_stage(StageEvaluationRequest {
        campaign,
        task_id: options.task_id,
        report,
        resource_usage: options.resource_usage,
        failure_class: options.failure_class,
        root_cause_summary: options.root_cause_summary,
        what_was_learned: options.what_was_learned,
    });
    let bundle = write_campaign_stage_evaluation(&paths, &update)?;
    println_stage_evaluation(&update, &bundle);
    Ok(())
}

fn evolve_tick(args: &[String]) -> Result<(), CliError> {
    let options = parse_background_tick_options(args)?;
    if !options.dry_run {
        return Err(CliError::Usage);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let campaigns = read_background_tick_campaigns(&paths, &options.campaign_ids)?;
    let external_workloads = parse_background_tick_external_workloads(
        &options.external_workloads,
        &options.external_workload_estimates,
        &options.external_human_checkpoints,
    )?;
    let resources = parse_background_tick_resources(&options.resources, &campaigns)?;
    let task_estimates = parse_background_tick_estimates(&options.estimates, &campaigns)?;
    let now = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: options.scheduler_service_id,
        campaigns,
        external_workloads,
        resources,
        task_estimates,
        max_concurrent_campaigns: options.max_concurrent_campaigns,
        max_concurrent_workloads: options.max_concurrent_workloads,
        idle_window_open: options.idle_window_open,
        reset_window_open: options.reset_window_open,
        dry_run: true,
        now,
    });
    let background_paths = write_background_scheduling_plan(&paths, &plan)?;
    println_background_tick(&plan, &background_paths);
    Ok(())
}

fn evolve_decide(args: &[String], outcome: CandidateDecisionOutcome) -> Result<(), CliError> {
    let (paths, mut record) = resolve_job_from_args(args, true)?;
    ensure_awaiting_approval(&record)?;
    let note = flag_value(args, "--note").unwrap_or_default();
    let decision = CandidateDecision::new(
        &record.job,
        outcome.clone(),
        "cli-supervisor",
        record.candidate_workspace.path.display().to_string(),
        record.bundle.bundle_path.clone(),
        record.verification_outcome.clone(),
        record.changed_files.clone(),
        note,
    );
    let decision_path = PathBuf::from(&record.bundle.bundle_path).join("approval-decision.json");
    write_json(&decision_path, &decision)?;
    record.job.status = decision.resulting_status.clone();
    record.bundle.decision_path = Some(decision_path.display().to_string());
    record.decision = Some(decision);
    write_bundle_manifest(&record)?;
    if let Some(job_path) = &record.bundle.job_path {
        write_json(Path::new(job_path), &record.job)?;
    }
    write_stored_job(&paths, &record)?;
    if outcome == CandidateDecisionOutcome::Approved {
        commit_and_fast_forward_approved_candidate(&record)?;
    }
    println!("job: {}", record.job.job_id);
    println!("status: {}", job_status_label(&record.job.status));
    println!("worktree: {}", record.candidate_workspace.path.display());
    println!("bundle: {}", record.bundle.bundle_path);
    println!("decision: {}", decision_path.display());
    Ok(())
}

fn commit_and_fast_forward_approved_candidate(record: &StoredEvolutionJob) -> Result<(), CliError> {
    let worktree = &record.candidate_workspace.path;
    run_git_command(worktree, &["add", "-A"])?;
    let status = run_git_command(worktree, &["status", "--porcelain"])?;
    if !status.trim().is_empty() {
        run_git_command(
            worktree,
            &[
                "commit",
                "-m",
                &format!("Apply evolution job {}", record.job.job_id),
            ],
        )?;
    }
    let source_root = Path::new(&record.job.edit_scope.workspace_root);
    run_git_command(
        source_root,
        &[
            "merge",
            "--ff-only",
            &record.candidate_workspace.branch_name,
        ],
    )?;
    Ok(())
}

fn run_git_command(cwd: &Path, args: &[&str]) -> Result<String, CliError> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|source| CliError::Io {
            path: format!("git {}", args.join(" ")),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(CliError::Process {
            command: format!("git {}", args.join(" ")),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn evolve_status(args: &[String]) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    if let Some(job_id) = job_flag(args)? {
        let job_path = paths.job_path(&job_id);
        let record_path = if job_path.exists() {
            job_path
        } else {
            paths.legacy_job_path(&job_id)
        };
        let record = read_stored_job(&record_path)?;
        print_job_status(&record);
        return Ok(());
    }

    let mut records = list_job_records(&paths)?;
    records.sort_by(|left, right| left.job.job_id.cmp(&right.job.job_id));
    if records.is_empty() {
        println!("no evolution jobs");
        return Ok(());
    }
    for record in &records {
        print_job_status(record);
    }
    Ok(())
}

fn evolve_show(args: &[String]) -> Result<(), CliError> {
    let (_paths, record) = resolve_job_from_args(args, false)?;
    let artifact = if has_flag(args, "--diff") {
        record.bundle.diff_path.clone()
    } else if has_flag(args, "--scorecard") {
        record.bundle.scorecard_path.clone()
    } else if has_flag(args, "--decision") {
        record.bundle.decision_path.clone()
    } else if has_flag(args, "--packet") {
        Some(record.bundle.mutation_packet_path.clone())
    } else {
        record.bundle.diff_path.clone()
    }
    .ok_or_else(|| CliError::MissingArtifact {
        artifact: "requested artifact".to_string(),
    })?;
    let content = fs::read_to_string(&artifact).map_err(|source| CliError::Io {
        path: artifact.clone(),
        detail: source.to_string(),
    })?;
    println!("{content}");
    Ok(())
}

fn evolve_show_packet(args: &[String]) -> Result<(), CliError> {
    let mut show_args = args.to_vec();
    show_args.push("--packet".to_string());
    evolve_show(&show_args)
}

fn evolve_verify(args: &[String]) -> Result<(), CliError> {
    let (paths, mut record) = resolve_job_from_args(args, false)?;
    let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());
    let (scorecard, results) = harness
        .run_verification_plan(
            record.job.verification.clone(),
            &record.candidate_workspace.path,
        )
        .map_err(CliError::Eval)?;
    let scorecard_path = PathBuf::from(&record.bundle.bundle_path).join("scorecard.json");
    write_json(&scorecard_path, &scorecard)?;
    for result in &results {
        let safe_name = safe_file_stem(&result.name);
        let path = PathBuf::from(&record.bundle.bundle_path)
            .join("command-logs")
            .join(format!("{safe_name}.txt"));
        fs::write(
            &path,
            format!(
                "name: {}\ncommand: {}\nexpected_exit_code: {}\nexit_code: {}\nduration_ms: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
                result.name,
                result.command,
                result.expected_exit_code,
                result.exit_code,
                result.duration_ms,
                result.stdout,
                result.stderr
            ),
        )
        .map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    record.verification_outcome = Some(scorecard.outcome.clone());
    record.bundle.scorecard_path = Some(scorecard_path.display().to_string());
    write_stored_job(&paths, &record)?;
    println!("job: {}", record.job.job_id);
    println!("verification: {}", outcome_label(&scorecard.outcome));
    println!("scorecard: {}", scorecard_path.display());
    Ok(())
}

fn evolve_cleanup(args: &[String]) -> Result<(), CliError> {
    let (paths, record) = resolve_job_from_args(args, false)?;
    let path = &record.candidate_workspace.path;
    if path.exists() {
        let output = std::process::Command::new("git")
            .args(["worktree", "remove", "--force"])
            .arg(path)
            .current_dir(&record.job.edit_scope.workspace_root)
            .output();
        match output {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                fs::remove_dir_all(path).map_err(|source| CliError::Io {
                    path: path.display().to_string(),
                    detail: format!(
                        "git worktree remove failed: {}; remove_dir_all failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim(),
                        source
                    ),
                })?;
            }
            Err(error) => {
                fs::remove_dir_all(path).map_err(|source| CliError::Io {
                    path: path.display().to_string(),
                    detail: format!(
                        "git worktree remove could not start: {}; remove_dir_all failed: {}",
                        error, source
                    ),
                })?;
            }
        }
    }
    let job_dir = paths.job_dir(&record.job.job_id);
    if job_dir.exists() {
        fs::remove_dir_all(&job_dir).map_err(|source| CliError::Io {
            path: job_dir.display().to_string(),
            detail: source.to_string(),
        })?;
    } else {
        let legacy_record = paths.legacy_job_path(&record.job.job_id);
        if legacy_record.exists() {
            fs::remove_file(&legacy_record).map_err(|source| CliError::Io {
                path: legacy_record.display().to_string(),
                detail: source.to_string(),
            })?;
        }
    }
    let bundle_path = PathBuf::from(&record.bundle.bundle_path);
    if bundle_path.exists() && bundle_path != job_dir {
        fs::remove_dir_all(&bundle_path).map_err(|source| CliError::Io {
            path: bundle_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    println!("removed job: {}", record.job.job_id);
    println!("removed worktree: {}", path.display());
    Ok(())
}

fn evolve_job(args: &[String]) -> Result<(), CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "create" => {
            let mut propose_args = rest.to_vec();
            propose_args.push("--dry-run".to_string());
            evolve_propose(&propose_args)
        }
        _ => Err(CliError::Usage),
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum DfsCliCommand {
    Help,
    Status(DfsCommonOptions),
    FileSystem {
        file_system_id: DfsFileSystemId,
        options: DfsCommonOptions,
    },
    Object {
        object_id: DfsObjectId,
        options: DfsCommonOptions,
    },
    Conflicts(DfsFilterOptions),
    Repair(DfsFilterOptions),
    Pin(DfsPinCliCommand),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
struct DfsCommonOptions {
    metadata_root: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
struct DfsFilterOptions {
    common: DfsCommonOptions,
    file_system_id: Option<DfsFileSystemId>,
    object_id: Option<DfsObjectId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum DfsPinCliCommand {
    List(DfsPinListOptions),
    Add(DfsPinAddOptions),
    Remove(DfsPinRemoveOptions),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
struct DfsPinListOptions {
    pin_root: Option<PathBuf>,
    file_system_id: Option<DfsFileSystemId>,
    json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct DfsPinAddOptions {
    pin_root: Option<PathBuf>,
    file_system_id: DfsFileSystemId,
    path: String,
    object_id: Option<DfsObjectId>,
    node_id: Option<NodeId>,
    mode: DfsPinMode,
    json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct DfsPinRemoveOptions {
    pin_root: Option<PathBuf>,
    file_system_id: DfsFileSystemId,
    path: String,
    node_id: Option<NodeId>,
    json: bool,
}

fn parse_dfs_command(args: &[String]) -> Result<DfsCliCommand, CliError> {
    match args.split_first() {
        None => Ok(DfsCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(DfsCliCommand::Help)
        }
        Some((command, rest)) if command == "status" => {
            Ok(DfsCliCommand::Status(parse_dfs_common_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "filesystem" | "fs") => {
            let (id, rest) = rest.split_first().ok_or(CliError::Usage)?;
            Ok(DfsCliCommand::FileSystem {
                file_system_id: parse_dfs_file_system_id(id)?,
                options: parse_dfs_common_options(rest)?,
            })
        }
        Some((command, rest)) if command == "object" => {
            let (id, rest) = rest.split_first().ok_or(CliError::Usage)?;
            Ok(DfsCliCommand::Object {
                object_id: parse_dfs_object_id(id)?,
                options: parse_dfs_common_options(rest)?,
            })
        }
        Some((command, rest)) if command == "conflicts" => {
            Ok(DfsCliCommand::Conflicts(parse_dfs_filter_options(rest)?))
        }
        Some((command, rest)) if command == "repair" => {
            Ok(DfsCliCommand::Repair(parse_dfs_filter_options(rest)?))
        }
        Some((command, rest)) if command == "pin" => {
            Ok(DfsCliCommand::Pin(parse_dfs_pin_command(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_dfs_common_options(args: &[String]) -> Result<DfsCommonOptions, CliError> {
    let mut options = DfsCommonOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--metadata-root" => {
                index += 1;
                let Some(path) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.metadata_root = Some(PathBuf::from(path));
            }
            "--json" => options.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_dfs_filter_options(args: &[String]) -> Result<DfsFilterOptions, CliError> {
    let mut options = DfsFilterOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--metadata-root" => {
                index += 1;
                let Some(path) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.common.metadata_root = Some(PathBuf::from(path));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                let Some(file_system_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.file_system_id = Some(parse_dfs_file_system_id(file_system_id)?);
            }
            "--object" => {
                index += 1;
                let Some(object_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.object_id = Some(parse_dfs_object_id(object_id)?);
            }
            "--json" => options.common.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_dfs_pin_command(args: &[String]) -> Result<DfsPinCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "list" => {
            Ok(DfsPinCliCommand::List(parse_dfs_pin_list_options(rest)?))
        }
        Some((command, rest)) if command == "add" => {
            Ok(DfsPinCliCommand::Add(parse_dfs_pin_add_options(rest)?))
        }
        Some((command, rest)) if command == "remove" => Ok(DfsPinCliCommand::Remove(
            parse_dfs_pin_remove_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

fn parse_dfs_pin_list_options(args: &[String]) -> Result<DfsPinListOptions, CliError> {
    let mut options = DfsPinListOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--pin-root" => {
                index += 1;
                let Some(path) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.pin_root = Some(PathBuf::from(path));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                let Some(file_system_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.file_system_id = Some(parse_dfs_file_system_id(file_system_id)?);
            }
            "--json" => options.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_dfs_pin_add_options(args: &[String]) -> Result<DfsPinAddOptions, CliError> {
    let mut pin_root = None;
    let mut file_system_id = None;
    let mut path = None;
    let mut object_id = None;
    let mut node_id = None;
    let mut mode = DfsPinMode::Content;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--pin-root" => {
                index += 1;
                pin_root = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                file_system_id = Some(parse_dfs_file_system_id(
                    args.get(index).ok_or(CliError::Usage)?,
                )?);
            }
            "--path" => {
                index += 1;
                path = Some(non_empty_string(
                    args.get(index).ok_or(CliError::Usage)?,
                    "path",
                )?);
            }
            "--object" => {
                index += 1;
                object_id = Some(parse_dfs_object_id(
                    args.get(index).ok_or(CliError::Usage)?,
                )?);
            }
            "--node" => {
                index += 1;
                node_id = Some(parse_node_id(args.get(index).ok_or(CliError::Usage)?)?);
            }
            "--mode" => {
                index += 1;
                mode = parse_dfs_pin_mode(args.get(index).ok_or(CliError::Usage)?)?;
            }
            "--json" => json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(DfsPinAddOptions {
        pin_root,
        file_system_id: file_system_id.unwrap_or_else(default_dfs_file_system_id),
        path: path.ok_or(CliError::Usage)?,
        object_id,
        node_id,
        mode,
        json,
    })
}

fn parse_dfs_pin_remove_options(args: &[String]) -> Result<DfsPinRemoveOptions, CliError> {
    let mut pin_root = None;
    let mut file_system_id = None;
    let mut path = None;
    let mut node_id = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--pin-root" => {
                index += 1;
                pin_root = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                file_system_id = Some(parse_dfs_file_system_id(
                    args.get(index).ok_or(CliError::Usage)?,
                )?);
            }
            "--path" => {
                index += 1;
                path = Some(non_empty_string(
                    args.get(index).ok_or(CliError::Usage)?,
                    "path",
                )?);
            }
            "--node" => {
                index += 1;
                node_id = Some(parse_node_id(args.get(index).ok_or(CliError::Usage)?)?);
            }
            "--json" => json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(DfsPinRemoveOptions {
        pin_root,
        file_system_id: file_system_id.unwrap_or_else(default_dfs_file_system_id),
        path: path.ok_or(CliError::Usage)?,
        node_id,
        json,
    })
}

fn run_dfs_cli_command(command: DfsCliCommand) -> Result<(), CliError> {
    match command {
        DfsCliCommand::Help => {
            print_dfs_help();
            Ok(())
        }
        DfsCliCommand::Status(options) => {
            let backend = open_dfs_metadata_backend(&options)?;
            let status = DfsOperatorInspector::new(&backend).status()?;
            if options.json {
                print_json("<dfs-status>", &status)?;
            } else {
                println!("DFS status:");
                println!("  filesystems: {}", status.file_system_count);
                println!("  objects: {}", status.object_count);
                println!("  versions: {}", status.version_count);
                println!("  receipts: {}", status.receipt_count);
                println!("  intents: {}", status.intent_count);
                println!("  pending writes: {}", status.pending_write_count);
                println!("  pending repairs: {}", status.pending_repair_count);
                println!("  open conflicts: {}", status.open_conflict_count);
            }
            Ok(())
        }
        DfsCliCommand::FileSystem {
            file_system_id,
            options,
        } => {
            let backend = open_dfs_metadata_backend(&options)?;
            let inspector = DfsOperatorInspector::new(&backend);
            let Some(file_system) = inspector.file_system(&file_system_id)? else {
                return Err(CliError::MissingArtifact {
                    artifact: format!("DFS filesystem {file_system_id}"),
                });
            };
            if options.json {
                print_json("<dfs-filesystem>", &file_system)?;
            } else {
                println!("DFS filesystem: {}", file_system.file_system.file_system_id);
                println!("  name: {}", file_system.file_system.display_name);
                println!("  owner: {}", file_system.file_system.owner_node_id);
                println!(
                    "  subtree policies: {}",
                    file_system.file_system.subtree_policies.len()
                );
                println!("  objects: {}", file_system.object_count);
                println!("  pending repairs: {}", file_system.pending_repair_count);
                println!("  open conflicts: {}", file_system.open_conflict_count);
            }
            Ok(())
        }
        DfsCliCommand::Object { object_id, options } => {
            let backend = open_dfs_metadata_backend(&options)?;
            let inspector = DfsOperatorInspector::new(&backend);
            let Some(object) = inspector.object(&object_id)? else {
                return Err(CliError::MissingArtifact {
                    artifact: format!("DFS object {object_id}"),
                });
            };
            if options.json {
                print_json("<dfs-object>", &object)?;
            } else {
                println!("DFS object: {}", object.object.object_id);
                println!("  filesystem: {}", object.object.file_system_id);
                println!("  path: {}", object.object.path);
                println!("  owner: {}", object.object.owner_node_id);
                println!("  policy: {:?}", object.object.policy);
                println!("  visible version: {}", object.object.version);
                println!(
                    "  content hash: {}",
                    option_label(&object.object.content_hash)
                );
                println!(
                    "  locations: {}",
                    join_labels(&object.object.current_locations)
                );
                println!("  agent state: {:?}", object.agent_state);
                println!("  versions: {}", object.versions.len());
                println!("  receipts: {}", object.receipts.len());
                println!("  intents: {}", object.intents.len());
                println!("  pending repairs: {}", object.repairs.len());
                println!(
                    "  open conflicts: {}",
                    object
                        .conflicts
                        .iter()
                        .filter(|conflict| conflict.status
                            == sinomo_runtime::dfs::DfsConflictStatus::Open)
                        .count()
                );
            }
            Ok(())
        }
        DfsCliCommand::Conflicts(options) => {
            let backend = open_dfs_metadata_backend(&options.common)?;
            let conflicts = DfsOperatorInspector::new(&backend)
                .conflicts(options.file_system_id.as_ref(), options.object_id.as_ref())?;
            if options.common.json {
                print_json("<dfs-conflicts>", &conflicts)?;
            } else if conflicts.is_empty() {
                println!("No DFS conflicts.");
            } else {
                for conflict in conflicts {
                    println!(
                        "{} object={} local={} remote={} status={:?} reason={}",
                        conflict.conflict_id,
                        conflict.object_id,
                        conflict.local_version,
                        conflict.remote_version,
                        conflict.status,
                        conflict.reason
                    );
                }
            }
            Ok(())
        }
        DfsCliCommand::Repair(options) => {
            let backend = open_dfs_metadata_backend(&options.common)?;
            let repairs = DfsOperatorInspector::new(&backend)
                .repairs(options.file_system_id.as_ref(), options.object_id.as_ref())?;
            if options.common.json {
                print_json("<dfs-repair>", &repairs)?;
            } else if repairs.is_empty() {
                println!("No DFS repairs are pending.");
            } else {
                for repair in repairs {
                    println!(
                        "{} object={} version={} missing={} hash={}",
                        repair.intent_id,
                        repair.object_id,
                        repair.version,
                        repair.missing_replica_node_id,
                        option_label(&repair.content_hash)
                    );
                }
            }
            Ok(())
        }
        DfsCliCommand::Pin(command) => run_dfs_pin_command(command),
    }
}

fn run_dfs_pin_command(command: DfsPinCliCommand) -> Result<(), CliError> {
    match command {
        DfsPinCliCommand::List(options) => {
            let store = LocalDfsPinStore::new(dfs_pin_root(options.pin_root)?);
            let pins = store.list_pins(options.file_system_id.as_ref())?;
            if options.json {
                print_json("<dfs-pins>", &pins)?;
            } else if pins.is_empty() {
                println!("No DFS pins.");
            } else {
                for pin in pins {
                    println!(
                        "{} path={} node={} mode={:?} object={}",
                        pin.file_system_id,
                        pin.path,
                        pin.pinned_by_node_id,
                        pin.mode,
                        pin.object_id
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "-".to_string())
                    );
                }
            }
            Ok(())
        }
        DfsPinCliCommand::Add(options) => {
            let store = LocalDfsPinStore::new(dfs_pin_root(options.pin_root)?);
            let node_id = options.node_id.unwrap_or_else(default_dfs_node_id);
            let record = DfsPinRecord {
                file_system_id: options.file_system_id,
                path: options.path,
                object_id: options.object_id,
                pinned_by_node_id: node_id,
                mode: options.mode,
                created_at: OffsetDateTime::now_utc(),
            };
            store.put_pin(&record)?;
            if options.json {
                print_json("<dfs-pin>", &record)?;
            } else {
                println!(
                    "pinned {} path={} node={} mode={:?}",
                    record.file_system_id, record.path, record.pinned_by_node_id, record.mode
                );
            }
            Ok(())
        }
        DfsPinCliCommand::Remove(options) => {
            let store = LocalDfsPinStore::new(dfs_pin_root(options.pin_root)?);
            let removed = store.remove_pin(
                &options.file_system_id,
                &options.path,
                options.node_id.as_ref(),
            )?;
            if options.json {
                print_json(
                    "<dfs-pin-remove>",
                    &serde_json::json!({
                        "file_system_id": options.file_system_id,
                        "path": options.path,
                        "removed": removed,
                    }),
                )?;
            } else if removed {
                println!(
                    "removed pin for {} path={}",
                    options.file_system_id, options.path
                );
            } else {
                println!(
                    "no matching pin for {} path={}",
                    options.file_system_id, options.path
                );
            }
            Ok(())
        }
    }
}

fn open_dfs_metadata_backend(
    options: &DfsCommonOptions,
) -> Result<LocalJsonMetadataBackend, CliError> {
    LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        dfs_metadata_root(options.metadata_root.clone())?,
    )
    .map_err(CliError::from)
}

fn dfs_metadata_root(explicit: Option<PathBuf>) -> Result<PathBuf, CliError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(data_dir.dfs_dir().join("metadata").join("root"))
}

fn dfs_pin_root(explicit: Option<PathBuf>) -> Result<PathBuf, CliError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(data_dir.dfs_dir().join("pins"))
}

fn parse_dfs_file_system_id(value: &str) -> Result<DfsFileSystemId, CliError> {
    DfsFileSystemId::new(value).map_err(|source| CliError::Parse {
        detail: format!("invalid DFS filesystem id `{value}`: {source}"),
    })
}

fn default_dfs_file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::new("sifs").expect("valid default DFS filesystem id")
}

fn parse_dfs_object_id(value: &str) -> Result<DfsObjectId, CliError> {
    DfsObjectId::new(value).map_err(|source| CliError::Parse {
        detail: format!("invalid DFS object id `{value}`: {source}"),
    })
}

fn parse_node_id(value: &str) -> Result<NodeId, CliError> {
    NodeId::new(value).map_err(|source| CliError::Parse {
        detail: format!("invalid node id `{value}`: {source}"),
    })
}

fn parse_dfs_pin_mode(value: &str) -> Result<DfsPinMode, CliError> {
    match value {
        "metadata" => Ok(DfsPinMode::Metadata),
        "content" => Ok(DfsPinMode::Content),
        _ => Err(CliError::Parse {
            detail: format!("invalid DFS pin mode `{value}`"),
        }),
    }
}

fn default_dfs_node_id() -> NodeId {
    cluster_paths_from_defaults()
        .and_then(|paths| read_cluster_state(&paths))
        .map(|state| state.local_node_id)
        .unwrap_or_else(|_| NodeId::new("node_local").expect("valid fallback node id"))
}

fn non_empty_string(value: &str, field: &str) -> Result<String, CliError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CliError::Parse {
            detail: format!("{field} cannot be empty"),
        });
    }
    Ok(trimmed.to_string())
}

fn print_json<T: Serialize>(label: &str, value: &T) -> Result<(), CliError> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|source| CliError::Json {
            path: label.to_string(),
            detail: source.to_string(),
        })?
    );
    Ok(())
}

fn option_label(value: &Option<String>) -> String {
    value.clone().unwrap_or_else(|| "-".to_string())
}

fn join_labels<T: ToString>(values: &[T]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelDefaultOptions {
    provider: String,
    model: String,
    repo: Option<PathBuf>,
}

#[derive(Debug, Clone)]
enum ModelsCliCommand {
    Help,
    List(ModelListOptions),
    Default(ModelDefaultOptions),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelDefaultRepoMode {
    Allow,
    Reject,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelListOptions {
    provider: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CurrentDefaultModel {
    provider: String,
    configured_model: String,
    model: String,
    config_path: PathBuf,
}

fn current_default_model() -> Result<CurrentDefaultModel, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let model_paths = ModelPaths::new(&data_dir);
    let evolution_paths = EvolutionPaths::new(&data_dir);
    let model_config = read_model_config(&model_paths).ok();
    let evolution_config = read_config(&evolution_paths).ok();
    let provider = model_config
        .as_ref()
        .and_then(|config| config.default_provider.clone())
        .or_else(|| {
            evolution_config
                .as_ref()
                .and_then(|config| config.default_provider.clone())
        })
        .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
    let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
        detail: format!("unknown provider `{provider}`"),
    })?;
    let configured_model = model_config
        .as_ref()
        .and_then(|config| config.default_model.clone())
        .or_else(|| {
            evolution_config
                .as_ref()
                .and_then(|config| config.default_model.clone())
        })
        .unwrap_or_else(|| PROVIDER_DEFAULT_MODEL.to_string());
    let model = effective_model_name(&provider, &configured_model);
    Ok(CurrentDefaultModel {
        provider,
        configured_model,
        model,
        config_path: model_paths.config,
    })
}

fn effective_model_name(provider: &str, configured_model: &str) -> String {
    if configured_model != PROVIDER_DEFAULT_MODEL {
        return normalize_model_id_for_provider(provider, configured_model);
    }
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => GitHubCopilotConfig::default().default_model,
        GitHubCopilotCliProvider::PROVIDER_ID => GitHubCopilotCliConfig::default().model,
        CodexCliProvider::PROVIDER_ID => CODEX_CLI_DEFAULT_MODEL.to_string(),
        CODEX_PROVIDER_ID => CODEX_DEFAULT_MODEL.to_string(),
        ClaudeCodeProvider::PROVIDER_ID => CLAUDE_CODE_DEFAULT_MODEL.to_string(),
        AnthropicProvider::PROVIDER_ID => AnthropicConfig::default().default_model,
        OpenAiProvider::PROVIDER_ID => OpenAiConfig::default().default_model,
        GeminiProvider::PROVIDER_ID => GeminiConfig::default().default_model,
        NvidiaProvider::PROVIDER_ID => NvidiaConfig::default().default_model,
        _ => configured_model.to_string(),
    }
}

fn parse_models_command(
    args: &[String],
    repo_mode: ModelDefaultRepoMode,
) -> Result<ModelsCliCommand, CliError> {
    match args.split_first() {
        None => Ok(ModelsCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(ModelsCliCommand::Help)
        }
        Some((command, rest)) if command == "list" => {
            Ok(ModelsCliCommand::List(parse_models_list_options(rest)?))
        }
        Some((command, rest)) if command == "default" => Ok(ModelsCliCommand::Default(
            parse_model_default_options(rest, repo_mode)?,
        )),
        _ => Err(CliError::Usage),
    }
}

fn run_models_command(command: ModelsCliCommand) -> Result<(), CliError> {
    match command {
        ModelsCliCommand::Help => models_help(),
        ModelsCliCommand::List(options) => models_list(options),
        ModelsCliCommand::Default(options) => models_default(options),
    }
}

fn models_help() -> Result<(), CliError> {
    let current = current_default_model()?;
    println!("Current default model:");
    println!("  provider: {}", current.provider);
    println!("  model: {}", current.model);
    println!("  configured model: {}", current.configured_model);
    println!("  config: {}", current.config_path.display());
    println!();
    print_models_help();
    Ok(())
}

fn models_list(options: ModelListOptions) -> Result<(), CliError> {
    let explicit_provider = options.provider.is_some();
    let providers = match options.provider {
        Some(provider) => {
            vec![
                normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
                    detail: format!("unknown provider `{provider}`"),
                })?,
            ]
        }
        None => default_model_provider_ids()
            .iter()
            .map(|value| value.to_string())
            .collect(),
    };
    for provider in providers {
        let models = if explicit_provider {
            Some(provider_available_model_ids(&provider)?)
        } else {
            provider_available_model_ids(&provider).ok()
        };
        if let Some(models) = models {
            if !models.is_empty() {
                println!("{provider}:");
                for model in models {
                    println!("  {model}");
                }
            }
        }
    }
    Ok(())
}

fn parse_models_list_options(args: &[String]) -> Result<ModelListOptions, CliError> {
    match args {
        [] => Ok(ModelListOptions { provider: None }),
        [flag, provider] if flag == "--provider" => Ok(ModelListOptions {
            provider: Some(provider.clone()),
        }),
        _ => Err(CliError::Usage),
    }
}

fn models_default(options: ModelDefaultOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    models_default_with_data_dir(options, &data_dir)
}

fn models_default_with_data_dir(
    options: ModelDefaultOptions,
    data_dir: &SinomoDataDir,
) -> Result<(), CliError> {
    data_dir.ensure_root_layout()?;
    let model_paths = ModelPaths::new(data_dir);
    model_paths.ensure()?;
    let config = ModelConfig {
        default_provider: Some(options.provider),
        default_model: Some(options.model),
    };
    write_json(&model_paths.config, &config)?;

    if let Some(repo) = options.repo {
        let evolution_paths = EvolutionPaths::new(data_dir);
        evolution_paths.ensure()?;
        let existing = read_config(&evolution_paths).ok();
        let config = EvolutionConfig {
            repo: repo.display().to_string(),
            default_provider: config.default_provider.clone(),
            default_model: config.default_model.clone(),
        };
        let config = if let Some(existing) = existing {
            EvolutionConfig {
                repo: config.repo,
                default_provider: config.default_provider.or(existing.default_provider),
                default_model: config.default_model.or(existing.default_model),
            }
        } else {
            config
        };
        write_json(&evolution_paths.config, &config)?;
    }

    println!(
        "default provider: {}",
        config.default_provider.as_deref().unwrap_or("")
    );
    println!(
        "default model: {}",
        config.default_model.as_deref().unwrap_or("")
    );
    println!("config: {}", model_paths.config.display());
    Ok(())
}

fn parse_model_default_options(
    args: &[String],
    repo_mode: ModelDefaultRepoMode,
) -> Result<ModelDefaultOptions, CliError> {
    let provider = flag_value(args, "--provider").ok_or(CliError::Usage)?;
    let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
        detail: format!("unknown provider `{provider}`"),
    })?;
    let model = flag_value(args, "--model").ok_or(CliError::Usage)?;
    let model = normalize_model_id_for_provider(&provider, &model);
    let repo = match (repo_mode, flag_value(args, "--repo")) {
        (ModelDefaultRepoMode::Allow, repo) => repo.map(PathBuf::from),
        (ModelDefaultRepoMode::Reject, None) => None,
        (ModelDefaultRepoMode::Reject, Some(_)) => return Err(CliError::Usage),
    };
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" | "--model" => index += 2,
            "--repo" if repo_mode == ModelDefaultRepoMode::Allow => index += 2,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(ModelDefaultOptions {
        provider,
        model,
        repo,
    })
}

fn normalize_model_id_for_provider(provider: &str, model: &str) -> String {
    match provider {
        NvidiaProvider::PROVIDER_ID => normalize_nvidia_model_id(model),
        _ => model.to_string(),
    }
}

fn default_model_provider_ids() -> &'static [&'static str] {
    &[
        GitHubCopilotProvider::PROVIDER_ID,
        GitHubCopilotCliProvider::PROVIDER_ID,
        CodexCliProvider::PROVIDER_ID,
        CODEX_PROVIDER_ID,
        ClaudeCodeProvider::PROVIDER_ID,
        AnthropicProvider::PROVIDER_ID,
        OpenAiProvider::PROVIDER_ID,
        GeminiProvider::PROVIDER_ID,
        NvidiaProvider::PROVIDER_ID,
    ]
}

fn normalize_model_provider(provider: &str) -> Option<String> {
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => Some(GitHubCopilotProvider::PROVIDER_ID.to_string()),
        GitHubCopilotCliProvider::PROVIDER_ID => {
            Some(GitHubCopilotCliProvider::PROVIDER_ID.to_string())
        }
        CodexCliProvider::PROVIDER_ID => Some(CodexCliProvider::PROVIDER_ID.to_string()),
        CODEX_PROVIDER_ID => Some(CODEX_PROVIDER_ID.to_string()),
        ClaudeCodeProvider::PROVIDER_ID | "anthropic-claude-code" => {
            Some(ClaudeCodeProvider::PROVIDER_ID.to_string())
        }
        AnthropicProvider::PROVIDER_ID | "claude" => {
            Some(AnthropicProvider::PROVIDER_ID.to_string())
        }
        OpenAiProvider::PROVIDER_ID | "openai-api" => Some(OpenAiProvider::PROVIDER_ID.to_string()),
        GeminiProvider::PROVIDER_ID | "google-gemini" => {
            Some(GeminiProvider::PROVIDER_ID.to_string())
        }
        NvidiaProvider::PROVIDER_ID | "nvidia-nim" => Some(NvidiaProvider::PROVIDER_ID.to_string()),
        _ => None,
    }
}

fn provider_available_model_ids(provider: &str) -> Result<Vec<String>, CliError> {
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => {
            let (provider, _) = github_copilot_provider()?;
            provider.list_models().map_err(CliError::LanguageModel)
        }
        GitHubCopilotCliProvider::PROVIDER_ID
        | CodexCliProvider::PROVIDER_ID
        | ClaudeCodeProvider::PROVIDER_ID => Err(CliError::Parse {
            detail: format!("provider `{provider}` does not use Sinomo-managed authentication"),
        }),
        CODEX_PROVIDER_ID => {
            let (provider, _) = codex_provider()?;
            provider.list_models().map_err(CliError::LanguageModel)
        }
        AnthropicProvider::PROVIDER_ID => {
            let (provider, _) = anthropic_provider()?;
            provider.api_key()?;
            Ok(vec![AnthropicConfig::default().default_model])
        }
        OpenAiProvider::PROVIDER_ID => {
            let (provider, _) = openai_provider()?;
            provider.api_key()?;
            Ok(vec![OpenAiConfig::default().default_model])
        }
        GeminiProvider::PROVIDER_ID => {
            let (provider, _) = gemini_provider()?;
            provider.api_key()?;
            Ok(vec![GeminiConfig::default().default_model])
        }
        NvidiaProvider::PROVIDER_ID => {
            let (provider, _) = nvidia_provider()?;
            provider.list_models().map_err(CliError::LanguageModel)
        }
        _ => Err(CliError::Parse {
            detail: format!("unknown provider `{provider}`"),
        }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UsageOptions {
    provider: Option<String>,
    json: bool,
    help: bool,
}

fn parse_usage_options(args: &[String]) -> Result<UsageOptions, CliError> {
    let mut provider = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "help" | "--help" | "-h" => {
                return Ok(UsageOptions {
                    provider: None,
                    json: false,
                    help: true,
                });
            }
            "--json" => {
                json = true;
                index += 1;
            }
            "--provider" => {
                let value = args.get(index + 1).ok_or_else(|| CliError::Parse {
                    detail: "--provider requires a provider id".to_string(),
                })?;
                provider = Some(value.clone());
                index += 2;
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unknown usage option `{value}`"),
                });
            }
        }
    }
    Ok(UsageOptions {
        provider,
        json,
        help: false,
    })
}

fn usage(options: UsageOptions) -> Result<(), CliError> {
    if options.help {
        print_usage_help();
        return Ok(());
    }
    let explicit_provider = options.provider.is_some();
    let providers = match options.provider {
        Some(provider) => {
            vec![
                normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
                    detail: format!("unknown provider `{provider}`"),
                })?,
            ]
        }
        None => subscription_usage_provider_ids()
            .iter()
            .map(|value| value.to_string())
            .collect(),
    };

    let mut snapshots = Vec::new();
    for provider in providers {
        match provider_usage(&provider) {
            Ok(snapshot) => snapshots.push(snapshot),
            Err(error) if explicit_provider => return Err(error),
            Err(_) => {}
        }
    }

    if options.json {
        let value = if explicit_provider {
            snapshots
                .into_iter()
                .next()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|source| CliError::Json {
                    path: "<usage>".to_string(),
                    detail: source.to_string(),
                })?
                .unwrap_or_else(|| serde_json::json!(null))
        } else {
            serde_json::to_value(&snapshots).map_err(|source| CliError::Json {
                path: "<usage>".to_string(),
                detail: source.to_string(),
            })?
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|source| CliError::Json {
                path: "<usage>".to_string(),
                detail: source.to_string(),
            })?
        );
        return Ok(());
    }

    if snapshots.is_empty() {
        println!("No authenticated subscription provider usage is available.");
        println!(
            "Run `sinomo usage --provider codex` or `sinomo usage --provider github-copilot` for details."
        );
        return Ok(());
    }
    for (index, snapshot) in snapshots.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print_provider_usage(snapshot);
    }
    Ok(())
}

fn subscription_usage_provider_ids() -> &'static [&'static str] {
    &[CODEX_PROVIDER_ID, GitHubCopilotProvider::PROVIDER_ID]
}

fn provider_usage(provider: &str) -> Result<LanguageModelProviderUsage, CliError> {
    match provider {
        CODEX_PROVIDER_ID => {
            let (provider, _) = codex_provider()?;
            provider.usage().map_err(CliError::LanguageModel)
        }
        GitHubCopilotProvider::PROVIDER_ID => {
            let (provider, _) = github_copilot_provider()?;
            provider.usage().map_err(CliError::LanguageModel)
        }
        _ => Err(CliError::Parse {
            detail: format!("provider `{provider}` does not expose subscription usage"),
        }),
    }
}

fn print_provider_usage(snapshot: &LanguageModelProviderUsage) {
    println!("{}:", snapshot.provider);
    if let Some(plan) = &snapshot.plan {
        println!("  plan: {plan}");
    }
    if let Some(credits) = &snapshot.credits {
        let mut parts = Vec::new();
        if let Some(has_credits) = credits.has_credits {
            parts.push(format!("has_credits={has_credits}"));
        }
        if let Some(unlimited) = credits.unlimited {
            parts.push(format!("unlimited={unlimited}"));
        }
        if let Some(balance) = &credits.balance {
            parts.push(format!("balance={balance}"));
        }
        if !parts.is_empty() {
            println!("  credits: {}", parts.join(", "));
        }
    }
    if snapshot.limits.is_empty() {
        println!("  limits: unavailable");
        return;
    }
    println!("  limits:");
    for limit in &snapshot.limits {
        let label = limit.name.as_deref().unwrap_or(&limit.id);
        let scope = limit.scope.as_deref().unwrap_or("usage");
        let mut parts = Vec::new();
        if let Some(used) = limit.used_percent {
            parts.push(format!("{used:.1}% used"));
        } else if let Some(remaining) = limit.remaining_percent {
            parts.push(format!("{remaining:.1}% remaining"));
        }
        if let Some(remaining) = limit.remaining {
            parts.push(format!("remaining={remaining:.1}"));
        }
        if let Some(entitlement) = limit.entitlement {
            parts.push(format!("entitlement={entitlement:.1}"));
        }
        if limit.unlimited == Some(true) {
            parts.push("unlimited".to_string());
        }
        if limit.exhausted == Some(true) {
            parts.push("exhausted".to_string());
        }
        if let Some(reset_at) = limit.reset_at_unix {
            parts.push(format!("resets={}", format_usage_reset_at(reset_at)));
        }
        println!("    {label} ({scope}): {}", parts.join(", "));
    }
}

fn format_usage_reset_at(reset_at_unix: i64) -> String {
    OffsetDateTime::from_unix_timestamp(reset_at_unix)
        .ok()
        .and_then(|datetime| datetime.format(&Rfc3339).ok())
        .unwrap_or_else(|| reset_at_unix.to_string())
}

#[derive(Clone, Debug)]
struct ProposeOptions {
    objective: String,
    provider: Option<String>,
    model: Option<String>,
    repo: Option<PathBuf>,
    allow: Vec<String>,
    verify: Vec<String>,
    target_ring: TrustRing,
    dry_run: bool,
    horizon: Option<String>,
    budget_policy: Option<String>,
    reserve_floors: Vec<String>,
    approval_policy: Option<String>,
    max_wall_time: Option<String>,
    pause_after_proposal: bool,
    approve_campaign: bool,
}

fn parse_propose_options(args: &[String]) -> Result<ProposeOptions, CliError> {
    let mut objective = None;
    let mut provider = None;
    let mut model = None;
    let mut repo = None;
    let mut allow = Vec::new();
    let mut verify = Vec::new();
    let mut target_ring = TrustRing::R1TrustedRuntime;
    let mut dry_run = false;
    let mut horizon = None;
    let mut budget_policy = None;
    let mut reserve_floors = Vec::new();
    let mut approval_policy = None;
    let mut max_wall_time = None;
    let mut pause_after_proposal = false;
    let mut approve_campaign = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--objective" => {
                index += 1;
                objective = args.get(index).cloned();
            }
            "--provider" => {
                index += 1;
                provider = args.get(index).cloned();
            }
            "--model" => {
                index += 1;
                model = args.get(index).cloned();
            }
            "--repo" => {
                index += 1;
                repo = args.get(index).map(PathBuf::from);
            }
            "--allow" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    allow.push(value.clone());
                }
            }
            "--verify" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    verify.push(value.clone());
                }
            }
            "--target-ring" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    target_ring = parse_trust_ring(value)?;
                }
            }
            "--horizon" => {
                index += 1;
                horizon = args.get(index).cloned();
            }
            "--budget-policy" => {
                index += 1;
                budget_policy = args.get(index).cloned();
            }
            "--reserve" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    reserve_floors.push(value.clone());
                }
            }
            "--approval" => {
                index += 1;
                approval_policy = args.get(index).cloned();
            }
            "--max-wall-time" => {
                index += 1;
                max_wall_time = args.get(index).cloned();
            }
            "--pause-after-proposal" => {
                pause_after_proposal = true;
            }
            "--approve" => {
                approve_campaign = true;
            }
            "--dry-run" | "--no-model" => {
                dry_run = true;
            }
            value if value.starts_with("--") => return Err(CliError::Usage),
            value => {
                objective = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(ProposeOptions {
        objective: objective.ok_or(CliError::Usage)?,
        provider,
        model,
        repo,
        allow,
        verify,
        target_ring,
        dry_run,
        horizon,
        budget_policy,
        reserve_floors,
        approval_policy,
        max_wall_time,
        pause_after_proposal,
        approve_campaign,
    })
}

#[derive(Clone, Debug)]
struct ScheduleOptions {
    campaign_id: String,
    resources: Vec<String>,
    estimates: Vec<String>,
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct BackgroundTickOptions {
    scheduler_service_id: String,
    campaign_ids: Vec<String>,
    external_workloads: Vec<String>,
    external_workload_estimates: Vec<String>,
    external_human_checkpoints: Vec<String>,
    resources: Vec<String>,
    estimates: Vec<String>,
    max_concurrent_campaigns: usize,
    max_concurrent_workloads: usize,
    idle_window_open: bool,
    reset_window_open: bool,
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct IterationOptions {
    campaign_id: String,
    task_id: Option<String>,
    intended_paths: Vec<String>,
    verification_commands: Vec<String>,
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct StageEvaluationOptions {
    campaign_id: String,
    job_id: EvolutionJobId,
    task_id: String,
    failure_class: Option<EvolutionFailureClass>,
    root_cause_summary: Option<String>,
    what_was_learned: Vec<String>,
    resource_usage: Vec<ResourceUsageEstimate>,
}

fn parse_stage_evaluation_options(args: &[String]) -> Result<StageEvaluationOptions, CliError> {
    let mut campaign_id = None;
    let mut job_id = None;
    let mut task_id = None;
    let mut failure_class = None;
    let mut root_cause_summary = None;
    let mut what_was_learned = Vec::new();
    let mut resource_usage = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--job" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                job_id =
                    Some(
                        value
                            .parse::<EvolutionJobId>()
                            .map_err(|source| CliError::Parse {
                                detail: format!("invalid evolution job id `{value}`: {source}"),
                            })?,
                    );
            }
            "--task" => {
                index += 1;
                task_id = args.get(index).cloned();
                if task_id.is_none() {
                    return Err(CliError::Usage);
                }
            }
            "--failure-class" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                failure_class = Some(parse_failure_class(value)?);
            }
            "--root-cause" => {
                index += 1;
                root_cause_summary = args.get(index).cloned();
                if root_cause_summary.is_none() {
                    return Err(CliError::Usage);
                }
            }
            "--learned" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    what_was_learned.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--usage" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    resource_usage.push(parse_stage_resource_usage(value, task_id.as_deref())?);
                } else {
                    return Err(CliError::Usage);
                }
            }
            value if value.starts_with("--") => return Err(CliError::Usage),
            value => {
                if campaign_id.is_some() {
                    return Err(CliError::Usage);
                }
                campaign_id = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(StageEvaluationOptions {
        campaign_id: campaign_id.ok_or(CliError::Usage)?,
        job_id: job_id.ok_or(CliError::Usage)?,
        task_id: task_id.ok_or(CliError::Usage)?,
        failure_class,
        root_cause_summary,
        what_was_learned,
        resource_usage,
    })
}

fn parse_failure_class(value: &str) -> Result<EvolutionFailureClass, CliError> {
    match value {
        "build-test-failure" => Ok(EvolutionFailureClass::BuildTestFailure),
        "eval-regression" => Ok(EvolutionFailureClass::EvalRegression),
        "artifact-quality-failure" => Ok(EvolutionFailureClass::ArtifactQualityFailure),
        "rollout-failure" => Ok(EvolutionFailureClass::RolloutFailure),
        "resource-exhaustion" => Ok(EvolutionFailureClass::ResourceExhaustion),
        "repeated-implementation-failure" => {
            Ok(EvolutionFailureClass::RepeatedImplementationFailure)
        }
        "foundation-blocker" => Ok(EvolutionFailureClass::FoundationBlocker),
        "ambiguous-metric" => Ok(EvolutionFailureClass::AmbiguousMetric),
        "wrong-task-selection" => Ok(EvolutionFailureClass::WrongTaskSelection),
        "inconclusive-evidence" => Ok(EvolutionFailureClass::InconclusiveEvidence),
        _ => Err(CliError::Parse {
            detail: format!("unknown failure class `{value}`"),
        }),
    }
}

fn parse_stage_resource_usage(
    value: &str,
    default_task_id: Option<&str>,
) -> Result<ResourceUsageEstimate, CliError> {
    let parts: Vec<&str> = value.split(':').collect();
    let (task_id, resource, estimated, actual) = match parts.as_slice() {
        [resource, estimated] => (
            default_task_id.ok_or_else(|| CliError::Parse {
                detail: format!(
                    "usage `{value}` needs --task first or task:resource:estimated[:actual]"
                ),
            })?,
            resource.trim(),
            estimated.trim(),
            None,
        ),
        [resource, estimated, actual] => (
            default_task_id.ok_or_else(|| CliError::Parse {
                detail: format!(
                    "usage `{value}` needs --task first or task:resource:estimated[:actual]"
                ),
            })?,
            resource.trim(),
            estimated.trim(),
            Some(actual.trim()),
        ),
        [task_id, resource, estimated, actual] => (
            task_id.trim(),
            resource.trim(),
            estimated.trim(),
            Some(actual.trim()),
        ),
        _ => {
            return Err(CliError::Parse {
                detail: format!(
                    "usage `{value}` must look like resource:estimated[:actual] or task:resource:estimated:actual"
                ),
            });
        }
    };
    let estimated_units = estimated.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid estimated usage units in `{value}`: {source}"),
    })?;
    let actual_units = actual
        .map(|actual| {
            actual.parse::<u64>().map_err(|source| CliError::Parse {
                detail: format!("invalid actual usage units in `{value}`: {source}"),
            })
        })
        .transpose()?;
    Ok(ResourceUsageEstimate {
        task_id: task_id.to_string(),
        resource: resource.to_string(),
        estimated_units,
        actual_units,
    })
}

fn parse_iteration_options(args: &[String]) -> Result<IterationOptions, CliError> {
    let mut campaign_id = None;
    let mut task_id = None;
    let mut intended_paths = Vec::new();
    let mut verification_commands = Vec::new();
    let mut dry_run = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--task" => {
                index += 1;
                task_id = args.get(index).cloned();
                if task_id.is_none() {
                    return Err(CliError::Usage);
                }
            }
            "--path" | "--allow" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    intended_paths.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--verify" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    verification_commands.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--dry-run" | "--preview" => {
                dry_run = true;
            }
            value if value.starts_with("--") => return Err(CliError::Usage),
            value => {
                if campaign_id.is_some() {
                    return Err(CliError::Usage);
                }
                campaign_id = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(IterationOptions {
        campaign_id: campaign_id.ok_or(CliError::Usage)?,
        task_id,
        intended_paths,
        verification_commands,
        dry_run,
    })
}

fn parse_background_tick_options(args: &[String]) -> Result<BackgroundTickOptions, CliError> {
    let mut scheduler_service_id = "sinomo-cluster-scheduler".to_string();
    let mut campaign_ids = Vec::new();
    let mut external_workloads = Vec::new();
    let mut external_workload_estimates = Vec::new();
    let mut external_human_checkpoints = Vec::new();
    let mut resources = Vec::new();
    let mut estimates = Vec::new();
    let mut max_concurrent_campaigns = 1usize;
    let mut max_concurrent_workloads = 1usize;
    let mut idle_window_open = false;
    let mut reset_window_open = false;
    let mut dry_run = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--cluster" | "--cluster-id" => {
                return Err(CliError::Parse {
                    detail: "cluster identifiers are not supported; scheduler operations always target the current cluster".to_string(),
                });
            }
            "--scheduler-service" | "--scheduler-service-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "scheduler service id must not be empty".to_string(),
                    });
                }
                scheduler_service_id = value.clone();
            }
            "--campaign" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    campaign_ids.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--workload" | "--external-workload" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    external_workloads.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--workload-estimate" | "--external-estimate" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    external_workload_estimates.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--workload-human-checkpoint" | "--external-human-checkpoint" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    external_human_checkpoints.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--resource" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    resources.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--estimate" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    estimates.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--max-concurrent" | "--max-concurrent-campaigns" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                max_concurrent_campaigns =
                    value.parse::<usize>().map_err(|source| CliError::Parse {
                        detail: format!(
                            "invalid max concurrent campaign count `{value}`: {source}"
                        ),
                    })?;
                if max_concurrent_campaigns == 0 {
                    return Err(CliError::Parse {
                        detail: "max concurrent campaign count must be at least 1".to_string(),
                    });
                }
                max_concurrent_workloads = max_concurrent_workloads.max(max_concurrent_campaigns);
            }
            "--max-concurrent-workloads" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                max_concurrent_workloads =
                    value.parse::<usize>().map_err(|source| CliError::Parse {
                        detail: format!(
                            "invalid max concurrent workload count `{value}`: {source}"
                        ),
                    })?;
                if max_concurrent_workloads == 0 {
                    return Err(CliError::Parse {
                        detail: "max concurrent workload count must be at least 1".to_string(),
                    });
                }
            }
            "--idle-window-open" => idle_window_open = true,
            "--reset-window-open" => reset_window_open = true,
            "--dry-run" | "--preview" => dry_run = true,
            value if value.starts_with("--") => return Err(CliError::Usage),
            value => campaign_ids.push(value.to_string()),
        }
        index += 1;
    }
    Ok(BackgroundTickOptions {
        scheduler_service_id,
        campaign_ids,
        external_workloads,
        external_workload_estimates,
        external_human_checkpoints,
        resources,
        estimates,
        max_concurrent_campaigns,
        max_concurrent_workloads,
        idle_window_open,
        reset_window_open,
        dry_run,
    })
}

fn parse_schedule_options(args: &[String]) -> Result<ScheduleOptions, CliError> {
    let mut campaign_id = None;
    let mut resources = Vec::new();
    let mut estimates = Vec::new();
    let mut dry_run = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--resource" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    resources.push(value.clone());
                }
            }
            "--estimate" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    estimates.push(value.clone());
                }
            }
            "--dry-run" | "--preview" => {
                dry_run = true;
            }
            value if value.starts_with("--") => return Err(CliError::Usage),
            value => {
                if campaign_id.is_some() {
                    return Err(CliError::Usage);
                }
                campaign_id = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(ScheduleOptions {
        campaign_id: campaign_id.ok_or(CliError::Usage)?,
        resources,
        estimates,
        dry_run,
    })
}

fn parse_schedule_resources(
    values: &[String],
    campaign: &EvolutionCampaign,
) -> Result<Vec<ResourceClassDefinition>, CliError> {
    if values.is_empty() {
        return Ok(campaign
            .budget_policy
            .reserve_floors
            .iter()
            .map(|floor| ResourceClassDefinition {
                resource_id: floor.resource.clone(),
                unit: "percent".to_string(),
                total_units: 100,
                used_units: 0,
                reserve_floor_percent: floor.floor_percent,
                reset_window: "current reset window".to_string(),
                notes: "derived from campaign reserve-floor config".to_string(),
            })
            .collect());
    }

    values
        .iter()
        .map(|value| parse_schedule_resource(value, campaign))
        .collect()
}

fn parse_background_tick_resources(
    values: &[String],
    campaigns: &[EvolutionCampaign],
) -> Result<Vec<ResourceClassDefinition>, CliError> {
    if values.is_empty() {
        let mut resources = Vec::new();
        for campaign in campaigns {
            for floor in &campaign.budget_policy.reserve_floors {
                if let Some(resource) =
                    resources
                        .iter_mut()
                        .find(|resource: &&mut ResourceClassDefinition| {
                            resource.resource_id == floor.resource
                        })
                {
                    resource.reserve_floor_percent =
                        resource.reserve_floor_percent.max(floor.floor_percent);
                } else {
                    resources.push(ResourceClassDefinition {
                        resource_id: floor.resource.clone(),
                        unit: "percent".to_string(),
                        total_units: 100,
                        used_units: 0,
                        reserve_floor_percent: floor.floor_percent,
                        reset_window: "current reset window".to_string(),
                        notes: "derived from background campaign reserve floors".to_string(),
                    });
                }
            }
        }
        return Ok(resources);
    }
    let fallback = campaigns
        .first()
        .cloned()
        .unwrap_or_else(|| EvolutionCampaign {
            campaign_id: "background_tick".to_string(),
            status: EvolutionCampaignStatus::Scheduled,
            objective_card: sinomo_evolution::ObjectiveCard {
                objective_id: "objective_background_tick".to_string(),
                human_text: "background scheduler tick".to_string(),
                interpreted_goal: "background scheduler tick".to_string(),
                non_goals: Vec::new(),
                assumptions: Vec::new(),
                constraints: Vec::new(),
                success_metrics: Vec::new(),
                failure_conditions: Vec::new(),
                safety_notes: Vec::new(),
                resource_policy: "background".to_string(),
                approval_policy: "manual-only".to_string(),
                initial_horizon: "24h".to_string(),
                assumptions_needing_human_review: Vec::new(),
            },
            stages: Vec::new(),
            near_term_tasks: Vec::new(),
            iteration_results: Vec::new(),
            decisions: Vec::new(),
            budget_policy: sinomo_evolution::CampaignBudgetPolicy {
                policy_id: "background".to_string(),
                reserve_floors: Vec::new(),
                scheduling_notes: Vec::new(),
                confidence: "unknown".to_string(),
                debt_note: "unknown".to_string(),
            },
            resource_schedule_preview: sinomo_evolution::ResourceSchedulePreview {
                horizon: "24h".to_string(),
                entries: Vec::new(),
            },
            approval_checkpoint: sinomo_evolution::ApprovalCheckpoint {
                policy: "manual-only".to_string(),
                status: sinomo_evolution::ApprovalCheckpointStatus::Approved,
                required_before: "background tick".to_string(),
                editable_fields: Vec::new(),
                possible_actions: Vec::new(),
            },
            target_ring: TrustRing::R1TrustedRuntime,
            dry_run: true,
            capability_registry_required: Some(false),
            created_at: "unknown-time".to_string(),
            updated_at: "unknown-time".to_string(),
            source_repo: None,
        });
    values
        .iter()
        .map(|value| parse_schedule_resource(value, &fallback))
        .collect()
}

fn parse_schedule_resource(
    value: &str,
    campaign: &EvolutionCampaign,
) -> Result<ResourceClassDefinition, CliError> {
    let mut parts = value.split(':');
    let resource_id = parts.next().unwrap_or_default().trim();
    let usage = parts.next().ok_or_else(|| CliError::Parse {
        detail: format!("resource `{value}` must look like resource:used/total[:reset-window]"),
    })?;
    let reset_window = parts.next().unwrap_or("current reset window");
    if parts.next().is_some() || resource_id.is_empty() {
        return Err(CliError::Parse {
            detail: format!("resource `{value}` must look like resource:used/total[:reset-window]"),
        });
    }
    let (used, total) = usage.split_once('/').ok_or_else(|| CliError::Parse {
        detail: format!("resource `{value}` must include used/total units"),
    })?;
    let used_units = used.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid used units in resource `{value}`: {source}"),
    })?;
    let total_units = total.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid total units in resource `{value}`: {source}"),
    })?;
    if total_units == 0 || used_units > total_units {
        return Err(CliError::Parse {
            detail: format!("resource `{value}` must have 0 <= used <= total and total > 0"),
        });
    }
    let reserve_floor_percent = campaign
        .budget_policy
        .reserve_floors
        .iter()
        .find(|floor| floor.resource == resource_id)
        .map(|floor| floor.floor_percent)
        .unwrap_or(0);
    Ok(ResourceClassDefinition {
        resource_id: resource_id.to_string(),
        unit: "percent".to_string(),
        total_units,
        used_units,
        reserve_floor_percent,
        reset_window: reset_window.to_string(),
        notes: "provided by schedule dry-run command".to_string(),
    })
}

fn parse_schedule_estimates(
    values: &[String],
    campaign: &EvolutionCampaign,
    resources: &[ResourceClassDefinition],
) -> Result<Vec<ResourceUsageEstimate>, CliError> {
    if values.is_empty() {
        let Some(task) = campaign.near_term_tasks.first() else {
            return Ok(Vec::new());
        };
        let resource = resources
            .first()
            .map(|resource| resource.resource_id.clone())
            .unwrap_or_else(|| "local-agent-wall-time".to_string());
        return Ok(vec![ResourceUsageEstimate {
            task_id: task.task_id.clone(),
            resource,
            estimated_units: 10,
            actual_units: None,
        }]);
    }

    values
        .iter()
        .map(|value| parse_schedule_estimate(value, campaign))
        .collect()
}

fn parse_background_tick_estimates(
    values: &[String],
    campaigns: &[EvolutionCampaign],
) -> Result<Vec<ResourceUsageEstimate>, CliError> {
    if values.is_empty() {
        return Ok(Vec::new());
    }
    let Some(first_campaign) = campaigns.first() else {
        return Err(CliError::Parse {
            detail: "background estimates need at least one campaign".to_string(),
        });
    };
    values
        .iter()
        .map(|value| parse_schedule_estimate(value, first_campaign))
        .collect()
}

fn parse_schedule_estimate(
    value: &str,
    campaign: &EvolutionCampaign,
) -> Result<ResourceUsageEstimate, CliError> {
    let parts: Vec<&str> = value.split(':').collect();
    let (task_id, resource, units) = match parts.as_slice() {
        [resource, units] => {
            let Some(task) = campaign.near_term_tasks.first() else {
                return Err(CliError::Parse {
                    detail: "cannot infer task id because campaign has no near-term tasks"
                        .to_string(),
                });
            };
            (task.task_id.clone(), resource.trim(), units.trim())
        }
        [task_id, resource, units] => (task_id.trim().to_string(), resource.trim(), units.trim()),
        _ => {
            return Err(CliError::Parse {
                detail: format!(
                    "estimate `{value}` must look like resource:units or task:resource:units"
                ),
            });
        }
    };
    if resource.is_empty() || units.is_empty() {
        return Err(CliError::Parse {
            detail: format!("estimate `{value}` must include resource and units"),
        });
    }
    let estimated_units = units.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid estimated units in `{value}`: {source}"),
    })?;
    Ok(ResourceUsageEstimate {
        task_id,
        resource: resource.to_string(),
        estimated_units,
        actual_units: None,
    })
}

#[derive(Clone, Debug)]
struct CliMutationModel {
    provider: String,
    model: String,
    data_dir: SinomoDataDir,
}

impl MutationModel for CliMutationModel {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
    ) -> Result<sinomo_evolution::ModelMutationResult, MutationModelError> {
        match self.provider.as_str() {
            GitHubCopilotProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
                let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
                    client_id: env::var("SINOMO_COPILOT_CLIENT_ID")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().client_id),
                    github_host: env::var("SINOMO_COPILOT_GITHUB_HOST")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().github_host),
                    api_base_url: env::var("SINOMO_COPILOT_API_BASE_URL")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().api_base_url),
                    default_model: model_or_default(
                        &self.model,
                        GitHubCopilotConfig::default().default_model,
                    ),
                    token_store_path: Some(credentials_path),
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            GitHubCopilotCliProvider::PROVIDER_ID => {
                let provider = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
                    model: model_or_default(&self.model, GitHubCopilotCliConfig::default().model),
                    ..GitHubCopilotCliConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            CodexCliProvider::PROVIDER_ID => {
                let provider = CodexCliProvider::new(CodexCliConfig {
                    model: model_or_default(&self.model, CODEX_CLI_DEFAULT_MODEL.to_string()),
                    working_dir: Some(workspace.path.clone()),
                    ..CodexCliConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            CODEX_PROVIDER_ID => {
                let provider = codex_provider_for_model(&self.model);
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            ClaudeCodeProvider::PROVIDER_ID | "anthropic-claude-code" => {
                let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
                    model: model_or_default(&self.model, CLAUDE_CODE_DEFAULT_MODEL.to_string()),
                    working_dir: Some(workspace.path.clone()),
                    ..ClaudeCodeConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            AnthropicProvider::PROVIDER_ID | "claude" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(AnthropicProvider::PROVIDER_ID);
                let provider = AnthropicProvider::new(AnthropicConfig {
                    default_model: model_or_default(
                        &self.model,
                        AnthropicConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..AnthropicConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            OpenAiProvider::PROVIDER_ID | "openai-api" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(OpenAiProvider::PROVIDER_ID);
                let provider = OpenAiProvider::new(OpenAiConfig {
                    default_model: model_or_default(
                        &self.model,
                        OpenAiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..OpenAiConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            GeminiProvider::PROVIDER_ID | "google-gemini" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GeminiProvider::PROVIDER_ID);
                let provider = GeminiProvider::new(GeminiConfig {
                    default_model: model_or_default(
                        &self.model,
                        GeminiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..GeminiConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            NvidiaProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(NvidiaProvider::PROVIDER_ID);
                let provider = NvidiaProvider::new(NvidiaConfig {
                    api_base_url: env::var("SINOMO_NVIDIA_API_BASE_URL")
                        .unwrap_or_else(|_| NvidiaConfig::default().api_base_url),
                    default_model: model_or_default(
                        &self.model,
                        NvidiaConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            _ => Err(MutationModelError::LanguageModel(
                LanguageModelError::ProviderInvocationFailed {
                    provider: self.provider.clone(),
                    model: self.model.clone(),
                    detail: "unknown evolution model provider".to_string(),
                },
            )),
        }
    }
}

fn model_or_default(model: &str, default_model: String) -> String {
    if model == PROVIDER_DEFAULT_MODEL {
        default_model
    } else {
        model.to_string()
    }
}

fn codex_provider_for_model(model: &str) -> CodexProvider {
    let default_model = model_or_default(model, CODEX_DEFAULT_MODEL.to_string());
    let config_default = OpenAiResponsesCodexConfig::default();
    let client_config = OpenAiResponsesCodexConfig {
        api_base_url: env::var("SINOMO_CODEX_API_BASE_URL")
            .unwrap_or_else(|_| config_default.api_base_url.clone()),
        auth_issuer: env::var("SINOMO_CODEX_AUTH_ISSUER")
            .unwrap_or_else(|_| config_default.auth_issuer.clone()),
        client_id: env::var("SINOMO_CODEX_CLIENT_ID")
            .unwrap_or_else(|_| config_default.client_id.clone()),
        client_version: config_default.client_version,
        default_model: default_model.clone(),
        credentials_path: config_default.credentials_path,
    };
    CodexProvider::new(
        CodexConfig {
            default_model,
            ..CodexConfig::default()
        },
        OpenAiResponsesCodexClient::new(client_config),
        CodexToolRegistry::new(),
        DefaultCodexPromptBuilder::default(),
        DefaultCodexContextManager::default(),
    )
}

fn write_report_record(paths: &EvolutionPaths, report: &EvolutionReport) -> Result<(), CliError> {
    let record = StoredEvolutionJob {
        job: report.job.clone(),
        candidate_workspace: report.candidate_workspace.clone(),
        bundle: report.bundle.clone(),
        verification_outcome: report
            .scorecard
            .as_ref()
            .map(|scorecard| scorecard.outcome.clone()),
        changed_files: report.changed_files.clone(),
        decision: None,
    };
    write_stored_job(paths, &record)?;
    if let Some(job_path) = &record.bundle.job_path {
        write_json(Path::new(job_path), &record.job)?;
    }
    Ok(())
}

fn write_bundle_manifest(record: &StoredEvolutionJob) -> Result<(), CliError> {
    write_json(
        &PathBuf::from(&record.bundle.bundle_path).join("manifest.json"),
        &record.bundle,
    )
}

fn write_stored_job(paths: &EvolutionPaths, record: &StoredEvolutionJob) -> Result<(), CliError> {
    write_json(&paths.job_path(&record.job.job_id), record)
}

fn read_stored_job(path: &Path) -> Result<StoredEvolutionJob, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn evolution_report_from_record(record: StoredEvolutionJob) -> Result<EvolutionReport, CliError> {
    let scorecard = if let Some(scorecard_path) = &record.bundle.scorecard_path {
        let path = PathBuf::from(scorecard_path);
        if path.exists() {
            Some(read_json_file::<EvaluationScorecard>(&path)?)
        } else {
            None
        }
    } else {
        None
    };
    Ok(EvolutionReport {
        job: record.job,
        candidate_workspace: record.candidate_workspace,
        bundle: record.bundle,
        scorecard,
        changed_files: record.changed_files,
    })
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn list_job_records(paths: &EvolutionPaths) -> Result<Vec<StoredEvolutionJob>, CliError> {
    if !paths.jobs.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in fs::read_dir(&paths.jobs).map_err(|source| CliError::Io {
        path: paths.jobs.display().to_string(),
        detail: source.to_string(),
    })? {
        let entry = entry.map_err(|source| CliError::Io {
            path: paths.jobs.display().to_string(),
            detail: source.to_string(),
        })?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            let record_path = entry_path.join("record.json");
            if record_path.exists() {
                records.push(read_stored_job(&record_path)?);
            }
        } else if entry_path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            records.push(read_stored_job(&entry_path)?);
        }
    }
    Ok(records)
}

fn resolve_job_from_args(
    args: &[String],
    waiting_only: bool,
) -> Result<(EvolutionPaths, StoredEvolutionJob), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    if let Some(job_id) = job_flag(args)? {
        let job_path = paths.job_path(&job_id);
        let record_path = if job_path.exists() {
            job_path
        } else {
            paths.legacy_job_path(&job_id)
        };
        let record = read_stored_job(&record_path)?;
        return Ok((paths, record));
    }

    let records = list_job_records(&paths)?;
    let candidates: Vec<_> = records
        .into_iter()
        .filter(|record| {
            if waiting_only {
                record.job.status == EvolutionJobStatus::AwaitingApproval
            } else {
                !is_terminal_status(&record.job.status)
            }
        })
        .collect();
    match candidates.as_slice() {
        [record] => Ok((paths, record.clone())),
        [] => Err(CliError::NoActiveJob),
        _ => Err(CliError::AmbiguousJob {
            job_ids: candidates
                .iter()
                .map(|record| record.job.job_id.to_string())
                .collect(),
        }),
    }
}

fn ensure_awaiting_approval(record: &StoredEvolutionJob) -> Result<(), CliError> {
    if record.job.status == EvolutionJobStatus::AwaitingApproval {
        return Ok(());
    }
    Err(CliError::InvalidJobState {
        job_id: record.job.job_id.to_string(),
        status: job_status_label(&record.job.status).to_string(),
        expected: "awaiting-approval".to_string(),
    })
}

fn read_config(paths: &EvolutionPaths) -> Result<EvolutionConfig, CliError> {
    let content = fs::read_to_string(&paths.config).map_err(|source| CliError::Io {
        path: paths.config.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: paths.config.display().to_string(),
        detail: source.to_string(),
    })
}

fn read_model_config(paths: &ModelPaths) -> Result<ModelConfig, CliError> {
    let content = fs::read_to_string(&paths.config).map_err(|source| CliError::Io {
        path: paths.config.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: paths.config.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(path, content).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_campaign_bundle(
    paths: &EvolutionPaths,
    campaign: &EvolutionCampaign,
) -> Result<CampaignBundlePaths, CliError> {
    let bundle = CampaignBundlePaths::new(paths, &campaign.campaign_id);
    fs::create_dir_all(&bundle.root).map_err(|source| CliError::Io {
        path: bundle.root.display().to_string(),
        detail: source.to_string(),
    })?;
    write_json(&bundle.campaign_json, campaign)?;
    write_json(&bundle.objective_card_json, &campaign.objective_card)?;
    write_json(&bundle.stages_json, &campaign.stages)?;
    write_json(&bundle.near_term_tasks_json, &campaign.near_term_tasks)?;
    write_json(
        &bundle.resource_schedule_preview_json,
        &campaign.resource_schedule_preview,
    )?;
    write_json(
        &bundle.approval_checkpoint_json,
        &campaign.approval_checkpoint,
    )?;
    let mut decisions = String::new();
    for decision in &campaign.decisions {
        decisions.push_str(
            &serde_json::to_string(decision).map_err(|source| CliError::Json {
                path: bundle.decisions_jsonl.display().to_string(),
                detail: source.to_string(),
            })?,
        );
        decisions.push('\n');
    }
    fs::write(&bundle.decisions_jsonl, decisions).map_err(|source| CliError::Io {
        path: bundle.decisions_jsonl.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(bundle)
}

fn read_campaign(paths: &EvolutionPaths, campaign_id: &str) -> Result<EvolutionCampaign, CliError> {
    let path = paths.campaign_path(campaign_id);
    let content = fs::read_to_string(&path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_campaign_schedule_dry_run(
    paths: &EvolutionPaths,
    campaign: &mut EvolutionCampaign,
    dry_run: &ResourceSchedulerDryRun,
) -> Result<CampaignBundlePaths, CliError> {
    let previous_status = campaign.status.clone();
    campaign.status = match dry_run.aggregate_decision {
        CampaignScheduleDecision::PausedBudget => EvolutionCampaignStatus::PausedBudget,
        CampaignScheduleDecision::Delayed => EvolutionCampaignStatus::Scheduled,
        CampaignScheduleDecision::Scheduled => EvolutionCampaignStatus::Scheduled,
    };
    campaign.updated_at = dry_run.generated_at.clone();
    let decision_kind = if dry_run.aggregate_decision == CampaignScheduleDecision::PausedBudget {
        CampaignDecisionKind::Paused
    } else if matches!(previous_status, EvolutionCampaignStatus::PausedBudget) {
        CampaignDecisionKind::Resumed
    } else {
        CampaignDecisionKind::ResourcePolicySelected
    };
    campaign.decisions.push(EvolutionCampaignDecision {
        decision_id: format!("decision_{}", campaign.decisions.len() + 1),
        kind: decision_kind,
        summary: match dry_run.aggregate_decision {
            CampaignScheduleDecision::PausedBudget => {
                "Paused because one or more resource leases would violate reserve floors"
                    .to_string()
            }
            CampaignScheduleDecision::Delayed => {
                "Delayed because the scheduler had no runnable lease requests".to_string()
            }
            CampaignScheduleDecision::Scheduled => {
                "Scheduled because all previewed resource leases preserve reserve floors"
                    .to_string()
            }
        },
        rationale:
            "Phase 1 resource scheduler dry run gates dispatch before any agent work starts."
                .to_string(),
        evidence_refs: vec![
            "scheduler-dry-run.json".to_string(),
            "resource-ledger.json".to_string(),
            "lease-decisions.jsonl".to_string(),
        ],
        decided_by: "sinomo evolve schedule --dry-run".to_string(),
        decided_at: dry_run.generated_at.clone(),
    });

    let bundle = write_campaign_bundle(paths, campaign)?;
    write_json(&bundle.resource_ledger_json, &dry_run.resource_ledger)?;
    write_json(&bundle.scheduler_dry_run_json, dry_run)?;
    write_jsonl(&bundle.lease_requests_jsonl, &dry_run.lease_requests)?;
    write_jsonl(&bundle.lease_decisions_jsonl, &dry_run.lease_decisions)?;
    Ok(bundle)
}

fn write_campaign_iteration_dry_run(
    paths: &EvolutionPaths,
    campaign: &mut EvolutionCampaign,
    plan: &ScheduledIterationPlan,
) -> Result<CampaignBundlePaths, CliError> {
    let now = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    campaign.status = EvolutionCampaignStatus::Scheduled;
    campaign.updated_at = now.clone();
    campaign.decisions.push(EvolutionCampaignDecision {
        decision_id: format!("decision_{}", campaign.decisions.len() + 1),
        kind: CampaignDecisionKind::TaskSelected,
        summary: format!(
            "Prepared dry-run scheduled iteration {} for task {}",
            plan.iteration_id, plan.task.task_id
        ),
        rationale:
            "Phase 2 strategy-plan execution writes an inspectable handoff before any agent work starts."
                .to_string(),
        evidence_refs: vec![
            "iteration-plan.json".to_string(),
            "iteration-request.json".to_string(),
            "iteration-dispatches.jsonl".to_string(),
        ],
        decided_by: "sinomo evolve iterate --dry-run".to_string(),
        decided_at: now,
    });

    let bundle = write_campaign_bundle(paths, campaign)?;
    write_json(&bundle.iteration_plan_json, plan)?;
    write_json(&bundle.iteration_request_json, &plan.evolution_request)?;
    write_jsonl(&bundle.iteration_dispatches_jsonl, &[plan.clone()])?;
    Ok(bundle)
}

fn write_campaign_stage_evaluation(
    paths: &EvolutionPaths,
    update: &StageEvaluationUpdate,
) -> Result<CampaignBundlePaths, CliError> {
    let bundle = write_campaign_bundle(paths, &update.campaign)?;
    write_json(&bundle.stage_scorecard_json, &update.stage_scorecard)?;
    append_jsonl(&bundle.stage_scorecards_jsonl, &update.stage_scorecard)?;
    if let Some(adaptation) = &update.adaptation {
        write_json(&bundle.adaptation_json, adaptation)?;
        append_jsonl(&bundle.adaptations_jsonl, adaptation)?;
    }
    Ok(bundle)
}

fn write_background_scheduling_plan(
    paths: &EvolutionPaths,
    plan: &BackgroundSchedulingPlan,
) -> Result<BackgroundSchedulerPaths, CliError> {
    let background_paths = BackgroundSchedulerPaths::new(paths);
    background_paths.ensure()?;
    write_json(&background_paths.plan_json, plan)?;
    write_json(&background_paths.status_json, plan)?;
    for event in &plan.timer_events {
        append_jsonl(&background_paths.events_jsonl, event)?;
    }
    for update in &plan.campaigns {
        let mut campaign = update.campaign.clone();
        campaign.updated_at = plan.generated_at.clone();
        campaign
            .decisions
            .push(background_campaign_decision(plan, update));
        let bundle = write_campaign_bundle(paths, &campaign)?;
        if let Some(iteration) = &update.scheduled_iteration {
            write_json(&bundle.iteration_plan_json, iteration)?;
            write_json(&bundle.iteration_request_json, &iteration.evolution_request)?;
            append_jsonl(&bundle.iteration_dispatches_jsonl, iteration)?;
        }
    }
    Ok(background_paths)
}

fn background_campaign_decision(
    plan: &BackgroundSchedulingPlan,
    update: &sinomo_evolution::BackgroundCampaignUpdate,
) -> EvolutionCampaignDecision {
    let kind = match update.action {
        sinomo_evolution::BackgroundSchedulingAction::PauseForBudget => {
            CampaignDecisionKind::Paused
        }
        sinomo_evolution::BackgroundSchedulingAction::DispatchIterationDryRun
            if matches!(
                update.previous_status,
                EvolutionCampaignStatus::PausedBudget
            ) =>
        {
            CampaignDecisionKind::Resumed
        }
        sinomo_evolution::BackgroundSchedulingAction::DispatchIterationDryRun => {
            CampaignDecisionKind::TaskSelected
        }
        sinomo_evolution::BackgroundSchedulingAction::HoldHumanCheckpoint => {
            CampaignDecisionKind::Paused
        }
        _ => CampaignDecisionKind::ResourcePolicySelected,
    };
    EvolutionCampaignDecision {
        decision_id: format!("decision_{}", update.campaign.decisions.len() + 1),
        kind,
        summary: format!("Background scheduler action: {:?}", update.action),
        rationale: update.reason.clone(),
        evidence_refs: vec![
            "background-scheduler/scheduler-plan.json".to_string(),
            "background-scheduler/scheduler-events.jsonl".to_string(),
        ],
        decided_by: "sinomo evolve tick --dry-run".to_string(),
        decided_at: plan.generated_at.clone(),
    }
}

fn read_background_tick_campaigns(
    paths: &EvolutionPaths,
    campaign_ids: &[String],
) -> Result<Vec<EvolutionCampaign>, CliError> {
    if !campaign_ids.is_empty() {
        return campaign_ids
            .iter()
            .map(|campaign_id| read_campaign(paths, campaign_id))
            .collect();
    }
    let mut campaigns = Vec::new();
    if !paths.campaigns.exists() {
        return Ok(campaigns);
    }
    let entries = fs::read_dir(&paths.campaigns).map_err(|source| CliError::Io {
        path: paths.campaigns.display().to_string(),
        detail: source.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| CliError::Io {
            path: paths.campaigns.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = entry.path().join("campaign.json");
        if path.exists() {
            let content = fs::read_to_string(&path).map_err(|source| CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
            campaigns.push(
                serde_json::from_str(&content).map_err(|source| CliError::Json {
                    path: path.display().to_string(),
                    detail: source.to_string(),
                })?,
            );
        }
    }
    campaigns.sort_by(|left: &EvolutionCampaign, right: &EvolutionCampaign| {
        left.campaign_id.cmp(&right.campaign_id)
    });
    Ok(campaigns)
}

fn parse_background_tick_external_workloads(
    workload_specs: &[String],
    estimate_specs: &[String],
    checkpoint_specs: &[String],
) -> Result<Vec<ClusterLongHorizonWorkload>, CliError> {
    let mut workloads = Vec::new();
    for spec in workload_specs {
        let workload = parse_external_workload_spec(spec)?;
        if workloads
            .iter()
            .any(|existing: &ClusterLongHorizonWorkload| {
                existing.system == workload.system && existing.workload_id == workload.workload_id
            })
        {
            return Err(CliError::Parse {
                detail: format!(
                    "duplicate external workload `{}`",
                    external_workload_key(&workload)
                ),
            });
        }
        workloads.push(workload);
    }
    for spec in estimate_specs {
        let (system, workload_id, estimate) = parse_external_workload_estimate(spec)?;
        let workload = ensure_external_workload(&mut workloads, &system, &workload_id);
        workload.resource_estimates.push(estimate);
    }
    for spec in checkpoint_specs {
        let (system, workload_id) = parse_external_workload_key(spec)?;
        let workload = ensure_external_workload(&mut workloads, &system, &workload_id);
        workload.human_checkpoint_required = true;
        if workload.status == ClusterLongHorizonWorkloadStatus::Ready {
            workload.status = ClusterLongHorizonWorkloadStatus::PausedHuman;
        }
    }
    Ok(workloads)
}

fn ensure_external_workload<'a>(
    workloads: &'a mut Vec<ClusterLongHorizonWorkload>,
    system: &str,
    workload_id: &str,
) -> &'a mut ClusterLongHorizonWorkload {
    if let Some(index) = workloads
        .iter()
        .position(|workload| workload.system == system && workload.workload_id == workload_id)
    {
        return &mut workloads[index];
    }
    workloads.push(ClusterLongHorizonWorkload {
        workload_id: workload_id.to_string(),
        system: system.to_string(),
        title: workload_id.to_string(),
        status: ClusterLongHorizonWorkloadStatus::Ready,
        resource_estimates: Vec::new(),
        human_checkpoint_required: false,
        terminal: false,
        evidence_refs: Vec::new(),
    });
    workloads
        .last_mut()
        .expect("just inserted external workload")
}

fn parse_external_workload_spec(value: &str) -> Result<ClusterLongHorizonWorkload, CliError> {
    let parts: Vec<&str> = value.splitn(4, ':').collect();
    let [system, workload_id, status, title] = match parts.as_slice() {
        [system, workload_id, status] => [*system, *workload_id, *status, *workload_id],
        [system, workload_id, status, title] => [*system, *workload_id, *status, *title],
        _ => {
            return Err(CliError::Parse {
                detail: format!(
                    "external workload `{value}` must look like system:workload_id:status[:title]"
                ),
            });
        }
    };
    if system.trim().is_empty() || workload_id.trim().is_empty() {
        return Err(CliError::Parse {
            detail: format!("external workload `{value}` needs non-empty system and workload id"),
        });
    }
    let status = parse_external_workload_status(status)?;
    let terminal = matches!(
        status,
        ClusterLongHorizonWorkloadStatus::Complete | ClusterLongHorizonWorkloadStatus::Cancelled
    );
    Ok(ClusterLongHorizonWorkload {
        workload_id: workload_id.trim().to_string(),
        system: system.trim().to_string(),
        title: title.trim().to_string(),
        status,
        resource_estimates: Vec::new(),
        human_checkpoint_required: false,
        terminal,
        evidence_refs: vec!["sinomo evolve tick external workload input".to_string()],
    })
}

fn parse_external_workload_estimate(
    value: &str,
) -> Result<(String, String, ResourceUsageEstimate), CliError> {
    let parts: Vec<&str> = value.split(':').collect();
    let [system, workload_id, resource, units] = match parts.as_slice() {
        [system, workload_id, resource, units] => [*system, *workload_id, *resource, *units],
        _ => {
            return Err(CliError::Parse {
                detail: format!(
                    "external workload estimate `{value}` must look like system:workload_id:resource:units"
                ),
            });
        }
    };
    if system.trim().is_empty() || workload_id.trim().is_empty() || resource.trim().is_empty() {
        return Err(CliError::Parse {
            detail: format!(
                "external workload estimate `{value}` needs non-empty system, workload id, and resource"
            ),
        });
    }
    let estimated_units = units
        .trim()
        .parse::<u64>()
        .map_err(|source| CliError::Parse {
            detail: format!("invalid external workload estimate `{value}`: {source}"),
        })?;
    Ok((
        system.trim().to_string(),
        workload_id.trim().to_string(),
        ResourceUsageEstimate {
            task_id: workload_id.trim().to_string(),
            resource: resource.trim().to_string(),
            estimated_units,
            actual_units: None,
        },
    ))
}

fn parse_external_workload_key(value: &str) -> Result<(String, String), CliError> {
    let parts: Vec<&str> = value.split(':').collect();
    let [system, workload_id] = match parts.as_slice() {
        [system, workload_id] => [*system, *workload_id],
        _ => {
            return Err(CliError::Parse {
                detail: format!(
                    "external workload key `{value}` must look like system:workload_id"
                ),
            });
        }
    };
    if system.trim().is_empty() || workload_id.trim().is_empty() {
        return Err(CliError::Parse {
            detail: format!(
                "external workload key `{value}` needs non-empty system and workload id"
            ),
        });
    }
    Ok((system.trim().to_string(), workload_id.trim().to_string()))
}

fn parse_external_workload_status(
    value: &str,
) -> Result<ClusterLongHorizonWorkloadStatus, CliError> {
    match value {
        "ready" => Ok(ClusterLongHorizonWorkloadStatus::Ready),
        "running" => Ok(ClusterLongHorizonWorkloadStatus::Running),
        "paused-budget" => Ok(ClusterLongHorizonWorkloadStatus::PausedBudget),
        "paused-human" => Ok(ClusterLongHorizonWorkloadStatus::PausedHuman),
        "complete" => Ok(ClusterLongHorizonWorkloadStatus::Complete),
        "cancelled" | "canceled" => Ok(ClusterLongHorizonWorkloadStatus::Cancelled),
        _ => Err(CliError::Parse {
            detail: format!(
                "unknown external workload status `{value}`; expected ready, running, paused-budget, paused-human, complete, or cancelled"
            ),
        }),
    }
}

fn external_workload_key(workload: &ClusterLongHorizonWorkload) -> String {
    format!("{}:{}", workload.system, workload.workload_id)
}

fn append_jsonl<T: Serialize>(path: &Path, value: &T) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    let line = serde_json::to_string(value).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    use std::io::Write;
    writeln!(file, "{line}").map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_jsonl<T: Serialize>(path: &Path, values: &[T]) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut lines = String::new();
    for value in values {
        lines.push_str(
            &serde_json::to_string(value).map_err(|source| CliError::Json {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?,
        );
        lines.push('\n');
    }
    fs::write(path, lines).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn parse_reserve_floors(values: &[String]) -> Result<Vec<ReserveFloor>, CliError> {
    values
        .iter()
        .map(|value| parse_reserve_floor(value))
        .collect()
}

fn parse_reserve_floor(value: &str) -> Result<ReserveFloor, CliError> {
    let (resource, floor) = value.rsplit_once(':').ok_or_else(|| CliError::Parse {
        detail: format!("reserve floor `{value}` must look like resource:30%"),
    })?;
    let floor = floor
        .trim_end_matches('%')
        .parse::<u8>()
        .map_err(|source| CliError::Parse {
            detail: format!("invalid reserve floor `{value}`: {source}"),
        })?;
    if floor > 100 || resource.trim().is_empty() {
        return Err(CliError::Parse {
            detail: format!(
                "reserve floor `{value}` must have a non-empty resource and 0-100 percent"
            ),
        });
    }
    Ok(ReserveFloor {
        resource: resource.to_string(),
        floor_percent: floor,
    })
}

fn println_campaign_proposal(campaign: &EvolutionCampaign, bundle: &CampaignBundlePaths) {
    println!("campaign: {}", campaign.campaign_id);
    println!("status: {:?}", campaign.status);
    println!("objective: {}", campaign.objective_card.human_text);
    println!("horizon: {}", campaign.objective_card.initial_horizon);
    println!("budget policy: {}", campaign.budget_policy.policy_id);
    println!("approval: {:?}", campaign.approval_checkpoint.status);
    println!("bundle: {}", bundle.root.display());
    println!("campaign json: {}", bundle.campaign_json.display());
    println!("stages: {}", campaign.stages.len());
    println!("near-term tasks: {}", campaign.near_term_tasks.len());
}

fn println_scheduler_dry_run(dry_run: &ResourceSchedulerDryRun, bundle: &CampaignBundlePaths) {
    println!("campaign: {}", dry_run.campaign_id);
    println!("scheduler decision: {:?}", dry_run.aggregate_decision);
    println!("preview horizon: {}", dry_run.preview_horizon);
    println!("current reset window: {}", dry_run.current_reset_window);
    println!("resource ledger: {}", bundle.resource_ledger_json.display());
    println!(
        "scheduler dry run: {}",
        bundle.scheduler_dry_run_json.display()
    );
    println!("lease requests: {}", dry_run.lease_requests.len());
    println!("lease decisions: {}", dry_run.lease_decisions.len());
    for task in &dry_run.task_decisions {
        println!(
            "task {}: {:?} ({})",
            task.task_id, task.decision, task.explanation
        );
    }
}

fn println_iteration_dry_run(plan: &ScheduledIterationPlan, bundle: &CampaignBundlePaths) {
    println!("campaign: {}", plan.campaign_id);
    println!("iteration: {}", plan.iteration_id);
    println!("task: {}", plan.task.task_id);
    println!("dry run: {}", plan.evolution_request.dry_run);
    println!("iteration plan: {}", bundle.iteration_plan_json.display());
    println!(
        "iteration request: {}",
        bundle.iteration_request_json.display()
    );
    println!(
        "iteration dispatches: {}",
        bundle.iteration_dispatches_jsonl.display()
    );
}

fn println_stage_evaluation(update: &StageEvaluationUpdate, bundle: &CampaignBundlePaths) {
    println!("campaign: {}", update.stage_scorecard.campaign_id);
    println!("stage: {}", update.stage_scorecard.stage_id);
    println!("task: {}", update.stage_scorecard.task_id);
    println!("outcome: {:?}", update.stage_scorecard.outcome);
    println!("promotion: {:?}", update.stage_scorecard.promotion);
    println!("campaign status: {:?}", update.campaign.status);
    println!("stage scorecard: {}", bundle.stage_scorecard_json.display());
    println!(
        "stage scorecards: {}",
        bundle.stage_scorecards_jsonl.display()
    );
    if update.adaptation.is_some() {
        println!("adaptation: {}", bundle.adaptation_json.display());
        println!("adaptations: {}", bundle.adaptations_jsonl.display());
    }
}

fn println_background_tick(plan: &BackgroundSchedulingPlan, paths: &BackgroundSchedulerPaths) {
    println!("background tick: {}", plan.tick_id);
    println!("scheduler service: {}", plan.scheduler_service_id);
    println!("dry run: {}", plan.dry_run);
    println!("campaign concurrency cap: {}", plan.concurrency_cap);
    println!(
        "workload concurrency cap: {}",
        plan.workload_concurrency_cap
    );
    println!(
        "cluster resources: {}",
        plan.cluster_resources_after_tick.len()
    );
    println!("campaign updates: {}", plan.campaigns.len());
    println!(
        "external workload updates: {}",
        plan.external_workloads.len()
    );
    println!("active campaigns: {}", plan.active_campaigns.len());
    println!("held campaigns: {}", plan.held_campaigns.len());
    println!("active workloads: {}", plan.active_workloads.len());
    println!("held workloads: {}", plan.held_workloads.len());
    println!("human checkpoints: {}", plan.human_checkpoints.len());
    println!("timer events: {}", plan.timer_events.len());
    println!("scheduler plan: {}", paths.plan_json.display());
    println!("scheduler status: {}", paths.status_json.display());
    println!("scheduler events: {}", paths.events_jsonl.display());
    for update in &plan.campaigns {
        println!(
            "campaign {}: {:?} ({})",
            update.campaign.campaign_id, update.action, update.reason
        );
    }
    for update in &plan.external_workloads {
        println!(
            "workload {}:{}: {:?} ({})",
            update.workload.system, update.workload.workload_id, update.action, update.reason
        );
    }
}

fn println_report(report: &EvolutionReport) {
    let verification = report
        .scorecard
        .as_ref()
        .map(|scorecard| outcome_label(&scorecard.outcome))
        .unwrap_or("not-run");
    println!("job: {}", report.job.job_id);
    println!("status: {}", job_status_label(&report.job.status));
    println!("worktree: {}", report.candidate_workspace.path.display());
    println!("bundle: {}", report.bundle.bundle_path);
    println!("verification: {verification}");
}

fn print_job_status(record: &StoredEvolutionJob) {
    println!("job: {}", record.job.job_id);
    println!("status: {}", job_status_label(&record.job.status));
    println!("objective: {}", record.job.objective);
    println!("worktree: {}", record.candidate_workspace.path.display());
    println!("bundle: {}", record.bundle.bundle_path);
    println!(
        "verification: {}",
        record
            .verification_outcome
            .as_ref()
            .map(outcome_label)
            .unwrap_or("unknown")
    );
    if let Some(decision) = &record.decision {
        println!("decision: {}", decision_outcome_label(&decision.outcome));
    }
    println!();
}

fn job_flag(args: &[String]) -> Result<Option<EvolutionJobId>, CliError> {
    flag_value(args, "--job")
        .map(EvolutionJobId::new)
        .transpose()
        .map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })
}

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn parse_trust_ring(value: &str) -> Result<TrustRing, CliError> {
    match value {
        "r0" | "r0-continuity-root" => Ok(TrustRing::R0ContinuityRoot),
        "r1" | "r1-trusted-runtime" => Ok(TrustRing::R1TrustedRuntime),
        "r2" | "r2-trusted-service" => Ok(TrustRing::R2TrustedService),
        "r3" | "r3-sandboxed-module" => Ok(TrustRing::R3SandboxedModule),
        "r4" | "r4-disposable-workspace" => Ok(TrustRing::R4DisposableWorkspace),
        "r5" | "r5-model-provider" => Ok(TrustRing::R5ModelProvider),
        _ => Err(CliError::Parse {
            detail: format!("unknown trust ring `{value}`"),
        }),
    }
}

fn is_terminal_status(status: &EvolutionJobStatus) -> bool {
    matches!(
        status,
        EvolutionJobStatus::Approved
            | EvolutionJobStatus::Rejected
            | EvolutionJobStatus::Restored
            | EvolutionJobStatus::Failed
    )
}

fn job_status_label(status: &EvolutionJobStatus) -> &'static str {
    match status {
        EvolutionJobStatus::AwaitingApproval => "awaiting-approval",
        EvolutionJobStatus::Approved => "approved",
        EvolutionJobStatus::Rejected => "rejected",
        EvolutionJobStatus::Created => "created",
        EvolutionJobStatus::Failed => "failed",
        _ => "in-progress",
    }
}

fn outcome_label(outcome: &EvaluationOutcome) -> &'static str {
    match outcome {
        EvaluationOutcome::Passed => "passed",
        EvaluationOutcome::Failed => "failed",
        EvaluationOutcome::Inconclusive => "inconclusive",
    }
}

fn decision_outcome_label(outcome: &CandidateDecisionOutcome) -> &'static str {
    match outcome {
        CandidateDecisionOutcome::Approved => "approved",
        CandidateDecisionOutcome::Rejected => "rejected",
        CandidateDecisionOutcome::Restored => "restored",
    }
}

fn safe_file_stem(value: &str) -> String {
    let stem: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let stem = stem.trim_matches('-');
    if stem.is_empty() {
        "command".to_string()
    } else {
        stem.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderName {
    GitHubCopilot,
    Codex,
    Anthropic,
    OpenAi,
    Gemini,
    Nvidia,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderCliCommand {
    Help,
    Auth(ProviderName),
    Test(ProviderName),
}

fn parse_provider_command(args: &[String]) -> Result<ProviderCliCommand, CliError> {
    match args {
        [] => return Ok(ProviderCliCommand::Help),
        [command] if matches!(command.as_str(), "help" | "--help" | "-h") => {
            return Ok(ProviderCliCommand::Help);
        }
        _ => {}
    }

    let [action, provider] = args else {
        return Err(CliError::Usage);
    };

    if !matches!(action.as_str(), "auth" | "test") {
        return Err(CliError::Usage);
    }

    let provider = match provider.as_str() {
        "github-copilot" => ProviderName::GitHubCopilot,
        "codex" => ProviderName::Codex,
        "anthropic" => ProviderName::Anthropic,
        "openai" => ProviderName::OpenAi,
        "gemini" => ProviderName::Gemini,
        "nvidia" => ProviderName::Nvidia,
        _ => {
            return Err(CliError::Parse {
                detail: format!("unknown provider `{provider}`"),
            });
        }
    };

    match action.as_str() {
        "auth" => Ok(ProviderCliCommand::Auth(provider)),
        "test" => Ok(ProviderCliCommand::Test(provider)),
        _ => unreachable!("provider action was checked before provider parsing"),
    }
}

fn run_provider_command(command: ProviderCliCommand) -> Result<(), CliError> {
    match command {
        ProviderCliCommand::Help => {
            print_providers_help();
            Ok(())
        }
        ProviderCliCommand::Auth(ProviderName::GitHubCopilot) => auth_github_copilot(),
        ProviderCliCommand::Test(ProviderName::GitHubCopilot) => test_github_copilot(),
        ProviderCliCommand::Auth(ProviderName::Codex) => auth_codex(),
        ProviderCliCommand::Test(ProviderName::Codex) => test_codex(),
        ProviderCliCommand::Auth(ProviderName::Anthropic) => auth_anthropic(),
        ProviderCliCommand::Test(ProviderName::Anthropic) => test_anthropic(),
        ProviderCliCommand::Auth(ProviderName::OpenAi) => auth_openai(),
        ProviderCliCommand::Test(ProviderName::OpenAi) => test_openai(),
        ProviderCliCommand::Auth(ProviderName::Gemini) => auth_gemini(),
        ProviderCliCommand::Test(ProviderName::Gemini) => test_gemini(),
        ProviderCliCommand::Auth(ProviderName::Nvidia) => auth_nvidia(),
        ProviderCliCommand::Test(ProviderName::Nvidia) => test_nvidia(),
    }
}

pub fn auth_github_copilot() -> Result<(), CliError> {
    let (provider, credentials_path) = github_copilot_provider()?;
    let device = provider.device_login_start()?;

    println!("Open this URL in your browser:");
    println!("{}", device.verification_uri);
    println!();
    println!("Enter this code:");
    println!("{}", device.user_code);
    println!();
    println!("Waiting for GitHub authorization...");

    let started = Instant::now();
    let expires_in = if device.expires_in == 0 {
        Duration::from_secs(900)
    } else {
        Duration::from_secs(device.expires_in)
    };
    let mut interval = Duration::from_secs(device.interval.max(5));

    loop {
        if started.elapsed() >= expires_in {
            return Err(CliError::AuthExpired);
        }

        thread::sleep(interval);
        let poll = provider.device_login_poll(&device.device_code)?;
        if poll.access_token.is_some() {
            provider.test_connection()?;
            println!(
                "GitHub Copilot credentials saved to {}",
                credentials_path.display()
            );
            return Ok(());
        }

        match poll.error.as_deref() {
            Some("authorization_pending") | None => {}
            Some("slow_down") => {
                interval += Duration::from_secs(poll.interval.unwrap_or(5));
            }
            Some("expired_token") => return Err(CliError::AuthExpired),
            Some("access_denied") => return Err(CliError::AuthDenied),
            Some(error) => {
                return Err(CliError::AuthProvider {
                    detail: poll.error_description.unwrap_or_else(|| error.to_string()),
                });
            }
        }
    }
}

pub fn test_github_copilot() -> Result<(), CliError> {
    let (provider, credentials_path) = github_copilot_provider()?;
    println!(
        "Testing GitHub Copilot credentials at {}",
        credentials_path.display()
    );
    provider.github_token()?;
    println!("GitHub token: ok");
    provider.test_connection()?;
    println!("Copilot API models endpoint: ok");
    println!("GitHub Copilot provider connection: ok");
    Ok(())
}

pub fn auth_anthropic() -> Result<(), CliError> {
    let (provider, credentials_path) = anthropic_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "Anthropic",
        &[
            "SINOMO_ANTHROPIC_API_KEY",
            "ANTHROPIC_API_KEY",
            "CLAUDE_API_KEY",
            "SINOMO_CLAUDE_API_KEY",
        ],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing Anthropic provider connection...");
    provider.test_connection()?;
    println!(
        "Anthropic credentials saved to {}",
        credentials_path.display()
    );
    Ok(())
}

pub fn test_anthropic() -> Result<(), CliError> {
    let (provider, credentials_path) = anthropic_provider()?;
    println!(
        "Testing Anthropic credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("Anthropic API key: ok");
    provider.test_connection()?;
    println!("Anthropic provider connection: ok");
    Ok(())
}

pub fn auth_openai() -> Result<(), CliError> {
    let (provider, credentials_path) = openai_provider()?;
    let (api_key, source) =
        api_key_from_env_or_prompt("OpenAI", &["SINOMO_OPENAI_API_KEY", "OPENAI_API_KEY"])?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing OpenAI provider connection...");
    provider.test_connection()?;
    println!("OpenAI credentials saved to {}", credentials_path.display());
    Ok(())
}

pub fn test_openai() -> Result<(), CliError> {
    let (provider, credentials_path) = openai_provider()?;
    println!(
        "Testing OpenAI credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("OpenAI API key: ok");
    provider.test_connection()?;
    println!("OpenAI provider connection: ok");
    Ok(())
}

pub fn auth_codex() -> Result<(), CliError> {
    let (provider, auth_path) = codex_provider()?;
    let device = provider.device_login_start()?;

    println!("Open this URL in your browser:");
    println!("{}", device.verification_uri);
    println!();
    println!("Enter this code:");
    println!("{}", device.user_code);
    println!();
    println!("Waiting for ChatGPT authorization...");

    let started = Instant::now();
    let expires_in = Duration::from_secs(device.expires_in);
    let interval = Duration::from_secs(device.interval.max(5));

    loop {
        if started.elapsed() >= expires_in {
            return Err(CliError::AuthExpired);
        }
        thread::sleep(interval);
        let poll = provider.device_login_poll(&device)?;
        if poll.tokens.is_some() {
            println!("Testing Codex provider connection...");
            provider.test_connection()?;
            break;
        }
    }
    println!("Codex credentials saved to {}", auth_path.display());
    Ok(())
}

pub fn test_codex() -> Result<(), CliError> {
    let (provider, auth_path) = codex_provider()?;
    println!("Testing Codex credentials at {}", auth_path.display());
    provider.chatgpt_tokens()?;
    println!("Codex ChatGPT auth: ok");
    provider.test_connection()?;
    println!("Codex provider connection: ok");
    Ok(())
}

pub fn auth_gemini() -> Result<(), CliError> {
    let (provider, credentials_path) = gemini_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "Gemini",
        &[
            "SINOMO_GEMINI_API_KEY",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "GOOGLE_AI_API_KEY",
        ],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing Gemini provider connection...");
    provider.test_connection()?;
    println!("Gemini credentials saved to {}", credentials_path.display());
    Ok(())
}

pub fn test_gemini() -> Result<(), CliError> {
    let (provider, credentials_path) = gemini_provider()?;
    println!(
        "Testing Gemini credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("Gemini API key: ok");
    provider.test_connection()?;
    println!("Gemini provider connection: ok");
    Ok(())
}

pub fn auth_nvidia() -> Result<(), CliError> {
    let (provider, credentials_path) = nvidia_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "NVIDIA",
        &["SINOMO_NVIDIA_API_KEY", "NVIDIA_API_KEY", "NIM_API_KEY"],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing NVIDIA provider connection...");
    provider.test_connection()?;
    println!("NVIDIA credentials saved to {}", credentials_path.display());
    Ok(())
}

pub fn test_nvidia() -> Result<(), CliError> {
    let (provider, credentials_path) = nvidia_provider()?;
    println!(
        "Testing NVIDIA credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("NVIDIA API key: ok");
    provider.test_connection()?;
    println!("NVIDIA provider connection: ok");
    Ok(())
}

fn github_copilot_provider() -> Result<(GitHubCopilotProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(GitHubCopilotProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
    let client_id = env::var("SINOMO_COPILOT_CLIENT_ID")
        .unwrap_or_else(|_| GitHubCopilotConfig::default().client_id);
    let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
        client_id,
        token_store_path: Some(credentials_path.clone()),
        ..GitHubCopilotConfig::default()
    });
    Ok((provider, credentials_path))
}

fn anthropic_provider() -> Result<(AnthropicProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(AnthropicProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(AnthropicProvider::PROVIDER_ID);
    let provider = AnthropicProvider::new(AnthropicConfig {
        api_base_url: env::var("SINOMO_ANTHROPIC_API_BASE_URL")
            .or_else(|_| env::var("SINOMO_CLAUDE_API_BASE_URL"))
            .unwrap_or_else(|_| AnthropicConfig::default().api_base_url),
        default_model: AnthropicConfig::default().default_model,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn openai_provider() -> Result<(OpenAiProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(OpenAiProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(OpenAiProvider::PROVIDER_ID);
    let provider = OpenAiProvider::new(OpenAiConfig {
        api_base_url: env::var("SINOMO_OPENAI_API_BASE_URL")
            .unwrap_or_else(|_| OpenAiConfig::default().api_base_url),
        default_model: OpenAiConfig::default().default_model,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn codex_provider() -> Result<(CodexProvider, PathBuf), CliError> {
    let provider = codex_provider_for_model(PROVIDER_DEFAULT_MODEL);
    let auth_path = provider
        .credentials_path()
        .ok_or_else(|| CliError::AuthProvider {
            detail: "could not resolve Sinomo Codex credentials path; set SINOMO_DATA_DIR or SINOMO_CODEX_CREDENTIALS_FILE".to_string(),
        })?;
    Ok((provider, auth_path))
}

fn gemini_provider() -> Result<(GeminiProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(GeminiProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(GeminiProvider::PROVIDER_ID);
    let provider = GeminiProvider::new(GeminiConfig {
        api_base_url: env::var("SINOMO_GEMINI_API_BASE_URL")
            .unwrap_or_else(|_| GeminiConfig::default().api_base_url),
        default_model: GeminiConfig::default().default_model,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn nvidia_provider() -> Result<(NvidiaProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(NvidiaProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(NvidiaProvider::PROVIDER_ID);
    let provider = NvidiaProvider::new(NvidiaConfig {
        api_base_url: env::var("SINOMO_NVIDIA_API_BASE_URL")
            .unwrap_or_else(|_| NvidiaConfig::default().api_base_url),
        default_model: NvidiaConfig::default().default_model,
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn api_key_from_env_or_prompt(
    provider_name: &str,
    env_vars: &[&str],
) -> Result<(String, String), CliError> {
    for env_var in env_vars {
        if let Ok(api_key) = env::var(env_var) {
            let api_key = api_key.trim();
            if !api_key.is_empty() {
                return Ok((api_key.to_string(), format!("env:{env_var}")));
            }
        }
    }

    println!("No {provider_name} API key found in environment. Paste an API key to store locally.");
    println!("Input will be visible in this terminal.");
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|source| CliError::AuthProvider {
            detail: format!("failed to read API key: {source}"),
        })?;
    let api_key = line.trim();
    if api_key.is_empty() {
        return Err(CliError::AuthProvider {
            detail: format!("{provider_name} API key was empty"),
        });
    }
    Ok((api_key.to_string(), "prompt".to_string()))
}

pub fn print_usage() {
    println!("Usage:");
    println!("  sinomo");
    println!("  sinomo help");
    println!("  sinomo cluster");
    println!("  sinomo install");
    println!("  sinomo uninstall");
    println!("  sinomo daemon");
    println!("  sinomo rollout");
    println!("  sinomo dfs");
    println!("  sinomo models");
    println!("  sinomo usage");
    println!("  sinomo providers");
    println!("  sinomo evolve");
    println!();
    println!("Run `sinomo <subcommand> help` for detailed commands.");
    println!();
    println!("CUI:");
    println!(
        "  Run `sinomo` with no arguments to connect the command-line user interface to the local daemon when available."
    );
    println!("  Inside the CUI, prefix any CLI command with `/`, for example `/models list`.");
    println!("  Use `/quit` or `/exit` to end the session.");
    println!();
    println!("Environment:");
    println!("  SINOMO_DATA_DIR             Defaults to ~/.sinomo");
    println!("  SINOMO_CODEX_CREDENTIALS_FILE  Overrides Sinomo's Codex credentials file");
    println!("  SINOMO_CODEX_AUTH_ISSUER       Overrides the Codex ChatGPT auth issuer");
    println!("  SINOMO_CODEX_CLIENT_ID         Overrides the Codex ChatGPT OAuth client ID");
    println!("  SINOMO_CODEX_API_BASE_URL      Overrides the Codex ChatGPT backend API base URL");
    println!("  ANTHROPIC_API_KEY           Used by `providers auth/test anthropic`");
    println!("  OPENAI_API_KEY              Used by `providers auth/test openai`");
    println!("  GEMINI_API_KEY              Used by `providers auth/test gemini`");
    println!("  NVIDIA_API_KEY              Used by `providers auth/test nvidia`");
    println!("  SINOMO_NVIDIA_API_BASE_URL  Overrides the NVIDIA API base URL");
    println!(
        "  SINOMO_COPILOT_CLIENT_ID    Defaults to {}",
        GitHubCopilotConfig::default().client_id
    );
    println!(
        "                               GitHub App client IDs are not accepted by Copilot token exchange"
    );
}

fn print_cluster_help() {
    println!("Cluster:");
    println!("  sinomo cluster");
    println!("      Show cluster commands.");
    println!(
        "  sinomo cluster init [--name <name>] [--cluster-id cluster_...] [--node-id node_...]"
    );
    println!("      Bootstrap a new single-node cluster and clear orphan-node state.");
    println!(
        "  sinomo cluster invite [--introducer <host:port>] [--ttl-seconds <seconds>] [--output <path>]"
    );
    println!(
        "      Create a portable join invite. Pass a LAN-reachable introducer address for remote nodes."
    );
    println!(
        "  sinomo cluster join <invite> [--name <name>] [--cluster-id cluster_...] [--node-id node_...]"
    );
    println!(
        "      Record intent to join an existing cluster from a signed sinomo-join URI or invite file."
    );
    println!("  sinomo cluster status");
    println!("      Show local cluster bootstrap state, or report that this node is orphaned.");
    println!("  sinomo cluster peers");
    println!("      List known peers. Fails while the node is orphaned.");
    println!("  sinomo cluster leave");
    println!("      Remove local cluster bootstrap state.");
}

fn print_install_help() {
    println!("Install:");
    println!(
        "  sinomo install [--host <host>] [--port <port>] [--shadow-port <port>] [--no-start]"
    );
    println!(
        "      Create or update the local install, add boot entries for the main and shadow daemons, then start them."
    );
    println!("  sinomo uninstall [--no-stop]");
    println!("      Remove Sinomo boot entries and stop daemons. The data directory is retained.");
}

fn print_daemon_help() {
    println!("Daemon:");
    println!("  sinomo daemon");
    println!("      Show daemon commands.");
    println!("  sinomo daemon run [--host <host>] [--port <port>]");
    println!("      Run the daemon in the foreground. Defaults to 127.0.0.1:21353.");
    println!("  sinomo daemon start [--host <host>] [--port <port>]");
    println!("      Start the daemon as a detached background process.");
    println!("  sinomo daemon status");
    println!("      Probe the data-directory lock and daemon listener.");
    println!("  sinomo daemon stop");
    println!("      Ask the running daemon to shut down.");
    println!("  sinomo daemon shadow run|start|status|stop [--host <host>] [--port <port>]");
    println!("      Control the shadow daemon. Defaults to 127.0.0.1:21354.");
}

fn print_rollout_help() {
    println!("Rollout:");
    println!("  sinomo rollout");
    println!("      Show rollout commands.");
    println!("  sinomo rollout init-source [--repo <path>]");
    println!("      Initialize or merge an external git checkout into the managed source repo.");
    println!("  sinomo rollout import-source [--repo <path>]");
    println!(
        "      Merge external repo changes into the managed source repo without retaining external remotes."
    );
    println!("  sinomo rollout export-source --repo <path>");
    println!(
        "      Merge managed source changes, including self-evolution commits, into an external repo."
    );
    println!("  sinomo rollout apply-external [--repo <path>] [--host <host>] [--port <port>]");
    println!(
        "      Import external repo changes into managed source, then build and apply the managed source."
    );
    println!("  sinomo rollout apply [--repo <path>] [--host <host>] [--port <port>]");
    println!(
        "      Build the data-directory source repo, install bin/sinomo and bin/sinomo-shadow, restart shadow supervision, then verify main health."
    );
    println!(
        "  sinomo rollout cluster [--repo <path> | --binary <path>] [--target <host:port>] [--remote-build] [--remote-repo <path>] [--skip-local]"
    );
    println!(
        "      Build or use a dev binary for compatible nodes, or ask targets to build from their own source repo, then restart each node through its shadow daemon."
    );
    println!("  sinomo rollout status");
    println!("      Show source, binary, and rollout state paths.");
}

fn print_dfs_help() {
    println!("DFS:");
    println!("  sinomo dfs");
    println!("      Show DFS commands.");
    println!("  sinomo dfs status [--metadata-root <path>] [--json]");
    println!("      Summarize local shard metadata, writes, repairs, and conflicts.");
    println!("  sinomo dfs filesystem <file_system_id> [--metadata-root <path>] [--json]");
    println!("      Inspect one filesystem.");
    println!("  sinomo dfs object <object_id> [--metadata-root <path>] [--json]");
    println!("      Inspect one object with versions, receipts, intents, conflicts, and repairs.");
    println!(
        "  sinomo dfs conflicts [--filesystem <file_system_id>] [--object <object_id>] [--metadata-root <path>] [--json]"
    );
    println!("      List conflict records.");
    println!(
        "  sinomo dfs repair [--filesystem <file_system_id>] [--object <object_id>] [--metadata-root <path>] [--json]"
    );
    println!("      List inferred missing replica repairs.");
    println!("  sinomo dfs pin list [--filesystem <file_system_id>] [--pin-root <path>] [--json]");
    println!(
        "  sinomo dfs pin add --path <path> [--filesystem <file_system_id>] [--object <object_id>] [--node <node_id>] [--mode metadata|content] [--pin-root <path>] [--json]"
    );
    println!(
        "  sinomo dfs pin remove --path <path> [--filesystem <file_system_id>] [--node <node_id>] [--pin-root <path>] [--json]"
    );
}

fn print_models_help() {
    println!("Models:");
    println!("  sinomo models");
    println!("      Show the current default model and this help.");
    println!("  sinomo models list [--provider <provider>]");
    println!("      List available or known models.");
    println!("  sinomo models default --provider <provider> --model <model>");
    println!("      Set the default model for CUI and model-backed commands.");
    println!();
    println!("Providers:");
    for provider in default_model_provider_ids() {
        println!("  {provider}");
    }
}

fn print_usage_help() {
    println!("Usage:");
    println!("  sinomo usage [--provider <provider>] [--json]");
    println!("      Show subscription usage for authenticated providers.");
    println!();
    println!("Providers:");
    for provider in subscription_usage_provider_ids() {
        println!("  {provider}");
    }
}

fn print_providers_help() {
    println!("Providers:");
    println!("  sinomo providers");
    println!("      Show provider commands.");
    println!("  sinomo providers auth <provider>");
    println!("      Authenticate a provider and store credentials in Sinomo's data dir.");
    println!("  sinomo providers test <provider>");
    println!("      Validate stored provider credentials.");
    println!();
    println!("Provider IDs:");
    println!("  github-copilot");
    println!("  codex");
    println!("  anthropic");
    println!("  openai");
    println!("  gemini");
    println!("  nvidia");
}

fn print_evolve_help() {
    println!("Evolution:");
    println!("  sinomo evolve");
    println!("      Show evolution commands.");
    println!(
        "  sinomo evolve propose \"<objective>\" [--repo <path>] [--provider <provider>] [--model <model>] [--verify \"<command>\"]"
    );
    println!(
        "      With --dry-run or --pause-after-proposal, write a phase-0 campaign bundle under $SINOMO_DATA_DIR/evolution/campaigns/."
    );
    println!(
        "      Campaign flags: --objective <text> --horizon <duration> --budget-policy <policy> --reserve <resource:percent%> --approval <policy> --max-wall-time <duration> [--approve]"
    );
    println!(
        "  sinomo evolve schedule <campaign> --dry-run [--resource <resource:used/total[:window]>] [--estimate <resource:units>]"
    );
    println!(
        "      Write phase-1 resource-ledger, lease request/decision, scheduler dry-run, and pause/schedule decision artifacts."
    );
    println!(
        "  sinomo evolve iterate <campaign> --dry-run [--task <task_id>] [--path <glob>] [--verify \"<command>\"]"
    );
    println!(
        "      Write phase-2 scheduled-iteration handoff artifacts without starting an implementation agent."
    );
    println!(
        "  sinomo evolve evaluate <campaign> --job <evojob_...> --task <task_id> [--failure-class <class>] [--root-cause <text>] [--learned <text>] [--usage <resource:estimated[:actual]>]"
    );
    println!(
        "      Write phase-3 stage scorecard/adaptation artifacts and update campaign state after candidate evaluation."
    );
    println!(
        "  sinomo evolve tick --dry-run [--scheduler-service <service_id>] [--campaign <campaign>] [--workload <system:id:status[:title]>] [--workload-estimate <system:id:resource:units>] [--resource <resource:used/total[:window]>] [--estimate <task:resource:units>] [--max-concurrent <n>] [--max-concurrent-workloads <n>] [--idle-window-open] [--reset-window-open]"
    );
    println!(
        "      Write phase-4 background scheduler plan/status/event artifacts and dry-run iteration handoffs without autonomous execution."
    );
    println!("  sinomo evolve approve [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve reject [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve restore [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve status [--job evojob_...]");
    println!("  sinomo evolve show [--diff|--scorecard|--decision|--packet] [--job evojob_...]");
    println!("  sinomo evolve verify [--job evojob_...]");
    println!("  sinomo evolve packet [--job evojob_...]");
    println!("  sinomo evolve cleanup [--job evojob_...]");
    println!("  sinomo evolve job create \"<objective>\" [--repo <path>]");
    println!("  sinomo evolve models ...");
    println!("      Compatibility alias for `sinomo models ...`.");
}

#[derive(Debug)]
pub enum CliError {
    Usage,
    AuthDenied,
    AuthExpired,
    AuthProvider {
        detail: String,
    },
    NoActiveJob,
    AmbiguousJob {
        job_ids: Vec<String>,
    },
    InvalidJobState {
        job_id: String,
        status: String,
        expected: String,
    },
    ClusterOrphan,
    MissingArtifact {
        artifact: String,
    },
    Parse {
        detail: String,
    },
    Io {
        path: String,
        detail: String,
    },
    Json {
        path: String,
        detail: String,
    },
    LocalData(sinomo_runtime::local::LocalDataError),
    EventStore(sinomo_runtime::events::EventStoreError),
    DfsMetadata(sinomo_runtime::dfs::MetadataBackendError),
    DfsPin(sinomo_runtime::dfs::DfsPinStoreError),
    Kv(KvError),
    LanguageModel(LanguageModelError),
    Evolution(sinomo_evolution::EvolutionError),
    Eval(sinomo_runtime::eval::EvalError),
    DaemonAlreadyRunning {
        lock_file: String,
        detail: String,
    },
    DaemonNotRunning {
        detail: String,
    },
    Network {
        address: String,
        detail: String,
    },
    Process {
        command: String,
        status: i32,
        stderr: String,
    },
}

impl CliError {
    fn stdio(source: io::Error) -> Self {
        Self::Io {
            path: "stdio".to_string(),
            detail: source.to_string(),
        }
    }
}

impl From<sinomo_runtime::local::LocalDataError> for CliError {
    fn from(error: sinomo_runtime::local::LocalDataError) -> Self {
        Self::LocalData(error)
    }
}

impl From<sinomo_runtime::events::EventStoreError> for CliError {
    fn from(error: sinomo_runtime::events::EventStoreError) -> Self {
        Self::EventStore(error)
    }
}

impl From<sinomo_runtime::dfs::MetadataBackendError> for CliError {
    fn from(error: sinomo_runtime::dfs::MetadataBackendError) -> Self {
        Self::DfsMetadata(error)
    }
}

impl From<sinomo_runtime::dfs::DfsPinStoreError> for CliError {
    fn from(error: sinomo_runtime::dfs::DfsPinStoreError) -> Self {
        Self::DfsPin(error)
    }
}

impl From<KvError> for CliError {
    fn from(error: KvError) -> Self {
        Self::Kv(error)
    }
}

impl From<LanguageModelError> for CliError {
    fn from(error: LanguageModelError) -> Self {
        Self::LanguageModel(error)
    }
}

impl From<sinomo_evolution::EvolutionError> for CliError {
    fn from(error: sinomo_evolution::EvolutionError) -> Self {
        Self::Evolution(error)
    }
}

impl From<RolloutError> for CliError {
    fn from(error: RolloutError) -> Self {
        match error {
            RolloutError::Io { path, detail } => Self::Io { path, detail },
            RolloutError::Json { path, detail } => Self::Json { path, detail },
            RolloutError::Process {
                command,
                status,
                stderr,
            } => Self::Process {
                command,
                status,
                stderr,
            },
            RolloutError::MissingArtifact { artifact } => Self::MissingArtifact { artifact },
        }
    }
}

impl From<sinomo_runtime::eval::EvalError> for CliError {
    fn from(error: sinomo_runtime::eval::EvalError) -> Self {
        Self::Eval(error)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage => write!(formatter, "unknown command; run `sinomo help`"),
            Self::AuthDenied => write!(formatter, "device authorization was denied"),
            Self::AuthExpired => write!(formatter, "device authorization expired"),
            Self::AuthProvider { detail } => write!(formatter, "provider auth failed: {detail}"),
            Self::NoActiveJob => write!(formatter, "no active evolution job"),
            Self::AmbiguousJob { job_ids } => {
                write!(
                    formatter,
                    "multiple active evolution jobs; rerun with --job ({})",
                    job_ids.join(", ")
                )
            }
            Self::InvalidJobState {
                job_id,
                status,
                expected,
            } => write!(
                formatter,
                "evolution job {job_id} is {status}; expected {expected}"
            ),
            Self::ClusterOrphan => write!(
                formatter,
                "this node is not part of a cluster; run `sinomo cluster init` or `sinomo cluster join <invite>`"
            ),
            Self::MissingArtifact { artifact } => {
                write!(formatter, "{artifact} has not been written")
            }
            Self::Parse { detail } => write!(formatter, "failed to parse command: {detail}"),
            Self::Io { path, detail } => write!(formatter, "filesystem error at {path}: {detail}"),
            Self::Json { path, detail } => write!(formatter, "json error at {path}: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::EventStore(error) => write!(formatter, "{error}"),
            Self::DfsMetadata(error) => write!(formatter, "{error}"),
            Self::DfsPin(error) => write!(formatter, "{error}"),
            Self::Kv(error) => write!(formatter, "{error}"),
            Self::LanguageModel(error) => write!(formatter, "{error}"),
            Self::Evolution(error) => write!(formatter, "{error}"),
            Self::Eval(error) => write!(formatter, "{error}"),
            Self::DaemonAlreadyRunning { lock_file, detail } => {
                write!(
                    formatter,
                    "sinomo daemon is already running or locked at {lock_file}: {detail}"
                )
            }
            Self::DaemonNotRunning { detail } => {
                write!(formatter, "sinomo daemon is not running: {detail}")
            }
            Self::Network { address, detail } => {
                write!(formatter, "network error at {address}: {detail}")
            }
            Self::Process {
                command,
                status,
                stderr,
            } => write!(
                formatter,
                "process `{command}` failed with status {status}: {stderr}"
            ),
        }
    }
}

impl std::error::Error for CliError {}

#[cfg(test)]
mod tests {
    use super::{
        CliCommand, CliError, ClusterCliCommand, ClusterInitOptions, ClusterInviteOptions,
        ClusterJoinOptions, ClusterJoinRegistrationRequest, ClusterPaths,
        ClusterRolloutBuildRequest, ClusterSyncRequest, DaemonCliCommand, DaemonClusterStatus,
        DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonStatus, DfsCliCommand,
        DfsCommonOptions, DfsFilterOptions, DfsPinCliCommand, EvolutionJobStatus, EvolutionPaths,
        InstallLayout, InstallOptions, InteractionModel, InteractionRuntime, InteractionSession,
        InteractionSurface, InteractionSurfaceKind, ModelBackedInteractionHandler,
        ModelDefaultRepoMode, ModelsCliCommand, ProviderCliCommand, ProviderName,
        RolloutCliCommand, RolloutMainActivation, RolloutOptions, SelfRolloutPaths,
        UninstallOptions, authenticate_daemon_command, cli_command_requires_cluster_state,
        cluster_init_with_paths, cluster_invite_with_paths, cluster_join_with_paths,
        daemon_status_matches_options, decode_cluster_join_invite, encode_cluster_join_invite,
        ensure_daemon_cluster_members, ensure_rollout_layout, handle_daemon_cluster_join,
        handle_daemon_cluster_kv_txn, handle_daemon_cluster_sync, handle_daemon_rollout_build,
        install_should_update_binary, install_with_layout, is_terminal_status,
        models_default_with_data_dir, normalize_model_provider, parse_cli_command,
        parse_cluster_command, parse_cluster_join_invite_argument, parse_daemon_command,
        parse_dfs_command, parse_install_options, parse_model_default_options,
        parse_models_list_options, parse_propose_options, parse_provider_command,
        parse_rollout_command, parse_slash_command, parse_uninstall_options, parse_usage_options,
        read_cluster_members_from_kv, read_cluster_state, run_cui_with_handler_io,
        run_cui_with_handler_io_and_daemon, run_evolve_command, sign_cluster_join_invite,
        sign_daemon_command_from_data_dir, stage_rollout_main_activation, sync_cluster_state_to_kv,
        uninstall_with_layout,
    };
    use sinomo_lm::{
        LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
    };
    use sinomo_runtime::core::{
        ClusterBootstrap, ClusterId, ClusterInviteId, ClusterJoinInvite, ClusterMemberRole,
        ClusterMemberState, EvolutionJobId, NodeId, SessionId, TrustDomain, TrustRing,
    };
    use sinomo_runtime::events::{EventStore, InMemoryEventStore, ReplayQuery};
    use sinomo_runtime::kv::{KvCompare, KvOp, KvTxn};
    use sinomo_runtime::local::SinomoDataDir;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn provider_commands_parse_action_before_provider() {
        assert_eq!(
            parse_provider_command(&["auth".to_string(), "gemini".to_string(),])
                .expect("provider command"),
            ProviderCliCommand::Auth(ProviderName::Gemini)
        );
        assert_eq!(
            parse_provider_command(&["test".to_string(), "github-copilot".to_string(),])
                .expect("provider command"),
            ProviderCliCommand::Test(ProviderName::GitHubCopilot)
        );
        assert_eq!(
            parse_provider_command(&["auth".to_string(), "codex".to_string(),])
                .expect("provider command"),
            ProviderCliCommand::Auth(ProviderName::Codex)
        );
    }

    #[test]
    fn provider_commands_reject_provider_before_action_order() {
        assert!(matches!(
            parse_provider_command(&["gemini".to_string(), "auth".to_string()]),
            Err(CliError::Usage)
        ));
    }

    #[test]
    fn top_level_cli_parser_routes_subcommands() {
        assert!(matches!(
            parse_cli_command(&[]).expect("cui"),
            CliCommand::Cui
        ));
        assert!(matches!(
            parse_cli_command(&["help".to_string()]).expect("help"),
            CliCommand::Help
        ));
        assert!(matches!(
            parse_cli_command(&["daemon".to_string(), "status".to_string()]).expect("daemon"),
            CliCommand::Daemon(DaemonCliCommand::Status(_))
        ));
        assert!(matches!(
            parse_cli_command(&["daemon".to_string()]).expect("daemon help"),
            CliCommand::Daemon(DaemonCliCommand::Help)
        ));
        assert!(matches!(
            parse_cli_command(&["cluster".to_string(), "status".to_string()])
                .expect("cluster status"),
            CliCommand::Cluster(ClusterCliCommand::Status)
        ));
        assert!(matches!(
            parse_cli_command(&["install".to_string(), "help".to_string()]).expect("install help"),
            CliCommand::InstallHelp
        ));
        assert!(matches!(
            parse_cli_command(&["install".to_string(), "--no-start".to_string()]).expect("install"),
            CliCommand::Install(InstallOptions { start: false, .. })
        ));
        assert!(matches!(
            parse_cli_command(&["uninstall".to_string(), "--no-stop".to_string()])
                .expect("uninstall"),
            CliCommand::Uninstall(UninstallOptions { stop: false })
        ));
        assert!(matches!(
            parse_cli_command(&[
                "providers".to_string(),
                "test".to_string(),
                "openai".to_string()
            ])
            .expect("providers"),
            CliCommand::Providers(ProviderCliCommand::Test(ProviderName::OpenAi))
        ));
        assert!(matches!(
            parse_cli_command(&["models".to_string(), "list".to_string()]).expect("models"),
            CliCommand::Models(ModelsCliCommand::List(_))
        ));
        assert!(matches!(
            parse_cli_command(&["models".to_string()]).expect("models help"),
            CliCommand::Models(ModelsCliCommand::Help)
        ));
        assert!(matches!(
            parse_cli_command(&["model".to_string(), "list".to_string()]).expect("model alias"),
            CliCommand::Models(ModelsCliCommand::List(_))
        ));
        assert!(matches!(
            parse_cli_command(&[
                "usage".to_string(),
                "--provider".to_string(),
                "codex".to_string()
            ])
            .expect("usage"),
            CliCommand::Usage(_)
        ));
        assert!(matches!(
            parse_cli_command(&["providers".to_string()]).expect("providers help"),
            CliCommand::Providers(ProviderCliCommand::Help)
        ));
        assert!(matches!(
            parse_cli_command(&["rollout".to_string(), "status".to_string()]).expect("rollout"),
            CliCommand::Rollout(RolloutCliCommand::Status)
        ));
        assert!(matches!(
            parse_cli_command(&["dfs".to_string(), "status".to_string()]).expect("dfs"),
            CliCommand::Dfs(DfsCliCommand::Status(_))
        ));
        assert!(matches!(
            parse_cli_command(&["evolve".to_string()]).expect("evolve help"),
            CliCommand::Evolve(super::EvolveCliCommand::Help)
        ));
    }

    #[test]
    fn install_parser_accepts_custom_daemon_ports() {
        assert_eq!(
            parse_install_options(&[
                "--host".to_string(),
                "127.0.0.1".to_string(),
                "--port".to_string(),
                "21400".to_string(),
                "--shadow-port".to_string(),
                "21401".to_string(),
                "--no-start".to_string(),
            ])
            .expect("install options"),
            InstallOptions {
                host: "127.0.0.1".to_string(),
                port: 21400,
                shadow_port: 21401,
                start: false,
            }
        );
        assert_eq!(
            parse_uninstall_options(&["--no-stop".to_string()]).expect("uninstall options"),
            UninstallOptions { stop: false }
        );
    }

    #[test]
    fn install_writes_boot_entries_and_uninstall_retains_data_dir() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-install-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let layout = InstallLayout::new(&data_dir, root.join("startup"));

        install_with_layout(
            InstallOptions {
                host: "127.0.0.1".to_string(),
                port: 21400,
                shadow_port: 21401,
                start: false,
            },
            &layout,
        )
        .expect("install");

        let main_entry = std::fs::read_to_string(&layout.main_boot_entry).expect("main boot");
        let shadow_entry = std::fs::read_to_string(&layout.shadow_boot_entry).expect("shadow boot");
        assert!(layout.binary.exists());
        assert!(layout.shadow_binary.exists());
        assert!(main_entry.contains("SINOMO_DATA_DIR"));
        assert!(shadow_entry.contains("SINOMO_SHADOW_MAIN_BINARY"));
        assert!(shadow_entry.contains(&layout.shadow_binary.display().to_string()));
        if cfg!(windows) {
            assert!(main_entry.contains("daemon start"));
            assert!(shadow_entry.contains("daemon shadow start"));
        } else {
            assert!(main_entry.contains("daemon run"));
            assert!(shadow_entry.contains("daemon shadow run"));
        }
        assert!(main_entry.contains("21400"));
        assert!(shadow_entry.contains("21401"));

        uninstall_with_layout(UninstallOptions { stop: false }, &layout).expect("uninstall");
        assert!(!layout.main_boot_entry.exists());
        assert!(!layout.shadow_boot_entry.exists());
        assert!(layout.data_dir.exists());
        assert!(layout.binary.exists());
        assert!(layout.shadow_binary.exists());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn install_detects_existing_binary_update() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-install-update-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let destination = root.join("bin").join(if cfg!(windows) {
            "sinomo.exe"
        } else {
            "sinomo"
        });

        assert!(!install_should_update_binary(&destination).expect("missing destination"));
        std::fs::create_dir_all(destination.parent().expect("parent")).expect("mkdir");
        std::fs::write(&destination, b"old binary").expect("write old binary");
        assert!(install_should_update_binary(&destination).expect("existing destination"));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn install_detects_daemon_option_mismatch() {
        let status = DaemonStatus {
            pid: 4242,
            host: "127.0.0.1".to_string(),
            port: 21_353,
            address: "127.0.0.1:21353".to_string(),
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            started_at: "2026-05-04T00:00:00Z".to_string(),
            data_dir: PathBuf::from("data"),
            lock_file: PathBuf::from("data/daemon/daemon.lock"),
            cluster: None,
        };

        assert!(daemon_status_matches_options(&status, "127.0.0.1", 21_353));
        assert!(!daemon_status_matches_options(&status, "0.0.0.0", 21_353));
        assert!(!daemon_status_matches_options(&status, "127.0.0.1", 21_354));
    }

    #[test]
    fn orphan_guard_applies_to_distributed_runtime_commands() {
        assert!(cli_command_requires_cluster_state(&CliCommand::Daemon(
            DaemonCliCommand::Status(DaemonRunOptions::default())
        )));
        assert!(cli_command_requires_cluster_state(&CliCommand::Rollout(
            RolloutCliCommand::Status
        )));
        assert!(cli_command_requires_cluster_state(&CliCommand::Evolve(
            super::EvolveCliCommand::Help
        )));
        assert!(!cli_command_requires_cluster_state(&CliCommand::Cluster(
            ClusterCliCommand::Status
        )));
        assert!(!cli_command_requires_cluster_state(&CliCommand::Dfs(
            DfsCliCommand::Help
        )));
        assert!(cli_command_requires_cluster_state(&CliCommand::Models(
            ModelsCliCommand::Help
        )));
    }

    fn signed_test_invite_uri(
        root: &std::path::Path,
        cluster_id: &str,
        issuer_node_id: &str,
    ) -> String {
        let identity = super::load_or_create_main_node_identity(root).expect("test identity");
        let mut invite = ClusterJoinInvite {
            invite_id: ClusterInviteId::generate(),
            cluster_id: ClusterId::new(cluster_id).expect("cluster"),
            cluster_name: Some("test cluster".to_string()),
            introducers: Vec::new(),
            token_id: "token_test".to_string(),
            expires_at: time::OffsetDateTime::now_utc() + time::Duration::seconds(60),
            trust_domain: TrustDomain::new("trust.local").expect("trust"),
            allowed_member_roles: vec![ClusterMemberRole::Learner, ClusterMemberRole::NonVoting],
            issuer_node_id: NodeId::new(issuer_node_id).expect("issuer node"),
            issuer_public_key: identity.public_key.clone(),
            signature: String::new(),
        };
        invite.signature = sign_cluster_join_invite(&invite, &identity).expect("sign invite");
        encode_cluster_join_invite(&invite).expect("invite uri")
    }

    #[test]
    fn dfs_parser_accepts_operator_and_pin_commands() {
        let metadata_root = std::env::temp_dir().join("sinomo-cli-dfs-meta");
        assert_eq!(
            parse_dfs_command(&[
                "status".to_string(),
                "--metadata-root".to_string(),
                metadata_root.display().to_string(),
                "--json".to_string(),
            ])
            .expect("dfs status"),
            DfsCliCommand::Status(DfsCommonOptions {
                metadata_root: Some(metadata_root.clone()),
                json: true,
            })
        );
        assert_eq!(
            parse_dfs_command(&[
                "conflicts".to_string(),
                "--filesystem".to_string(),
                "sifs".to_string(),
                "--object".to_string(),
                "obj_artifact".to_string(),
            ])
            .expect("dfs conflicts"),
            DfsCliCommand::Conflicts(DfsFilterOptions {
                common: DfsCommonOptions::default(),
                file_system_id: Some(
                    sinomo_runtime::core::DfsFileSystemId::new("sifs").expect("filesystem")
                ),
                object_id: Some(
                    sinomo_runtime::core::DfsObjectId::new("obj_artifact").expect("object")
                ),
            })
        );
        assert!(matches!(
            parse_dfs_command(&[
                "pin".to_string(),
                "add".to_string(),
                "--path".to_string(),
                "src/lib.rs".to_string(),
                "--mode".to_string(),
                "metadata".to_string(),
            ])
            .expect("dfs pin add"),
            DfsCliCommand::Pin(DfsPinCliCommand::Add(options))
                if options.mode == sinomo_runtime::dfs::DfsPinMode::Metadata
                    && options.path == "src/lib.rs"
        ));
    }

    #[test]
    fn cluster_parser_accepts_init_and_join_options() {
        assert_eq!(
            parse_cluster_command(&[
                "init".to_string(),
                "--name".to_string(),
                "home".to_string(),
                "--cluster-id".to_string(),
                "cluster_home".to_string(),
                "--node-id".to_string(),
                "node_desktop".to_string(),
            ])
            .expect("cluster init"),
            ClusterCliCommand::Init(ClusterInitOptions {
                cluster_id: Some(ClusterId::new("cluster_home").expect("cluster")),
                node_id: Some(NodeId::new("node_desktop").expect("node")),
                name: "home".to_string(),
            })
        );
        assert_eq!(
            parse_cluster_command(&[
                "join".to_string(),
                "sinomo-join://token".to_string(),
                "--name".to_string(),
                "laptop".to_string(),
            ])
            .expect("cluster join"),
            ClusterCliCommand::Join(ClusterJoinOptions {
                invite: "sinomo-join://token".to_string(),
                cluster_id: None,
                node_id: None,
                name: "laptop".to_string(),
            })
        );
        assert_eq!(
            parse_cluster_command(&[
                "invite".to_string(),
                "--introducer".to_string(),
                "192.168.1.10:21353".to_string(),
                "--ttl-seconds".to_string(),
                "60".to_string(),
                "--output".to_string(),
                "invite.txt".to_string(),
            ])
            .expect("cluster invite"),
            ClusterCliCommand::Invite(ClusterInviteOptions {
                ttl_seconds: 60,
                introducers: vec!["192.168.1.10:21353".to_string()],
                output: Some(PathBuf::from("invite.txt")),
            })
        );
    }

    #[test]
    fn cluster_init_persists_local_bootstrap_state() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-init-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));

        cluster_init_with_paths(
            ClusterInitOptions {
                cluster_id: Some(ClusterId::new("cluster_test").expect("cluster")),
                node_id: Some(NodeId::new("node_local").expect("node")),
                name: "test cluster".to_string(),
            },
            &paths,
        )
        .expect("cluster init");

        let state = read_cluster_state(&paths).expect("cluster state");
        assert_eq!(
            state.cluster_id,
            ClusterId::new("cluster_test").expect("cluster")
        );
        assert_eq!(
            state.local_node_id,
            NodeId::new("node_local").expect("node")
        );
        assert_eq!(state.term, 1);
        assert_eq!(state.kv_revision, 1);
        assert_eq!(state.members.len(), 2);
        let main = state
            .members
            .iter()
            .find(|member| member.node_id == NodeId::new("node_local").expect("node"))
            .expect("main daemon member");
        assert_eq!(main.role, ClusterMemberRole::Voter);
        assert_eq!(main.state, ClusterMemberState::Active);
        assert!(!main.node_public_key.trim().is_empty());
        assert_eq!(main.endpoints[0].address, "127.0.0.1:21353");
        let shadow = state
            .members
            .iter()
            .find(|member| member.node_id == NodeId::new("node_local-shadow").expect("node"))
            .expect("shadow daemon member");
        assert_eq!(shadow.role, ClusterMemberRole::NonVoting);
        assert_eq!(shadow.state, ClusterMemberState::Active);
        assert!(!shadow.node_public_key.trim().is_empty());
        assert_ne!(shadow.node_public_key, main.node_public_key);
        assert_eq!(shadow.endpoints[0].address, "127.0.0.1:21354");
        let kv_members = read_cluster_members_from_kv(&paths).expect("kv members");
        assert_eq!(kv_members.len(), 2);
        let kv_main = kv_members
            .iter()
            .find(|member| member.node_id == NodeId::new("node_local").expect("node"))
            .expect("kv main");
        assert_eq!(kv_main.state, ClusterMemberState::Active);
        assert_eq!(kv_main.node_public_key, main.node_public_key);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn daemon_auth_accepts_signed_local_command_and_rejects_tampering() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-daemon-auth-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        cluster_init_with_paths(
            ClusterInitOptions {
                cluster_id: Some(ClusterId::new("cluster_auth").expect("cluster")),
                node_id: Some(NodeId::new("node_auth").expect("node")),
                name: "auth cluster".to_string(),
            },
            &paths,
        )
        .expect("cluster init");
        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));

        let signed =
            sign_daemon_command_from_data_dir(&data_dir, "STATUS").expect("signed command");
        assert_eq!(
            authenticate_daemon_command(&daemon_paths, &signed).expect("auth command"),
            "STATUS"
        );
        assert!(matches!(
            authenticate_daemon_command(&daemon_paths, "STATUS"),
            Err(CliError::AuthDenied)
        ));

        let tampered = signed.replace("\"command\":\"STATUS\"", "\"command\":\"SHUTDOWN\"");
        assert!(matches!(
            authenticate_daemon_command(&daemon_paths, &tampered),
            Err(CliError::AuthDenied)
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_join_records_pending_learner_state() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-join-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));

        cluster_join_with_paths(
            ClusterJoinOptions {
                invite: signed_test_invite_uri(&root, "cluster_joined", "node_host"),
                cluster_id: None,
                node_id: Some(NodeId::new("node_joiner").expect("node")),
                name: "joiner".to_string(),
            },
            &paths,
        )
        .expect("cluster join");

        let state = read_cluster_state(&paths).expect("cluster state");
        assert_eq!(
            state.cluster_id,
            ClusterId::new("cluster_joined").expect("cluster")
        );
        assert_eq!(state.term, 0);
        assert_eq!(state.kv_revision, 0);
        assert_eq!(state.members[0].role, ClusterMemberRole::Learner);
        assert_eq!(state.members[0].state, ClusterMemberState::Pending);
        assert!(!state.members[0].node_public_key.trim().is_empty());
        let shadow = state
            .members
            .iter()
            .find(|member| member.node_id == NodeId::new("node_joiner-shadow").expect("node"))
            .expect("shadow daemon member");
        assert_eq!(shadow.role, ClusterMemberRole::NonVoting);
        assert_eq!(shadow.state, ClusterMemberState::Pending);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_invite_generates_portable_join_payload() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-invite-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        let output = root.join("join.txt");
        cluster_init_with_paths(
            ClusterInitOptions {
                cluster_id: Some(ClusterId::new("cluster_invite").expect("cluster")),
                node_id: Some(NodeId::new("node_host").expect("node")),
                name: "invite cluster".to_string(),
            },
            &paths,
        )
        .expect("cluster init");

        let rendered = cluster_invite_with_paths(
            ClusterInviteOptions {
                ttl_seconds: 120,
                introducers: vec!["192.168.1.10:21353".to_string()],
                output: Some(output.clone()),
            },
            &paths,
        )
        .expect("cluster invite");
        let parsed = decode_cluster_join_invite(&rendered.uri).expect("decode invite");

        assert!(rendered.uri.starts_with(super::SINOMO_JOIN_URI_PREFIX));
        assert_eq!(
            parsed.cluster_id,
            ClusterId::new("cluster_invite").expect("cluster")
        );
        assert_eq!(parsed.cluster_name.as_deref(), Some("invite cluster"));
        assert_eq!(parsed.introducers.len(), 1);
        assert_eq!(parsed.introducers[0].address, "192.168.1.10:21353");
        assert!(rendered.persisted_path.exists());
        assert_eq!(
            std::fs::read_to_string(output).expect("invite file").trim(),
            rendered.join_command
        );
        assert!(rendered.join_command.starts_with("sinomo cluster join "));
        assert_eq!(
            parse_cluster_join_invite_argument(&rendered.join_command).expect("parse join command"),
            parsed
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_join_uses_structured_invite_cluster_identity() {
        let host_root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-host-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let join_root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-join-invite-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
        let join_paths = ClusterPaths::new(&SinomoDataDir::new(&join_root));
        cluster_init_with_paths(
            ClusterInitOptions {
                cluster_id: Some(ClusterId::new("cluster_remote").expect("cluster")),
                node_id: Some(NodeId::new("node_host").expect("node")),
                name: "remote cluster".to_string(),
            },
            &host_paths,
        )
        .expect("cluster init");
        let rendered = cluster_invite_with_paths(
            ClusterInviteOptions {
                ttl_seconds: 120,
                introducers: vec!["192.168.1.10:21353".to_string()],
                output: None,
            },
            &host_paths,
        )
        .expect("cluster invite");
        let mut offline_invite = rendered.invite.clone();
        offline_invite.introducers.clear();
        let host_identity =
            super::load_or_create_main_node_identity(&host_root).expect("host identity");
        offline_invite.signature =
            sign_cluster_join_invite(&offline_invite, &host_identity).expect("resign invite");
        let offline_uri = encode_cluster_join_invite(&offline_invite).expect("offline invite");

        cluster_join_with_paths(
            ClusterJoinOptions {
                invite: offline_uri,
                cluster_id: None,
                node_id: Some(NodeId::new("node_remote").expect("node")),
                name: "remote".to_string(),
            },
            &join_paths,
        )
        .expect("cluster join");
        let state = read_cluster_state(&join_paths).expect("cluster state");

        assert_eq!(
            state.cluster_id,
            ClusterId::new("cluster_remote").expect("cluster")
        );
        assert_eq!(state.cluster_name, "remote cluster");
        assert!(matches!(state.bootstrap, ClusterBootstrap::Joined { .. }));
        std::fs::remove_dir_all(host_root).ok();
        std::fs::remove_dir_all(join_root).ok();
    }

    #[test]
    fn introducer_records_joined_members_and_returns_peer_view() {
        let host_root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-introducer-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let join_root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-joining-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
        let join_paths = ClusterPaths::new(&SinomoDataDir::new(&join_root));
        cluster_init_with_paths(
            ClusterInitOptions {
                cluster_id: Some(ClusterId::new("cluster_register").expect("cluster")),
                node_id: Some(NodeId::new("node_host").expect("node")),
                name: "registered cluster".to_string(),
            },
            &host_paths,
        )
        .expect("cluster init");
        let rendered = cluster_invite_with_paths(
            ClusterInviteOptions {
                ttl_seconds: 120,
                introducers: vec!["127.0.0.1:21353".to_string()],
                output: None,
            },
            &host_paths,
        )
        .expect("cluster invite");
        let mut offline_invite = rendered.invite.clone();
        offline_invite.introducers.clear();
        let host_identity =
            super::load_or_create_main_node_identity(&host_root).expect("host identity");
        offline_invite.signature =
            sign_cluster_join_invite(&offline_invite, &host_identity).expect("resign invite");
        let offline_uri = encode_cluster_join_invite(&offline_invite).expect("offline invite");
        cluster_join_with_paths(
            ClusterJoinOptions {
                invite: offline_uri,
                cluster_id: None,
                node_id: Some(NodeId::new("node_remote").expect("node")),
                name: "remote".to_string(),
            },
            &join_paths,
        )
        .expect("cluster join local");
        let remote_state = read_cluster_state(&join_paths).expect("remote state");
        let invite = match &remote_state.bootstrap {
            ClusterBootstrap::Joined { invite } => invite.clone(),
            ClusterBootstrap::InitializedLocally => panic!("expected joined state"),
        };
        let request = ClusterJoinRegistrationRequest {
            invite,
            members: remote_state.members,
        };
        let payload = serde_json::to_string(&request).expect("request");
        let response =
            handle_daemon_cluster_join(&DaemonPaths::with_root(host_root.join("daemon")), &payload)
                .expect("introducer join");

        assert!(response.accepted);
        assert!(response.kv_snapshot.is_some());
        let host_state = read_cluster_state(&host_paths).expect("host state");
        assert!(host_state.members.iter().any(|member| {
            member.node_id == NodeId::new("node_remote").expect("node")
                && member.state == ClusterMemberState::Learner
        }));
        let host_kv_members = read_cluster_members_from_kv(&host_paths).expect("host kv members");
        assert!(host_kv_members.iter().any(|member| {
            member.node_id == NodeId::new("node_remote").expect("node")
                && member.state == ClusterMemberState::Learner
        }));
        assert!(
            response
                .state
                .members
                .iter()
                .any(|member| { member.node_id == NodeId::new("node_remote").expect("node") })
        );
        std::fs::remove_dir_all(host_root).ok();
        std::fs::remove_dir_all(join_root).ok();
    }

    #[test]
    fn daemon_cluster_kv_txn_persists_snapshot() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-kv-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        cluster_init_with_paths(
            ClusterInitOptions {
                cluster_id: Some(ClusterId::new("cluster_kv").expect("cluster")),
                node_id: Some(NodeId::new("node_kv").expect("node")),
                name: "kv cluster".to_string(),
            },
            &paths,
        )
        .expect("cluster init");

        let txn = KvTxn {
            compares: vec![KvCompare::Missing {
                key: "/services/test".to_string(),
            }],
            success: vec![KvOp::Put {
                key: "/services/test".to_string(),
                value: br#"{"healthy":true}"#.to_vec(),
                lease_id: None,
            }],
            failure: Vec::new(),
        };
        let payload = serde_json::to_string(&txn).expect("txn json");
        let response =
            handle_daemon_cluster_kv_txn(&DaemonPaths::with_root(root.join("daemon")), &payload)
                .expect("kv txn");

        assert!(response.succeeded);
        let store = super::read_cluster_kv_store(&paths).expect("kv store");
        let service = store.get("/services/test").expect("service entry");
        assert_eq!(service.value, br#"{"healthy":true}"#.to_vec());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn daemon_cluster_sync_merges_peer_members_and_kv_snapshot() {
        let host_root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-sync-host-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let peer_root = std::env::temp_dir().join(format!(
            "sinomo-cli-cluster-sync-peer-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let host_paths = ClusterPaths::new(&SinomoDataDir::new(&host_root));
        let peer_paths = ClusterPaths::new(&SinomoDataDir::new(&peer_root));
        let cluster_id = ClusterId::new("cluster_sync").expect("cluster");
        cluster_init_with_paths(
            ClusterInitOptions {
                cluster_id: Some(cluster_id.clone()),
                node_id: Some(NodeId::new("node_host").expect("node")),
                name: "sync cluster".to_string(),
            },
            &host_paths,
        )
        .expect("cluster init");

        let host_identity =
            super::load_or_create_main_node_identity(&host_root).expect("host identity");
        let mut invite = ClusterJoinInvite {
            invite_id: ClusterInviteId::new("invite_sync").expect("invite"),
            cluster_id: cluster_id.clone(),
            cluster_name: Some("sync cluster".to_string()),
            introducers: Vec::new(),
            token_id: "token_sync".to_string(),
            expires_at: time::OffsetDateTime::now_utc() + time::Duration::seconds(60),
            trust_domain: TrustDomain::new("trust.local").expect("trust"),
            allowed_member_roles: vec![ClusterMemberRole::Learner],
            issuer_node_id: NodeId::new("node_host").expect("node"),
            issuer_public_key: host_identity.public_key.clone(),
            signature: String::new(),
        };
        invite.signature = sign_cluster_join_invite(&invite, &host_identity).expect("sign invite");
        cluster_join_with_paths(
            ClusterJoinOptions {
                invite: encode_cluster_join_invite(&invite).expect("invite uri"),
                cluster_id: None,
                node_id: Some(NodeId::new("node_peer").expect("node")),
                name: "peer".to_string(),
            },
            &peer_paths,
        )
        .expect("cluster join");

        let mut peer_state = read_cluster_state(&peer_paths).expect("peer state");
        for member in &mut peer_state.members {
            member.state = match member.role {
                ClusterMemberRole::Learner => ClusterMemberState::Learner,
                ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
                ClusterMemberRole::Voter => ClusterMemberState::Voter,
            };
        }
        ensure_daemon_cluster_members(
            &mut peer_state,
            &DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 22_353,
            },
            &DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 22_354,
            },
            time::OffsetDateTime::now_utc(),
        );
        let peer_snapshot = sync_cluster_state_to_kv(&peer_paths, &peer_state).expect("peer kv");
        let request = ClusterSyncRequest {
            state: peer_state,
            kv_snapshot: peer_snapshot,
        };
        let response = handle_daemon_cluster_sync(
            &DaemonPaths::with_root(host_root.join("daemon")),
            &serde_json::to_string(&request).expect("sync request"),
        )
        .expect("host sync");

        assert!(response.accepted);
        let host_members = read_cluster_members_from_kv(&host_paths).expect("host kv members");
        assert!(
            host_members
                .iter()
                .any(|member| member.node_id == NodeId::new("node_peer").expect("node"))
        );

        let response_request = ClusterSyncRequest {
            state: response.state,
            kv_snapshot: response.kv_snapshot,
        };
        let peer_response = handle_daemon_cluster_sync(
            &DaemonPaths::with_root(peer_root.join("daemon")),
            &serde_json::to_string(&response_request).expect("sync response request"),
        )
        .expect("peer sync");

        assert!(peer_response.accepted);
        let peer_members = read_cluster_members_from_kv(&peer_paths).expect("peer kv members");
        assert!(
            peer_members
                .iter()
                .any(|member| member.node_id == NodeId::new("node_host").expect("node"))
        );
        std::fs::remove_dir_all(host_root).ok();
        std::fs::remove_dir_all(peer_root).ok();
    }

    #[test]
    fn rollout_parser_accepts_apply_status_and_init_source() {
        assert_eq!(
            parse_rollout_command(&["status".to_string()]).expect("status"),
            RolloutCliCommand::Status
        );
        assert_eq!(
            parse_rollout_command(&["apply".to_string()]).expect("apply"),
            RolloutCliCommand::Apply(RolloutOptions::default())
        );
        assert_eq!(
            parse_rollout_command(&[
                "import-source".to_string(),
                "--repo".to_string(),
                "/work/sinomo".to_string()
            ])
            .expect("import-source"),
            RolloutCliCommand::ImportSource(RolloutOptions {
                repo: Some(std::path::PathBuf::from("/work/sinomo")),
                ..RolloutOptions::default()
            })
        );
        assert_eq!(
            parse_rollout_command(&[
                "export-source".to_string(),
                "--repo".to_string(),
                "/work/sinomo".to_string()
            ])
            .expect("export-source"),
            RolloutCliCommand::ExportSource(RolloutOptions {
                repo: Some(std::path::PathBuf::from("/work/sinomo")),
                ..RolloutOptions::default()
            })
        );
        assert_eq!(
            parse_rollout_command(&[
                "apply-external".to_string(),
                "--repo".to_string(),
                "/work/sinomo".to_string()
            ])
            .expect("apply-external"),
            RolloutCliCommand::ApplyExternal(RolloutOptions {
                repo: Some(std::path::PathBuf::from("/work/sinomo")),
                ..RolloutOptions::default()
            })
        );
        assert_eq!(
            parse_rollout_command(&[
                "init-source".to_string(),
                "--repo".to_string(),
                "/work/sinomo".to_string()
            ])
            .expect("init-source"),
            RolloutCliCommand::InitSource(RolloutOptions {
                repo: Some(std::path::PathBuf::from("/work/sinomo")),
                host: "127.0.0.1".to_string(),
                port: 21_353,
                ..RolloutOptions::default()
            })
        );
        assert_eq!(
            parse_rollout_command(&[
                "cluster".to_string(),
                "--repo".to_string(),
                "/work/sinomo".to_string(),
                "--target".to_string(),
                "192.168.1.20:21353".to_string(),
                "--shadow-port".to_string(),
                "21454".to_string(),
                "--skip-local".to_string(),
            ])
            .expect("cluster"),
            RolloutCliCommand::Cluster(RolloutOptions {
                repo: Some(std::path::PathBuf::from("/work/sinomo")),
                targets: vec!["192.168.1.20:21353".to_string()],
                shadow_port: 21_454,
                skip_local: true,
                ..RolloutOptions::default()
            })
        );
        assert_eq!(
            parse_rollout_command(&[
                "cluster".to_string(),
                "--remote-build".to_string(),
                "--remote-repo".to_string(),
                "/srv/sinomo-agent".to_string(),
                "--target".to_string(),
                "10.14.1.11:21353".to_string(),
                "--skip-local".to_string(),
            ])
            .expect("remote-build cluster"),
            RolloutCliCommand::Cluster(RolloutOptions {
                remote_build: true,
                remote_repo: Some(std::path::PathBuf::from("/srv/sinomo-agent")),
                targets: vec!["10.14.1.11:21353".to_string()],
                skip_local: true,
                ..RolloutOptions::default()
            })
        );
    }

    #[test]
    fn daemon_rollout_build_rejects_missing_remote_repo() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-rollout-build-missing-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let request = ClusterRolloutBuildRequest {
            source: "test".to_string(),
            remote_repo: None,
            host: "127.0.0.1".to_string(),
            port: 21_353,
            shadow_port: 21_354,
            requested_at: "now".to_string(),
        };
        let payload = serde_json::to_string(&request).expect("request json");
        let response =
            handle_daemon_rollout_build(&DaemonPaths::with_root(root.join("daemon")), &payload)
                .expect("build response");

        assert_eq!(response.status, "rejected");
        assert!(
            response
                .detail
                .contains("remote source repo is not initialized")
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rollout_main_activation_requires_running_shadow() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-rollout-shadow-required-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("layout");
        let error = stage_rollout_main_activation(
            &root,
            &rollout_paths,
            RolloutMainActivation {
                source: "test".to_string(),
                main_binary: root.join("bin").join("candidate-main"),
                shadow_binary: root.join("bin").join("candidate-shadow"),
                staged_main_binary: None,
                staged_shadow_binary: None,
                previous_main_binary: Some(root.join("bin").join("previous-main")),
                main_host: "0.0.0.0".to_string(),
                main_port: 21_353,
                shadow_port: 21_354,
                requested_at: "now".to_string(),
                mode: "test".to_string(),
            },
        )
        .expect_err("shadow required");

        assert!(matches!(error, CliError::DaemonNotRunning { .. }));
        assert!(!super::rollout_main_activation_path(&rollout_paths).exists());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rollout_parser_reuses_daemon_endpoint_defaults() {
        assert_eq!(
            parse_rollout_command(&[
                "apply".to_string(),
                "--host".to_string(),
                "0.0.0.0".to_string(),
                "--port".to_string(),
                "21400".to_string()
            ])
            .expect("apply custom"),
            RolloutCliCommand::Apply(RolloutOptions {
                repo: None,
                host: "0.0.0.0".to_string(),
                port: 21_400,
                ..RolloutOptions::default()
            })
        );
    }

    #[test]
    fn daemon_parser_supports_shadow_daemon_on_separate_default_port() {
        assert_eq!(
            parse_daemon_command(&["shadow".to_string(), "run".to_string()]).expect("shadow"),
            DaemonCliCommand::ShadowRun(DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21_354,
            })
        );
        assert_eq!(
            parse_daemon_command(&["shadow".to_string(), "status".to_string()]).expect("shadow"),
            DaemonCliCommand::ShadowStatus(DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21_354,
            })
        );
    }

    #[test]
    fn daemon_paths_for_shadow_are_separate_from_main_daemon() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-daemon-shadow-paths-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let main = super::DaemonPaths::new(&data_dir);
        let shadow = super::DaemonPaths::for_shadow(&data_dir);

        assert_eq!(main.root, root.join("daemon"));
        assert_eq!(shadow.root, root.join("shadow-daemon"));
        assert_ne!(main.lock, shadow.lock);
        assert_ne!(main.status, shadow.status);
    }

    #[test]
    fn daemon_parser_uses_port_21353_by_default() {
        assert_eq!(
            parse_daemon_command(&["run".to_string()]).expect("daemon run"),
            DaemonCliCommand::Run(DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21_353,
            })
        );
        assert_eq!(
            parse_daemon_command(&["status".to_string()]).expect("daemon status"),
            DaemonCliCommand::Status(DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21_353,
            })
        );
        assert_eq!(
            parse_daemon_command(&[
                "run".to_string(),
                "--host".to_string(),
                "0.0.0.0".to_string(),
                "--port".to_string(),
                "21354".to_string(),
            ])
            .expect("daemon run custom"),
            DaemonCliCommand::Run(DaemonRunOptions {
                host: "0.0.0.0".to_string(),
                port: 21_354,
            })
        );
    }

    #[test]
    fn daemon_paths_live_under_data_directory() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-daemon-paths-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let paths = super::DaemonPaths::new(&SinomoDataDir::new(&root));

        assert_eq!(paths.root, root.join("daemon"));
        assert_eq!(paths.lock, root.join("daemon").join("daemon.lock"));
        assert_eq!(paths.status, root.join("daemon").join("status.json"));
    }

    #[test]
    fn daemon_lock_prevents_duplicate_instances_and_preserves_probe_status() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-daemon-lock-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let paths = super::DaemonPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("daemon paths");

        let lock = super::DaemonLock::acquire(&paths, &DaemonRunOptions::default(), None)
            .expect("first daemon lock");
        assert!(matches!(
            super::probe_daemon_status(&paths, Duration::from_millis(1)),
            DaemonProbeStatus::Locked { .. }
        ));
        assert!(super::DaemonLock::acquire(&paths, &DaemonRunOptions::default(), None).is_err());

        drop(lock);
        assert!(matches!(
            super::probe_daemon_status(&paths, Duration::from_millis(1)),
            DaemonProbeStatus::NotRunning
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn evolve_propose_parser_accepts_common_flags() {
        let options = parse_propose_options(&[
            "improve scorecards".to_string(),
            "--provider".to_string(),
            "github-copilot".to_string(),
            "--model".to_string(),
            "gpt-5.4".to_string(),
            "--allow".to_string(),
            "crates/sinomo-eval/**".to_string(),
            "--verify".to_string(),
            "cargo test -p sinomo-eval".to_string(),
            "--target-ring".to_string(),
            "r1".to_string(),
            "--dry-run".to_string(),
        ])
        .expect("options");

        assert_eq!(options.objective, "improve scorecards");
        assert_eq!(options.provider.as_deref(), Some("github-copilot"));
        assert_eq!(options.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(options.allow, vec!["crates/sinomo-eval/**"]);
        assert_eq!(options.verify, vec!["cargo test -p sinomo-eval"]);
        assert_eq!(options.target_ring, TrustRing::R1TrustedRuntime);
        assert!(options.dry_run);
    }

    #[test]
    fn evolve_propose_parser_accepts_campaign_phase_zero_flags() {
        let options = parse_propose_options(&[
            "--objective".to_string(),
            "speedrun minecraft".to_string(),
            "--horizon".to_string(),
            "30 days".to_string(),
            "--budget-policy".to_string(),
            "free-and-prepaid".to_string(),
            "--reserve".to_string(),
            "coding-plan-weekly:30%".to_string(),
            "--reserve".to_string(),
            "free-api-openrouter:40%".to_string(),
            "--approval".to_string(),
            "stage-rollout".to_string(),
            "--max-wall-time".to_string(),
            "10m".to_string(),
            "--pause-after-proposal".to_string(),
            "--dry-run".to_string(),
        ])
        .expect("options");

        assert_eq!(options.objective, "speedrun minecraft");
        assert_eq!(options.horizon.as_deref(), Some("30 days"));
        assert_eq!(options.budget_policy.as_deref(), Some("free-and-prepaid"));
        assert_eq!(
            options.reserve_floors,
            vec!["coding-plan-weekly:30%", "free-api-openrouter:40%"]
        );
        assert_eq!(options.approval_policy.as_deref(), Some("stage-rollout"));
        assert_eq!(options.max_wall_time.as_deref(), Some("10m"));
        assert!(options.pause_after_proposal);
        assert!(options.dry_run);
    }

    #[test]
    fn evolve_tick_parser_accepts_phase_four_background_flags() {
        let command = super::parse_evolve_command(&[
            "tick".to_string(),
            "--dry-run".to_string(),
            "--scheduler-service".to_string(),
            "sinomo-cluster-scheduler".to_string(),
            "--max-concurrent".to_string(),
            "1".to_string(),
            "--max-concurrent-workloads".to_string(),
            "4".to_string(),
            "--workload".to_string(),
            "backup:backup-index-01:ready:Backup index".to_string(),
            "--workload-estimate".to_string(),
            "backup:backup-index-01:cluster-background-hours:40".to_string(),
            "--workload-human-checkpoint".to_string(),
            "memory:dataset-curation-42".to_string(),
            "--resource".to_string(),
            "coding-plan-weekly:75/100:weekly".to_string(),
            "--estimate".to_string(),
            "coding-plan-weekly:10".to_string(),
            "--reset-window-open".to_string(),
            "--idle-window-open".to_string(),
        ])
        .expect("tick command");
        assert!(matches!(command, super::EvolveCliCommand::Tick(_)));

        let options = super::parse_background_tick_options(&[
            "--dry-run".to_string(),
            "--scheduler-service".to_string(),
            "sinomo-cluster-scheduler".to_string(),
            "--max-concurrent".to_string(),
            "1".to_string(),
            "--max-concurrent-workloads".to_string(),
            "4".to_string(),
            "--workload".to_string(),
            "backup:backup-index-01:ready:Backup index".to_string(),
            "--workload-estimate".to_string(),
            "backup:backup-index-01:cluster-background-hours:40".to_string(),
            "--workload-human-checkpoint".to_string(),
            "memory:dataset-curation-42".to_string(),
            "--resource".to_string(),
            "coding-plan-weekly:75/100:weekly".to_string(),
            "--estimate".to_string(),
            "coding-plan-weekly:10".to_string(),
            "--reset-window-open".to_string(),
            "--idle-window-open".to_string(),
        ])
        .expect("tick options");

        assert!(options.dry_run);
        assert_eq!(options.scheduler_service_id, "sinomo-cluster-scheduler");
        assert_eq!(options.max_concurrent_campaigns, 1);
        assert_eq!(options.max_concurrent_workloads, 4);
        assert_eq!(options.resources, vec!["coding-plan-weekly:75/100:weekly"]);
        assert_eq!(options.estimates, vec!["coding-plan-weekly:10"]);
        assert_eq!(
            options.external_workloads,
            vec!["backup:backup-index-01:ready:Backup index"]
        );
        assert_eq!(
            options.external_workload_estimates,
            vec!["backup:backup-index-01:cluster-background-hours:40"]
        );
        assert_eq!(
            options.external_human_checkpoints,
            vec!["memory:dataset-curation-42"]
        );
        let workloads = super::parse_background_tick_external_workloads(
            &options.external_workloads,
            &options.external_workload_estimates,
            &options.external_human_checkpoints,
        )
        .expect("external workloads");
        assert_eq!(workloads.len(), 2);
        assert_eq!(workloads[0].system, "backup");
        assert_eq!(
            workloads[0].resource_estimates[0].resource,
            "cluster-background-hours"
        );
        assert_eq!(workloads[0].resource_estimates[0].estimated_units, 40);
        assert!(workloads[1].human_checkpoint_required);
        assert!(options.reset_window_open);
        assert!(options.idle_window_open);
    }

    #[test]
    fn evolve_tick_parser_rejects_cluster_identifier_flags() {
        let result = super::parse_background_tick_options(&[
            "--dry-run".to_string(),
            "--cluster".to_string(),
            "home-cluster".to_string(),
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn evolve_evaluate_parser_accepts_phase_three_stage_flags() {
        let command = super::parse_evolve_command(&[
            "evaluate".to_string(),
            "camp_demo".to_string(),
            "--job".to_string(),
            "evojob_demo".to_string(),
            "--task".to_string(),
            "task_demo".to_string(),
            "--failure-class".to_string(),
            "foundation-blocker".to_string(),
            "--root-cause".to_string(),
            "stage evidence missing from packet".to_string(),
            "--learned".to_string(),
            "add stage evidence before retry".to_string(),
        ])
        .expect("evaluate command");
        assert!(matches!(command, super::EvolveCliCommand::Evaluate(_)));

        let options = super::parse_stage_evaluation_options(&[
            "camp_demo".to_string(),
            "--job".to_string(),
            "evojob_demo".to_string(),
            "--task".to_string(),
            "task_demo".to_string(),
            "--failure-class".to_string(),
            "foundation-blocker".to_string(),
            "--root-cause".to_string(),
            "stage evidence missing from packet".to_string(),
            "--learned".to_string(),
            "add stage evidence before retry".to_string(),
        ])
        .expect("stage evaluation options");

        assert_eq!(options.campaign_id, "camp_demo");
        assert_eq!(options.job_id.as_str(), "evojob_demo");
        assert_eq!(options.task_id, "task_demo");
        assert_eq!(
            options.failure_class,
            Some(sinomo_evolution::EvolutionFailureClass::FoundationBlocker)
        );
        assert_eq!(
            options.root_cause_summary.as_deref(),
            Some("stage evidence missing from packet")
        );
        assert_eq!(
            options.what_was_learned,
            vec!["add stage evidence before retry".to_string()]
        );
    }

    #[test]
    fn evolve_iterate_parser_accepts_phase_two_dry_run_flags() {
        let command = super::parse_evolve_command(&[
            "iterate".to_string(),
            "camp_demo".to_string(),
            "--task".to_string(),
            "task_demo".to_string(),
            "--path".to_string(),
            "crates/sinomo-evolution/**".to_string(),
            "--verify".to_string(),
            "cargo test -p sinomo-evolution --lib".to_string(),
            "--dry-run".to_string(),
        ])
        .expect("iterate command");
        assert!(matches!(command, super::EvolveCliCommand::Iterate(_)));

        let options = super::parse_iteration_options(&[
            "camp_demo".to_string(),
            "--task".to_string(),
            "task_demo".to_string(),
            "--path".to_string(),
            "crates/sinomo-evolution/**".to_string(),
            "--verify".to_string(),
            "cargo test -p sinomo-evolution --lib".to_string(),
            "--dry-run".to_string(),
        ])
        .expect("iteration options");

        assert_eq!(options.campaign_id, "camp_demo");
        assert_eq!(options.task_id.as_deref(), Some("task_demo"));
        assert_eq!(
            options.intended_paths,
            vec!["crates/sinomo-evolution/**".to_string()]
        );
        assert_eq!(
            options.verification_commands,
            vec!["cargo test -p sinomo-evolution --lib".to_string()]
        );
        assert!(options.dry_run);
    }

    #[test]
    fn campaign_iteration_dry_run_persists_phase_two_handoff_artifacts() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-campaign-iterate-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let mut campaign = sinomo_evolution::propose_evolution_campaign(
            sinomo_evolution::CampaignProposalRequest {
                objective: "improve scheduler iteration dispatch".to_string(),
                horizon: "14 days".to_string(),
                budget_policy: "free-and-prepaid".to_string(),
                reserve_floors: vec![sinomo_evolution::ReserveFloor {
                    resource: "coding-plan-weekly".to_string(),
                    floor_percent: 30,
                }],
                approval_policy: "stage-rollout".to_string(),
                max_wall_time: Some("10m".to_string()),
                pause_after_proposal: false,
                dry_run: true,
                approved: true,
                target_ring: TrustRing::R1TrustedRuntime,
                source_repo: Some("/repo".to_string()),
            },
        );
        campaign.status = sinomo_evolution::EvolutionCampaignStatus::Scheduled;
        let task_id = campaign.near_term_tasks[0].task_id.clone();
        super::write_campaign_bundle(&paths, &campaign).expect("bundle");
        let plan = sinomo_evolution::prepare_scheduled_iteration(
            sinomo_evolution::ScheduledIterationRequest {
                campaign: campaign.clone(),
                task_id: Some(task_id.clone()),
                intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                extra_verification_commands: vec![
                    "cargo test -p sinomo-evolution --lib".to_string(),
                ],
                dry_run: true,
            },
        )
        .expect("iteration plan");

        let bundle = super::write_campaign_iteration_dry_run(&paths, &mut campaign, &plan)
            .expect("iteration artifacts");

        assert!(bundle.iteration_plan_json.exists());
        assert!(bundle.iteration_request_json.exists());
        assert!(bundle.iteration_dispatches_jsonl.exists());
        let persisted: sinomo_evolution::EvolutionCampaign = serde_json::from_str(
            &std::fs::read_to_string(&bundle.campaign_json).expect("campaign json"),
        )
        .expect("campaign parse");
        assert_eq!(
            persisted.status,
            sinomo_evolution::EvolutionCampaignStatus::Scheduled
        );
        assert!(
            std::fs::read_to_string(&bundle.iteration_dispatches_jsonl)
                .expect("dispatch jsonl")
                .contains(&plan.iteration_id)
        );
        assert!(
            std::fs::read_to_string(&bundle.decisions_jsonl)
                .expect("decisions jsonl")
                .contains("TaskSelected")
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn evolve_schedule_parser_accepts_phase_one_resource_dry_run_flags() {
        let command = super::parse_evolve_command(&[
            "schedule".to_string(),
            "camp_demo".to_string(),
            "--resource".to_string(),
            "coding-plan-weekly:65/100:weekly".to_string(),
            "--estimate".to_string(),
            "coding-plan-weekly:10".to_string(),
            "--dry-run".to_string(),
        ])
        .expect("schedule command");
        assert!(matches!(command, super::EvolveCliCommand::Schedule(_)));

        let options = super::parse_schedule_options(&[
            "camp_demo".to_string(),
            "--resource".to_string(),
            "coding-plan-weekly:65/100:weekly".to_string(),
            "--estimate".to_string(),
            "coding-plan-weekly:10".to_string(),
            "--dry-run".to_string(),
        ])
        .expect("schedule options");

        assert_eq!(options.campaign_id, "camp_demo");
        assert_eq!(options.resources, vec!["coding-plan-weekly:65/100:weekly"]);
        assert_eq!(options.estimates, vec!["coding-plan-weekly:10"]);
        assert!(options.dry_run);
    }

    #[test]
    fn campaign_scheduler_dry_run_persists_phase_one_artifacts() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-campaign-schedule-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let mut campaign = sinomo_evolution::propose_evolution_campaign(
            sinomo_evolution::CampaignProposalRequest {
                objective: "improve scheduler".to_string(),
                horizon: "14 days".to_string(),
                budget_policy: "free-and-prepaid".to_string(),
                reserve_floors: vec![sinomo_evolution::ReserveFloor {
                    resource: "coding-plan-weekly".to_string(),
                    floor_percent: 30,
                }],
                approval_policy: "stage-rollout".to_string(),
                max_wall_time: Some("10m".to_string()),
                pause_after_proposal: false,
                dry_run: true,
                approved: true,
                target_ring: TrustRing::R1TrustedRuntime,
                source_repo: Some("/repo".to_string()),
            },
        );
        let task_id = campaign.near_term_tasks[0].task_id.clone();
        let _bundle = super::write_campaign_bundle(&paths, &campaign).expect("bundle");
        let dry_run = sinomo_evolution::dry_run_resource_scheduler(
            sinomo_evolution::ResourceSchedulerDryRunRequest {
                campaign: campaign.clone(),
                preview_horizon: "24h".to_string(),
                resources: vec![sinomo_evolution::ResourceClassDefinition {
                    resource_id: "coding-plan-weekly".to_string(),
                    unit: "percent".to_string(),
                    total_units: 100,
                    used_units: 65,
                    reserve_floor_percent: 30,
                    reset_window: "weekly".to_string(),
                    notes: "test budget".to_string(),
                }],
                task_estimates: vec![sinomo_evolution::ResourceUsageEstimate {
                    task_id,
                    resource: "coding-plan-weekly".to_string(),
                    estimated_units: 10,
                    actual_units: Some(65),
                }],
            },
        );

        let bundle = super::write_campaign_schedule_dry_run(&paths, &mut campaign, &dry_run)
            .expect("scheduler artifacts");

        assert!(bundle.resource_ledger_json.exists());
        assert!(bundle.scheduler_dry_run_json.exists());
        assert!(bundle.lease_requests_jsonl.exists());
        assert!(bundle.lease_decisions_jsonl.exists());
        let persisted: sinomo_evolution::EvolutionCampaign = serde_json::from_str(
            &std::fs::read_to_string(&bundle.campaign_json).expect("campaign json"),
        )
        .expect("campaign parse");
        assert_eq!(
            persisted.status,
            sinomo_evolution::EvolutionCampaignStatus::PausedBudget
        );
        assert!(
            std::fs::read_to_string(&bundle.decisions_jsonl)
                .expect("decisions jsonl")
                .contains("Paused")
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn campaign_scheduler_dry_run_records_resume_when_budget_recovers() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-campaign-resume-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let mut campaign = sinomo_evolution::propose_evolution_campaign(
            sinomo_evolution::CampaignProposalRequest {
                objective: "improve scheduler".to_string(),
                horizon: "14 days".to_string(),
                budget_policy: "free-and-prepaid".to_string(),
                reserve_floors: vec![sinomo_evolution::ReserveFloor {
                    resource: "coding-plan-weekly".to_string(),
                    floor_percent: 30,
                }],
                approval_policy: "stage-rollout".to_string(),
                max_wall_time: Some("10m".to_string()),
                pause_after_proposal: false,
                dry_run: true,
                approved: true,
                target_ring: TrustRing::R1TrustedRuntime,
                source_repo: Some("/repo".to_string()),
            },
        );
        campaign.status = sinomo_evolution::EvolutionCampaignStatus::PausedBudget;
        let task_id = campaign.near_term_tasks[0].task_id.clone();
        super::write_campaign_bundle(&paths, &campaign).expect("bundle");
        let dry_run = sinomo_evolution::dry_run_resource_scheduler(
            sinomo_evolution::ResourceSchedulerDryRunRequest {
                campaign: campaign.clone(),
                preview_horizon: "24h".to_string(),
                resources: vec![sinomo_evolution::ResourceClassDefinition {
                    resource_id: "coding-plan-weekly".to_string(),
                    unit: "percent".to_string(),
                    total_units: 100,
                    used_units: 20,
                    reserve_floor_percent: 30,
                    reset_window: "weekly".to_string(),
                    notes: "test budget".to_string(),
                }],
                task_estimates: vec![sinomo_evolution::ResourceUsageEstimate {
                    task_id,
                    resource: "coding-plan-weekly".to_string(),
                    estimated_units: 10,
                    actual_units: Some(20),
                }],
            },
        );

        let bundle = super::write_campaign_schedule_dry_run(&paths, &mut campaign, &dry_run)
            .expect("scheduler artifacts");

        let persisted: sinomo_evolution::EvolutionCampaign = serde_json::from_str(
            &std::fs::read_to_string(&bundle.campaign_json).expect("campaign json"),
        )
        .expect("campaign parse");
        assert_eq!(
            persisted.status,
            sinomo_evolution::EvolutionCampaignStatus::Scheduled
        );
        assert!(
            std::fs::read_to_string(&bundle.decisions_jsonl)
                .expect("decisions jsonl")
                .contains("Resumed")
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn campaign_writer_persists_phase_zero_bundle_under_data_dir() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-campaign-bundle-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let campaign = sinomo_evolution::propose_evolution_campaign(
            sinomo_evolution::CampaignProposalRequest {
                objective: "improve scheduler".to_string(),
                horizon: "14 days".to_string(),
                budget_policy: "free-and-prepaid".to_string(),
                reserve_floors: vec![sinomo_evolution::ReserveFloor {
                    resource: "coding-plan-weekly".to_string(),
                    floor_percent: 25,
                }],
                approval_policy: "stage-rollout".to_string(),
                max_wall_time: Some("10m".to_string()),
                pause_after_proposal: true,
                dry_run: true,
                approved: false,
                target_ring: TrustRing::R1TrustedRuntime,
                source_repo: Some("/repo".to_string()),
            },
        );

        let bundle = super::write_campaign_bundle(&paths, &campaign).expect("bundle");

        assert_eq!(bundle.root, paths.campaign_dir(&campaign.campaign_id));
        assert!(bundle.campaign_json.exists());
        assert!(bundle.objective_card_json.exists());
        assert!(bundle.stages_json.exists());
        assert!(bundle.near_term_tasks_json.exists());
        assert!(bundle.resource_schedule_preview_json.exists());
        assert!(bundle.approval_checkpoint_json.exists());
        assert!(bundle.decisions_jsonl.exists());
        let persisted: sinomo_evolution::EvolutionCampaign = serde_json::from_str(
            &std::fs::read_to_string(&bundle.campaign_json).expect("campaign json"),
        )
        .expect("campaign parse");
        assert_eq!(persisted.campaign_id, campaign.campaign_id);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn slash_commands_reuse_cli_argument_shape() {
        let args = parse_slash_command(
            "/evolve propose \"improve scorecards\" --verify \"cargo check\" --dry-run",
        )
        .expect("parse")
        .expect("slash command");

        assert_eq!(
            args,
            vec![
                "evolve",
                "propose",
                "improve scorecards",
                "--verify",
                "cargo check",
                "--dry-run"
            ]
        );
        assert_eq!(parse_slash_command("hello sinomo").expect("parse"), None);
        assert_eq!(
            parse_slash_command("/evolve approve --note \"\"")
                .expect("parse")
                .expect("slash command"),
            vec!["evolve", "approve", "--note", ""]
        );
    }

    #[test]
    fn interaction_runtime_records_message_input_as_events() {
        let session = InteractionSession::new(
            SessionId::new("sess_cli_test").expect("session"),
            NodeId::new("node_cli_test").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        let mut runtime = InteractionRuntime::new(
            InMemoryEventStore::new(),
            fixture_handler("hello from fixture"),
            session,
        );

        let outputs = runtime
            .ingest_user_input("start a conversation")
            .expect("input");

        assert_eq!(outputs.len(), 1);
        assert_eq!(
            outputs[0],
            super::InteractionOutput::Line("sinomo: hello from fixture".to_string())
        );
        let events = runtime.store().replay(ReplayQuery::all()).expect("replay");
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].event.kind.as_str(), "interaction.input.received");
        assert_eq!(
            events[1].event.kind.as_str(),
            "interaction.message.received"
        );
        assert_eq!(events[2].event.kind.as_str(), "interaction.model.requested");
        assert_eq!(events[3].event.kind.as_str(), "interaction.model.responded");
        assert_eq!(
            events[1].event.links.caused_by,
            vec![events[0].event.event_id.clone()]
        );
        assert_eq!(
            events[3].event.links.caused_by,
            vec![events[2].event.event_id.clone()]
        );
    }

    #[test]
    fn cui_loop_starts_and_exits_from_quit_command() {
        let mut output = Vec::new();

        run_cui_with_handler_io(
            Cursor::new("/quit\n"),
            &mut output,
            fixture_handler("unused"),
        )
        .expect("cui");

        let output = String::from_utf8(output).expect("utf8");
        assert!(output.contains("Sinomo CUI"));
        assert!(output.contains("ending session"));
    }

    #[test]
    fn cui_loop_uses_connected_daemon_cluster_node_when_available() {
        let mut output = Vec::new();
        let daemon = DaemonStatus {
            pid: 4242,
            host: "0.0.0.0".to_string(),
            port: 21_353,
            address: "127.0.0.1:21353".to_string(),
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            started_at: "2026-05-04T00:00:00Z".to_string(),
            data_dir: PathBuf::from("data"),
            lock_file: PathBuf::from("data/daemon/daemon.lock"),
            cluster: Some(DaemonClusterStatus {
                cluster_id: ClusterId::new("cluster_cui").expect("cluster"),
                node_id: NodeId::new("node_daemon").expect("node"),
                process: super::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
                member_role: ClusterMemberRole::Voter,
                member_state: ClusterMemberState::Active,
            }),
        };

        run_cui_with_handler_io_and_daemon(
            Cursor::new("/quit\n"),
            &mut output,
            fixture_handler("unused"),
            Some(daemon),
        )
        .expect("cui");

        let output = String::from_utf8(output).expect("utf8");
        assert!(output.contains("node: node_daemon"));
        assert!(output.contains("daemon: connected pid=4242 address=127.0.0.1:21353"));
        assert!(output.contains("cluster: cluster_cui"));
    }

    fn fixture_handler(
        response: &'static str,
    ) -> ModelBackedInteractionHandler<FixtureInteractionModel> {
        ModelBackedInteractionHandler::new(
            FixtureInteractionModel { response },
            "fixture",
            "fixture-model",
        )
    }

    #[derive(Clone, Debug)]
    struct FixtureInteractionModel {
        response: &'static str,
    }

    impl InteractionModel for FixtureInteractionModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            assert_eq!(request.provider, "fixture");
            assert_eq!(request.model, "fixture-model");
            assert!(request.messages[1].content.contains("start a conversation"));
            Ok(LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content: self.response.to_string(),
                usage: Some(LanguageModelUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(1),
                }),
            })
        }
    }

    #[test]
    fn model_list_parser_accepts_optional_provider() {
        assert_eq!(
            parse_models_list_options(&[]).expect("empty args"),
            super::ModelListOptions { provider: None }
        );
        assert_eq!(
            parse_models_list_options(&["--provider".to_string(), "github-copilot".to_string()])
                .expect("provider"),
            super::ModelListOptions {
                provider: Some("github-copilot".to_string())
            }
        );
        assert!(matches!(
            parse_models_list_options(&["github-copilot".to_string()]),
            Err(CliError::Usage)
        ));
    }

    #[test]
    fn usage_parser_accepts_provider_and_json() {
        let options = parse_usage_options(&[
            "--provider".to_string(),
            "codex".to_string(),
            "--json".to_string(),
        ])
        .expect("usage options");

        assert_eq!(options.provider.as_deref(), Some("codex"));
        assert!(options.json);
        assert!(!options.help);
    }

    #[test]
    fn model_default_parser_accepts_provider_model() {
        let options = parse_model_default_options(
            &[
                "--provider".to_string(),
                "openai-api".to_string(),
                "--model".to_string(),
                "gpt-5.5".to_string(),
            ],
            ModelDefaultRepoMode::Reject,
        )
        .expect("options");

        assert_eq!(options.provider, "openai");
        assert_eq!(options.model, "gpt-5.5");
        assert_eq!(options.repo, None);
    }

    #[test]
    fn model_default_parser_normalizes_nvidia_provider_qualified_model() {
        let options = parse_model_default_options(
            &[
                "--provider".to_string(),
                "nvidia".to_string(),
                "--model".to_string(),
                "nvidia/nvidia/nemotron-3-super-120b-a12b".to_string(),
            ],
            ModelDefaultRepoMode::Reject,
        )
        .expect("options");

        assert_eq!(options.provider, "nvidia");
        assert_eq!(options.model, "nvidia/nemotron-3-super-120b-a12b");
    }

    #[test]
    fn evolve_model_default_parser_accepts_repo_for_compatibility() {
        let options = parse_model_default_options(
            &[
                "--provider".to_string(),
                "openai-api".to_string(),
                "--model".to_string(),
                "gpt-5.5".to_string(),
                "--repo".to_string(),
                "/repo".to_string(),
            ],
            ModelDefaultRepoMode::Allow,
        )
        .expect("options");

        assert_eq!(options.provider, "openai");
        assert_eq!(options.model, "gpt-5.5");
        assert_eq!(options.repo.as_deref(), Some(std::path::Path::new("/repo")));
    }

    #[test]
    fn models_default_writes_global_model_config() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-models-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);

        models_default_with_data_dir(
            super::ModelDefaultOptions {
                provider: "openai".to_string(),
                model: "gpt-5.5".to_string(),
                repo: None,
            },
            &data_dir,
        )
        .expect("write model defaults");

        let config_path = root.join("config").join("models.json");
        let config = serde_json::from_str::<serde_json::Value>(
            &std::fs::read_to_string(&config_path).expect("model config"),
        )
        .expect("model config json");
        assert_eq!(config["default_provider"], "openai");
        assert_eq!(config["default_model"], "gpt-5.5");

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn model_default_parser_requires_known_provider() {
        let error = parse_model_default_options(
            &[
                "--provider".to_string(),
                "unknown".to_string(),
                "--model".to_string(),
                "model".to_string(),
            ],
            ModelDefaultRepoMode::Reject,
        )
        .expect_err("unknown provider rejected");

        assert!(matches!(error, CliError::Parse { .. }));
    }

    #[test]
    fn model_provider_aliases_normalize_to_supported_provider_ids() {
        assert_eq!(
            normalize_model_provider("google-gemini").as_deref(),
            Some("gemini")
        );
        assert_eq!(
            normalize_model_provider("anthropic-claude-code").as_deref(),
            Some("claude-code")
        );
        assert_eq!(
            normalize_model_provider("github-copilot").as_deref(),
            Some("github-copilot")
        );
        assert_eq!(normalize_model_provider("codex-api"), None);
        assert_eq!(normalize_model_provider("openai-codex"), None);
        assert_eq!(normalize_model_provider("openai-codex-cli"), None);
    }

    #[test]
    fn active_job_terminal_statuses_are_explicit() {
        assert!(is_terminal_status(&EvolutionJobStatus::Approved));
        assert!(is_terminal_status(&EvolutionJobStatus::Rejected));
        assert!(!is_terminal_status(&EvolutionJobStatus::AwaitingApproval));
    }

    #[test]
    fn evolve_init_command_is_removed() {
        let error = run_evolve_command(&["init".to_string()]).expect_err("init removed");

        assert!(matches!(error, CliError::Usage));
    }

    #[test]
    fn evolution_paths_store_record_inside_job_folder() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-layout-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let paths = EvolutionPaths::new(&SinomoDataDir::new(&root));
        let job_id = EvolutionJobId::new("evojob_layout").expect("job id");

        assert_eq!(
            paths.job_path(&job_id),
            root.join("evolution")
                .join("jobs")
                .join("evojob_layout")
                .join("record.json")
        );
        assert_eq!(
            paths.legacy_job_path(&job_id),
            root.join("evolution")
                .join("jobs")
                .join("evojob_layout.json")
        );
    }
}
