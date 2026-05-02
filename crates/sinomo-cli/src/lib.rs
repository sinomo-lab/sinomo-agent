//! Shared Sinomo command-line handlers.

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_evolution::{
    CampaignDecision as EvolutionCampaignDecision, CampaignDecisionKind, CampaignProposalRequest,
    CampaignScheduleDecision, CandidateWorkspace, EvolutionCampaign, EvolutionCampaignStatus,
    EvolutionOrchestrator, EvolutionReport, EvolutionRequest, GitWorkspaceAdapter,
    LanguageModelMutationBridge, LocalCandidateBundleStore, MutationModel, MutationModelError,
    ReserveFloor, ResourceClassDefinition, ResourceSchedulerDryRun, ResourceSchedulerDryRunRequest,
    ResourceUsageEstimate, ScheduledIterationPlan, ScheduledIterationRequest,
    dry_run_resource_scheduler, prepare_scheduled_iteration, propose_evolution_campaign,
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
    CandidateBundleManifest, CandidateDecision, CandidateDecisionOutcome, Confidence,
    EvaluationGate, EvaluationOutcome, EventEnvelope, EventId, EventKind, EvolutionJob,
    EvolutionJobId, EvolutionJobStatus, LatencyClass, MutationPacket, NodeId, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, SessionId, Source, TrustRing, VerificationCommand,
};
use sinomo_runtime::eval::{ExecutingEvalHarness, LocalCommandRunner};
use sinomo_runtime::events::{EventStore, InMemoryEventStore, ReplayDirection, ReplayQuery};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::rollout::{
    MainExitKind, MainExitObservation, SelfRolloutPaths, classify_main_exit,
};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    run_parsed_cli_command(parse_cli_command(&args)?)
}

fn run_cli_command(args: &[String]) -> Result<(), CliError> {
    run_parsed_cli_command(parse_cli_command(args)?)
}

fn run_parsed_cli_command(command: CliCommand) -> Result<(), CliError> {
    match command {
        CliCommand::Cui => run_cui(),
        CliCommand::Help => {
            print_usage();
            Ok(())
        }
        CliCommand::Evolve(command) => run_evolve_cli_command(command),
        CliCommand::Daemon(command) => run_daemon_cli_command(command),
        CliCommand::Rollout(command) => run_rollout_cli_command(command),
        CliCommand::Models(command) => run_models_command(command),
        CliCommand::Providers(command) => run_provider_command(command),
        CliCommand::Usage(options) => usage(options),
    }
}

#[derive(Debug, Clone)]
enum CliCommand {
    Cui,
    Help,
    Evolve(EvolveCliCommand),
    Daemon(DaemonCliCommand),
    Rollout(RolloutCliCommand),
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
        Some((command, rest)) if command == "evolve" => {
            Ok(CliCommand::Evolve(parse_evolve_command(rest)?))
        }
        Some((command, rest)) if command == "daemon" => {
            Ok(CliCommand::Daemon(parse_daemon_command(rest)?))
        }
        Some((command, rest)) if command == "rollout" => {
            Ok(CliCommand::Rollout(parse_rollout_command(rest)?))
        }
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

const SINOMO_DAEMON_DEFAULT_HOST: &str = "127.0.0.1";
const SINOMO_DAEMON_DEFAULT_PORT: u16 = 21_353;
const SINOMO_SHADOW_DAEMON_DEFAULT_PORT: u16 = 21_354;
const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(400);
const SHADOW_MAIN_HOST_ENV: &str = "SINOMO_SHADOW_MAIN_HOST";
const SHADOW_MAIN_PORT_ENV: &str = "SINOMO_SHADOW_MAIN_PORT";

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
    started_at: String,
    data_dir: PathBuf,
    lock_file: PathBuf,
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
    fn acquire(paths: &DaemonPaths, options: &DaemonRunOptions) -> Result<Self, CliError> {
        paths.ensure()?;
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

        let status = DaemonStatus::new(options, paths);
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
    fn new(options: &DaemonRunOptions, paths: &DaemonPaths) -> Self {
        Self {
            pid: std::process::id(),
            host: options.host.clone(),
            port: options.port,
            address: daemon_connect_address(&options.host, options.port),
            started_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
            data_dir: paths.root.parent().unwrap_or(&paths.root).to_path_buf(),
            lock_file: paths.lock.clone(),
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

fn shadow_daemon_paths_from_defaults() -> Result<DaemonPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(DaemonPaths::for_shadow(&data_dir))
}

fn run_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    run_daemon_with_paths(options, daemon_paths_from_defaults()?, "daemon")
}

fn run_shadow_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let shadow_paths = DaemonPaths::for_shadow(&data_dir);
    let main_paths = DaemonPaths::new(&data_dir);
    let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
    let main_options = shadow_main_options_from_env()?;
    run_shadow_daemon_with_paths(
        options,
        shadow_paths,
        main_paths,
        rollout_paths,
        main_options,
    )
}

fn run_shadow_daemon_with_paths(
    options: DaemonRunOptions,
    shadow_paths: DaemonPaths,
    main_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    main_options: DaemonRunOptions,
) -> Result<(), CliError> {
    let _lock = DaemonLock::acquire(&shadow_paths, &options)?;
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

    let mut supervised_main: Option<Child> = None;
    let mut main_restart_suppressed = false;
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

        if !main_restart_suppressed
            && supervised_main.is_none()
            && rollout_paths.main_binary.exists()
            && matches!(
                probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT),
                DaemonProbeStatus::NotRunning
            )
        {
            supervised_main = Some(spawn_main_for_shadow(
                &rollout_paths.main_binary,
                &main_options,
            )?);
        }

        if let Some(child) = supervised_main.as_mut() {
            if let Some(status) = child.try_wait().map_err(|source| CliError::Io {
                path: rollout_paths.main_binary.display().to_string(),
                detail: source.to_string(),
            })? {
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
                    supervised_main = Some(spawn_main_for_shadow(
                        &rollout_paths.main_binary,
                        &main_options,
                    )?);
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

fn run_daemon_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    label: &str,
) -> Result<(), CliError> {
    let _lock = DaemonLock::acquire(&paths, &options)?;
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = TcpListener::bind(&bind_address).map_err(|source| CliError::Network {
        address: bind_address.clone(),
        detail: source.to_string(),
    })?;
    println!("sinomo {label} listening on {bind_address}");

    for stream in listener.incoming() {
        let mut stream = stream.map_err(|source| CliError::Network {
            address: bind_address.clone(),
            detail: source.to_string(),
        })?;
        if handle_daemon_connection(&mut stream, &paths)? {
            break;
        }
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
    start_daemon_with_paths(
        options,
        shadow_daemon_paths_from_defaults()?,
        &["daemon", "shadow", "run"],
        "shadow daemon",
    )
}

fn start_daemon_with_paths(
    options: DaemonRunOptions,
    paths: DaemonPaths,
    command_prefix: &[&str],
    label: &str,
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
                "sinomo {label} started: pid={} address={}",
                status.pid, status.address
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
            println!("  address: {}", status.address);
            println!("  lock: {}", status.lock_file.display());
            println!("  status: {}", paths.status.display());
            Ok(())
        }
        DaemonProbeStatus::Locked { status } => {
            println!("sinomo {label} lock is held but the listener did not answer");
            if let Some(status) = status {
                println!("  pid: {}", status.pid);
                println!("  address: {}", status.address);
            } else {
                println!(
                    "  address: {}",
                    daemon_connect_address(&options.host, options.port)
                );
            }
            println!("  lock: {}", paths.lock.display());
            Ok(())
        }
        DaemonProbeStatus::NotRunning => {
            println!("sinomo {label} is not running");
            println!(
                "  default address: {}",
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
        if send_daemon_command(&status.address, "STATUS", timeout).is_ok() {
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
    let mut stream = TcpStream::connect(address).map_err(|source| CliError::Network {
        address: address.to_string(),
        detail: source.to_string(),
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

#[derive(Debug, Clone, Eq, PartialEq)]
enum RolloutCliCommand {
    Help,
    InitSource(RolloutOptions),
    Apply(RolloutOptions),
    Status,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct RolloutOptions {
    repo: Option<PathBuf>,
    host: String,
    port: u16,
}

impl Default for RolloutOptions {
    fn default() -> Self {
        Self {
            repo: None,
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
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
        Some((command, rest)) if command == "apply" => {
            Ok(RolloutCliCommand::Apply(parse_rollout_options(rest)?))
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
        RolloutCliCommand::Apply(options) => rollout_apply(options),
        RolloutCliCommand::Status => rollout_status(),
    }
}

fn rollout_paths_from_defaults() -> Result<SelfRolloutPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(SelfRolloutPaths::new(&data_dir.root()))
}

fn rollout_ensure_layout(paths: &SelfRolloutPaths) -> Result<(), CliError> {
    for path in [&paths.root, &paths.bin] {
        fs::create_dir_all(path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    if let Some(parent) = paths.source_repo.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(())
}

fn rollout_init_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    rollout_ensure_layout(&paths)?;
    ensure_source_repo(&paths, options.repo.as_deref())?;
    println!("source repo: {}", paths.source_repo.display());
    Ok(())
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
    Ok(())
}

fn rollout_apply(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    rollout_ensure_layout(&paths)?;
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
    restart_shadow_daemon_from_binary(&plan.install_shadow_binary, &options)?;
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

fn ensure_source_repo(paths: &SelfRolloutPaths, repo: Option<&Path>) -> Result<(), CliError> {
    if paths.source_repo.join(".git").exists() {
        return Ok(());
    }
    let source = match repo {
        Some(repo) => repo.to_path_buf(),
        None => env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?,
    };
    let output = Command::new("git")
        .arg("clone")
        .arg(&source)
        .arg(&paths.source_repo)
        .output()
        .map_err(|source| CliError::Io {
            path: "git clone".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(CliError::Process {
            command: format!(
                "git clone {} {}",
                source.display(),
                paths.source_repo.display()
            ),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(())
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
        &["daemon", "shadow", "run"],
        options,
        paths,
        "shadow daemon",
    )
}

fn start_daemon_binary(
    binary: &Path,
    command_prefix: &[&str],
    options: &RolloutOptions,
    paths: DaemonPaths,
    label: &str,
) -> Result<(), CliError> {
    let mut command = Command::new(binary);
    command.args(command_prefix);
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
                "{label} healthy: pid={} address={}",
                status.pid, status.address
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
                "main daemon healthy: pid={} address={}",
                status.pid, status.address
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
    run_cui_with_handler_io(&mut reader, &mut writer, handler)
}

pub fn run_cui_with_handler_io<R: BufRead, W: Write, H: InteractionEventHandler>(
    mut reader: R,
    mut writer: W,
    handler: H,
) -> Result<(), CliError> {
    let session = InteractionSession::new(
        SessionId::generate(),
        NodeId::new("node_cli").expect("valid node id"),
        InteractionSurface::new("surface.cli", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());

    writeln!(writer, "Sinomo CUI").map_err(CliError::stdio)?;
    writeln!(writer, "session: {}", session.session_id).map_err(CliError::stdio)?;
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
        rollout_ensure_layout(&rollout_paths)?;
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
struct IterationOptions {
    campaign_id: String,
    task_id: Option<String>,
    intended_paths: Vec<String>,
    verification_commands: Vec<String>,
    dry_run: bool,
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
    println!("  sinomo daemon");
    println!("  sinomo rollout");
    println!("  sinomo models");
    println!("  sinomo usage");
    println!("  sinomo providers");
    println!("  sinomo evolve");
    println!();
    println!("Run `sinomo <subcommand> help` for detailed commands.");
    println!();
    println!("CUI:");
    println!("  Run `sinomo` with no arguments to start the command-line user interface.");
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
    println!(
        "      Create or reuse the data-directory source repo at $SINOMO_DATA_DIR/source/sinomo."
    );
    println!("  sinomo rollout apply [--repo <path>] [--host <host>] [--port <port>]");
    println!(
        "      Build the data-directory source repo, install bin/sinomo-main and bin/sinomo-shadow, restart shadow supervision, then verify main health."
    );
    println!("  sinomo rollout status");
    println!("      Show source, binary, and rollout state paths.");
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
            Self::MissingArtifact { artifact } => {
                write!(formatter, "{artifact} has not been written")
            }
            Self::Parse { detail } => write!(formatter, "failed to parse command: {detail}"),
            Self::Io { path, detail } => write!(formatter, "filesystem error at {path}: {detail}"),
            Self::Json { path, detail } => write!(formatter, "json error at {path}: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::EventStore(error) => write!(formatter, "{error}"),
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
        CliCommand, CliError, DaemonCliCommand, DaemonProbeStatus, DaemonRunOptions,
        EvolutionJobStatus, EvolutionPaths, InteractionModel, InteractionRuntime,
        InteractionSession, InteractionSurface, InteractionSurfaceKind,
        ModelBackedInteractionHandler, ModelDefaultRepoMode, ModelsCliCommand, ProviderCliCommand,
        ProviderName, RolloutCliCommand, RolloutOptions, is_terminal_status,
        models_default_with_data_dir, normalize_model_provider, parse_cli_command,
        parse_daemon_command, parse_model_default_options, parse_models_list_options,
        parse_propose_options, parse_provider_command, parse_rollout_command, parse_slash_command,
        parse_usage_options, run_cui_with_handler_io, run_evolve_command,
    };
    use sinomo_lm::{
        LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
    };
    use sinomo_runtime::core::{EvolutionJobId, NodeId, SessionId, TrustRing};
    use sinomo_runtime::events::{EventStore, InMemoryEventStore, ReplayQuery};
    use sinomo_runtime::local::SinomoDataDir;
    use std::io::Cursor;
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
            parse_cli_command(&["evolve".to_string()]).expect("evolve help"),
            CliCommand::Evolve(super::EvolveCliCommand::Help)
        ));
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
                "init-source".to_string(),
                "--repo".to_string(),
                "/work/sinomo".to_string()
            ])
            .expect("init-source"),
            RolloutCliCommand::InitSource(RolloutOptions {
                repo: Some(std::path::PathBuf::from("/work/sinomo")),
                host: "127.0.0.1".to_string(),
                port: 21_353,
            })
        );
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

        let lock = super::DaemonLock::acquire(&paths, &DaemonRunOptions::default())
            .expect("first daemon lock");
        assert!(matches!(
            super::probe_daemon_status(&paths, Duration::from_millis(1)),
            DaemonProbeStatus::Locked { .. }
        ));
        assert!(super::DaemonLock::acquire(&paths, &DaemonRunOptions::default()).is_err());

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
