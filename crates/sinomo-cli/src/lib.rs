//! Shared Sinomo command-line handlers.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    CandidateBundleManifest, EvaluationGate, EvaluationOutcome, EvolutionJob, EvolutionJobId,
    EvolutionJobStatus, MutationPacket, TrustRing, VerificationCommand,
};
use sinomo_eval::{ExecutingEvalHarness, LocalCommandRunner};
use sinomo_evolution::{
    CandidateWorkspace, EvolutionOrchestrator, EvolutionReport, EvolutionRequest,
    GitWorkspaceAdapter, LanguageModelMutationBridge, LocalCandidateBundleStore, MutationModel,
    MutationModelError, parse_git_status_paths,
};
use sinomo_lm::{
    AnthropicConfig, AnthropicProvider, CLAUDE_CODE_DEFAULT_MODEL, CODEX_CLI_DEFAULT_MODEL,
    ClaudeCodeConfig, ClaudeCodeProvider, CodexCliConfig, CodexCliProvider, GeminiConfig,
    GeminiProvider, GitHubCopilotCliConfig, GitHubCopilotCliProvider, GitHubCopilotConfig,
    GitHubCopilotProvider, LanguageModelError, OpenAiConfig, OpenAiProvider,
    PROVIDER_DEFAULT_MODEL,
};
use sinomo_local::SinomoDataDir;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    if args.first().map(String::as_str) == Some("evolve") {
        return run_evolve_command(&args[1..]);
    }

    if let Some(command) = parse_provider_command(&args) {
        return run_provider_command(command);
    }

    match args.as_slice() {
        [help] if help == "help" || help == "--help" || help == "-h" => {
            print_usage();
            Ok(())
        }
        [] => {
            print_usage();
            Ok(())
        }
        _ => Err(CliError::Usage),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct EvolutionConfig {
    repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_model: Option<String>,
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
}

#[derive(Clone, Debug)]
struct EvolutionPaths {
    root: PathBuf,
    jobs: PathBuf,
    logs: PathBuf,
    config: PathBuf,
}

impl EvolutionPaths {
    fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("evolution");
        Self {
            jobs: root.join("jobs"),
            logs: root.join("logs"),
            config: root.join("config.json"),
            root,
        }
    }

    fn ensure(&self) -> Result<(), CliError> {
        for path in [&self.root, &self.jobs, &self.logs] {
            fs::create_dir_all(path).map_err(|source| CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(())
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

fn run_evolve_command(args: &[String]) -> Result<(), CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "propose" => evolve_propose(rest),
        Some((command, rest)) if command == "keep" => {
            evolve_decide(rest, EvolutionJobStatus::HumanKept)
        }
        Some((command, rest)) if command == "reject" => {
            evolve_decide(rest, EvolutionJobStatus::HumanRejected)
        }
        Some((command, rest)) if command == "status" => evolve_status(rest),
        Some((command, rest)) if command == "show" => evolve_show(rest),
        Some((command, rest)) if command == "verify" => evolve_verify(rest),
        Some((command, rest)) if command == "review" => evolve_review(rest),
        Some((command, rest)) if command == "packet" => evolve_show_packet(rest),
        Some((command, rest)) if command == "cleanup" => evolve_cleanup(rest),
        Some((command, rest)) if command == "job" => evolve_job(rest),
        _ => {
            print_usage();
            Err(CliError::Usage)
        }
    }
}

fn evolve_propose(args: &[String]) -> Result<(), CliError> {
    let options = parse_propose_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let config = read_config(&paths).ok();
    let repo = options
        .repo
        .or_else(|| config.as_ref().map(|config| PathBuf::from(&config.repo)))
        .unwrap_or(env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?);
    let provider = options
        .provider
        .or_else(|| {
            config
                .as_ref()
                .and_then(|config| config.default_provider.clone())
        })
        .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
    let model = options.model.or_else(|| {
        config
            .as_ref()
            .and_then(|config| config.default_model.clone())
    });
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

fn evolve_decide(args: &[String], status: EvolutionJobStatus) -> Result<(), CliError> {
    let (paths, mut record) = resolve_job_from_args(args, true)?;
    record.job.status = status.clone();
    write_stored_job(&paths, &record)?;
    let decision = match status {
        EvolutionJobStatus::HumanKept => "kept",
        EvolutionJobStatus::HumanRejected => "rejected",
        _ => "updated",
    };
    let path = PathBuf::from(&record.bundle.bundle_path).join("human-decision.md");
    fs::write(
        &path,
        format!(
            "# Human Decision\n\njob: {}\ndecision: {}\nworktree: {}\n",
            record.job.job_id,
            decision,
            record.candidate_workspace.path.display()
        ),
    )
    .map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    println!("job: {}", record.job.job_id);
    println!("status: {}", job_status_label(&record.job.status));
    println!("worktree: {}", record.candidate_workspace.path.display());
    println!("bundle: {}", record.bundle.bundle_path);
    Ok(())
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
    } else if has_flag(args, "--review") {
        record.bundle.review_path.clone()
    } else if has_flag(args, "--scorecard") {
        record.bundle.scorecard_path.clone()
    } else if has_flag(args, "--packet") {
        Some(record.bundle.mutation_packet_path.clone())
    } else {
        record.bundle.review_path.clone()
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

fn evolve_review(args: &[String]) -> Result<(), CliError> {
    let (paths, mut record) = resolve_job_from_args(args, false)?;
    let changed_files = current_changed_files(&record.candidate_workspace.path)?;
    let review = format!(
        "# Sinomo Evolution Candidate\n\njob: {}\nstatus: {}\nobjective: {}\nworktree: {}\nverification: {}\nchanged_files:\n{}\n\nrecovery: {}\n",
        record.job.job_id,
        job_status_label(&record.job.status),
        record.job.objective,
        record.candidate_workspace.path.display(),
        record
            .verification_outcome
            .as_ref()
            .map(outcome_label)
            .unwrap_or("unknown"),
        changed_files
            .iter()
            .map(|path| format!("- {path}"))
            .collect::<Vec<_>>()
            .join("\n"),
        record.job.recovery.note
    );
    let review_path = PathBuf::from(&record.bundle.bundle_path).join("human-review.md");
    fs::write(&review_path, review).map_err(|source| CliError::Io {
        path: review_path.display().to_string(),
        detail: source.to_string(),
    })?;
    record.bundle.review_path = Some(review_path.display().to_string());
    record.changed_files = changed_files;
    write_stored_job(&paths, &record)?;
    println!("review: {}", review_path.display());
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
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
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
            "--dry-run" | "--no-model" => {
                dry_run = true;
            }
            "--no-review" => {}
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
            CodexCliProvider::PROVIDER_ID | "openai-codex-cli" => {
                let provider = CodexCliProvider::new(CodexCliConfig {
                    model: model_or_default(&self.model, CODEX_CLI_DEFAULT_MODEL.to_string()),
                    working_dir: Some(workspace.path.clone()),
                    ..CodexCliConfig::default()
                });
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
    };
    write_stored_job(paths, &record)?;
    if let Some(job_path) = &record.bundle.job_path {
        write_json(Path::new(job_path), &record.job)?;
    }
    Ok(())
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
                record.job.status == EvolutionJobStatus::HumanReviewRequested
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

fn current_changed_files(worktree: &Path) -> Result<Vec<String>, CliError> {
    let output = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(worktree)
        .output()
        .map_err(|source| CliError::Io {
            path: worktree.display().to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(CliError::Io {
            path: worktree.display().to_string(),
            detail: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }
    parse_git_status_paths(&String::from_utf8_lossy(&output.stdout))
        .map_err(|detail| CliError::Parse { detail })
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
    println!(
        "review: {}",
        report
            .bundle
            .review_path
            .as_deref()
            .unwrap_or("not-written")
    );
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
        EvolutionJobStatus::HumanKept
            | EvolutionJobStatus::HumanRejected
            | EvolutionJobStatus::Restored
            | EvolutionJobStatus::Failed
    )
}

fn job_status_label(status: &EvolutionJobStatus) -> &'static str {
    match status {
        EvolutionJobStatus::HumanReviewRequested => "waiting-human-decision",
        EvolutionJobStatus::HumanKept => "human-kept",
        EvolutionJobStatus::HumanRejected => "human-rejected",
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
    Anthropic,
    OpenAi,
    Gemini,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderCliCommand {
    Auth(ProviderName),
    Test(ProviderName),
}

fn parse_provider_command(args: &[String]) -> Option<ProviderCliCommand> {
    let [providers, action, provider] = args else {
        return None;
    };
    if providers != "providers" {
        return None;
    }

    let provider = match provider.as_str() {
        "github-copilot" => ProviderName::GitHubCopilot,
        "anthropic" => ProviderName::Anthropic,
        "openai" => ProviderName::OpenAi,
        "gemini" => ProviderName::Gemini,
        _ => return None,
    };

    match action.as_str() {
        "auth" => Some(ProviderCliCommand::Auth(provider)),
        "test" => Some(ProviderCliCommand::Test(provider)),
        _ => None,
    }
}

fn run_provider_command(command: ProviderCliCommand) -> Result<(), CliError> {
    match command {
        ProviderCliCommand::Auth(ProviderName::GitHubCopilot) => auth_github_copilot(),
        ProviderCliCommand::Test(ProviderName::GitHubCopilot) => test_github_copilot(),
        ProviderCliCommand::Auth(ProviderName::Anthropic) => auth_anthropic(),
        ProviderCliCommand::Test(ProviderName::Anthropic) => test_anthropic(),
        ProviderCliCommand::Auth(ProviderName::OpenAi) => auth_openai(),
        ProviderCliCommand::Test(ProviderName::OpenAi) => test_openai(),
        ProviderCliCommand::Auth(ProviderName::Gemini) => auth_gemini(),
        ProviderCliCommand::Test(ProviderName::Gemini) => test_gemini(),
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
    let (api_key, source) = api_key_from_env_or_prompt(
        "OpenAI",
        &[
            "SINOMO_OPENAI_API_KEY",
            "OPENAI_API_KEY",
            "CODEX_API_KEY",
            "SINOMO_CODEX_API_KEY",
        ],
    )?;
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
        default_model: env::var("SINOMO_ANTHROPIC_MODEL")
            .or_else(|_| env::var("SINOMO_CLAUDE_MODEL"))
            .unwrap_or_else(|_| AnthropicConfig::default().default_model),
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
            .or_else(|_| env::var("SINOMO_CODEX_API_BASE_URL"))
            .unwrap_or_else(|_| OpenAiConfig::default().api_base_url),
        default_model: env::var("SINOMO_OPENAI_MODEL")
            .or_else(|_| env::var("SINOMO_CODEX_MODEL"))
            .unwrap_or_else(|_| OpenAiConfig::default().default_model),
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn gemini_provider() -> Result<(GeminiProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(GeminiProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(GeminiProvider::PROVIDER_ID);
    let provider = GeminiProvider::new(GeminiConfig {
        api_base_url: env::var("SINOMO_GEMINI_API_BASE_URL")
            .unwrap_or_else(|_| GeminiConfig::default().api_base_url),
        default_model: env::var("SINOMO_GEMINI_MODEL")
            .unwrap_or_else(|_| GeminiConfig::default().default_model),
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
    println!(
        "  sinomo evolve propose \"<objective>\" [--repo <path>] [--provider github-copilot] [--verify \"cargo check\"]"
    );
    println!("  sinomo evolve keep [--job evojob_...]");
    println!("  sinomo evolve reject [--job evojob_...]");
    println!("  sinomo evolve status [--job evojob_...]");
    println!("  sinomo evolve show [--diff|--review|--scorecard|--packet] [--job evojob_...]");
    println!("  sinomo evolve verify|review|packet|cleanup [--job evojob_...]");
    println!("  sinomo providers auth github-copilot");
    println!("  sinomo providers test github-copilot");
    println!("  sinomo providers auth anthropic");
    println!("  sinomo providers test anthropic");
    println!("  sinomo providers auth openai");
    println!("  sinomo providers test openai");
    println!("  sinomo providers auth gemini");
    println!("  sinomo providers test gemini");
    println!("  sinomo-app providers auth github-copilot");
    println!("  sinomo-app providers test github-copilot");
    println!("  sinomo-app providers auth anthropic");
    println!("  sinomo-app providers test anthropic");
    println!("  sinomo-app providers auth openai");
    println!("  sinomo-app providers test openai");
    println!("  sinomo-app providers auth gemini");
    println!("  sinomo-app providers test gemini");
    println!();
    println!("Environment:");
    println!("  SINOMO_DATA_DIR             Defaults to ~/.sinomo");
    println!("  ANTHROPIC_API_KEY           Used by `providers auth/test anthropic`");
    println!("  OPENAI_API_KEY              Used by `providers auth/test openai`");
    println!("  GEMINI_API_KEY              Used by `providers auth/test gemini`");
    println!(
        "  SINOMO_ANTHROPIC_MODEL      Defaults to {}",
        AnthropicConfig::default().default_model
    );
    println!(
        "  SINOMO_OPENAI_MODEL         Defaults to {}",
        OpenAiConfig::default().default_model
    );
    println!(
        "  SINOMO_GEMINI_MODEL         Defaults to {}",
        GeminiConfig::default().default_model
    );
    println!(
        "  SINOMO_COPILOT_CLIENT_ID    Defaults to {}",
        GitHubCopilotConfig::default().client_id
    );
    println!(
        "                               GitHub App client IDs are not accepted by Copilot token exchange"
    );
}

#[derive(Debug)]
pub enum CliError {
    Usage,
    AuthDenied,
    AuthExpired,
    AuthProvider { detail: String },
    NoActiveJob,
    AmbiguousJob { job_ids: Vec<String> },
    MissingArtifact { artifact: String },
    Parse { detail: String },
    Io { path: String, detail: String },
    Json { path: String, detail: String },
    LocalData(sinomo_local::LocalDataError),
    LanguageModel(LanguageModelError),
    Evolution(sinomo_evolution::EvolutionError),
    Eval(sinomo_eval::EvalError),
}

impl From<sinomo_local::LocalDataError> for CliError {
    fn from(error: sinomo_local::LocalDataError) -> Self {
        Self::LocalData(error)
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

impl From<sinomo_eval::EvalError> for CliError {
    fn from(error: sinomo_eval::EvalError) -> Self {
        Self::Eval(error)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage => write!(formatter, "unknown command; run `sinomo help`"),
            Self::AuthDenied => write!(formatter, "GitHub device authorization was denied"),
            Self::AuthExpired => write!(formatter, "GitHub device authorization expired"),
            Self::AuthProvider { detail } => write!(formatter, "provider auth failed: {detail}"),
            Self::NoActiveJob => write!(formatter, "no active evolution job"),
            Self::AmbiguousJob { job_ids } => {
                write!(
                    formatter,
                    "multiple active evolution jobs; rerun with --job ({})",
                    job_ids.join(", ")
                )
            }
            Self::MissingArtifact { artifact } => {
                write!(formatter, "{artifact} has not been written")
            }
            Self::Parse { detail } => write!(formatter, "failed to parse command: {detail}"),
            Self::Io { path, detail } => write!(formatter, "filesystem error at {path}: {detail}"),
            Self::Json { path, detail } => write!(formatter, "json error at {path}: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::LanguageModel(error) => write!(formatter, "{error}"),
            Self::Evolution(error) => write!(formatter, "{error}"),
            Self::Eval(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for CliError {}

#[cfg(test)]
mod tests {
    use super::{
        CliError, EvolutionJobStatus, EvolutionPaths, ProviderCliCommand, ProviderName,
        is_terminal_status, parse_propose_options, parse_provider_command, run_evolve_command,
    };
    use sinomo_core::{EvolutionJobId, TrustRing};
    use sinomo_local::SinomoDataDir;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn provider_commands_parse_action_before_provider() {
        assert_eq!(
            parse_provider_command(&[
                "providers".to_string(),
                "auth".to_string(),
                "gemini".to_string(),
            ]),
            Some(ProviderCliCommand::Auth(ProviderName::Gemini))
        );
        assert_eq!(
            parse_provider_command(&[
                "providers".to_string(),
                "test".to_string(),
                "github-copilot".to_string(),
            ]),
            Some(ProviderCliCommand::Test(ProviderName::GitHubCopilot))
        );
    }

    #[test]
    fn provider_commands_reject_provider_before_action_order() {
        assert_eq!(
            parse_provider_command(&[
                "providers".to_string(),
                "gemini".to_string(),
                "auth".to_string(),
            ]),
            None
        );
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
    fn active_job_terminal_statuses_are_explicit() {
        assert!(is_terminal_status(&EvolutionJobStatus::HumanKept));
        assert!(is_terminal_status(&EvolutionJobStatus::HumanRejected));
        assert!(!is_terminal_status(
            &EvolutionJobStatus::HumanReviewRequested
        ));
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
