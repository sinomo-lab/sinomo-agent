use serde::{Deserialize, Serialize};
use sinomo_agent::{
    Agent, AgentConfig, AgentContextBudget, AgentLoopInput, AgentPolicyBundle, AgentRole,
    AgentStoragePaths, AgentToolRegistry, BoundedContext, InteractionModel, InteractionSession,
    InteractionSurface, InteractionSurfaceKind, LocalWorkspaceAgentConfig,
    default_coding_agent_tools, user_interaction_input_event,
};
use sinomo_lm::{
    LanguageModelError, LanguageModelRequest, LanguageModelResponse, ModelPaths,
    ProviderAccountConfig, complete_configured_model_request_with_account, load_model_config,
    normalize_model_provider, provider_account_config,
};
use sinomo_runtime::core::ContextPacket;
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::NodeId;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Clone, Debug)]
struct Options {
    workspace: PathBuf,
    task_json: PathBuf,
    prediction_out: PathBuf,
    report_out: PathBuf,
    provider: String,
    model: String,
    account: Option<String>,
    max_steps: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct SweBenchTask {
    repo: String,
    instance_id: String,
    base_commit: String,
    problem_statement: String,
    #[serde(default)]
    hints_text: String,
    #[serde(default)]
    difficulty: String,
}

#[derive(Clone)]
struct ConfiguredBenchmarkModel {
    data_dir: SinomoDataDir,
    provider: String,
    model: String,
    account: Option<ProviderAccountConfig>,
    working_dir: PathBuf,
}

impl InteractionModel for ConfiguredBenchmarkModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        complete_configured_model_request_with_account(
            &self.data_dir,
            &self.provider,
            &self.model,
            self.account.as_ref(),
            Some(self.working_dir.clone()),
            request,
        )
    }
}

#[derive(Debug, Serialize)]
struct SweBenchPrediction {
    instance_id: String,
    model_name_or_path: String,
    model_patch: String,
}

#[derive(Debug, Serialize)]
struct SweBenchAgentReport {
    task: SweBenchTaskReport,
    provider: String,
    model: String,
    workspace: String,
    prediction_path: String,
    run_trace_path: String,
    patch_bytes: usize,
    stop_reason: String,
    model_turns: usize,
    tool_results: usize,
    final_text: String,
}

#[derive(Debug, Serialize)]
struct SweBenchTaskReport {
    repo: String,
    instance_id: String,
    base_commit: String,
    difficulty: String,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h"))
    {
        println!("{}", usage());
        return Ok(());
    }
    let options = parse_options(args)?;
    let task = read_task(&options.task_json)?;
    let provider = normalize_model_provider(&options.provider)
        .ok_or_else(|| format!("unknown provider `{}`", options.provider))?;
    let data_dir = SinomoDataDir::from_env_or_home().map_err(|source| source.to_string())?;
    let account = configured_account(&data_dir, &provider, options.account.as_deref())?;
    let session = InteractionSession::new(
        SessionId::generate(),
        NodeId::from_legacy_named("node_swebench_agent").expect("valid benchmark node id"),
        InteractionSurface::new("benchmark.swebench", InteractionSurfaceKind::CommandLine),
    );
    let storage_paths = AgentStoragePaths::new(
        options
            .report_out
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("agent-artifacts")
            .join(&task.instance_id),
    );
    let mut workspace_config = LocalWorkspaceAgentConfig::new(&options.workspace)
        .allowing_shell_commands(true)
        .allowing_file_writes(true)
        .with_agent_storage(&storage_paths, &session.session_id);
    workspace_config.max_read_bytes = 256 * 1024;
    workspace_config.max_command_output_bytes = 128 * 1024;
    workspace_config.max_search_matches = 300;
    let model = ConfiguredBenchmarkModel {
        data_dir,
        provider: provider.clone(),
        model: options.model.clone(),
        account,
        working_dir: options.workspace.clone(),
    };
    let config = AgentConfig::new(provider.clone(), options.model.clone())
        .with_workspace_root(&options.workspace)
        .with_context_budget(AgentContextBudget::for_loaded_model(
            &provider,
            &options.model,
            None,
        ))
        .with_max_steps(options.max_steps)
        .with_max_model_repair_attempts(2);
    let agent = Agent::new(model, provider.clone(), options.model.clone())
        .with_config(config)
        .with_policy(AgentPolicyBundle::default_for_role(AgentRole::Deliberation))
        .with_tool_registry(AgentToolRegistry::local_workspace_with_config(
            workspace_config,
        ));
    let latest_event = user_interaction_input_event(&session, &task_prompt(&task));
    let input = AgentLoopInput {
        context: BoundedContext {
            session_id: session.session_id.clone(),
            events: Vec::new(),
            packet: ContextPacket::default(),
        },
        session,
        latest_event,
        async_update: None,
        evolution_context: None,
        tool_specs: default_coding_agent_tools(),
    };
    let run = agent.run(&input).map_err(|source| source.to_string())?;
    add_untracked_to_diff(&options.workspace)?;
    let patch = git_output(&options.workspace, &["diff", "--binary", "--"])?;
    let prediction = SweBenchPrediction {
        instance_id: task.instance_id.clone(),
        model_name_or_path: format!("{provider}/{}", options.model),
        model_patch: patch.clone(),
    };
    write_json(&options.prediction_out, &prediction)?;
    let run_trace_path = options.report_out.with_extension("run.json");
    write_json(&run_trace_path, &run)?;
    let report = SweBenchAgentReport {
        task: SweBenchTaskReport {
            repo: task.repo,
            instance_id: task.instance_id,
            base_commit: task.base_commit,
            difficulty: task.difficulty,
        },
        provider,
        model: options.model,
        workspace: options.workspace.display().to_string(),
        prediction_path: options.prediction_out.display().to_string(),
        run_trace_path: run_trace_path.display().to_string(),
        patch_bytes: patch.len(),
        stop_reason: format!("{:?}", run.stop_reason),
        model_turns: run.model_turns.len(),
        tool_results: run.tool_results.len(),
        final_text: run.final_decision.text,
    };
    write_json(&options.report_out, &report)?;
    Ok(())
}

fn parse_options(args: Vec<String>) -> Result<Options, String> {
    let mut workspace = None;
    let mut task_json = None;
    let mut prediction_out = None;
    let mut report_out = None;
    let mut provider = "deepseek".to_string();
    let mut model = "deepseek-v4-flash".to_string();
    let mut account = None;
    let mut max_steps = 20usize;
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace" => workspace = Some(next_path(&args, &mut index, "--workspace")?),
            "--task-json" => task_json = Some(next_path(&args, &mut index, "--task-json")?),
            "--prediction-out" => {
                prediction_out = Some(next_path(&args, &mut index, "--prediction-out")?)
            }
            "--report-out" => report_out = Some(next_path(&args, &mut index, "--report-out")?),
            "--provider" => provider = next_value(&args, &mut index, "--provider")?,
            "--model" => model = next_value(&args, &mut index, "--model")?,
            "--account" => account = Some(next_value(&args, &mut index, "--account")?),
            "--max-steps" => {
                let value = next_value(&args, &mut index, "--max-steps")?;
                max_steps = value
                    .parse::<usize>()
                    .map_err(|source| format!("invalid --max-steps `{value}`: {source}"))?
                    .max(1);
            }
            other => return Err(format!("unexpected argument `{other}`\n\n{}", usage())),
        }
        index += 1;
    }
    Ok(Options {
        workspace: workspace.ok_or_else(|| format!("missing --workspace\n\n{}", usage()))?,
        task_json: task_json.ok_or_else(|| format!("missing --task-json\n\n{}", usage()))?,
        prediction_out: prediction_out
            .ok_or_else(|| format!("missing --prediction-out\n\n{}", usage()))?,
        report_out: report_out.ok_or_else(|| format!("missing --report-out\n\n{}", usage()))?,
        provider,
        model,
        account,
        max_steps,
    })
}

fn usage() -> String {
    "usage: sinomo-swebench-agent --workspace <path> --task-json <path> --prediction-out <path> --report-out <path> [--provider deepseek] [--model deepseek-v4-flash] [--account name] [--max-steps n]".to_string()
}

fn next_path(args: &[String], index: &mut usize, flag: &str) -> Result<PathBuf, String> {
    Ok(PathBuf::from(next_value(args, index, flag)?))
}

fn next_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn read_task(path: &Path) -> Result<SweBenchTask, String> {
    let content = fs::read_to_string(path)
        .map_err(|source| format!("failed to read {}: {source}", path.display()))?;
    serde_json::from_str(&content)
        .map_err(|source| format!("failed to parse {}: {source}", path.display()))
}

fn configured_account(
    data_dir: &SinomoDataDir,
    provider: &str,
    account: Option<&str>,
) -> Result<Option<ProviderAccountConfig>, String> {
    let Some(account_name) = account else {
        return Ok(None);
    };
    let config = load_model_config(&ModelPaths::new(data_dir)).map_err(|source| {
        format!("failed to load model config for account `{account_name}`: {source}")
    })?;
    Ok(provider_account_config(&config, provider, Some(account_name)).cloned())
}

fn task_prompt(task: &SweBenchTask) -> String {
    let hints = if task.hints_text.trim().is_empty() {
        "No hints were provided.".to_string()
    } else {
        task.hints_text.clone()
    };
    format!(
        r#"Solve this SWE-bench Verified task in the current repository checkout.

Instance: {instance_id}
Repository: {repo}
Base commit: {base_commit}
Difficulty: {difficulty}

Problem statement:
{problem_statement}

Hints:
{hints}

Work only in regular source files. Do not edit tests or generated lock/config files unless the issue explicitly requires it.
Use the workspace tools to inspect files, edit with apply_patch, and run focused checks when practical.
When you are done, return a concise summary and leave the final patch in the working tree."#,
        instance_id = task.instance_id,
        repo = task.repo,
        base_commit = task.base_commit,
        difficulty = task.difficulty,
        problem_statement = task.problem_statement,
        hints = hints,
    )
}

fn add_untracked_to_diff(workspace: &Path) -> Result<(), String> {
    run_git(workspace, &["add", "-N", "."]).map(|_| ())
}

fn git_output(workspace: &Path, args: &[&str]) -> Result<String, String> {
    let output = run_git(workspace, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn run_git(workspace: &Path, args: &[&str]) -> Result<std::process::Output, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(workspace)
        .output()
        .map_err(|source| format!("failed to run git {}: {source}", args.join(" ")))?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(format!(
            "git {} failed with {:?}: {}",
            args.join(" "),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|source| format!("failed to create {}: {source}", parent.display()))?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|source| source.to_string())?;
    fs::write(path, content)
        .map_err(|source| format!("failed to write {}: {source}", path.display()))
}
