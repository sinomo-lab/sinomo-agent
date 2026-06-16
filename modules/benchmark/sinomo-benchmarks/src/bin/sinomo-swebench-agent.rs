use serde::{Deserialize, Serialize};
use sinomo_agent::{
    Agent, AgentConfig, AgentContextBudget, AgentItem, AgentLoopInput, AgentModelEvent, AgentOp,
    AgentRole, AgentRun, AgentRunModelMetrics, AgentSession, AgentSessionRuntime, AgentStopReason,
    AgentStoragePaths, AgentToolPairIntegrity, AgentToolRegistry, AgentToolStatus,
    AgentToolSurface, BoundedContext, FileAgentTraceStore, InteractionModel, InteractionSession,
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
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

#[derive(Clone, Debug)]
struct Options {
    workspace: PathBuf,
    task_json: PathBuf,
    prediction_out: PathBuf,
    report_out: PathBuf,
    agent_trace_out: Option<PathBuf>,
    provider: String,
    model: String,
    account: Option<String>,
    max_steps: Option<usize>,
    repair_steps: usize,
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
    repair_run_trace_path: Option<String>,
    agent_trace_path: String,
    agent_trace_records: usize,
    agent_event_count: usize,
    agent_item_count: usize,
    agent_tool_call_items: usize,
    agent_tool_result_items: usize,
    orphan_tool_calls: usize,
    orphan_tool_results: usize,
    patch_bytes: usize,
    changed_files: Vec<String>,
    initial_post_patch_check: Option<SweBenchPostPatchCheck>,
    model_repair_post_patch_check: Option<SweBenchPostPatchCheck>,
    post_patch_check: Option<SweBenchPostPatchCheck>,
    repair_attempted: bool,
    repair_reason: Option<String>,
    repair_model_turns: usize,
    repair_tool_results: usize,
    deterministic_repair_attempted: bool,
    deterministic_repair_changed_files: Vec<String>,
    stop_reason: String,
    coding_validation_status: Option<String>,
    coding_stop_recommendation: Option<String>,
    coding_blocked_reason: Option<String>,
    coding_validation_attempts: usize,
    coding_validation_failure_kinds: Vec<String>,
    model_turns: usize,
    tool_results: usize,
    command_observation_turns: usize,
    command_tool_results: usize,
    cancelled_tool_results: usize,
    cancelled_runs: usize,
    first_successful_patch_turn: Option<usize>,
    extra_turns_after_patch: usize,
    agent_input_tokens: u64,
    agent_output_tokens: u64,
    agent_model_metrics: AgentRunModelMetrics,
    final_text: String,
}

#[derive(Debug, Serialize)]
struct SweBenchTaskReport {
    repo: String,
    instance_id: String,
    base_commit: String,
    difficulty: String,
}

#[derive(Clone, Debug, Serialize)]
struct SweBenchPostPatchCheck {
    command: String,
    passed: bool,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
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
    let agent_trace_path = options.agent_trace_out.clone().unwrap_or_else(|| {
        storage_paths
            .traces_dir
            .join(format!("{}.jsonl", task.instance_id))
    });
    if agent_trace_path.exists() {
        fs::remove_file(&agent_trace_path).map_err(|source| {
            format!(
                "failed to clear existing agent trace {}: {source}",
                agent_trace_path.display()
            )
        })?;
    }
    let agent = benchmark_agent(
        model.clone(),
        &provider,
        &options.model,
        &options.workspace,
        workspace_config.clone(),
        options.max_steps,
        &agent_trace_path,
    );
    let input = agent_loop_input(&session, &task_prompt(&task, options.max_steps));
    let runtime = AgentSessionRuntime::new(
        agent,
        AgentSession::new(session.session_id.clone(), AgentRole::Deliberation),
    );
    let run = submit_runtime_turn(&runtime, AgentOp::start_turn(input.clone()))?;
    let mut runs = vec![run];
    add_untracked_to_diff(&options.workspace)?;
    let mut patch = current_patch(&options.workspace)?;
    let mut changed_files = current_changed_files(&options.workspace)?;
    let mut post_patch_check = run_post_patch_check(&options.workspace, &changed_files)?;
    let initial_post_patch_check = post_patch_check.clone();
    let mut repair_attempted = false;
    let mut repair_reason = None;
    let mut repair_run_trace_path = None;
    let mut repair_model_turns = 0usize;
    let mut repair_tool_results = 0usize;
    let mut model_repair_post_patch_check = None;
    let repair_prompt = if post_patch_failed(&post_patch_check) && options.repair_steps > 0 {
        repair_reason = Some("post-patch-check-failure".to_string());
        Some(post_patch_repair_prompt(
            post_patch_check.as_ref().expect("checked above"),
            &changed_files,
            &patch,
            options.repair_steps,
        ))
    } else if patch.trim().is_empty() && options.repair_steps > 0 {
        repair_reason = Some("empty-patch".to_string());
        Some(empty_patch_repair_prompt(&task, options.repair_steps))
    } else {
        None
    };
    if let Some(repair_prompt) = repair_prompt {
        repair_attempted = true;
        let repair_agent = benchmark_agent(
            model,
            &provider,
            &options.model,
            &options.workspace,
            workspace_config,
            Some(options.repair_steps),
            &agent_trace_path,
        );
        let repair_runtime = AgentSessionRuntime::new(
            repair_agent,
            runtime.session_snapshot().map_err(|source| {
                format!("failed to snapshot agent session for repair runtime: {source}")
            })?,
        );
        repair_runtime
            .submit_op(AgentOp::follow_up(repair_prompt))
            .map_err(|source| source.to_string())?;
        let repair_input = agent_loop_input(
            &input.session,
            "Repair the current patch after the post-patch check failure.",
        );
        let repair_run =
            submit_runtime_turn(&repair_runtime, AgentOp::continue_turn(repair_input))?;
        repair_model_turns = repair_run.model_turns.len();
        repair_tool_results = repair_run.tool_results.len();
        let path = options.report_out.with_extension("repair.run.json");
        write_json(&path, &repair_run)?;
        repair_run_trace_path = Some(path);
        runs.push(repair_run);
        add_untracked_to_diff(&options.workspace)?;
        patch = current_patch(&options.workspace)?;
        changed_files = current_changed_files(&options.workspace)?;
        post_patch_check = run_post_patch_check(&options.workspace, &changed_files)?;
        model_repair_post_patch_check = post_patch_check.clone();
    }
    let mut deterministic_repair_attempted = false;
    let mut deterministic_repair_changed_files = Vec::new();
    if python_indentation_check_failed(&post_patch_check) {
        deterministic_repair_changed_files =
            repair_python_indentation_from_git(&options.workspace, &changed_files)?;
        deterministic_repair_attempted = !deterministic_repair_changed_files.is_empty();
        if deterministic_repair_attempted {
            add_untracked_to_diff(&options.workspace)?;
            patch = current_patch(&options.workspace)?;
            changed_files = current_changed_files(&options.workspace)?;
            post_patch_check = run_post_patch_check(&options.workspace, &changed_files)?;
        }
    }
    let (agent_input_tokens, agent_output_tokens) = agent_usage_totals(&runs);
    let agent_model_metrics = combined_agent_model_metrics(&runs);
    let command_observation_turns = command_observation_turns(&runs);
    let command_tool_results = command_tool_results(&runs);
    let cancelled_tool_results = cancelled_tool_results(&runs);
    let cancelled_runs = cancelled_runs(&runs);
    let tool_pair_integrity = AgentToolPairIntegrity::from_runs(&runs);
    let first_successful_patch_turn = first_successful_patch_turn(&runs);
    let extra_turns_after_patch = extra_turns_after_patch(&runs);
    let prediction = SweBenchPrediction {
        instance_id: task.instance_id.clone(),
        model_name_or_path: format!("{provider}/{}", options.model),
        model_patch: patch.clone(),
    };
    write_json(&options.prediction_out, &prediction)?;
    let run_trace_path = options.report_out.with_extension("run.json");
    write_json(&run_trace_path, &runs[0])?;
    let agent_trace_records = count_lines(&agent_trace_path)?;
    let last_run = runs.last().expect("at least one agent run");
    let coding_state = last_run.coding_state.as_ref();
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
        repair_run_trace_path: repair_run_trace_path.map(|path| path.display().to_string()),
        agent_trace_path: agent_trace_path.display().to_string(),
        agent_trace_records,
        agent_event_count: runs.iter().map(|run| run.events.len()).sum(),
        agent_item_count: runs.iter().map(|run| run.items.len()).sum(),
        agent_tool_call_items: tool_pair_integrity.tool_calls,
        agent_tool_result_items: tool_pair_integrity.tool_results,
        orphan_tool_calls: tool_pair_integrity.orphan_tool_calls,
        orphan_tool_results: tool_pair_integrity.orphan_tool_results,
        patch_bytes: patch.len(),
        changed_files,
        initial_post_patch_check,
        model_repair_post_patch_check,
        post_patch_check,
        repair_attempted,
        repair_reason,
        repair_model_turns,
        repair_tool_results,
        deterministic_repair_attempted,
        deterministic_repair_changed_files,
        stop_reason: format!("{:?}", last_run.stop_reason),
        coding_validation_status: coding_state
            .map(|state| serde_string_label(&state.validation_status)),
        coding_stop_recommendation: coding_state
            .map(|state| serde_string_label(&state.stop_recommendation)),
        coding_blocked_reason: coding_state
            .and_then(|state| state.blocked_reason.as_ref())
            .map(serde_string_label),
        coding_validation_attempts: coding_validation_attempt_count(&runs),
        coding_validation_failure_kinds: coding_validation_failure_kinds(&runs),
        model_turns: runs.iter().map(|run| run.model_turns.len()).sum(),
        tool_results: runs.iter().map(|run| run.tool_results.len()).sum(),
        command_observation_turns,
        command_tool_results,
        cancelled_tool_results,
        cancelled_runs,
        first_successful_patch_turn,
        extra_turns_after_patch,
        agent_input_tokens,
        agent_output_tokens,
        agent_model_metrics,
        final_text: last_run.final_decision.text.clone(),
    };
    write_json(&options.report_out, &report)?;
    Ok(())
}

fn submit_runtime_turn(
    runtime: &AgentSessionRuntime<ConfiguredBenchmarkModel>,
    op: AgentOp,
) -> Result<AgentRun, String> {
    let op_id = op.op_id.clone();
    let result = runtime.submit_op(op).map_err(|source| source.to_string())?;
    if !result.accepted {
        let reason = result
            .events
            .iter()
            .find_map(|event| match &event.payload {
                sinomo_agent::AgentEventPayload::OperationRejected { reason, .. } => {
                    Some(reason.clone())
                }
                _ => None,
            })
            .unwrap_or_else(|| "turn was rejected".to_string());
        return Err(reason);
    }
    runtime
        .join_active_turn()
        .map_err(|source| source.to_string())?;
    runtime
        .turn_result(&op_id)
        .map_err(|source| source.to_string())?
        .and_then(|outcome| outcome.run)
        .ok_or_else(|| format!("runtime turn `{op_id}` completed without an agent run"))
}

fn benchmark_agent(
    model: ConfiguredBenchmarkModel,
    provider: &str,
    model_id: &str,
    workspace: &Path,
    workspace_config: LocalWorkspaceAgentConfig,
    max_steps: Option<usize>,
    agent_trace_path: &Path,
) -> Agent<ConfiguredBenchmarkModel> {
    let mut config = AgentConfig::new(provider.to_string(), model_id.to_string())
        .with_workspace_root(workspace)
        .with_prompt_reference_expansion(false)
        .with_context_budget(AgentContextBudget::for_loaded_model(
            provider, model_id, None,
        ))
        .with_max_model_repair_attempts(2);
    if let Some(max_steps) = max_steps {
        config = config.with_max_steps(max_steps);
    }
    Agent::new(model, provider.to_string(), model_id.to_string())
        .with_config(config)
        .with_tool_surface(AgentToolSurface::default_for_role(AgentRole::Deliberation))
        .with_tool_registry(AgentToolRegistry::local_workspace_with_config(
            workspace_config,
        ))
        .with_trace_store(FileAgentTraceStore::new(agent_trace_path))
}

fn agent_loop_input(session: &InteractionSession, message: &str) -> AgentLoopInput {
    let latest_event = user_interaction_input_event(session, message);
    AgentLoopInput {
        context: BoundedContext {
            session_id: session.session_id.clone(),
            events: Vec::new(),
            packet: ContextPacket::default(),
        },
        session: session.clone(),
        latest_event,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: default_coding_agent_tools(),
    }
}

fn agent_usage_totals(runs: &[AgentRun]) -> (u64, u64) {
    runs.iter()
        .flat_map(|run| run.model_events.iter())
        .filter_map(|event| {
            if let AgentModelEvent::Usage {
                usage: Some(usage), ..
            } = event
            {
                Some((
                    usage.input_tokens.unwrap_or_default(),
                    usage.output_tokens.unwrap_or_default(),
                ))
            } else {
                None
            }
        })
        .fold((0, 0), |(input_total, output_total), (input, output)| {
            (input_total + input, output_total + output)
        })
}

fn combined_agent_model_metrics(runs: &[AgentRun]) -> AgentRunModelMetrics {
    let mut metrics = AgentRunModelMetrics::default();
    if let Some(last_run) = runs.last() {
        metrics.output_format = last_run.model_metrics.output_format.clone();
        metrics.final_decision_source = last_run.model_metrics.final_decision_source.clone();
    }
    for run in runs {
        let run_metrics = &run.model_metrics;
        metrics.model_turn_count = metrics
            .model_turn_count
            .saturating_add(run_metrics.model_turn_count);
        metrics.model_response_count = metrics
            .model_response_count
            .saturating_add(run_metrics.model_response_count);
        metrics.model_response_parse_failure_count = metrics
            .model_response_parse_failure_count
            .saturating_add(run_metrics.model_response_parse_failure_count);
        metrics.strict_json_decision_turn_count = metrics
            .strict_json_decision_turn_count
            .saturating_add(run_metrics.strict_json_decision_turn_count);
        metrics.text_fallback_turn_count = metrics
            .text_fallback_turn_count
            .saturating_add(run_metrics.text_fallback_turn_count);
        metrics.native_tool_call_turn_count = metrics
            .native_tool_call_turn_count
            .saturating_add(run_metrics.native_tool_call_turn_count);
        metrics.native_tool_call_count = metrics
            .native_tool_call_count
            .saturating_add(run_metrics.native_tool_call_count);
        metrics.vendor_fallback_tool_call_turn_count = metrics
            .vendor_fallback_tool_call_turn_count
            .saturating_add(run_metrics.vendor_fallback_tool_call_turn_count);
        metrics.vendor_fallback_tool_call_count = metrics
            .vendor_fallback_tool_call_count
            .saturating_add(run_metrics.vendor_fallback_tool_call_count);
        metrics.repair_attempt_count = metrics
            .repair_attempt_count
            .saturating_add(run_metrics.repair_attempt_count);
        metrics.repair_request_model_visible_bytes = metrics
            .repair_request_model_visible_bytes
            .saturating_add(run_metrics.repair_request_model_visible_bytes);
        metrics.reported_usage.response_count = metrics
            .reported_usage
            .response_count
            .saturating_add(run_metrics.reported_usage.response_count);
        metrics.reported_usage.input_tokens = metrics
            .reported_usage
            .input_tokens
            .saturating_add(run_metrics.reported_usage.input_tokens);
        metrics.reported_usage.output_tokens = metrics
            .reported_usage
            .output_tokens
            .saturating_add(run_metrics.reported_usage.output_tokens);
        metrics.repair_reported_usage.response_count = metrics
            .repair_reported_usage
            .response_count
            .saturating_add(run_metrics.repair_reported_usage.response_count);
        metrics.repair_reported_usage.input_tokens = metrics
            .repair_reported_usage
            .input_tokens
            .saturating_add(run_metrics.repair_reported_usage.input_tokens);
        metrics.repair_reported_usage.output_tokens = metrics
            .repair_reported_usage
            .output_tokens
            .saturating_add(run_metrics.repair_reported_usage.output_tokens);
    }
    metrics
}

fn command_observation_turns(runs: &[AgentRun]) -> usize {
    let mut turns = BTreeSet::new();
    for (run_index, run) in runs.iter().enumerate() {
        for outcome in &run.tool_results {
            if outcome.call.tool == "run_command"
                && (nonempty_output_field(&outcome.output, "stdout")
                    || nonempty_output_field(&outcome.output, "stderr")
                    || nonempty_output_field(&outcome.output, "aggregated_output"))
            {
                turns.insert((run_index, outcome.step_index));
            }
        }
    }
    turns.len()
}

fn command_tool_results(runs: &[AgentRun]) -> usize {
    runs.iter()
        .flat_map(|run| run.tool_results.iter())
        .filter(|outcome| outcome.call.tool == "run_command")
        .count()
}

fn cancelled_tool_results(runs: &[AgentRun]) -> usize {
    runs.iter()
        .flat_map(|run| run.tool_results.iter())
        .filter(|outcome| outcome.status == AgentToolStatus::Cancelled)
        .count()
}

fn cancelled_runs(runs: &[AgentRun]) -> usize {
    runs.iter()
        .filter(|run| run.stop_reason == AgentStopReason::Cancelled)
        .count()
}

fn coding_validation_attempt_count(runs: &[AgentRun]) -> usize {
    runs.iter()
        .filter_map(|run| run.coding_state.as_ref())
        .map(|state| state.validation_attempts.len())
        .sum()
}

fn coding_validation_failure_kinds(runs: &[AgentRun]) -> Vec<String> {
    runs.iter()
        .filter_map(|run| run.coding_state.as_ref())
        .flat_map(|state| state.validation_attempts.iter())
        .filter_map(|attempt| attempt.failure_kind.as_ref())
        .map(serde_string_label)
        .collect()
}

fn serde_string_label<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(value)) => value,
        Ok(value) => value.to_string(),
        Err(_) => "unserializable".to_string(),
    }
}

fn first_successful_patch_turn(runs: &[AgentRun]) -> Option<usize> {
    let mut turn_offset = 0usize;
    let mut first_patch_turn = None;
    for run in runs {
        if let Some(step_index) = run
            .tool_results
            .iter()
            .filter(|outcome| {
                outcome.call.tool == "apply_patch" && outcome.status == AgentToolStatus::Succeeded
            })
            .map(|outcome| outcome.step_index)
            .min()
        {
            first_patch_turn = Some(turn_offset.saturating_add(step_index));
            break;
        }
        turn_offset = turn_offset.saturating_add(run.model_turns.len());
    }
    first_patch_turn
}

fn extra_turns_after_patch(runs: &[AgentRun]) -> usize {
    runs.iter()
        .map(|run| {
            let Some(first_patch_step) = run
                .tool_results
                .iter()
                .filter(|outcome| {
                    outcome.call.tool == "apply_patch"
                        && outcome.status == AgentToolStatus::Succeeded
                })
                .map(|outcome| outcome.step_index)
                .min()
            else {
                return 0;
            };
            run.model_turns
                .iter()
                .filter(|turn| turn.step_index > first_patch_step)
                .count()
        })
        .sum()
}

fn nonempty_output_field(output: &serde_json::Value, key: &str) -> bool {
    output
        .get(key)
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}

fn parse_options(args: Vec<String>) -> Result<Options, String> {
    let mut workspace = None;
    let mut task_json = None;
    let mut prediction_out = None;
    let mut report_out = None;
    let mut agent_trace_out = None;
    let mut provider = "deepseek".to_string();
    let mut model = "deepseek-v4-flash".to_string();
    let mut account = None;
    let mut max_steps = None;
    let mut repair_steps = 6usize;
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace" => workspace = Some(next_path(&args, &mut index, "--workspace")?),
            "--task-json" => task_json = Some(next_path(&args, &mut index, "--task-json")?),
            "--prediction-out" => {
                prediction_out = Some(next_path(&args, &mut index, "--prediction-out")?)
            }
            "--report-out" => report_out = Some(next_path(&args, &mut index, "--report-out")?),
            "--agent-trace-out" => {
                agent_trace_out = Some(next_path(&args, &mut index, "--agent-trace-out")?)
            }
            "--provider" => provider = next_value(&args, &mut index, "--provider")?,
            "--model" => model = next_value(&args, &mut index, "--model")?,
            "--account" => account = Some(next_value(&args, &mut index, "--account")?),
            "--max-steps" => {
                let value = next_value(&args, &mut index, "--max-steps")?;
                max_steps = Some(
                    value
                        .parse::<usize>()
                        .map_err(|source| format!("invalid --max-steps `{value}`: {source}"))?
                        .max(1),
                );
            }
            "--repair-steps" => {
                let value = next_value(&args, &mut index, "--repair-steps")?;
                repair_steps = value
                    .parse::<usize>()
                    .map_err(|source| format!("invalid --repair-steps `{value}`: {source}"))?;
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
        agent_trace_out,
        provider,
        model,
        account,
        max_steps,
        repair_steps,
    })
}

fn usage() -> String {
    "usage: sinomo-swebench-agent --workspace <path> --task-json <path> --prediction-out <path> --report-out <path> [--agent-trace-out <path>] [--provider deepseek] [--model deepseek-v4-flash] [--account name] [--max-steps n] [--repair-steps n]\n\n--max-steps is optional; omit it for the continuous-agent default with no model-turn cap.".to_string()
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

fn task_prompt(task: &SweBenchTask, max_steps: Option<usize>) -> String {
    let hints = if task.hints_text.trim().is_empty() {
        "No hints were provided.".to_string()
    } else {
        task.hints_text.clone()
    };
    let turn_budget_guidance = max_steps.map_or_else(
        || {
            "No fixed model-turn cap is imposed by this benchmark adapter. Keep the run focused: inspect enough to make the minimal source patch, avoid redundant validation turns, and end with no tool calls once a patch is present or once no useful action remains.".to_string()
        },
        |max_steps| {
            format!(
                "You have at most {max_steps} model turns. Spend the first turns locating the bug, but reserve enough budget to apply a minimal source patch. If the remaining budget is small, prefer making the highest-confidence patch over continuing to inspect. End with no tool calls once a patch is present or once no useful action remains."
            )
        },
    );
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
Use the workspace tools to inspect files, edit with apply_patch, and run focused checks when practical. Workspace commands already start in the repository root; do not cd to /workspace, /home/user, benchmark artifact paths, or guessed checkout paths.
Failed command stderr/stdout are returned in the next tool_result; use that output as the command diagnostic instead of repeating the same command blindly.
The benchmark checkout usually does not have the target project's Python dependencies installed. Prefer source inspection plus the harness post-patch `python -m py_compile` check. Run import-heavy tests only when dependencies are already present; if stderr mentions missing modules, incompatible local Python tooling, or package installation requirements, stop using local validation and finish once the diff is minimal.
For apply_patch, submit one minimal patch as raw tool input or as a patch string field. Patch format: `*** Begin Patch`, one or more `*** Add File: path`, `*** Update File: path`, or `*** Delete File: path` hunks, then `*** End Patch`. Update hunks use `@@` or `@@ <exact context>` and lines prefixed with space, `-`, or `+`; do not use git unified range headers or dangling empty hunks.
Example update hunk:
*** Begin Patch
*** Update File: path/to/file.py
@@ nearby context
 unchanged
-old_value = old_call()
+old_value = new_call()
 unchanged
*** End Patch
If a check fails because stderr/stdout show missing local dependencies, package installation policy, or incompatible local tooling, do not spend repeated turns installing dependencies. Use a narrower available check, inspect git diff, and finish if the source patch is minimal and directly addresses the issue.
{turn_budget_guidance}
When you are done, return a concise summary and leave the final patch in the working tree."#,
        instance_id = &task.instance_id,
        repo = &task.repo,
        base_commit = &task.base_commit,
        difficulty = &task.difficulty,
        problem_statement = &task.problem_statement,
        hints = hints,
        turn_budget_guidance = turn_budget_guidance,
    )
}

fn count_lines(path: &Path) -> Result<usize, String> {
    let content = fs::read_to_string(path)
        .map_err(|source| format!("failed to read {}: {source}", path.display()))?;
    Ok(content.lines().count())
}

fn run_post_patch_check(
    workspace: &Path,
    changed_files: &[String],
) -> Result<Option<SweBenchPostPatchCheck>, String> {
    let python_files = changed_files
        .iter()
        .filter(|path| path.ends_with(".py"))
        .cloned()
        .collect::<Vec<_>>();
    if python_files.is_empty() {
        return Ok(None);
    }
    let (python, output) = run_python_compile(workspace, &python_files)?;
    let command_text = format!("{python} -m py_compile {}", python_files.join(" "));
    Ok(Some(SweBenchPostPatchCheck {
        command: command_text,
        passed: output.status.success(),
        exit_code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }))
}

fn run_python_compile(
    workspace: &Path,
    python_files: &[String],
) -> Result<(String, Output), String> {
    let mut missing = Vec::new();
    for python in ["python", "python3"] {
        let mut command = Command::new(python);
        command
            .arg("-m")
            .arg("py_compile")
            .args(python_files)
            .current_dir(workspace);
        match command.output() {
            Ok(output) => return Ok((python.to_string(), output)),
            Err(source) if source.kind() == ErrorKind::NotFound => {
                missing.push(python);
            }
            Err(source) => {
                return Err(format!(
                    "failed to run post-patch syntax check with {python} in {}: {source}",
                    workspace.display()
                ));
            }
        }
    }
    Err(format!(
        "failed to run post-patch syntax check in {}: none of {} are available",
        workspace.display(),
        missing.join(", ")
    ))
}

fn current_patch(workspace: &Path) -> Result<String, String> {
    git_output(workspace, &["diff", "--binary", "--"])
}

fn current_changed_files(workspace: &Path) -> Result<Vec<String>, String> {
    Ok(git_output(workspace, &["diff", "--name-only", "--"])?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect())
}

fn post_patch_failed(check: &Option<SweBenchPostPatchCheck>) -> bool {
    matches!(check, Some(check) if !check.passed)
}

fn python_indentation_check_failed(check: &Option<SweBenchPostPatchCheck>) -> bool {
    matches!(
        check,
        Some(check)
            if !check.passed
                && (check.stderr.contains("IndentationError")
                    || check.stdout.contains("IndentationError"))
    )
}

fn repair_python_indentation_from_git(
    workspace: &Path,
    changed_files: &[String],
) -> Result<Vec<String>, String> {
    let mut repaired = Vec::new();
    for file in changed_files
        .iter()
        .filter(|path| path.ends_with(".py"))
        .filter(|path| !path.contains('\n'))
    {
        let changed_lines = changed_new_line_numbers(workspace, file)?;
        if changed_lines.is_empty() {
            continue;
        }
        let base = git_output_owned(workspace, &["show".to_string(), format!("HEAD:{file}")])?;
        let path = workspace.join(file);
        let current = fs::read_to_string(&path)
            .map_err(|source| format!("failed to read {}: {source}", path.display()))?;
        let base_lines = split_lines_preserving_content(&base);
        let mut current_lines = split_lines_preserving_content(&current);
        let mut changed = false;
        for line_number in changed_lines {
            let Some(base_line) = base_lines.get(line_number.saturating_sub(1)) else {
                continue;
            };
            let Some(current_line) = current_lines.get_mut(line_number.saturating_sub(1)) else {
                continue;
            };
            let base_trimmed = base_line.trim_start();
            let current_trimmed = current_line.trim_start();
            if current_trimmed.is_empty()
                || !should_inherit_base_indentation(base_trimmed, current_trimmed)
            {
                continue;
            }
            let base_indent = leading_whitespace(base_line);
            let repaired_line = format!("{base_indent}{current_trimmed}");
            if repaired_line != *current_line {
                *current_line = repaired_line;
                changed = true;
            }
        }
        if changed {
            let mut content = current_lines.join("\n");
            if current.ends_with('\n') {
                content.push('\n');
            }
            fs::write(&path, content)
                .map_err(|source| format!("failed to write {}: {source}", path.display()))?;
            repaired.push(file.clone());
        }
    }
    Ok(repaired)
}

fn changed_new_line_numbers(workspace: &Path, file: &str) -> Result<BTreeSet<usize>, String> {
    let diff = git_output_owned(
        workspace,
        &[
            "diff".to_string(),
            "--unified=0".to_string(),
            "--".to_string(),
            file.to_string(),
        ],
    )?;
    let mut changed = BTreeSet::new();
    let mut new_line = 0usize;
    for line in diff.lines() {
        if line.starts_with("@@") {
            if let Some(start) = parse_new_hunk_start(line) {
                new_line = start;
            }
            continue;
        }
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if line.starts_with('+') {
            changed.insert(new_line);
            new_line += 1;
        } else if line.starts_with(' ') {
            new_line += 1;
        }
    }
    Ok(changed)
}

fn parse_new_hunk_start(line: &str) -> Option<usize> {
    let plus = line.find('+')?;
    let digits = line[plus + 1..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse().ok()
}

fn split_lines_preserving_content(value: &str) -> Vec<String> {
    value
        .trim_end_matches('\n')
        .split('\n')
        .map(ToString::to_string)
        .collect()
}

fn leading_whitespace(value: &str) -> &str {
    let end = value
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index))
        .unwrap_or(value.len());
    &value[..end]
}

fn should_inherit_base_indentation(base_trimmed: &str, current_trimmed: &str) -> bool {
    if base_trimmed == current_trimmed {
        return true;
    }
    if assignment_lhs(base_trimmed).is_some()
        && assignment_lhs(base_trimmed) == assignment_lhs(current_trimmed)
    {
        return true;
    }
    common_prefix_len(base_trimmed, current_trimmed) >= 8
}

fn assignment_lhs(value: &str) -> Option<&str> {
    value
        .split_once('=')
        .map(|(left, _)| left.trim_end())
        .filter(|left| !left.is_empty())
}

fn common_prefix_len(left: &str, right: &str) -> usize {
    left.chars()
        .zip(right.chars())
        .take_while(|(left, right)| left == right)
        .count()
}

fn post_patch_repair_prompt(
    check: &SweBenchPostPatchCheck,
    changed_files: &[String],
    patch: &str,
    repair_steps: usize,
) -> String {
    let changed = if changed_files.is_empty() {
        "No changed files were detected.".to_string()
    } else {
        changed_files.join("\n")
    };
    format!(
        r#"The patch currently in the working tree failed the post-patch check.

Repair the existing patch with the smallest source change that makes the check pass. Focus on the changed files first, avoid broad refactors, and finish with no tool calls once the patch is repaired or no useful repair remains. If stderr/stdout show missing local dependencies, package installation policy, or incompatible local tooling, do not spend repair turns on environment setup; inspect the diff and finish if the patch is already the best source fix.

You have at most {repair_steps} repair model turns.

Changed files:
{changed}

Current diff:
{patch}

Failed command:
{command}

stdout:
{stdout}

stderr:
{stderr}"#,
        repair_steps = repair_steps,
        changed = changed,
        patch = truncate_for_prompt(patch, 12000),
        command = &check.command,
        stdout = truncate_for_prompt(&check.stdout, 6000),
        stderr = truncate_for_prompt(&check.stderr, 6000),
    )
}

fn empty_patch_repair_prompt(task: &SweBenchTask, repair_steps: usize) -> String {
    let hints = if task.hints_text.trim().is_empty() {
        "No hints were provided.".to_string()
    } else {
        task.hints_text.clone()
    };
    format!(
        r#"The initial pass used its step budget without leaving a working-tree patch.

Apply the smallest high-confidence source patch for this SWE-bench task now. Prefer editing the most relevant source file over more investigation. Workspace commands start in the repository root. apply_patch expects the patch format shown in the main task prompt, not a git unified diff. Run only focused checks if they are cheap. If checks are blocked by environment setup, inspect the diff and finish once a minimal patch is present.

You have at most {repair_steps} repair model turns.

Instance: {instance_id}
Repository: {repo}
Base commit: {base_commit}
Difficulty: {difficulty}

Problem statement:
{problem_statement}

Hints:
{hints}"#,
        repair_steps = repair_steps,
        instance_id = task.instance_id,
        repo = task.repo,
        base_commit = task.base_commit,
        difficulty = task.difficulty,
        problem_statement = task.problem_statement,
        hints = hints,
    )
}

fn truncate_for_prompt(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}\n[truncated]")
    } else {
        truncated
    }
}

fn add_untracked_to_diff(workspace: &Path) -> Result<(), String> {
    run_git(workspace, &["add", "-N", "."]).map(|_| ())
}

fn git_output(workspace: &Path, args: &[&str]) -> Result<String, String> {
    let output = run_git(workspace, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn git_output_owned(workspace: &Path, args: &[String]) -> Result<String, String> {
    let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    git_output(workspace, &arg_refs)
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_agent::{AgentToolCall, AgentToolEvidence, TaskId};

    fn tool_call_item(item_id: TaskId, step_index: usize, call_index: usize) -> AgentItem {
        AgentItem::ToolCall {
            item_id,
            step_index,
            call_index,
            call: AgentToolCall {
                call_id: None,
                tool: "read_file".to_string(),
                input: serde_json::json!({"path": "src/lib.rs"}),
                r#async: false,
                summary: "read source".to_string(),
            },
        }
    }

    fn tool_result_item(call_id: TaskId, step_index: usize, call_index: usize) -> AgentItem {
        AgentItem::ToolResult {
            item_id: TaskId::generate(),
            step_index,
            call_index,
            call_id,
            native_call_id: None,
            tool_name: Some("read_file".to_string()),
            status: AgentToolStatus::Succeeded,
            summary: "read source".to_string(),
            output: serde_json::json!({"content": "ok"}),
            evidence: AgentToolEvidence::default(),
        }
    }

    #[test]
    fn tool_pair_integrity_pairs_by_step_and_call_index() {
        let paired_call = TaskId::generate();
        let paired_result = TaskId::generate();
        let orphan_call = TaskId::generate();
        let orphan_result = TaskId::generate();
        let items = vec![
            tool_call_item(paired_call, 0, 0),
            tool_result_item(paired_result, 0, 0),
            tool_call_item(orphan_call, 1, 0),
            tool_result_item(orphan_result, 2, 0),
        ];

        let metrics = AgentToolPairIntegrity::from_items(&items);

        assert_eq!(metrics.tool_calls, 2);
        assert_eq!(metrics.tool_results, 2);
        assert_eq!(metrics.orphan_tool_calls, 1);
        assert_eq!(metrics.orphan_tool_results, 1);
    }
}
