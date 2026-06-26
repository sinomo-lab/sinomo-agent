use super::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cell::Cell;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use sinomo_lm::{
    LM_PROVIDER_CONFIG_KEY, LanguageModelError, ModelConfig, model_config_definition,
    model_config_from_cluster_kv_snapshot, model_config_store, normalize_model_id_for_provider,
    normalize_model_provider, resolve_default_model,
};
use sinomo_runtime::cli_registry::{
    CliAutocompleteOption, CliAutocompleteSpec, CliAutocompleteValue, CliCommand,
    CliCommandContext, CliCommandError, CliCommandHandler, CliCommandHelp, CliCommandMetadata,
    CliCommandPolicy, CliParseError,
};
use sinomo_runtime::core::{EvaluationOutcome, EventEnvelope};
use sinomo_runtime::daemon::default_main_daemon_status as daemon_default_main_daemon_status;
use sinomo_runtime::eval::{ExecutingEvalHarness, LocalCommandRunner};
use sinomo_runtime::events::{EventStore, JsonlEventStore, ReplayDirection, ReplayQuery};
#[cfg(not(test))]
use sinomo_runtime::kv::KvTxnResponse;
use sinomo_runtime::kv::{InProcessKvStore, KvEntry, KvSnapshot, KvTxn};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::NodeId;
use sinomo_runtime::rollout::{
    SelfRolloutPaths, ensure_rollout_layout, ensure_rollout_source_repo,
};

const EVOLVE_POLICY: CliCommandPolicy = CliCommandPolicy::cluster_required();
const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
#[cfg(not(test))]
const DAEMON_CLUSTER_KV_TXN_TIMEOUT: Duration = Duration::from_secs(15);

const EVOLUTION_ITERATION_MEMORY_CONTEXT_LIMIT: usize = 8;

thread_local! {
    static EVOLUTION_CLI_FORCE_LOCAL_MODE: Cell<bool> = const { Cell::new(false) };
}

#[derive(Debug, Error)]
pub enum EvolutionCliError {
    #[error("invalid command usage")]
    Usage,
    #[error("{detail}")]
    Parse { detail: String },
    #[error("I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("JSON error at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error("daemon is not running: {detail}")]
    DaemonNotRunning { detail: String },
    #[error(transparent)]
    LocalData(#[from] sinomo_runtime::local::LocalDataError),
    #[error(transparent)]
    Config(#[from] sinomo_runtime::config::ConfigError),
    #[error(transparent)]
    Events(#[from] sinomo_runtime::events::EventStoreError),
    #[error(transparent)]
    Registry(#[from] sinomo_runtime::registry::RegistryError),
    #[error(transparent)]
    Network(#[from] sinomo_runtime::network::NetworkError),
    #[error(transparent)]
    Daemon(#[from] sinomo_runtime::daemon::DaemonError),
    #[error(transparent)]
    Rollout(#[from] sinomo_runtime::rollout::RolloutError),
    #[error(transparent)]
    LanguageModel(#[from] LanguageModelError),
    #[error(transparent)]
    Evolution(#[from] EvolutionError),
    #[error(transparent)]
    Persistence(#[from] EvolutionPersistenceError),
    #[error(transparent)]
    JobSelection(#[from] EvolutionJobSelectionError),
    #[error(transparent)]
    JobState(#[from] EvolutionJobStateError),
    #[error(transparent)]
    InputParse(#[from] EvolutionInputParseError),
}

pub type CliError = EvolutionCliError;

impl From<CliParseError> for EvolutionCliError {
    fn from(error: CliParseError) -> Self {
        match error {
            CliParseError::Usage => Self::Usage,
            CliParseError::Parse { detail } => Self::Parse { detail },
        }
    }
}

impl From<EvolutionCliError> for CliCommandError {
    fn from(error: EvolutionCliError) -> Self {
        CliCommandError::new(error.to_string())
    }
}

struct EvolutionCliLocalModeGuard {
    previous: bool,
}

impl Drop for EvolutionCliLocalModeGuard {
    fn drop(&mut self) {
        EVOLUTION_CLI_FORCE_LOCAL_MODE.with(|mode| mode.set(self.previous));
    }
}

fn enter_evolution_cli_force_local_mode(force_local: bool) -> EvolutionCliLocalModeGuard {
    let previous = cli_force_local_mode();
    EVOLUTION_CLI_FORCE_LOCAL_MODE.with(|mode| mode.set(previous || force_local));
    EvolutionCliLocalModeGuard { previous }
}

fn cli_force_local_mode() -> bool {
    EVOLUTION_CLI_FORCE_LOCAL_MODE.with(Cell::get)
}

#[derive(Debug, Default)]
pub struct EvolveCliCommandHandler;

pub fn evolve_cli_command_handler() -> EvolveCliCommandHandler {
    EvolveCliCommandHandler
}

impl CliCommandHandler for EvolveCliCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        evolve_cli_metadata()
    }

    fn help(&self) -> CliCommandHelp {
        evolve_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        evolve_cli_autocomplete()
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
        Ok(Box::new(EvolveCliCommandRunner {
            command: parse_evolve_command(args)?,
        }))
    }
}

#[derive(Debug)]
struct EvolveCliCommandRunner {
    command: EvolveCliCommand,
}

impl CliCommand for EvolveCliCommandRunner {
    fn name(&self) -> &'static str {
        "evolve"
    }

    fn policy(&self) -> CliCommandPolicy {
        EVOLVE_POLICY
    }

    fn is_help(&self) -> bool {
        matches!(
            &self.command,
            EvolveCliCommand::Help
                | EvolveCliCommand::TrustedSigners(TrustedSignersCliCommand::Help)
        )
    }

    fn help(&self) -> CliCommandHelp {
        evolve_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        evolve_cli_autocomplete()
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        let _guard = enter_evolution_cli_force_local_mode(context.force_local());
        if self.is_help() {
            return self.help().write(context);
        }
        run_evolve_cli_command(self.command).map_err(Into::into)
    }
}

fn evolve_cli_metadata() -> CliCommandMetadata {
    CliCommandMetadata::new(
        "evolve",
        &[],
        "Run evolution workflows for Sinomo or another target repository.",
        EVOLVE_POLICY,
    )
}

fn evolve_cli_help() -> CliCommandHelp {
    CliCommandHelp::new(
        "Evolve:",
        [
            "  sinomo evolve",
            "      Show evolution commands.",
            "  sinomo evolve propose \"<objective>\" [--repo <path>] [--provider <provider>] [--model <model>] [--verify \"<command>\"] [--benchmark swebench:<instance_id>] [--module-replay-report [name=]<path>] [--no-model]",
            "      Create an evolution job, candidate bundle, diff, and evaluation report for the target repository.",
            "  sinomo evolve propose \"<objective>\" --dry-run [--horizon <text>] [--budget-policy <policy>] [--reserve <resource:floor>] [--approval <policy>] [--pause-after-proposal] [--approve]",
            "      Create and persist a long-horizon campaign proposal without running a mutation model.",
            "  sinomo evolve schedule <campaign> --dry-run [--resource <resource:used/total[:window]>] [--estimate <resource:units>]",
            "      Preview resource-aware scheduling decisions for a campaign.",
            "  sinomo evolve iterate <campaign>|--campaign <id> [--dry-run] [--task <task_id>] [--repo <path>] [--provider <provider>] [--model <model>] [--path <glob>] [--verify \"<command>\"] [--benchmark <spec>] [--module-replay-report [name=]<path>]",
            "      Prepare or execute the next scheduled campaign iteration.",
            "  sinomo evolve evaluate <campaign> --job <evojob_...> --task <task_id> [--failure-class <class>] [--root-cause <text>] [--learned <text>] [--usage <resource:estimated[:actual]>]",
            "      Record stage-level evaluation and lesson memory for a campaign iteration.",
            "  sinomo evolve campaign approve|reject <campaign> [--note <note>]",
            "      Record the human checkpoint for a campaign proposal.",
            "  sinomo evolve tick --dry-run|--dispatch-approved [--scheduler-service <service_id>] [--campaign <campaign>] [--opportunity-policy <policy.json>] [--opportunity-events <events.json|jsonl>] [--opportunity-event-log <path|local>] [--opportunity-event-limit <n>] [--workload <system:id:status[:title]>] [--workload-estimate <system:id:resource:units>] [--resource <resource:used/total[:window]>] [--estimate <task:resource:units>] [--max-concurrent <n>] [--max-concurrent-workloads <n>] [--idle-window-open] [--reset-window-open]",
            "      Run the background scheduler tick and optionally dispatch approved work.",
            "  sinomo evolve approve [--job evojob_...] [--note <note>] [--override]",
            "      Approve the selected candidate after human review.",
            "  sinomo evolve reject [--job evojob_...] [--note <note>]",
            "      Reject the selected candidate.",
            "  sinomo evolve restore [--job evojob_...] [--note <note>]",
            "      Record that the operator restored from the selected candidate.",
            "  sinomo evolve status [--job evojob_...]",
            "      Show one job or all stored jobs.",
            "  sinomo evolve show [--diff|--scorecard|--decision|--packet|--regression-evidence|--lesson|--evidence-summary] [--job evojob_...]",
            "      Print a stored job artifact.",
            "  sinomo evolve verify [--job evojob_...]",
            "      Rerun verification for a stored job.",
            "  sinomo evolve packet [--job evojob_...]",
            "      Print the stored mutation packet.",
            "  sinomo evolve cleanup [--job evojob_...]",
            "      Remove a stored evolution job and its worktree.",
            "  sinomo evolve job create \"<objective>\" [--repo <path>]",
            "      Alias for proposal dry-run creation.",
            "  sinomo evolve trusted-signers list [--json] | publish | hydrate",
            "      Inspect or synchronize trusted artifact signers between local registry and cluster KV.",
        ],
    )
}

fn evolve_cli_autocomplete() -> CliAutocompleteSpec {
    CliAutocompleteSpec::new("evolve", "run evolution workflows").with_children([
        CliAutocompleteSpec::new("propose", "create an evolution proposal").with_options([
            CliAutocompleteOption::with_value(
                "--objective",
                "proposal objective",
                CliAutocompleteValue::new("text", "objective text", []),
            ),
            CliAutocompleteOption::with_value(
                "--repo",
                "target repository",
                CliAutocompleteValue::new("path", "target repository path", []),
            ),
            CliAutocompleteOption::with_value(
                "--allow",
                "allowed path glob",
                CliAutocompleteValue::new("glob", "allowed path glob", []),
            ),
            CliAutocompleteOption::with_value(
                "--verify",
                "verification command",
                CliAutocompleteValue::new("command", "verification command", []),
            ),
            CliAutocompleteOption::with_value(
                "--provider",
                "model provider",
                CliAutocompleteValue::new("provider", "language model provider", []),
            ),
            CliAutocompleteOption::with_value(
                "--model",
                "model id",
                CliAutocompleteValue::new("model", "model id", []),
            ),
            CliAutocompleteOption::flag("--dry-run", "persist a campaign proposal only"),
            CliAutocompleteOption::flag("--no-model", "create a candidate without model mutation"),
            CliAutocompleteOption::flag("--approve", "mark campaign proposal approved"),
            CliAutocompleteOption::flag("--pause-after-proposal", "pause after campaign proposal"),
        ]),
        CliAutocompleteSpec::new("approve", "approve a proposed job").with_options([
            CliAutocompleteOption::with_value(
                "--job",
                "evolution job id",
                CliAutocompleteValue::new("job", "evolution job id", []),
            ),
            CliAutocompleteOption::with_value(
                "--note",
                "decision note",
                CliAutocompleteValue::new("text", "decision note", []),
            ),
            CliAutocompleteOption::flag("--override", "override failed approval preflight"),
        ]),
        CliAutocompleteSpec::new("reject", "reject a proposed job").with_options([
            CliAutocompleteOption::with_value(
                "--job",
                "evolution job id",
                CliAutocompleteValue::new("job", "evolution job id", []),
            ),
            CliAutocompleteOption::with_value(
                "--note",
                "decision note",
                CliAutocompleteValue::new("text", "decision note", []),
            ),
        ]),
        CliAutocompleteSpec::new("restore", "record restoration").with_options([
            CliAutocompleteOption::with_value(
                "--job",
                "evolution job id",
                CliAutocompleteValue::new("job", "evolution job id", []),
            ),
            CliAutocompleteOption::with_value(
                "--note",
                "decision note",
                CliAutocompleteValue::new("text", "decision note", []),
            ),
        ]),
        CliAutocompleteSpec::new("status", "show evolution status").with_options([
            CliAutocompleteOption::with_value(
                "--job",
                "evolution job id",
                CliAutocompleteValue::new("job", "evolution job id", []),
            ),
        ]),
        CliAutocompleteSpec::new("show", "show stored job artifacts").with_options([
            CliAutocompleteOption::with_value(
                "--job",
                "evolution job id",
                CliAutocompleteValue::new("job", "evolution job id", []),
            ),
            CliAutocompleteOption::flag("--diff", "show candidate diff"),
            CliAutocompleteOption::flag("--scorecard", "show scorecard"),
            CliAutocompleteOption::flag("--decision", "show decision"),
            CliAutocompleteOption::flag("--packet", "show mutation packet"),
            CliAutocompleteOption::flag("--regression-evidence", "show regression evidence"),
            CliAutocompleteOption::flag("--lesson", "show evolution lesson"),
            CliAutocompleteOption::flag("--evidence-summary", "show evidence summary"),
        ]),
        CliAutocompleteSpec::new("verify", "rerun verification").with_options([
            CliAutocompleteOption::with_value(
                "--job",
                "evolution job id",
                CliAutocompleteValue::new("job", "evolution job id", []),
            ),
        ]),
        CliAutocompleteSpec::new("packet", "show mutation packet").with_options([
            CliAutocompleteOption::with_value(
                "--job",
                "evolution job id",
                CliAutocompleteValue::new("job", "evolution job id", []),
            ),
        ]),
        CliAutocompleteSpec::new("cleanup", "clean stored evolution artifacts").with_options([
            CliAutocompleteOption::with_value(
                "--job",
                "evolution job id",
                CliAutocompleteValue::new("job", "evolution job id", []),
            ),
        ]),
        CliAutocompleteSpec::new("job", "job utilities").with_children([CliAutocompleteSpec::new(
            "create",
            "create a dry-run proposal",
        )
        .with_values([CliAutocompleteValue::new("objective", "objective text", [])])]),
        CliAutocompleteSpec::new("schedule", "schedule evolution work").with_options([
            CliAutocompleteOption::with_value(
                "--resource",
                "resource ledger value",
                CliAutocompleteValue::new("resource", "resource:used/total[:window]", []),
            ),
            CliAutocompleteOption::with_value(
                "--estimate",
                "task estimate",
                CliAutocompleteValue::new("estimate", "resource:units", []),
            ),
            CliAutocompleteOption::flag("--dry-run", "preview scheduling"),
        ]),
        CliAutocompleteSpec::new("iterate", "prepare or execute an iteration").with_options([
            CliAutocompleteOption::with_value(
                "--campaign",
                "campaign id",
                CliAutocompleteValue::new("campaign", "campaign id", []),
            ),
            CliAutocompleteOption::with_value(
                "--task",
                "task id",
                CliAutocompleteValue::new("task", "task id", []),
            ),
            CliAutocompleteOption::with_value(
                "--repo",
                "target repository",
                CliAutocompleteValue::new("path", "target repository path", []),
            ),
            CliAutocompleteOption::with_value(
                "--provider",
                "model provider",
                CliAutocompleteValue::new("provider", "language model provider", []),
            ),
            CliAutocompleteOption::with_value(
                "--model",
                "model id",
                CliAutocompleteValue::new("model", "model id", []),
            ),
            CliAutocompleteOption::with_value(
                "--path",
                "allowed path glob",
                CliAutocompleteValue::new("glob", "allowed path glob", []),
            ),
            CliAutocompleteOption::with_value(
                "--verify",
                "verification command",
                CliAutocompleteValue::new("command", "verification command", []),
            ),
            CliAutocompleteOption::flag("--dry-run", "preview iteration only"),
        ]),
        CliAutocompleteSpec::new("campaign", "manage campaigns").with_children([
            CliAutocompleteSpec::new("approve", "approve campaign").with_options([
                CliAutocompleteOption::with_value(
                    "--campaign",
                    "campaign id",
                    CliAutocompleteValue::new("campaign", "campaign id", []),
                ),
                CliAutocompleteOption::with_value(
                    "--note",
                    "decision note",
                    CliAutocompleteValue::new("text", "decision note", []),
                ),
            ]),
            CliAutocompleteSpec::new("reject", "reject campaign").with_options([
                CliAutocompleteOption::with_value(
                    "--campaign",
                    "campaign id",
                    CliAutocompleteValue::new("campaign", "campaign id", []),
                ),
                CliAutocompleteOption::with_value(
                    "--note",
                    "decision note",
                    CliAutocompleteValue::new("text", "decision note", []),
                ),
            ]),
        ]),
        CliAutocompleteSpec::new("trusted-signers", "manage trusted signers").with_children([
            CliAutocompleteSpec::new("list", "list trusted signers")
                .with_options([CliAutocompleteOption::flag("--json", "emit JSON")]),
            CliAutocompleteSpec::new("publish", "publish local signers to cluster"),
            CliAutocompleteSpec::new("hydrate", "hydrate local signers from cluster"),
        ]),
        CliAutocompleteSpec::new("tick", "run background scheduler tick").with_options([
            CliAutocompleteOption::flag("--dry-run", "preview tick"),
            CliAutocompleteOption::flag("--dispatch-approved", "dispatch approved work"),
            CliAutocompleteOption::with_value(
                "--campaign",
                "campaign id",
                CliAutocompleteValue::new("campaign", "campaign id", []),
            ),
            CliAutocompleteOption::with_value(
                "--resource",
                "resource ledger value",
                CliAutocompleteValue::new("resource", "resource:used/total[:window]", []),
            ),
            CliAutocompleteOption::with_value(
                "--estimate",
                "task estimate",
                CliAutocompleteValue::new("estimate", "task:resource:units", []),
            ),
        ]),
    ])
}
pub(crate) fn resolve_job_from_args(
    args: &[String],
    waiting_only: bool,
) -> Result<(EvolutionPaths, StoredEvolutionJob), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let job_id = job_flag(args)?;
    let mode = if waiting_only {
        EvolutionJobSelectionMode::AwaitingApproval
    } else {
        EvolutionJobSelectionMode::Active
    };
    let record = select_stored_job(&paths, job_id.as_ref(), mode)?;
    Ok((paths, record))
}

pub(crate) fn println_campaign_proposal(
    campaign: &EvolutionCampaign,
    bundle: &CampaignBundlePaths,
) {
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

pub(crate) fn println_scheduler_dry_run(
    dry_run: &ResourceSchedulerDryRun,
    bundle: &CampaignBundlePaths,
) {
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

pub(crate) fn println_iteration_dry_run(
    plan: &ScheduledIterationPlan,
    bundle: &CampaignBundlePaths,
) {
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

fn attach_persisted_memory_to_iteration_plan(
    paths: &EvolutionPaths,
    plan: ScheduledIterationPlan,
) -> Result<ScheduledIterationPlan, CliError> {
    let lessons =
        read_recent_evolution_lesson_memory(paths, EVOLUTION_ITERATION_MEMORY_CONTEXT_LIMIT)?;
    let gates = read_recent_proposed_regression_gate_memory(
        paths,
        EVOLUTION_ITERATION_MEMORY_CONTEXT_LIMIT,
    )?;
    Ok(attach_evolution_memory_to_scheduled_iteration(
        plan,
        &lessons,
        &gates,
        EVOLUTION_ITERATION_MEMORY_CONTEXT_LIMIT,
    ))
}

fn attach_persisted_memory_to_background_plan(
    paths: &EvolutionPaths,
    plan: &mut BackgroundSchedulingPlan,
) -> Result<(), CliError> {
    let lessons =
        read_recent_evolution_lesson_memory(paths, EVOLUTION_ITERATION_MEMORY_CONTEXT_LIMIT)?;
    let gates = read_recent_proposed_regression_gate_memory(
        paths,
        EVOLUTION_ITERATION_MEMORY_CONTEXT_LIMIT,
    )?;
    for update in &mut plan.campaigns {
        if let Some(iteration) = update.scheduled_iteration.take() {
            update.scheduled_iteration = Some(attach_evolution_memory_to_scheduled_iteration(
                iteration,
                &lessons,
                &gates,
                EVOLUTION_ITERATION_MEMORY_CONTEXT_LIMIT,
            ));
        }
    }
    Ok(())
}

pub fn run_evolve_command(args: &[String]) -> Result<(), CliError> {
    run_evolve_cli_command(parse_evolve_command(args)?)
}

pub(crate) fn run_evolve_cli_command(command: EvolveCliCommand) -> Result<(), CliError> {
    match command {
        EvolveCliCommand::Help => {
            evolve_cli_help()
                .write(&mut CliCommandContext::new(
                    &mut std::io::stdout(),
                    &mut std::io::stderr(),
                ))
                .map_err(|error| CliError::Parse {
                    detail: error.to_string(),
                })?;
            Ok(())
        }
        EvolveCliCommand::Propose(args) => evolve_propose(&args),
        EvolveCliCommand::Schedule(args) => evolve_schedule(&args),
        EvolveCliCommand::Iterate(args) => evolve_iterate(&args),
        EvolveCliCommand::Evaluate(args) => evolve_evaluate(&args),
        EvolveCliCommand::Campaign(command) => evolve_campaign(command),
        EvolveCliCommand::Tick(args) => evolve_tick(&args),
        EvolveCliCommand::Decide(outcome, args) => evolve_decide(&args, outcome),
        EvolveCliCommand::Status(args) => evolve_status(&args),
        EvolveCliCommand::Show(args) => evolve_show(&args),
        EvolveCliCommand::Verify(args) => evolve_verify(&args),
        EvolveCliCommand::Packet(args) => evolve_show_packet(&args),
        EvolveCliCommand::Cleanup(args) => evolve_cleanup(&args),
        EvolveCliCommand::Job(args) => evolve_job(&args),
        EvolveCliCommand::TrustedSigners(command) => evolve_trusted_signers(command),
        EvolveCliCommand::Models(_) => Err(CliError::Parse {
            detail: "model configuration is handled by `sinomo models`; use `sinomo models ...`"
                .to_string(),
        }),
    }
}

#[derive(Clone, Debug, Serialize)]
struct TrustedSignersListView {
    source: String,
    count: usize,
    signers: Vec<sinomo_runtime::registry::TrustedArtifactSigner>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedSignersSyncReport {
    pub source: String,
    pub destination: String,
    pub count: usize,
}

fn evolve_trusted_signers(command: TrustedSignersCliCommand) -> Result<(), CliError> {
    match command {
        TrustedSignersCliCommand::Help => {
            evolve_cli_help()
                .write(&mut CliCommandContext::new(
                    &mut std::io::stdout(),
                    &mut std::io::stderr(),
                ))
                .map_err(|error| CliError::Parse {
                    detail: error.to_string(),
                })?;
            Ok(())
        }
        TrustedSignersCliCommand::List(options) => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let (source, signers) = read_trusted_signers_for_node(&data_dir)?;
            if options.json {
                print_json(
                    "<trusted-signers>",
                    &TrustedSignersListView {
                        source,
                        count: signers.len(),
                        signers,
                    },
                )?;
            } else {
                println!("trusted signers: {}", signers.len());
                println!("source: {source}");
                for signer in signers {
                    println!(
                        "  {} [{:?}] {}",
                        signer.signer_id, signer.status, signer.public_key
                    );
                    if !signer.reason.trim().is_empty() {
                        println!("    reason: {}", signer.reason);
                    }
                }
            }
            Ok(())
        }
        TrustedSignersCliCommand::Publish => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let report = publish_trusted_signers_to_cluster(&data_dir)?;
            println!("trusted signers published: {}", report.count);
            println!("source: {}", report.source);
            println!("destination: {}", report.destination);
            Ok(())
        }
        TrustedSignersCliCommand::Hydrate => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let report = hydrate_trusted_signers_from_cluster(&data_dir)?;
            println!("trusted signers hydrated: {}", report.count);
            println!("source: {}", report.source);
            println!("destination: {}", report.destination);
            Ok(())
        }
    }
}

pub fn registry_trust_uses_cluster(data_dir: &SinomoDataDir) -> bool {
    if cli_force_local_mode() {
        return false;
    }
    network::read_cluster_state(&ClusterPaths::new(data_dir)).is_ok()
}

pub fn read_trusted_signers_for_node(
    data_dir: &SinomoDataDir,
) -> Result<(String, Vec<sinomo_runtime::registry::TrustedArtifactSigner>), CliError> {
    if registry_trust_uses_cluster(data_dir) {
        let entries = registry_trusted_signer_cluster_entries(data_dir)?;
        let signers = sinomo_runtime::registry::trusted_artifact_signers_from_kv_entries(&entries)?;
        return Ok((
            sinomo_runtime::registry::CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX.to_string(),
            signers,
        ));
    }
    let path = sinomo_runtime::registry::trusted_artifact_signers_path(data_dir.root());
    let signers = sinomo_runtime::registry::read_trusted_artifact_signers(data_dir.root())?;
    Ok((path.display().to_string(), signers))
}

pub fn publish_trusted_signers_to_cluster(
    data_dir: &SinomoDataDir,
) -> Result<TrustedSignersSyncReport, CliError> {
    if !registry_trust_uses_cluster(data_dir) {
        return Err(CliError::DaemonNotRunning {
            detail:
                "trusted signer publish requires cluster mode; use `sinomo evolve trusted-signers list` for local records"
                    .to_string(),
        });
    }
    let signers = sinomo_runtime::registry::read_trusted_artifact_signers(data_dir.root())?;
    let txn = sinomo_runtime::registry::trusted_artifact_signers_kv_txn(&signers)?;
    commit_registry_trust_cluster_txn(data_dir, txn)?;
    Ok(TrustedSignersSyncReport {
        source: sinomo_runtime::registry::trusted_artifact_signers_path(data_dir.root())
            .display()
            .to_string(),
        destination: sinomo_runtime::registry::CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX.to_string(),
        count: signers.len(),
    })
}

pub fn hydrate_trusted_signers_from_cluster(
    data_dir: &SinomoDataDir,
) -> Result<TrustedSignersSyncReport, CliError> {
    if !registry_trust_uses_cluster(data_dir) {
        return Err(CliError::DaemonNotRunning {
            detail:
                "trusted signer hydrate requires cluster mode; use `sinomo --local evolve trusted-signers list` for local records"
                    .to_string(),
        });
    }
    let entries = registry_trusted_signer_cluster_entries(data_dir)?;
    let signers = sinomo_runtime::registry::trusted_artifact_signers_from_kv_entries(&entries)?;
    let path = sinomo_runtime::registry::write_trusted_artifact_signers(data_dir.root(), &signers)?;
    Ok(TrustedSignersSyncReport {
        source: sinomo_runtime::registry::CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX.to_string(),
        destination: path.display().to_string(),
        count: signers.len(),
    })
}

fn registry_trusted_signer_cluster_entries(
    data_dir: &SinomoDataDir,
) -> Result<Vec<KvEntry>, CliError> {
    let paths = ClusterPaths::new(data_dir);
    let snapshot = registry_trust_cluster_snapshot(&paths)?;
    let store =
        InProcessKvStore::restore(snapshot).map_err(sinomo_runtime::network::NetworkError::from)?;
    store
        .range_prefix(sinomo_runtime::registry::CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX)
        .map_err(sinomo_runtime::network::NetworkError::from)
        .map_err(Into::into)
}

fn registry_trust_cluster_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    let _ = paths;
    if let Some(status) = default_main_daemon_status()?
        && let Ok(snapshot) =
            daemon_fetch_cluster_kv_snapshot(&status.address, DAEMON_PROBE_TIMEOUT)
    {
        return Ok(snapshot);
    }
    #[cfg(test)]
    {
        return sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .map_err(network::NetworkError::from)
            .map_err(Into::into);
    }
    #[cfg(not(test))]
    {
        Err(CliError::DaemonNotRunning {
            detail: "clustered trusted signer registry requires the local daemon".to_string(),
        })
    }
}

#[cfg(test)]
fn commit_registry_trust_cluster_txn(data_dir: &SinomoDataDir, txn: KvTxn) -> Result<(), CliError> {
    let paths = ClusterPaths::new(data_dir);
    let state = network::read_cluster_state_without_kv(&paths).map_err(|error| match error {
        network::NetworkError::ClusterOrphan => {
            CliError::Network(network::NetworkError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            })
        }
        other => other.into(),
    })?;
    paths
        .kv_paths()
        .append_kv_command_with_response_and_publish(
            &state.local_node_id,
            sinomo_runtime::kv::KvCommand::Txn(txn),
            sinomo_runtime::kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(network::NetworkError::from)?;
    Ok(())
}

#[cfg(not(test))]
fn commit_registry_trust_cluster_txn(
    _data_dir: &SinomoDataDir,
    txn: KvTxn,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "clustered trusted signer registry writes require the local daemon".to_string(),
        });
    };
    let payload = sinomo_runtime::kv::cluster_kv_txn_rpc_payload(&txn)
        .map_err(network::NetworkError::from)?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &request,
        DAEMON_CLUSTER_KV_TXN_TIMEOUT,
    )?;
    response.error_detail().map_err(CliError::from)?;
    let _: KvTxnResponse = sinomo_runtime::kv::decode_cluster_kv_txn_rpc_body(&response.body)
        .map_err(network::NetworkError::from)?;
    Ok(())
}

fn resolve_evolution_repo(
    data_dir: &SinomoDataDir,
    selected_repo: Option<PathBuf>,
) -> Result<PathBuf, CliError> {
    if let Some(repo) = selected_repo {
        return Ok(repo);
    }
    let rollout_paths = SelfRolloutPaths::new(&data_dir.root());
    ensure_rollout_layout(&rollout_paths)?;
    let current_repo = env::current_dir().map_err(|source| CliError::Io {
        path: ".".to_string(),
        detail: source.to_string(),
    })?;
    ensure_rollout_source_repo(&rollout_paths, Some(&current_repo))?;
    Ok(rollout_paths.source_repo)
}

fn resolve_requested_evolution_model(
    provider: Option<String>,
    model: Option<String>,
    default_provider: String,
    default_model: String,
) -> Result<(String, String), CliError> {
    let provider = provider.unwrap_or(default_provider);
    let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
        detail: format!("unknown provider `{provider}`"),
    })?;
    let model = normalize_model_id_for_provider(&provider, &model.unwrap_or(default_model));
    Ok((provider, model))
}

fn default_main_daemon_status() -> Result<Option<sinomo_runtime::daemon::DaemonStatus>, CliError> {
    daemon_default_main_daemon_status(DAEMON_PROBE_TIMEOUT).map_err(Into::into)
}

fn daemon_fetch_cluster_kv_snapshot(
    address: &str,
    timeout: Duration,
) -> Result<KvSnapshot, sinomo_runtime::daemon::DaemonError> {
    let payload = sinomo_runtime::kv::cluster_kv_snapshot_rpc_payload()?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = sinomo_runtime::daemon::send_daemon_rpc(address, &request, timeout)?;
    response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_kv_snapshot_rpc_body(&response.body).map_err(Into::into)
}

fn model_config_uses_cluster(data_dir: &SinomoDataDir) -> bool {
    if cli_force_local_mode() {
        return false;
    }
    network::read_cluster_state(&ClusterPaths::new(data_dir)).is_ok()
}

fn load_model_config_for_node(
    data_dir: &SinomoDataDir,
) -> Result<(ModelConfig, PathBuf), CliError> {
    if model_config_uses_cluster(data_dir) {
        let paths = ClusterPaths::new(data_dir);
        let snapshot = model_config_cluster_snapshot(&paths)?;
        let config = model_config_from_cluster_kv_snapshot(&snapshot)?.unwrap_or_default();
        return Ok((config, paths.kv_snapshot));
    }
    let store = model_config_store()?;
    let config = store.get_typed(data_dir, None, LM_PROVIDER_CONFIG_KEY)?;
    Ok((config, model_config_definition()?.local_path(data_dir)))
}

fn model_config_cluster_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    let _ = paths;
    if let Some(status) = default_main_daemon_status()?
        && let Ok(snapshot) =
            daemon_fetch_cluster_kv_snapshot(&status.address, DAEMON_PROBE_TIMEOUT)
    {
        return Ok(snapshot);
    }
    #[cfg(test)]
    {
        return sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .map_err(network::NetworkError::from)
            .map_err(Into::into);
    }
    #[cfg(not(test))]
    {
        Err(CliError::DaemonNotRunning {
            detail: "clustered model config requires the local daemon".to_string(),
        })
    }
}

fn print_json<T: Serialize>(label: &str, value: &T) -> Result<(), CliError> {
    let output = serde_json::to_string_pretty(value).map_err(|source| CliError::Json {
        path: label.to_string(),
        detail: source.to_string(),
    })?;
    println!("{output}");
    Ok(())
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
    println!("opportunity intake: {}", plan.opportunity_intake.len());
    println!(
        "opportunity events skipped: {}",
        plan.opportunity_events_skipped.len()
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
    if !plan.opportunity_intake.is_empty() {
        println!(
            "opportunity intake json: {}",
            paths.opportunity_intake_json.display()
        );
        println!(
            "opportunity intake log: {}",
            paths.opportunity_intakes_jsonl.display()
        );
    }
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
    for intake in &plan.opportunity_intake {
        println!(
            "opportunity {}: {:?} accepted={} dispatch_allowed={} ({})",
            intake.campaign_id,
            intake.detection.signal_kind,
            intake.risk_gate.accepted,
            intake.risk_gate.dispatch_allowed,
            intake.risk_gate.reason
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
    if let Some(benchmark_results_path) = &report.bundle.benchmark_results_path {
        println!("benchmark results: {benchmark_results_path}");
    }
    if let Some(resource_usage_path) = &report.bundle.resource_usage_path {
        println!("resource usage: {resource_usage_path}");
    }
    if let Some(sifs_mirror_ref) = report
        .bundle
        .artifact_refs
        .iter()
        .find(|artifact_ref| artifact_ref.ends_with("/sifs-mirror.json"))
    {
        println!("sifs mirror: {sifs_mirror_ref}");
    }
    for artifact_ref in report
        .bundle
        .artifact_refs
        .iter()
        .filter(|artifact_ref| artifact_ref.starts_with("sifs://"))
        .take(3)
    {
        println!("artifact: {artifact_ref}");
    }
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

fn outcome_label(outcome: &EvaluationOutcome) -> &'static str {
    match outcome {
        EvaluationOutcome::Passed => "passed",
        EvaluationOutcome::Failed => "failed",
        EvaluationOutcome::Inconclusive => "inconclusive",
    }
}

struct PersistentEvolutionLifecycleObserver {
    paths: EvolutionPaths,
}

impl EvolutionLifecycleObserver for PersistentEvolutionLifecycleObserver {
    fn on_update(&self, update: &EvolutionLifecycleUpdate) -> Result<(), String> {
        write_stored_job(
            &self.paths,
            &StoredEvolutionJob {
                job: update.job.clone(),
                candidate_workspace: update.candidate_workspace.clone(),
                bundle: update.bundle.clone(),
                verification_outcome: update.verification_outcome.clone(),
                changed_files: update.changed_files.clone(),
                decision: None,
            },
        )
        .map_err(|error| format!("{} ({})", update.summary, error))
    }
}

pub(crate) fn evolve_propose(args: &[String]) -> Result<(), CliError> {
    let options = parse_propose_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let config = read_config(&paths).ok();
    let (model_config, _) = load_model_config_for_node(&data_dir)?;
    let evolution_model_config = config.as_ref().map(|config| sinomo_lm::ModelConfig {
        default_provider: config.default_provider.clone(),
        default_account: None,
        default_model: config.default_model.clone(),
        accounts: Vec::new(),
    });
    let default_model =
        resolve_default_model(Some(&model_config), evolution_model_config.as_ref())?;
    let repo = resolve_evolution_repo(
        &data_dir,
        options
            .repo
            .clone()
            .or_else(|| config.as_ref().map(|config| PathBuf::from(&config.repo))),
    )?;

    if options.dry_run || options.pause_after_proposal {
        let request = prepare_campaign_proposal_request(CampaignProposalInput {
            objective: options.objective.clone(),
            horizon: options.horizon.clone(),
            budget_policy: options.budget_policy.clone(),
            reserve_floor_specs: options.reserve_floors.clone(),
            approval_policy: options.approval_policy.clone(),
            max_wall_time: options.max_wall_time.clone(),
            pause_after_proposal: options.pause_after_proposal,
            dry_run: true,
            approved: options.approve_campaign,
            target_ring: options.target_ring.clone(),
            source_repo: Some(repo.display().to_string()),
        })?;
        let campaign = propose_evolution_campaign(request);
        let bundle = write_campaign_bundle(&paths, &campaign)?;
        println_campaign_proposal(&campaign, &bundle);
        return Ok(());
    }

    let prepared = prepare_evolution_request(EvolutionRequestInput {
        objective: options.objective,
        target_ring: options.target_ring,
        intended_paths: options.allow,
        verification_commands: options.verify,
        benchmark_specs: options.benchmark,
        module_replay_report_specs: options.module_replay_report,
        provider: options.provider,
        model: options.model,
        default_provider: Some(default_model.provider),
        default_model: Some(default_model.configured_model),
        dry_run: options.no_model,
    })?;
    let request = prepared.request;

    let orchestrator = EvolutionOrchestrator::new(
        GitWorkspaceAdapter::new(&repo, &paths.jobs),
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(&paths.jobs),
    );
    let model_runner = EvolutionAgentMutationModel {
        provider: prepared.provider,
        model: prepared.model,
        data_dir: data_dir.clone(),
        max_steps: None,
    };
    let lifecycle = PersistentEvolutionLifecycleObserver {
        paths: paths.clone(),
    };
    let mut report = if request.dry_run {
        orchestrator.propose_with_lifecycle(request, None, Some(&lifecycle))?
    } else {
        orchestrator.propose_with_lifecycle(request, Some(&model_runner), Some(&lifecycle))?
    };
    if let Some(sifs_root) = configured_sifs_artifact_root(config.as_ref()) {
        mirror_evolution_report_bundle_to_sifs(&data_dir, &mut report, &sifs_root)?;
    }

    write_report_record(&paths, &report)?;
    println_report(&report);
    Ok(())
}

pub(crate) fn evolve_schedule(args: &[String]) -> Result<(), CliError> {
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

pub(crate) fn evolve_iterate(args: &[String]) -> Result<(), CliError> {
    let options = parse_iteration_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let config = read_config(&paths).ok();
    let mut campaign = read_campaign(&paths, &options.campaign_id)?;
    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign: campaign.clone(),
        task_id: options.task_id.clone(),
        intended_paths: options.intended_paths.clone(),
        extra_verification_commands: options.verification_commands.clone(),
        dry_run: options.dry_run,
    })
    .map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })?;
    let plan = attach_persisted_memory_to_iteration_plan(&paths, plan)?;
    if options.dry_run {
        let bundle = write_campaign_iteration_dry_run(&paths, &mut campaign, &plan)?;
        println_iteration_dry_run(&plan, &bundle);
        return Ok(());
    }

    let (report, update, bundle) = execute_campaign_iteration_plan(
        &data_dir,
        &paths,
        config.as_ref(),
        campaign,
        plan,
        IterationExecutionOverrides {
            provider: options.provider.clone(),
            model: options.model.clone(),
            repo: options.repo.clone(),
            benchmark: options.benchmark.clone(),
            module_replay_report: options.module_replay_report.clone(),
        },
    )?;
    println_report(&report);
    println!("campaign: {}", update.campaign.campaign_id);
    println!("iteration: {}", bundle.iteration_plan_json.display());
    println!("task result: {}", bundle.iteration_result_json.display());
    println!("stage scorecard: {}", bundle.stage_scorecard_json.display());
    if let Some(resource_usage_path) = &report.bundle.resource_usage_path {
        println!("resource usage: {resource_usage_path}");
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct IterationExecutionOverrides {
    provider: Option<String>,
    model: Option<String>,
    repo: Option<PathBuf>,
    benchmark: Vec<String>,
    module_replay_report: Vec<String>,
}

fn execute_campaign_iteration_plan(
    data_dir: &SinomoDataDir,
    paths: &EvolutionPaths,
    config: Option<&EvolutionConfig>,
    campaign: EvolutionCampaign,
    mut plan: ScheduledIterationPlan,
    overrides: IterationExecutionOverrides,
) -> Result<(EvolutionReport, StageEvaluationUpdate, CampaignBundlePaths), CliError> {
    let (model_config, _) = load_model_config_for_node(&data_dir)?;
    let evolution_model_config = config.map(|config| sinomo_lm::ModelConfig {
        default_provider: config.default_provider.clone(),
        default_account: None,
        default_model: config.default_model.clone(),
        accounts: Vec::new(),
    });
    let default_model =
        resolve_default_model(Some(&model_config), evolution_model_config.as_ref())?;
    let (provider, model) = resolve_requested_evolution_model(
        overrides.provider.clone(),
        overrides.model.clone(),
        default_model.provider,
        default_model.configured_model,
    )?;
    let repo = resolve_evolution_repo(
        &data_dir,
        overrides
            .repo
            .clone()
            .or_else(|| campaign.source_repo.as_ref().map(PathBuf::from))
            .or_else(|| config.map(|config| PathBuf::from(&config.repo))),
    )?;

    plan.evolution_request.model_provider = Some(provider.clone());
    plan.evolution_request.model = Some(model.clone());
    merge_benchmark_gate_requests(
        &mut plan.evolution_request.benchmark_gates,
        parse_benchmark_gate_specs(&overrides.benchmark, &provider, &model)?,
    );
    merge_module_replay_report_refs(
        &mut plan.evolution_request.module_replay_reports,
        parse_module_replay_report_specs(&overrides.module_replay_report)?,
    );

    let orchestrator = EvolutionOrchestrator::new(
        GitWorkspaceAdapter::new(&repo, &paths.jobs),
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(&paths.jobs),
    );
    let model_runner = EvolutionAgentMutationModel {
        provider,
        model,
        data_dir: data_dir.clone(),
        max_steps: None,
    };
    let mut report = orchestrator.propose(plan.evolution_request.clone(), Some(&model_runner))?;
    if let Some(sifs_root) = configured_sifs_artifact_root(config) {
        mirror_evolution_report_bundle_to_sifs(data_dir, &mut report, &sifs_root)?;
    }
    write_report_record(&paths, &report)?;
    let resource_usage =
        candidate_resource_usage_estimates_for_report(&campaign, &plan.task.task_id, &report)?;

    let update = evaluate_campaign_stage(StageEvaluationRequest {
        campaign,
        task_id: plan.task.task_id.clone(),
        report: report.clone(),
        resource_usage,
        failure_class: None,
        root_cause_summary: None,
        what_was_learned: Vec::new(),
    });
    let bundle = write_campaign_iteration_evaluation(&paths, &plan, &update)?;
    Ok((report, update, bundle))
}

pub fn merge_benchmark_gate_requests(
    existing: &mut Vec<BenchmarkGateRequest>,
    additional: Vec<BenchmarkGateRequest>,
) {
    let mut seen = existing
        .iter()
        .map(benchmark_gate_request_key)
        .collect::<BTreeSet<_>>();
    for gate in additional {
        if seen.insert(benchmark_gate_request_key(&gate)) {
            existing.push(gate);
        }
    }
}

fn benchmark_gate_request_key(gate: &BenchmarkGateRequest) -> String {
    format!("{}:{}:{:?}", gate.name, gate.expected_exit_code, gate.kind)
}

pub fn merge_module_replay_report_refs(
    existing: &mut Vec<ModuleReplayReportRef>,
    additional: Vec<ModuleReplayReportRef>,
) {
    let mut seen = existing
        .iter()
        .map(module_replay_report_ref_key)
        .collect::<BTreeSet<_>>();
    for report in additional {
        if seen.insert(module_replay_report_ref_key(&report)) {
            existing.push(report);
        }
    }
}

fn module_replay_report_ref_key(report: &ModuleReplayReportRef) -> String {
    report.report_path.clone()
}

pub(crate) fn evolve_evaluate(args: &[String]) -> Result<(), CliError> {
    let options = parse_stage_evaluation_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let campaign = read_campaign(&paths, &options.campaign_id)?;
    let record = read_stored_job(&paths.job_path(&options.job_id))?;
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

pub(crate) fn evolve_campaign(command: CampaignCliCommand) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let (options, decision) = match command {
        CampaignCliCommand::Approve(options) => (options, CampaignCheckpointDecision::Approve),
        CampaignCliCommand::Reject(options) => (options, CampaignCheckpointDecision::Reject),
    };
    let campaign = read_campaign(&paths, &options.campaign_id)?;
    let campaign =
        decide_campaign_checkpoint(campaign, decision, options.note, "sinomo evolve campaign");
    let bundle = write_campaign_bundle(&paths, &campaign)?;
    println_campaign_proposal(&campaign, &bundle);
    Ok(())
}

pub fn read_background_tick_opportunity_inputs(
    data_dir: &SinomoDataDir,
    options: &BackgroundTickOptions,
) -> Result<
    (
        Vec<EventEnvelope>,
        Option<EvolutionOpportunityDetectionPolicy>,
        Option<NodeId>,
    ),
    CliError,
> {
    let policy = options
        .opportunity_policy
        .as_ref()
        .map(|path| read_json_cli::<EvolutionOpportunityDetectionPolicy>(path))
        .transpose()?;
    let has_explicit_event_source =
        !options.opportunity_event_files.is_empty() || options.opportunity_event_log.is_some();
    if policy.is_none() && has_explicit_event_source {
        return Err(CliError::Parse {
            detail:
                "opportunity event intake requires --opportunity-policy so events can be scoped safely"
                    .to_string(),
        });
    }

    let mut events = Vec::new();
    let event_log_path = options
        .opportunity_event_log
        .as_ref()
        .map(|path| resolve_background_tick_event_log_path(data_dir, path))
        .or_else(|| {
            policy
                .is_some()
                .then(|| data_dir.root().join("events").join("event-log.jsonl"))
        });
    if let Some(path) = event_log_path {
        if path.exists() {
            events.extend(read_background_tick_event_log(
                &path,
                options.opportunity_event_limit,
            )?);
        } else if options.opportunity_event_log.is_some() {
            return Err(CliError::Io {
                path: path.display().to_string(),
                detail: "opportunity event log does not exist".to_string(),
            });
        }
    }
    for path in &options.opportunity_event_files {
        events.extend(read_background_tick_event_file(path)?);
    }
    dedup_events_by_id(&mut events);
    Ok((events, policy, options.opportunity_planner_node_id.clone()))
}

fn resolve_background_tick_event_log_path(data_dir: &SinomoDataDir, path: &Path) -> PathBuf {
    if path == Path::new("default") || path == Path::new("local") {
        data_dir.root().join("events").join("event-log.jsonl")
    } else {
        path.to_path_buf()
    }
}

fn read_json_cli<T: DeserializeOwned>(path: &Path) -> Result<T, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn read_background_tick_event_log(
    path: &Path,
    limit: usize,
) -> Result<Vec<EventEnvelope>, CliError> {
    let store = JsonlEventStore::new(path);
    let stored = store.replay(ReplayQuery {
        direction: ReplayDirection::Reverse,
        limit: Some(limit),
        ..ReplayQuery::all()
    })?;
    Ok(stored.into_iter().map(|stored| stored.event).collect())
}

fn read_background_tick_event_file(path: &Path) -> Result<Vec<EventEnvelope>, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') || trimmed.starts_with('{') {
        return events_from_json_value(
            path,
            serde_json::from_str(trimmed).map_err(|source| CliError::Json {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?,
        );
    }
    let mut events = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value = serde_json::from_str(line).map_err(|source| CliError::Json {
            path: format!("{}:{}", path.display(), index + 1),
            detail: source.to_string(),
        })?;
        events.extend(events_from_json_value(path, value)?);
    }
    Ok(events)
}

fn events_from_json_value(
    path: &Path,
    value: serde_json::Value,
) -> Result<Vec<EventEnvelope>, CliError> {
    if let Some(array) = value.as_array() {
        return array
            .iter()
            .cloned()
            .map(|value| event_from_json_value(path, value))
            .collect();
    }
    if let Some(events) = value.get("events").and_then(|events| events.as_array()) {
        return events
            .iter()
            .cloned()
            .map(|value| event_from_json_value(path, value))
            .collect();
    }
    event_from_json_value(path, value).map(|event| vec![event])
}

fn event_from_json_value(path: &Path, value: serde_json::Value) -> Result<EventEnvelope, CliError> {
    let event_value = match value {
        serde_json::Value::Object(mut object) => object
            .remove("event")
            .unwrap_or(serde_json::Value::Object(object)),
        other => other,
    };
    serde_json::from_value(event_value).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn dedup_events_by_id(events: &mut Vec<EventEnvelope>) {
    let mut seen = BTreeSet::new();
    events.retain(|event| seen.insert(event.event_id.clone()));
}

pub(crate) fn evolve_tick(args: &[String]) -> Result<(), CliError> {
    let options = parse_background_tick_options(args)?;
    if !options.dry_run && !options.dispatch_approved {
        return Err(CliError::Usage);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let config = read_config(&paths).ok();
    let campaigns = read_background_tick_campaigns(&paths, &options.campaign_ids)?;
    let external_workloads = parse_background_tick_external_workloads(
        &options.external_workloads,
        &options.external_workload_estimates,
        &options.external_human_checkpoints,
    )?;
    let resources = parse_background_tick_resources(&options.resources, &campaigns)?;
    let task_estimates = parse_background_tick_estimates(&options.estimates, &campaigns)?;
    let (opportunity_events, opportunity_detection_policy, opportunity_planner_node_id) =
        read_background_tick_opportunity_inputs(&data_dir, &options)?;
    let previously_intaken_opportunity_event_ids =
        read_background_opportunity_intake_event_ids(&paths)?
            .into_iter()
            .collect();
    let now = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let mut plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: options.scheduler_service_id,
        campaigns,
        opportunity_events,
        previously_intaken_opportunity_event_ids,
        opportunity_detection_policy,
        opportunity_planner_node_id,
        external_workloads,
        resources,
        task_estimates,
        max_concurrent_campaigns: options.max_concurrent_campaigns,
        max_concurrent_workloads: options.max_concurrent_workloads,
        idle_window_open: options.idle_window_open,
        reset_window_open: options.reset_window_open,
        dry_run: options.dry_run,
        now,
    });
    attach_persisted_memory_to_background_plan(&paths, &mut plan)?;
    let background_paths = write_background_scheduling_plan(&paths, &plan)?;
    println_background_tick(&plan, &background_paths);
    if !plan.opportunity_events_skipped.is_empty() {
        println!(
            "opportunity events skipped as already intaken: {}",
            plan.opportunity_events_skipped.len()
        );
    }
    if options.dispatch_approved {
        for update in &plan.campaigns {
            if update.action != BackgroundSchedulingAction::DispatchIteration {
                continue;
            }
            let Some(iteration_plan) = &update.scheduled_iteration else {
                continue;
            };
            let campaign = read_campaign(&paths, &update.campaign.campaign_id)?;
            let (report, execution_update, bundle) = execute_campaign_iteration_plan(
                &data_dir,
                &paths,
                config.as_ref(),
                campaign,
                iteration_plan.clone(),
                IterationExecutionOverrides::default(),
            )?;
            println_report(&report);
            println!("campaign: {}", execution_update.campaign.campaign_id);
            println!("background iteration: {}", iteration_plan.iteration_id);
            println!("stage scorecard: {}", bundle.stage_scorecard_json.display());
            println!(
                "iteration result: {}",
                bundle.iteration_result_json.display()
            );
        }
    }
    Ok(())
}

pub(crate) fn evolve_decide(
    args: &[String],
    outcome: CandidateDecisionOutcome,
) -> Result<(), CliError> {
    let (paths, record) = resolve_job_from_args(args, true)?;
    ensure_stored_job_awaiting_approval(&record)?;
    let note = flag_value(args, "--note").unwrap_or_default();
    let override_preflight = has_flag(args, "--override");
    if outcome == CandidateDecisionOutcome::Approved && override_preflight && note.trim().is_empty()
    {
        return Err(CliError::Parse {
            detail: "`sinomo evolve approve --override` requires a non-empty --note".to_string(),
        });
    }
    let recorded = if outcome == CandidateDecisionOutcome::Approved {
        decide_stored_evolution_job_with_options(
            &paths,
            record,
            outcome,
            "cli-supervisor",
            note,
            CandidateDecisionOptions { override_preflight },
        )?
    } else {
        decide_stored_evolution_job(&paths, record, outcome, "cli-supervisor", note)?
    };
    println!("job: {}", recorded.record.job.job_id);
    println!("status: {}", job_status_label(&recorded.record.job.status));
    println!(
        "worktree: {}",
        recorded.record.candidate_workspace.path.display()
    );
    println!("bundle: {}", recorded.record.bundle.bundle_path);
    println!("decision: {}", recorded.decision_path.display());
    Ok(())
}

pub(crate) fn evolve_status(args: &[String]) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    if let Some(job_id) = job_flag(args)? {
        let record = read_stored_job(&paths.job_path(&job_id))?;
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

pub(crate) fn evolve_show(args: &[String]) -> Result<(), CliError> {
    let (_paths, record) = resolve_job_from_args(args, false)?;
    let artifact = if has_flag(args, "--diff") {
        StoredJobArtifactKind::Diff
    } else if has_flag(args, "--scorecard") {
        StoredJobArtifactKind::Scorecard
    } else if has_flag(args, "--decision") {
        StoredJobArtifactKind::Decision
    } else if has_flag(args, "--packet") {
        StoredJobArtifactKind::MutationPacket
    } else if has_flag(args, "--regression-evidence") {
        StoredJobArtifactKind::RegressionEvidence
    } else if has_flag(args, "--lesson") {
        StoredJobArtifactKind::EvolutionLesson
    } else if has_flag(args, "--evidence-summary") {
        StoredJobArtifactKind::EvidenceSummary
    } else {
        StoredJobArtifactKind::Diff
    };
    let content = read_stored_job_artifact(&record, artifact)?;
    println!("{content}");
    Ok(())
}

pub(crate) fn evolve_show_packet(args: &[String]) -> Result<(), CliError> {
    let mut show_args = args.to_vec();
    show_args.push("--packet".to_string());
    evolve_show(&show_args)
}

pub(crate) fn evolve_verify(args: &[String]) -> Result<(), CliError> {
    let (paths, record) = resolve_job_from_args(args, false)?;
    let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());
    let recorded = verify_stored_evolution_job(&paths, record, &harness)?;
    println!("job: {}", recorded.record.job.job_id);
    println!(
        "verification: {}",
        outcome_label(
            recorded
                .record
                .verification_outcome
                .as_ref()
                .expect("recorded verification outcome")
        )
    );
    println!("scorecard: {}", recorded.scorecard_path.display());
    Ok(())
}

pub(crate) fn evolve_cleanup(args: &[String]) -> Result<(), CliError> {
    let (paths, record) = resolve_job_from_args(args, false)?;
    let summary = cleanup_stored_evolution_job(&paths, &record).map_err(EvolutionError::from)?;
    println!("removed job: {}", summary.job_id);
    println!("removed worktree: {}", summary.worktree_path.display());
    Ok(())
}

pub(crate) fn evolve_job(args: &[String]) -> Result<(), CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "create" => {
            let mut propose_args = rest.to_vec();
            propose_args.push("--dry-run".to_string());
            evolve_propose(&propose_args)
        }
        _ => Err(CliError::Usage),
    }
}
