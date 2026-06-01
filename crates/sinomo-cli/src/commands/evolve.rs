use super::*;
use serde::Serialize;

#[derive(Debug, Clone)]
pub(crate) enum EvolveCliCommand {
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
    TrustedSigners(TrustedSignersCliCommand),
    Models(ModelsCliCommand),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum TrustedSignersCliCommand {
    Help,
    List(TrustedSignersListOptions),
    Publish,
    Hydrate,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub(crate) struct TrustedSignersListOptions {
    pub(crate) json: bool,
}

pub(crate) fn parse_evolve_command(args: &[String]) -> Result<EvolveCliCommand, CliError> {
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
        Some((command, rest))
            if matches!(command.as_str(), "trusted-signers" | "trust-signers") =>
        {
            Ok(EvolveCliCommand::TrustedSigners(
                parse_trusted_signers_command(rest)?,
            ))
        }
        Some((command, rest)) if command == "models" => {
            Ok(EvolveCliCommand::Models(parse_models_command(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_trusted_signers_command(
    args: &[String],
) -> Result<TrustedSignersCliCommand, CliError> {
    match args.split_first() {
        None => Ok(TrustedSignersCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(TrustedSignersCliCommand::Help)
        }
        Some((command, rest)) if command == "list" => Ok(TrustedSignersCliCommand::List(
            parse_trusted_signers_list_options(rest)?,
        )),
        Some((command, [])) if matches!(command.as_str(), "publish" | "push" | "sync") => {
            Ok(TrustedSignersCliCommand::Publish)
        }
        Some((command, [])) if matches!(command.as_str(), "hydrate" | "pull") => {
            Ok(TrustedSignersCliCommand::Hydrate)
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_trusted_signers_list_options(
    args: &[String],
) -> Result<TrustedSignersListOptions, CliError> {
    let mut options = TrustedSignersListOptions::default();
    for arg in args {
        match arg.as_str() {
            "--json" => options.json = true,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(options)
}

#[derive(Clone, Debug)]
pub(crate) struct ProposeOptions {
    pub(crate) objective: String,
    pub(crate) provider: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) repo: Option<PathBuf>,
    pub(crate) allow: Vec<String>,
    pub(crate) verify: Vec<String>,
    pub(crate) benchmark: Vec<String>,
    pub(crate) module_replay_report: Vec<String>,
    pub(crate) target_ring: TrustRing,
    pub(crate) dry_run: bool,
    pub(crate) horizon: Option<String>,
    pub(crate) budget_policy: Option<String>,
    pub(crate) reserve_floors: Vec<String>,
    pub(crate) approval_policy: Option<String>,
    pub(crate) max_wall_time: Option<String>,
    pub(crate) pause_after_proposal: bool,
    pub(crate) approve_campaign: bool,
}

pub(crate) fn parse_propose_options(args: &[String]) -> Result<ProposeOptions, CliError> {
    let mut objective = None;
    let mut provider = None;
    let mut model = None;
    let mut repo = None;
    let mut allow = Vec::new();
    let mut verify = Vec::new();
    let mut benchmark = Vec::new();
    let mut module_replay_report = Vec::new();
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
            "--benchmark" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    benchmark.push(value.clone());
                }
            }
            "--module-replay-report" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    module_replay_report.push(value.clone());
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
        benchmark,
        module_replay_report,
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
pub(crate) struct ScheduleOptions {
    pub(crate) campaign_id: String,
    pub(crate) resources: Vec<String>,
    pub(crate) estimates: Vec<String>,
    pub(crate) dry_run: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct BackgroundTickOptions {
    pub(crate) scheduler_service_id: String,
    pub(crate) campaign_ids: Vec<String>,
    pub(crate) external_workloads: Vec<String>,
    pub(crate) external_workload_estimates: Vec<String>,
    pub(crate) external_human_checkpoints: Vec<String>,
    pub(crate) resources: Vec<String>,
    pub(crate) estimates: Vec<String>,
    pub(crate) max_concurrent_campaigns: usize,
    pub(crate) max_concurrent_workloads: usize,
    pub(crate) idle_window_open: bool,
    pub(crate) reset_window_open: bool,
    pub(crate) dry_run: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct IterationOptions {
    pub(crate) campaign_id: String,
    pub(crate) task_id: Option<String>,
    pub(crate) provider: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) repo: Option<PathBuf>,
    pub(crate) intended_paths: Vec<String>,
    pub(crate) verification_commands: Vec<String>,
    pub(crate) benchmark: Vec<String>,
    pub(crate) module_replay_report: Vec<String>,
    pub(crate) dry_run: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct StageEvaluationOptions {
    pub(crate) campaign_id: String,
    pub(crate) job_id: EvolutionJobId,
    pub(crate) task_id: String,
    pub(crate) failure_class: Option<EvolutionFailureClass>,
    pub(crate) root_cause_summary: Option<String>,
    pub(crate) what_was_learned: Vec<String>,
    pub(crate) resource_usage: Vec<ResourceUsageEstimate>,
}

pub(crate) fn parse_stage_evaluation_options(
    args: &[String],
) -> Result<StageEvaluationOptions, CliError> {
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

pub(crate) fn parse_iteration_options(args: &[String]) -> Result<IterationOptions, CliError> {
    let mut campaign_id = None;
    let mut task_id = None;
    let mut provider = None;
    let mut model = None;
    let mut repo = None;
    let mut intended_paths = Vec::new();
    let mut verification_commands = Vec::new();
    let mut benchmark = Vec::new();
    let mut module_replay_report = Vec::new();
    let mut dry_run = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--campaign" => {
                index += 1;
                campaign_id = args.get(index).cloned();
                if campaign_id.is_none() {
                    return Err(CliError::Usage);
                }
            }
            "--task" => {
                index += 1;
                task_id = args.get(index).cloned();
                if task_id.is_none() {
                    return Err(CliError::Usage);
                }
            }
            "--provider" => {
                index += 1;
                provider = args.get(index).cloned();
                if provider.is_none() {
                    return Err(CliError::Usage);
                }
            }
            "--model" => {
                index += 1;
                model = args.get(index).cloned();
                if model.is_none() {
                    return Err(CliError::Usage);
                }
            }
            "--repo" => {
                index += 1;
                repo = args.get(index).map(PathBuf::from);
                if repo.is_none() {
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
            "--benchmark" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    benchmark.push(value.clone());
                } else {
                    return Err(CliError::Usage);
                }
            }
            "--module-replay-report" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    module_replay_report.push(value.clone());
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
        provider,
        model,
        repo,
        intended_paths,
        verification_commands,
        benchmark,
        module_replay_report,
        dry_run,
    })
}

pub(crate) fn parse_background_tick_options(
    args: &[String],
) -> Result<BackgroundTickOptions, CliError> {
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

pub(crate) fn parse_schedule_options(args: &[String]) -> Result<ScheduleOptions, CliError> {
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

#[cfg(test)]
pub(crate) fn run_evolve_command(args: &[String]) -> Result<(), CliError> {
    run_evolve_cli_command(parse_evolve_command(args)?)
}

pub(crate) fn run_evolve_cli_command(command: EvolveCliCommand) -> Result<(), CliError> {
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
        EvolveCliCommand::TrustedSigners(command) => evolve_trusted_signers(command),
        EvolveCliCommand::Models(command) => run_models_command(command),
    }
}

#[derive(Clone, Debug, Serialize)]
struct TrustedSignersListView {
    source: String,
    count: usize,
    signers: Vec<sinomo_runtime::registry::TrustedArtifactSigner>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TrustedSignersSyncReport {
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) count: usize,
}

fn evolve_trusted_signers(command: TrustedSignersCliCommand) -> Result<(), CliError> {
    match command {
        TrustedSignersCliCommand::Help => {
            print_evolve_help();
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

pub(crate) fn registry_trust_uses_cluster(data_dir: &SinomoDataDir) -> bool {
    if cli_force_local_mode() {
        return false;
    }
    network::read_cluster_state(&ClusterPaths::new(data_dir)).is_ok()
}

pub(crate) fn read_trusted_signers_for_node(
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

pub(crate) fn publish_trusted_signers_to_cluster(
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

pub(crate) fn hydrate_trusted_signers_from_cluster(
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
    let store = InProcessKvStore::restore(snapshot);
    Ok(store.range_prefix(sinomo_runtime::registry::CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX))
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
        return network::read_cluster_kv_snapshot(paths).map_err(Into::into);
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
    let daemon_paths = DaemonPaths::new(data_dir);
    daemon_handle_daemon_cluster_kv_txn_request(&daemon_paths, txn)?;
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
    let request = network::ObjectRpcRequest::encode_body(
        network::CLUSTER_KV_TXN_ROUTE,
        network::CLUSTER_KV_RPC_CATEGORY,
        &txn,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &request,
        DAEMON_CLUSTER_KV_TXN_TIMEOUT,
    )?;
    let _: KvTxnResponse = response.decode_body().map_err(CliError::from)?;
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
        dry_run: options.dry_run,
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
    let report = if request.dry_run {
        orchestrator.propose(request, None)?
    } else {
        orchestrator.propose(request, Some(&model_runner))?
    };

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
    let mut plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign: campaign.clone(),
        task_id: options.task_id.clone(),
        intended_paths: options.intended_paths.clone(),
        extra_verification_commands: options.verification_commands.clone(),
        dry_run: options.dry_run,
    })
    .map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })?;
    if options.dry_run {
        let bundle = write_campaign_iteration_dry_run(&paths, &mut campaign, &plan)?;
        println_iteration_dry_run(&plan, &bundle);
        return Ok(());
    }

    let (model_config, _) = load_model_config_for_node(&data_dir)?;
    let evolution_model_config = config.as_ref().map(|config| sinomo_lm::ModelConfig {
        default_provider: config.default_provider.clone(),
        default_account: None,
        default_model: config.default_model.clone(),
        accounts: Vec::new(),
    });
    let default_model =
        resolve_default_model(Some(&model_config), evolution_model_config.as_ref())?;
    let (provider, model) = resolve_requested_evolution_model(
        options.provider.clone(),
        options.model.clone(),
        default_model.provider,
        default_model.configured_model,
    )?;
    let repo = resolve_evolution_repo(
        &data_dir,
        options
            .repo
            .clone()
            .or_else(|| campaign.source_repo.as_ref().map(PathBuf::from))
            .or_else(|| config.as_ref().map(|config| PathBuf::from(&config.repo))),
    )?;

    plan.evolution_request.model_provider = Some(provider.clone());
    plan.evolution_request.model = Some(model.clone());
    plan.evolution_request.benchmark_gates =
        parse_benchmark_gate_specs(&options.benchmark, &provider, &model)?;
    plan.evolution_request.module_replay_reports =
        parse_module_replay_report_specs(&options.module_replay_report)?;

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
    let report = orchestrator.propose(plan.evolution_request.clone(), Some(&model_runner))?;
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
    println_report(&report);
    println!("campaign: {}", update.campaign.campaign_id);
    println!("iteration: {}", plan.iteration_id);
    println!("task: {}", plan.task.task_id);
    println!("stage scorecard: {}", bundle.stage_scorecard_json.display());
    println!(
        "iteration result: {}",
        bundle.iteration_result_json.display()
    );
    if let Some(resource_usage_path) = &report.bundle.resource_usage_path {
        println!("resource usage: {resource_usage_path}");
    }
    Ok(())
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

pub(crate) fn evolve_tick(args: &[String]) -> Result<(), CliError> {
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
    let summary = sinomo_evolution::cleanup_stored_evolution_job(&paths, &record)
        .map_err(sinomo_evolution::EvolutionError::from)?;
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
