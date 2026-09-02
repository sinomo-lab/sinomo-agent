use crate::{
    CandidateDecisionOutcome, EvolutionFailureClass, EvolutionJobId, ResourceUsageEstimate,
    parse_failure_class, parse_stage_resource_usage,
};
use sinomo_runtime::cli_registry::CliParseError;
use sinomo_runtime::core::TrustRing;
use sinomo_runtime::node::NodeId;
use std::path::PathBuf;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EvolveCliCommand {
    Help,
    Propose(Vec<String>),
    Schedule(Vec<String>),
    Iterate(Vec<String>),
    Evaluate(Vec<String>),
    Campaign(CampaignCliCommand),
    Tick(Vec<String>),
    Decide(CandidateDecisionOutcome, Vec<String>),
    Status(Vec<String>),
    Show(Vec<String>),
    Verify(Vec<String>),
    Packet(Vec<String>),
    Cleanup(Vec<String>),
    Job(Vec<String>),
    TrustedSigners(TrustedSignersCliCommand),
    Models(Vec<String>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CampaignCliCommand {
    Approve(CampaignDecisionCliOptions),
    Reject(CampaignDecisionCliOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CampaignDecisionCliOptions {
    pub campaign_id: String,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TrustedSignersCliCommand {
    Help,
    List(TrustedSignersListOptions),
    Publish,
    Hydrate,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct TrustedSignersListOptions {
    pub json: bool,
}

#[derive(Clone, Debug)]
pub struct ProposeOptions {
    pub objective: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub repo: Option<PathBuf>,
    pub allow: Vec<String>,
    pub verify: Vec<String>,
    pub benchmark: Vec<String>,
    pub module_replay_report: Vec<String>,
    pub target_ring: TrustRing,
    pub dry_run: bool,
    pub no_model: bool,
    pub horizon: Option<String>,
    pub budget_policy: Option<String>,
    pub reserve_floors: Vec<String>,
    pub approval_policy: Option<String>,
    pub max_wall_time: Option<String>,
    pub pause_after_proposal: bool,
    pub approve_campaign: bool,
}

#[derive(Clone, Debug)]
pub struct ScheduleOptions {
    pub campaign_id: String,
    pub resources: Vec<String>,
    pub estimates: Vec<String>,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct BackgroundTickOptions {
    pub scheduler_service_id: String,
    pub campaign_ids: Vec<String>,
    pub opportunity_event_files: Vec<PathBuf>,
    pub opportunity_event_log: Option<PathBuf>,
    pub opportunity_event_limit: usize,
    pub opportunity_policy: Option<PathBuf>,
    pub opportunity_planner_node_id: Option<NodeId>,
    pub external_workloads: Vec<String>,
    pub external_workload_estimates: Vec<String>,
    pub external_human_checkpoints: Vec<String>,
    pub resources: Vec<String>,
    pub estimates: Vec<String>,
    pub max_concurrent_campaigns: usize,
    pub max_concurrent_workloads: usize,
    pub idle_window_open: bool,
    pub reset_window_open: bool,
    pub dispatch_approved: bool,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct IterationOptions {
    pub campaign_id: String,
    pub task_id: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub repo: Option<PathBuf>,
    pub intended_paths: Vec<String>,
    pub verification_commands: Vec<String>,
    pub benchmark: Vec<String>,
    pub module_replay_report: Vec<String>,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct StageEvaluationOptions {
    pub campaign_id: String,
    pub job_id: EvolutionJobId,
    pub task_id: String,
    pub failure_class: Option<EvolutionFailureClass>,
    pub root_cause_summary: Option<String>,
    pub what_was_learned: Vec<String>,
    pub resource_usage: Vec<ResourceUsageEstimate>,
}

pub fn parse_evolve_command(args: &[String]) -> Result<EvolveCliCommand, CliParseError> {
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
        Some((command, rest)) if command == "campaign" => {
            Ok(EvolveCliCommand::Campaign(parse_campaign_command(rest)?))
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
        Some((command, rest)) if command == "models" => Ok(EvolveCliCommand::Models(rest.to_vec())),
        _ => Err(CliParseError::Usage),
    }
}

pub fn parse_campaign_command(args: &[String]) -> Result<CampaignCliCommand, CliParseError> {
    match args.split_first() {
        Some((command, rest)) if command == "approve" => Ok(CampaignCliCommand::Approve(
            parse_campaign_decision_options(rest)?,
        )),
        Some((command, rest)) if command == "reject" => Ok(CampaignCliCommand::Reject(
            parse_campaign_decision_options(rest)?,
        )),
        _ => Err(CliParseError::Usage),
    }
}

pub fn parse_campaign_decision_options(
    args: &[String],
) -> Result<CampaignDecisionCliOptions, CliParseError> {
    let mut campaign_id = None;
    let mut note = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--campaign" => {
                index += 1;
                campaign_id = args.get(index).cloned();
                if campaign_id.is_none() {
                    return Err(CliParseError::Usage);
                }
            }
            "--note" => {
                index += 1;
                note = args.get(index).cloned();
                if note.is_none() {
                    return Err(CliParseError::Usage);
                }
            }
            value if value.starts_with("--") => return Err(CliParseError::Usage),
            value => {
                if campaign_id.is_some() {
                    return Err(CliParseError::Usage);
                }
                campaign_id = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(CampaignDecisionCliOptions {
        campaign_id: campaign_id.ok_or(CliParseError::Usage)?,
        note,
    })
}

pub fn parse_trusted_signers_command(
    args: &[String],
) -> Result<TrustedSignersCliCommand, CliParseError> {
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
        _ => Err(CliParseError::Usage),
    }
}

pub fn parse_trusted_signers_list_options(
    args: &[String],
) -> Result<TrustedSignersListOptions, CliParseError> {
    let mut options = TrustedSignersListOptions::default();
    for arg in args {
        match arg.as_str() {
            "--json" => options.json = true,
            _ => return Err(CliParseError::Usage),
        }
    }
    Ok(options)
}

pub fn parse_propose_options(args: &[String]) -> Result<ProposeOptions, CliParseError> {
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
    let mut no_model = false;
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
            "--dry-run" => {
                dry_run = true;
            }
            "--no-model" => {
                no_model = true;
            }
            value if value.starts_with("--") => return Err(CliParseError::Usage),
            value => {
                objective = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(ProposeOptions {
        objective: objective.ok_or(CliParseError::Usage)?,
        provider,
        model,
        repo,
        allow,
        verify,
        benchmark,
        module_replay_report,
        target_ring,
        dry_run,
        no_model,
        horizon,
        budget_policy,
        reserve_floors,
        approval_policy,
        max_wall_time,
        pause_after_proposal,
        approve_campaign,
    })
}

pub fn parse_stage_evaluation_options(
    args: &[String],
) -> Result<StageEvaluationOptions, CliParseError> {
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
                    return Err(CliParseError::Usage);
                };
                job_id = Some(value.parse::<EvolutionJobId>().map_err(|source| {
                    CliParseError::Parse {
                        detail: format!("invalid evolution job id `{value}`: {source}"),
                    }
                })?);
            }
            "--task" => {
                index += 1;
                task_id = args.get(index).cloned();
                if task_id.is_none() {
                    return Err(CliParseError::Usage);
                }
            }
            "--failure-class" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                failure_class = Some(parse_failure_class(value).map_err(cli_parse_error)?);
            }
            "--root-cause" => {
                index += 1;
                root_cause_summary = args.get(index).cloned();
                if root_cause_summary.is_none() {
                    return Err(CliParseError::Usage);
                }
            }
            "--learned" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    what_was_learned.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--usage" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    resource_usage.push(
                        parse_stage_resource_usage(value, task_id.as_deref())
                            .map_err(cli_parse_error)?,
                    );
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            value if value.starts_with("--") => return Err(CliParseError::Usage),
            value => {
                if campaign_id.is_some() {
                    return Err(CliParseError::Usage);
                }
                campaign_id = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(StageEvaluationOptions {
        campaign_id: campaign_id.ok_or(CliParseError::Usage)?,
        job_id: job_id.ok_or(CliParseError::Usage)?,
        task_id: task_id.ok_or(CliParseError::Usage)?,
        failure_class,
        root_cause_summary,
        what_was_learned,
        resource_usage,
    })
}

pub fn parse_iteration_options(args: &[String]) -> Result<IterationOptions, CliParseError> {
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
                    return Err(CliParseError::Usage);
                }
            }
            "--task" => {
                index += 1;
                task_id = args.get(index).cloned();
                if task_id.is_none() {
                    return Err(CliParseError::Usage);
                }
            }
            "--provider" => {
                index += 1;
                provider = args.get(index).cloned();
                if provider.is_none() {
                    return Err(CliParseError::Usage);
                }
            }
            "--model" => {
                index += 1;
                model = args.get(index).cloned();
                if model.is_none() {
                    return Err(CliParseError::Usage);
                }
            }
            "--repo" => {
                index += 1;
                repo = args.get(index).map(PathBuf::from);
                if repo.is_none() {
                    return Err(CliParseError::Usage);
                }
            }
            "--path" | "--allow" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    intended_paths.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--verify" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    verification_commands.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--benchmark" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    benchmark.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--module-replay-report" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    module_replay_report.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--dry-run" | "--preview" => {
                dry_run = true;
            }
            value if value.starts_with("--") => return Err(CliParseError::Usage),
            value => {
                if campaign_id.is_some() {
                    return Err(CliParseError::Usage);
                }
                campaign_id = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(IterationOptions {
        campaign_id: campaign_id.ok_or(CliParseError::Usage)?,
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

pub fn parse_background_tick_options(
    args: &[String],
) -> Result<BackgroundTickOptions, CliParseError> {
    let mut scheduler_service_id = "sinomo-cluster-scheduler".to_string();
    let mut campaign_ids = Vec::new();
    let mut opportunity_event_files = Vec::new();
    let mut opportunity_event_log = None;
    let mut opportunity_event_limit = 200usize;
    let mut opportunity_policy = None;
    let mut opportunity_planner_node_id = None;
    let mut external_workloads = Vec::new();
    let mut external_workload_estimates = Vec::new();
    let mut external_human_checkpoints = Vec::new();
    let mut resources = Vec::new();
    let mut estimates = Vec::new();
    let mut max_concurrent_campaigns = 1usize;
    let mut max_concurrent_workloads = 1usize;
    let mut idle_window_open = false;
    let mut reset_window_open = false;
    let mut dispatch_approved = false;
    let mut dry_run = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--cluster" | "--cluster-id" => {
                return Err(CliParseError::Parse {
                    detail: "cluster identifiers are not supported; scheduler operations always target the current cluster".to_string(),
                });
            }
            "--scheduler-service" | "--scheduler-service-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliParseError::Parse {
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
                    return Err(CliParseError::Usage);
                }
            }
            "--opportunity-events" | "--opportunity-event-file" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    opportunity_event_files.push(PathBuf::from(value));
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--opportunity-event-log" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    opportunity_event_log = Some(PathBuf::from(value));
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--opportunity-event-limit" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                opportunity_event_limit =
                    value
                        .parse::<usize>()
                        .map_err(|source| CliParseError::Parse {
                            detail: format!("invalid opportunity event limit `{value}`: {source}"),
                        })?;
                if opportunity_event_limit == 0 {
                    return Err(CliParseError::Parse {
                        detail: "opportunity event limit must be at least 1".to_string(),
                    });
                }
            }
            "--opportunity-policy" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    opportunity_policy = Some(PathBuf::from(value));
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--opportunity-planner-node" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                opportunity_planner_node_id = Some(parse_node_id_cli(value)?);
            }
            "--workload" | "--external-workload" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    external_workloads.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--workload-estimate" | "--external-estimate" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    external_workload_estimates.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--workload-human-checkpoint" | "--external-human-checkpoint" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    external_human_checkpoints.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--resource" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    resources.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--estimate" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    estimates.push(value.clone());
                } else {
                    return Err(CliParseError::Usage);
                }
            }
            "--max-concurrent" | "--max-concurrent-campaigns" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                max_concurrent_campaigns =
                    value
                        .parse::<usize>()
                        .map_err(|source| CliParseError::Parse {
                            detail: format!(
                                "invalid max concurrent campaign count `{value}`: {source}"
                            ),
                        })?;
                if max_concurrent_campaigns == 0 {
                    return Err(CliParseError::Parse {
                        detail: "max concurrent campaign count must be at least 1".to_string(),
                    });
                }
                max_concurrent_workloads = max_concurrent_workloads.max(max_concurrent_campaigns);
            }
            "--max-concurrent-workloads" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                max_concurrent_workloads =
                    value
                        .parse::<usize>()
                        .map_err(|source| CliParseError::Parse {
                            detail: format!(
                                "invalid max concurrent workload count `{value}`: {source}"
                            ),
                        })?;
                if max_concurrent_workloads == 0 {
                    return Err(CliParseError::Parse {
                        detail: "max concurrent workload count must be at least 1".to_string(),
                    });
                }
            }
            "--idle-window-open" => idle_window_open = true,
            "--reset-window-open" => reset_window_open = true,
            "--dispatch-approved" => dispatch_approved = true,
            "--dry-run" | "--preview" => dry_run = true,
            value if value.starts_with("--") => return Err(CliParseError::Usage),
            value => campaign_ids.push(value.to_string()),
        }
        index += 1;
    }
    if dry_run && dispatch_approved {
        return Err(CliParseError::Parse {
            detail: "--dry-run and --dispatch-approved are mutually exclusive".to_string(),
        });
    }
    Ok(BackgroundTickOptions {
        scheduler_service_id,
        campaign_ids,
        opportunity_event_files,
        opportunity_event_log,
        opportunity_event_limit,
        opportunity_policy,
        opportunity_planner_node_id,
        external_workloads,
        external_workload_estimates,
        external_human_checkpoints,
        resources,
        estimates,
        max_concurrent_campaigns,
        max_concurrent_workloads,
        idle_window_open,
        reset_window_open,
        dispatch_approved,
        dry_run,
    })
}

pub fn parse_schedule_options(args: &[String]) -> Result<ScheduleOptions, CliParseError> {
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
            value if value.starts_with("--") => return Err(CliParseError::Usage),
            value => {
                if campaign_id.is_some() {
                    return Err(CliParseError::Usage);
                }
                campaign_id = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(ScheduleOptions {
        campaign_id: campaign_id.ok_or(CliParseError::Usage)?,
        resources,
        estimates,
        dry_run,
    })
}

fn parse_node_id_cli(value: &str) -> Result<NodeId, CliParseError> {
    value.parse::<NodeId>().or_else(|parse_error| {
        NodeId::from_legacy_named(value).map_err(|legacy_error| CliParseError::Parse {
            detail: format!(
                "invalid opportunity planner node id `{value}`: {parse_error}; legacy name fallback failed: {legacy_error}"
            ),
        })
    })
}

fn parse_trust_ring(value: &str) -> Result<TrustRing, CliParseError> {
    match value {
        "r0" | "r0-continuity-root" => Ok(TrustRing::R0ContinuityRoot),
        "r1" | "r1-trusted-runtime" => Ok(TrustRing::R1TrustedRuntime),
        "r2" | "r2-trusted-service" => Ok(TrustRing::R2TrustedService),
        "r3" | "r3-sandboxed-module" => Ok(TrustRing::R3SandboxedModule),
        "r4" | "r4-disposable-workspace" => Ok(TrustRing::R4DisposableWorkspace),
        "r5" | "r5-model-provider" => Ok(TrustRing::R5ModelProvider),
        _ => Err(CliParseError::Parse {
            detail: format!("unknown trust ring `{value}`"),
        }),
    }
}

fn cli_parse_error(error: impl std::fmt::Display) -> CliParseError {
    CliParseError::Parse {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_routes_evolve_commands() {
        assert!(matches!(
            parse_evolve_command(&["propose".to_string(), "fix".to_string()]),
            Ok(EvolveCliCommand::Propose(args)) if args == vec!["fix"]
        ));
        assert!(matches!(
            parse_evolve_command(&["run-background".to_string()]),
            Ok(EvolveCliCommand::Tick(args)) if args.is_empty()
        ));
        assert!(matches!(
            parse_evolve_command(&["restore".to_string(), "--job".to_string(), "job".to_string()]),
            Ok(EvolveCliCommand::Decide(CandidateDecisionOutcome::Restored, args))
                if args == vec!["--job", "job"]
        ));
        assert!(matches!(
            parse_evolve_command(&["models".to_string(), "list".to_string()]),
            Ok(EvolveCliCommand::Models(args)) if args == vec!["list"]
        ));
    }

    #[test]
    fn parser_routes_campaign_and_trusted_signer_commands() {
        let campaign = parse_evolve_command(&[
            "campaign".to_string(),
            "approve".to_string(),
            "camp_demo".to_string(),
            "--note".to_string(),
            "reviewed".to_string(),
        ])
        .expect("campaign");
        assert!(matches!(
            campaign,
            EvolveCliCommand::Campaign(CampaignCliCommand::Approve(
                CampaignDecisionCliOptions {
                    campaign_id,
                    note: Some(note),
                },
            )) if campaign_id == "camp_demo" && note == "reviewed"
        ));

        let signers = parse_evolve_command(&["trust-signers".to_string(), "push".to_string()])
            .expect("signers");
        assert!(matches!(
            signers,
            EvolveCliCommand::TrustedSigners(TrustedSignersCliCommand::Publish)
        ));
    }
}
