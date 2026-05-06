use super::*;

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
    Models(ModelsCliCommand),
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
        Some((command, rest)) if command == "models" => {
            Ok(EvolveCliCommand::Models(parse_models_command(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProposeOptions {
    pub(crate) objective: String,
    pub(crate) provider: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) repo: Option<PathBuf>,
    pub(crate) allow: Vec<String>,
    pub(crate) verify: Vec<String>,
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
    pub(crate) intended_paths: Vec<String>,
    pub(crate) verification_commands: Vec<String>,
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

pub(crate) fn parse_failure_class(value: &str) -> Result<EvolutionFailureClass, CliError> {
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

pub(crate) fn parse_stage_resource_usage(
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

pub(crate) fn parse_iteration_options(args: &[String]) -> Result<IterationOptions, CliError> {
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

pub(crate) fn parse_schedule_resources(
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

pub(crate) fn parse_background_tick_resources(
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

pub(crate) fn parse_schedule_resource(
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

pub(crate) fn parse_schedule_estimates(
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

pub(crate) fn parse_background_tick_estimates(
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

pub(crate) fn parse_schedule_estimate(
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
pub(crate) struct CliMutationModel {
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) data_dir: SinomoDataDir,
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

pub(crate) fn model_or_default(model: &str, default_model: String) -> String {
    if model == PROVIDER_DEFAULT_MODEL {
        default_model
    } else {
        model.to_string()
    }
}

pub(crate) fn codex_provider_for_model(model: &str) -> CodexProvider {
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

pub(crate) fn resolve_job_from_args(
    args: &[String],
    waiting_only: bool,
) -> Result<(EvolutionPaths, StoredEvolutionJob), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    if let Some(job_id) = job_flag(args)? {
        let record = read_stored_job(&paths.job_path(&job_id))?;
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

pub(crate) fn ensure_awaiting_approval(record: &StoredEvolutionJob) -> Result<(), CliError> {
    if record.job.status == EvolutionJobStatus::AwaitingApproval {
        return Ok(());
    }
    Err(CliError::InvalidJobState {
        job_id: record.job.job_id.to_string(),
        status: job_status_label(&record.job.status).to_string(),
        expected: "awaiting-approval".to_string(),
    })
}

pub(crate) fn read_model_config(paths: &ModelPaths) -> Result<ModelConfig, CliError> {
    let content = fs::read_to_string(&paths.config).map_err(|source| CliError::Io {
        path: paths.config.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: paths.config.display().to_string(),
        detail: source.to_string(),
    })
}

pub(crate) fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), CliError> {
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

pub(crate) fn parse_background_tick_external_workloads(
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

pub(crate) fn ensure_external_workload<'a>(
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

pub(crate) fn parse_external_workload_spec(
    value: &str,
) -> Result<ClusterLongHorizonWorkload, CliError> {
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

pub(crate) fn parse_external_workload_estimate(
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

pub(crate) fn parse_external_workload_key(value: &str) -> Result<(String, String), CliError> {
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

pub(crate) fn parse_external_workload_status(
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

pub(crate) fn external_workload_key(workload: &ClusterLongHorizonWorkload) -> String {
    format!("{}:{}", workload.system, workload.workload_id)
}

pub(crate) fn parse_reserve_floors(values: &[String]) -> Result<Vec<ReserveFloor>, CliError> {
    values
        .iter()
        .map(|value| parse_reserve_floor(value))
        .collect()
}

pub(crate) fn parse_reserve_floor(value: &str) -> Result<ReserveFloor, CliError> {
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct ModelConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) default_model: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ModelPaths {
    pub(crate) config: PathBuf,
}

impl ModelPaths {
    pub(crate) fn new(data_dir: &SinomoDataDir) -> Self {
        Self {
            config: data_dir.configs_dir().join("models.json"),
        }
    }

    pub(crate) fn ensure(&self) -> Result<(), CliError> {
        let Some(parent) = self.config.parent() else {
            return Ok(());
        };
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })
    }
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
        EvolveCliCommand::Models(command) => run_models_command(command),
    }
}

pub(crate) fn evolve_propose(args: &[String]) -> Result<(), CliError> {
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

pub(crate) fn commit_and_fast_forward_approved_candidate(
    record: &StoredEvolutionJob,
) -> Result<(), CliError> {
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

pub(crate) fn run_git_command(cwd: &Path, args: &[&str]) -> Result<String, CliError> {
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

pub(crate) fn evolve_show_packet(args: &[String]) -> Result<(), CliError> {
    let mut show_args = args.to_vec();
    show_args.push("--packet".to_string());
    evolve_show(&show_args)
}

pub(crate) fn evolve_verify(args: &[String]) -> Result<(), CliError> {
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

pub(crate) fn evolve_cleanup(args: &[String]) -> Result<(), CliError> {
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
