use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ModelDefaultOptions {
    pub(crate) provider: String,
    pub(crate) model: String,
}

#[derive(Debug, Clone)]
pub(crate) enum ModelsCliCommand {
    Help,
    List(ModelListOptions),
    Default(ModelDefaultOptions),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ModelListOptions {
    pub(crate) provider: Option<String>,
}

pub(crate) fn parse_models_command(args: &[String]) -> Result<ModelsCliCommand, CliError> {
    match args.split_first() {
        None => Ok(ModelsCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(ModelsCliCommand::Help)
        }
        Some((command, rest)) if command == "list" => {
            Ok(ModelsCliCommand::List(parse_models_list_options(rest)?))
        }
        Some((command, rest)) if command == "default" => Ok(ModelsCliCommand::Default(
            parse_model_default_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_models_list_options(args: &[String]) -> Result<ModelListOptions, CliError> {
    match args {
        [] => Ok(ModelListOptions { provider: None }),
        [flag, provider] if flag == "--provider" => Ok(ModelListOptions {
            provider: Some(provider.clone()),
        }),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_model_default_options(
    args: &[String],
) -> Result<ModelDefaultOptions, CliError> {
    let provider = flag_value(args, "--provider").ok_or(CliError::Usage)?;
    let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
        detail: format!("unknown provider `{provider}`"),
    })?;
    let model = flag_value(args, "--model").ok_or(CliError::Usage)?;
    let model = normalize_model_id_for_provider(&provider, &model);
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" | "--model" => index += 2,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(ModelDefaultOptions { provider, model })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UsageOptions {
    pub(crate) provider: Option<String>,
    pub(crate) json: bool,
    pub(crate) help: bool,
}

pub(crate) fn parse_usage_options(args: &[String]) -> Result<UsageOptions, CliError> {
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

pub(crate) fn current_default_model() -> Result<CurrentDefaultModel, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    resolve_current_default_model(&data_dir).map_err(CliError::LanguageModel)
}

pub(crate) fn run_models_command(command: ModelsCliCommand) -> Result<(), CliError> {
    match command {
        ModelsCliCommand::Help => models_help(),
        ModelsCliCommand::List(options) => models_list(options),
        ModelsCliCommand::Default(options) => models_default(options),
    }
}

pub(crate) fn models_help() -> Result<(), CliError> {
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

pub(crate) fn models_list(options: ModelListOptions) -> Result<(), CliError> {
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
    let catalogs = collect_provider_model_catalogs(providers, explicit_provider)
        .map_err(CliError::LanguageModel)?;
    for catalog in catalogs {
        println!("{}:", catalog.provider);
        for model in catalog.models {
            println!("  {model}");
        }
    }
    Ok(())
}

pub(crate) fn models_default(options: ModelDefaultOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    models_default_with_data_dir(options, &data_dir)
}

pub(crate) fn models_default_with_data_dir(
    options: ModelDefaultOptions,
    data_dir: &SinomoDataDir,
) -> Result<(), CliError> {
    let update = write_default_model_config(data_dir, options.provider, options.model)
        .map_err(CliError::LanguageModel)?;

    println!(
        "default provider: {}",
        update.config.default_provider.as_deref().unwrap_or("")
    );
    println!(
        "default model: {}",
        update.config.default_model.as_deref().unwrap_or("")
    );
    println!("config: {}", update.config_path.display());
    Ok(())
}

pub(crate) fn usage(options: UsageOptions) -> Result<(), CliError> {
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

    let snapshots =
        collect_provider_usage(providers, explicit_provider).map_err(CliError::LanguageModel)?;

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

pub(crate) fn print_provider_usage(snapshot: &LanguageModelProviderUsage) {
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

pub(crate) fn format_usage_reset_at(reset_at_unix: i64) -> String {
    OffsetDateTime::from_unix_timestamp(reset_at_unix)
        .ok()
        .and_then(|datetime| datetime.format(&Rfc3339).ok())
        .unwrap_or_else(|| reset_at_unix.to_string())
}

pub(crate) fn println_stage_evaluation(
    update: &StageEvaluationUpdate,
    bundle: &CampaignBundlePaths,
) {
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

pub(crate) fn println_background_tick(
    plan: &BackgroundSchedulingPlan,
    paths: &BackgroundSchedulerPaths,
) {
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

pub(crate) fn println_report(report: &EvolutionReport) {
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

pub(crate) fn print_job_status(record: &StoredEvolutionJob) {
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

pub(crate) fn job_flag(args: &[String]) -> Result<Option<EvolutionJobId>, CliError> {
    flag_value(args, "--job")
        .map(EvolutionJobId::new)
        .transpose()
        .map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })
}

pub(crate) fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
}

pub(crate) fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

pub(crate) fn parse_trust_ring(value: &str) -> Result<TrustRing, CliError> {
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

pub(crate) fn outcome_label(outcome: &EvaluationOutcome) -> &'static str {
    match outcome {
        EvaluationOutcome::Passed => "passed",
        EvaluationOutcome::Failed => "failed",
        EvaluationOutcome::Inconclusive => "inconclusive",
    }
}
