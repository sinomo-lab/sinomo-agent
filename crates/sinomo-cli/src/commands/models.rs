use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ModelDefaultOptions {
    pub(crate) provider: String,
    pub(crate) account: String,
    pub(crate) model: String,
}

#[derive(Debug, Clone)]
pub(crate) enum ModelsCliCommand {
    Help,
    List(ModelListOptions),
    Default(ModelDefaultOptions),
    Runtime(ModelRuntimeCliCommand),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ModelListOptions {
    pub(crate) provider: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ModelRuntimeCliCommand {
    Show,
    Set(ModelRuntimeSetOptions),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ModelRuntimeSetOptions {
    pub(crate) native_models_cache_dir: Option<String>,
    pub(crate) minicpm_v46_model_dir: Option<String>,
    pub(crate) minicpm_v46_max_input_tokens: Option<usize>,
    pub(crate) minicpm_v46_max_output_tokens: Option<usize>,
    pub(crate) pytorch_endpoint: Option<String>,
    pub(crate) pytorch_python: Option<String>,
    pub(crate) pytorch_device: Option<String>,
    pub(crate) pytorch_timeout_seconds: Option<u64>,
    pub(crate) pytorch_mock: Option<bool>,
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
        Some((command, rest)) if command == "runtime" => Ok(ModelsCliCommand::Runtime(
            parse_models_runtime_command(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_models_runtime_command(
    args: &[String],
) -> Result<ModelRuntimeCliCommand, CliError> {
    match args.split_first() {
        None => Ok(ModelRuntimeCliCommand::Show),
        Some((command, [])) if matches!(command.as_str(), "show" | "help" | "--help" | "-h") => {
            Ok(ModelRuntimeCliCommand::Show)
        }
        Some((command, rest)) if command == "set" => Ok(ModelRuntimeCliCommand::Set(
            parse_model_runtime_set_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_model_runtime_set_options(
    args: &[String],
) -> Result<ModelRuntimeSetOptions, CliError> {
    let mut options = ModelRuntimeSetOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--native-models-cache-dir" => {
                options.native_models_cache_dir = Some(
                    args.get(index + 1)
                        .ok_or(CliError::Usage)?
                        .trim()
                        .to_string(),
                );
                index += 2;
            }
            "--minicpm-v46-model-dir" => {
                options.minicpm_v46_model_dir = Some(
                    args.get(index + 1)
                        .ok_or(CliError::Usage)?
                        .trim()
                        .to_string(),
                );
                index += 2;
            }
            "--minicpm-v46-max-input-tokens" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                options.minicpm_v46_max_input_tokens =
                    Some(value.parse().map_err(|source| CliError::Parse {
                        detail: format!(
                            "invalid --minicpm-v46-max-input-tokens `{value}`: {source}"
                        ),
                    })?);
                index += 2;
            }
            "--minicpm-v46-max-output-tokens" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                options.minicpm_v46_max_output_tokens =
                    Some(value.parse().map_err(|source| CliError::Parse {
                        detail: format!(
                            "invalid --minicpm-v46-max-output-tokens `{value}`: {source}"
                        ),
                    })?);
                index += 2;
            }
            "--pytorch-endpoint" => {
                options.pytorch_endpoint = Some(
                    args.get(index + 1)
                        .ok_or(CliError::Usage)?
                        .trim()
                        .to_string(),
                );
                index += 2;
            }
            "--pytorch-python" => {
                options.pytorch_python = Some(
                    args.get(index + 1)
                        .ok_or(CliError::Usage)?
                        .trim()
                        .to_string(),
                );
                index += 2;
            }
            "--pytorch-device" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                if !matches!(value.as_str(), "auto" | "cpu" | "cuda") {
                    return Err(CliError::Parse {
                        detail: format!("invalid --pytorch-device `{value}`"),
                    });
                }
                options.pytorch_device = Some(value.clone());
                index += 2;
            }
            "--pytorch-timeout-seconds" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                options.pytorch_timeout_seconds =
                    Some(value.parse().map_err(|source| CliError::Parse {
                        detail: format!("invalid --pytorch-timeout-seconds `{value}`: {source}"),
                    })?);
                index += 2;
            }
            "--pytorch-mock" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                options.pytorch_mock = Some(parse_bool_flag_value(value)?);
                index += 2;
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(options)
}

fn parse_bool_flag_value(value: &str) -> Result<bool, CliError> {
    match value {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        _ => Err(CliError::Parse {
            detail: format!("invalid boolean value `{value}`"),
        }),
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
    let account = normalize_provider_account_name(flag_value(args, "--account").as_deref());
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" | "--model" | "--account" => index += 2,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(ModelDefaultOptions {
        provider,
        account,
        model,
    })
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
    if model_config_uses_cluster(&data_dir) {
        let (config, config_path) = load_model_config_for_node(&data_dir)?;
        let resolved = resolve_default_model(Some(&config), None)?;
        return Ok(CurrentDefaultModel {
            provider: resolved.provider,
            account: resolved.account,
            configured_model: resolved.configured_model,
            model: resolved.model,
            config_path,
        });
    }
    resolve_current_default_model(&data_dir).map_err(CliError::LanguageModel)
}

pub(crate) fn run_models_command(command: ModelsCliCommand) -> Result<(), CliError> {
    match command {
        ModelsCliCommand::Help => models_help(),
        ModelsCliCommand::List(options) => models_list(options),
        ModelsCliCommand::Default(options) => models_default(options),
        ModelsCliCommand::Runtime(command) => models_runtime(command),
    }
}

pub(crate) fn models_help() -> Result<(), CliError> {
    let current = current_default_model()?;
    println!("Current default model:");
    println!("  provider: {}", current.provider);
    println!("  account: {}", current.account);
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
    if !model_config_uses_cluster(data_dir) {
        let update =
            write_default_model_config(data_dir, options.provider.clone(), options.model.clone())
                .map_err(CliError::LanguageModel)?;
        let mut config = update.config;
        config.default_account = Some(options.account.clone());
        let config_path = write_model_config_for_node(data_dir, &config)?;
        println!(
            "default provider: {}",
            config.default_provider.as_deref().unwrap_or("")
        );
        println!(
            "default account: {}",
            config.default_account.as_deref().unwrap_or("")
        );
        println!(
            "default model: {}",
            config.default_model.as_deref().unwrap_or("")
        );
        println!("config: {}", config_path.display());
        return Ok(());
    }
    let (mut config, _) = load_model_config_for_node(data_dir)?;
    config.default_provider = Some(options.provider);
    config.default_account = Some(options.account);
    config.default_model = Some(options.model);
    let config_path = write_model_config_for_node(data_dir, &config)?;

    println!(
        "default provider: {}",
        config.default_provider.as_deref().unwrap_or("")
    );
    println!(
        "default account: {}",
        config.default_account.as_deref().unwrap_or("")
    );
    println!(
        "default model: {}",
        config.default_model.as_deref().unwrap_or("")
    );
    println!("config: {}", config_path.display());
    Ok(())
}

pub(crate) fn model_config_uses_cluster(data_dir: &SinomoDataDir) -> bool {
    network::read_cluster_state(&ClusterPaths::new(data_dir)).is_ok()
}

pub(crate) fn load_model_config_for_node(
    data_dir: &SinomoDataDir,
) -> Result<(ModelConfig, PathBuf), CliError> {
    if model_config_uses_cluster(data_dir) {
        let paths = ClusterPaths::new(data_dir);
        let snapshot = model_config_cluster_snapshot(&paths)?;
        let config = model_config_from_cluster_kv_snapshot(&snapshot)
            .map_err(CliError::LanguageModel)?
            .unwrap_or_default();
        return Ok((config, paths.kv_snapshot));
    }
    let paths = ModelPaths::new(data_dir);
    Ok((load_model_config(&paths).unwrap_or_default(), paths.config))
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
        return network::read_cluster_kv_snapshot(paths).map_err(Into::into);
    }
    #[cfg(not(test))]
    {
        Err(CliError::DaemonNotRunning {
            detail: "clustered model config requires the local daemon".to_string(),
        })
    }
}

pub(crate) fn write_model_config_for_node(
    data_dir: &SinomoDataDir,
    config: &ModelConfig,
) -> Result<PathBuf, CliError> {
    if model_config_uses_cluster(data_dir) {
        let paths = ClusterPaths::new(data_dir);
        paths.ensure()?;
        let txn = model_config_cluster_kv_txn(config).map_err(CliError::LanguageModel)?;
        let daemon_paths = DaemonPaths::new(data_dir);
        daemon_handle_daemon_cluster_kv_txn_request(&daemon_paths, txn)?;
        return Ok(paths.kv_snapshot);
    }
    let paths = ModelPaths::new(data_dir);
    save_model_config(&paths.config, config).map_err(CliError::LanguageModel)?;
    Ok(paths.config)
}

pub(crate) fn load_model_runtime_config_for_node(
    data_dir: &SinomoDataDir,
) -> Result<(ModelRuntimeConfig, PathBuf), CliError> {
    if model_config_uses_cluster(data_dir) {
        let paths = ClusterPaths::new(data_dir);
        let snapshot = model_config_cluster_snapshot(&paths)?;
        let config = model_runtime_config_from_cluster_kv_snapshot(&snapshot)
            .map_err(CliError::LanguageModel)?
            .unwrap_or_default();
        return Ok((config, paths.kv_snapshot));
    }
    Ok((
        ModelRuntimeConfig::default(),
        data_dir.configs_dir().join("models-runtime.json"),
    ))
}

pub(crate) fn write_model_runtime_config_for_node(
    data_dir: &SinomoDataDir,
    config: &ModelRuntimeConfig,
) -> Result<PathBuf, CliError> {
    if !model_config_uses_cluster(data_dir) {
        return Err(CliError::DaemonNotRunning {
            detail:
                "model runtime config is cluster scoped and requires an initialized local cluster"
                    .to_string(),
        });
    }
    let paths = ClusterPaths::new(data_dir);
    paths.ensure()?;
    let txn = model_runtime_config_cluster_kv_txn(config).map_err(CliError::LanguageModel)?;
    let daemon_paths = DaemonPaths::new(data_dir);
    daemon_handle_daemon_cluster_kv_txn_request(&daemon_paths, txn)?;
    Ok(paths.kv_snapshot)
}

pub(crate) fn models_runtime(command: ModelRuntimeCliCommand) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    match command {
        ModelRuntimeCliCommand::Show => {
            let (config, path) = load_model_runtime_config_for_node(&data_dir)?;
            println!("model runtime config:");
            println!(
                "  native models cache dir: {}",
                config.native_models_cache_dir.as_deref().unwrap_or("")
            );
            println!(
                "  minicpm-v46 model dir: {}",
                config.minicpm_v46.model_dir.as_deref().unwrap_or("")
            );
            println!(
                "  minicpm-v46 max input tokens: {}",
                config
                    .minicpm_v46
                    .max_input_tokens
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            );
            println!(
                "  minicpm-v46 max output tokens: {}",
                config
                    .minicpm_v46
                    .max_output_tokens
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            );
            println!(
                "  pytorch endpoint: {}",
                config.pytorch_bridge.endpoint.as_deref().unwrap_or("")
            );
            println!(
                "  pytorch python: {}",
                config.pytorch_bridge.python.as_deref().unwrap_or("")
            );
            println!(
                "  pytorch device: {}",
                config.pytorch_bridge.device.as_deref().unwrap_or("")
            );
            println!(
                "  pytorch timeout seconds: {}",
                config
                    .pytorch_bridge
                    .timeout_seconds
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            );
            println!(
                "  pytorch mock: {}",
                config
                    .pytorch_bridge
                    .mock
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            );
            println!("config: {}", path.display());
        }
        ModelRuntimeCliCommand::Set(options) => {
            let (mut config, _) = load_model_runtime_config_for_node(&data_dir)?;
            if let Some(value) = options.native_models_cache_dir {
                config.native_models_cache_dir = non_empty_option(value);
            }
            if let Some(value) = options.minicpm_v46_model_dir {
                config.minicpm_v46.model_dir = non_empty_option(value);
            }
            if options.minicpm_v46_max_input_tokens.is_some() {
                config.minicpm_v46.max_input_tokens = options.minicpm_v46_max_input_tokens;
            }
            if options.minicpm_v46_max_output_tokens.is_some() {
                config.minicpm_v46.max_output_tokens = options.minicpm_v46_max_output_tokens;
            }
            if let Some(value) = options.pytorch_endpoint {
                config.pytorch_bridge.endpoint = non_empty_option(value);
            }
            if let Some(value) = options.pytorch_python {
                config.pytorch_bridge.python = non_empty_option(value);
            }
            if let Some(value) = options.pytorch_device {
                config.pytorch_bridge.device = non_empty_option(value);
            }
            if options.pytorch_timeout_seconds.is_some() {
                config.pytorch_bridge.timeout_seconds = options.pytorch_timeout_seconds;
            }
            if options.pytorch_mock.is_some() {
                config.pytorch_bridge.mock = options.pytorch_mock;
            }
            let path = write_model_runtime_config_for_node(&data_dir, &config)?;
            println!("model runtime config updated");
            println!("config: {}", path.display());
            println!("kv key: {LM_RUNTIME_CONFIG_KV_KEY}");
        }
    }
    Ok(())
}

fn non_empty_option(value: String) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
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
