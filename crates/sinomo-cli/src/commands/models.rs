use super::*;
pub(crate) use sinomo_lm::cli::UsageOptions;
#[cfg(test)]
pub(crate) use sinomo_lm::cli::models::ModelRuntimeSetOptions;
use sinomo_lm::cli::models::models_cli_spec;
#[cfg(test)]
pub(crate) use sinomo_lm::cli::models::parse_models_command;
pub(crate) use sinomo_lm::cli::models::{
    LocalModelActionOptions, ModelDefaultOptions, ModelListOptions, ModelOptionValueUpdate,
    ModelOptionsCliCommand, ModelOptionsSetOptions, ModelOptionsTarget, ModelRuntimeCliCommand,
    ModelsCliCommand,
};
use sinomo_runtime::cli_registry::CliCommandError;
#[allow(unused_imports)]
use sinomo_runtime::cluster;
use sinomo_runtime::cluster::ClusterKvPathsExt;

pub(crate) const MODEL_CONFIG_CLUSTER_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(test)]
pub(crate) fn parse_usage_schema_for_test(args: &[String]) -> Result<UsageOptions, CliError> {
    sinomo_lm::cli::parse_usage_command(args).map_err(Into::into)
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

pub(crate) fn run_models_cli_command(
    command: ModelsCliCommand,
    context: &mut CliCommandContext<'_>,
) -> Result<(), CliCommandError> {
    let result = match command {
        ModelsCliCommand::Overview => return write_models_overview(context),
        ModelsCliCommand::Help(help) => return help.write(context),
        ModelsCliCommand::List(options) => models_list(options),
        ModelsCliCommand::Default(options) => models_default(options),
        ModelsCliCommand::Options(command) => models_options(command),
        ModelsCliCommand::Runtime(command) => models_runtime(command),
        ModelsCliCommand::Pull(options) => models_local_action(options, true),
        ModelsCliCommand::Load(options) => models_local_action(options, false),
    };
    result.map_err(|error| CliCommandError::new(error.to_string()))
}

fn models_local_action(options: LocalModelActionOptions, pull: bool) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let (model_config, _) = load_model_config_for_node(&data_dir)?;
    let account = provider_account_config(
        &model_config,
        &options.provider,
        Some(options.account.as_str()),
    );
    let runtime = if options.provider == OLLAMA_PROVIDER_ID {
        LocalResponsesRuntime::Ollama
    } else {
        LocalResponsesRuntime::LmStudio
    };
    let mut config = LocalResponsesConfig::new(runtime);
    if let Some(base_url) = account
        .and_then(|account| account.api_base_url.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        config.api_base_url = base_url.to_string();
    }
    config.default_model = options.model.clone();
    let provider = LocalResponsesProvider::new(config);
    if pull {
        provider.pull_model(&options.model)?;
        println!("pulled {} model: {}", options.provider, options.model);
    } else {
        provider.load_model(&options.model)?;
        println!("loaded {} model: {}", options.provider, options.model);
    }
    Ok(())
}

fn write_models_overview(context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
    let current =
        current_default_model().map_err(|error| CliCommandError::new(error.to_string()))?;
    context.stdout_line(format_args!("Current default model:"))?;
    context.stdout_line(format_args!("  provider: {}", current.provider))?;
    context.stdout_line(format_args!("  account: {}", current.account))?;
    context.stdout_line(format_args!("  model: {}", current.model))?;
    context.stdout_line(format_args!(
        "  configured model: {}",
        current.configured_model
    ))?;
    context.stdout_line(format_args!("  config: {}", current.config_path.display()))?;
    context.stdout_line(format_args!(""))?;
    models_cli_spec().help().write(context)
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

pub(crate) fn models_options(command: ModelOptionsCliCommand) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    match command {
        ModelOptionsCliCommand::Show(target) => {
            let (config, path) = load_model_config_for_node(&data_dir)?;
            print_model_options(&config, &target);
            println!("config: {}", path.display());
            Ok(())
        }
        ModelOptionsCliCommand::Set(options) => {
            let (mut config, _) = load_model_config_for_node(&data_dir)?;
            let provider = options
                .target
                .provider
                .clone()
                .expect("set parser requires provider");
            let account = mutable_provider_account(&mut config, &provider, &options.target.account);
            let scope = model_options_scope(account, options.target.model.as_deref());
            apply_model_options_set(scope, &options);
            let path = write_model_config_for_node(&data_dir, &config)?;
            println!("model provider options updated");
            print_model_options(
                &config,
                &ModelOptionsTarget {
                    provider: Some(provider),
                    account: options.target.account,
                    model: options.target.model,
                },
            );
            println!("config: {}", path.display());
            if model_config_uses_cluster(&data_dir) {
                println!("kv key: {LM_PROVIDER_CONFIG_KV_KEY}");
            } else {
                println!("mode: local");
            }
            Ok(())
        }
        ModelOptionsCliCommand::Clear(target) => {
            let (mut config, _) = load_model_config_for_node(&data_dir)?;
            let provider = target
                .provider
                .clone()
                .expect("clear parser requires provider");
            clear_model_options(
                &mut config,
                &provider,
                &target.account,
                target.model.as_deref(),
            );
            let path = write_model_config_for_node(&data_dir, &config)?;
            println!("model provider options cleared");
            print_model_options(
                &config,
                &ModelOptionsTarget {
                    provider: Some(provider),
                    account: target.account,
                    model: target.model,
                },
            );
            println!("config: {}", path.display());
            if model_config_uses_cluster(&data_dir) {
                println!("kv key: {LM_PROVIDER_CONFIG_KV_KEY}");
            } else {
                println!("mode: local");
            }
            Ok(())
        }
    }
}

fn print_model_options(config: &ModelConfig, target: &ModelOptionsTarget) {
    match target.provider.as_deref() {
        Some(provider) => {
            let account = config.accounts.iter().find(|candidate| {
                candidate.provider == provider
                    && normalize_provider_account_name(Some(&candidate.account)) == target.account
            });
            println!("model provider options:");
            println!("  provider: {provider}");
            println!("  account: {}", target.account);
            if let Some(model) = target.model.as_deref() {
                println!("  model: {model}");
            }
            match account {
                Some(account) => {
                    let options = if let Some(model) = target.model.as_deref() {
                        provider_options_for_model(Some(account), model)
                    } else {
                        account.options.clone()
                    };
                    print_language_model_provider_options("  ", &options);
                }
                None => {
                    println!("  status: not configured");
                    print_language_model_provider_options(
                        "  ",
                        &LanguageModelProviderOptions::default(),
                    );
                }
            }
        }
        None => {
            println!("model provider options:");
            if config.accounts.is_empty() {
                println!("  no provider accounts are configured");
                return;
            }
            for account in &config.accounts {
                println!(
                    "  {}:{}",
                    account.provider,
                    normalize_provider_account_name(Some(&account.account))
                );
                print_language_model_provider_options("    ", &account.options);
                for model_options in &account.model_options {
                    println!("    model: {}", model_options.model);
                    print_language_model_provider_options("      ", &model_options.options);
                }
            }
        }
    }
}

fn print_language_model_provider_options(prefix: &str, options: &LanguageModelProviderOptions) {
    println!(
        "{prefix}reasoning effort: {}",
        options
            .reasoning_effort
            .map(|value| value.to_string())
            .unwrap_or_default()
    );
    println!(
        "{prefix}reasoning budget tokens: {}",
        options
            .reasoning_budget_tokens
            .map(|value| value.to_string())
            .unwrap_or_default()
    );
    println!(
        "{prefix}text verbosity: {}",
        options
            .text_verbosity
            .map(|value| value.to_string())
            .unwrap_or_default()
    );
    println!(
        "{prefix}maximum context tokens: {}",
        options
            .max_context_tokens
            .map(|value| value.to_string())
            .unwrap_or_default()
    );
    println!(
        "{prefix}automatic compaction token limit: {}",
        options
            .auto_compact_token_limit
            .map(|value| value.to_string())
            .unwrap_or_default()
    );
    if options.vendor_options.is_empty() {
        println!("{prefix}vendor options: none");
    } else {
        println!("{prefix}vendor options:");
        for (key, value) in &options.vendor_options {
            println!("{prefix}  {key}={value}");
        }
    }
}

fn mutable_provider_account<'a>(
    config: &'a mut ModelConfig,
    provider: &str,
    account: &str,
) -> &'a mut ProviderAccountConfig {
    let account = normalize_provider_account_name(Some(account));
    if let Some(index) = config.accounts.iter().position(|candidate| {
        candidate.provider == provider
            && normalize_provider_account_name(Some(&candidate.account)) == account
    }) {
        return &mut config.accounts[index];
    }
    config.accounts.push(ProviderAccountConfig {
        provider: provider.to_string(),
        account,
        default_model: None,
        api_base_url: None,
        api_key: None,
        enabled: true,
        options: LanguageModelProviderOptions::default(),
        model_options: Vec::new(),
    });
    config.accounts.last_mut().expect("pushed provider account")
}

fn model_options_scope<'a>(
    account: &'a mut ProviderAccountConfig,
    model: Option<&str>,
) -> &'a mut LanguageModelProviderOptions {
    let Some(model) = model else {
        return &mut account.options;
    };
    let model = normalize_model_id_for_provider(&account.provider, model);
    if let Some(index) = account
        .model_options
        .iter()
        .position(|candidate| candidate.model == model)
    {
        return &mut account.model_options[index].options;
    }
    account.model_options.push(LanguageModelModelOptions {
        model,
        options: LanguageModelProviderOptions::default(),
    });
    &mut account
        .model_options
        .last_mut()
        .expect("pushed model options")
        .options
}

fn apply_model_options_set(
    options: &mut LanguageModelProviderOptions,
    updates: &ModelOptionsSetOptions,
) {
    if let Some(update) = &updates.reasoning_effort {
        match update {
            ModelOptionValueUpdate::Set(value) => options.reasoning_effort = Some(*value),
            ModelOptionValueUpdate::Clear => options.reasoning_effort = None,
        }
    }
    if let Some(update) = &updates.reasoning_budget_tokens {
        match update {
            ModelOptionValueUpdate::Set(value) => options.reasoning_budget_tokens = Some(*value),
            ModelOptionValueUpdate::Clear => options.reasoning_budget_tokens = None,
        }
    }
    if let Some(update) = &updates.text_verbosity {
        match update {
            ModelOptionValueUpdate::Set(value) => options.text_verbosity = Some(*value),
            ModelOptionValueUpdate::Clear => options.text_verbosity = None,
        }
    }
    if let Some(update) = &updates.max_context_tokens {
        match update {
            ModelOptionValueUpdate::Set(value) => options.max_context_tokens = Some(*value),
            ModelOptionValueUpdate::Clear => options.max_context_tokens = None,
        }
    }
    if let Some(update) = &updates.auto_compact_token_limit {
        match update {
            ModelOptionValueUpdate::Set(value) => options.auto_compact_token_limit = Some(*value),
            ModelOptionValueUpdate::Clear => options.auto_compact_token_limit = None,
        }
    }
    for (key, value) in &updates.vendor_options {
        options.vendor_options.insert(key.clone(), value.clone());
    }
    for key in &updates.clear_vendor_options {
        options.vendor_options.remove(key);
    }
}

fn clear_model_options(
    config: &mut ModelConfig,
    provider: &str,
    account: &str,
    model: Option<&str>,
) {
    let account = normalize_provider_account_name(Some(account));
    let Some(account) = config.accounts.iter_mut().find(|candidate| {
        candidate.provider == provider
            && normalize_provider_account_name(Some(&candidate.account)) == account
    }) else {
        return;
    };
    if let Some(model) = model {
        let model = normalize_model_id_for_provider(provider, model);
        account
            .model_options
            .retain(|candidate| candidate.model != model);
    } else {
        account.options = LanguageModelProviderOptions::default();
    }
}

pub(crate) fn model_config_uses_cluster(data_dir: &SinomoDataDir) -> bool {
    if cli_force_local_mode() {
        return false;
    }
    cluster::read_cluster_state(&ClusterPaths::new(data_dir)).is_ok()
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
        return Ok((config, paths.kv_paths().snapshot));
    }
    let store = model_config_store().map_err(CliError::LanguageModel)?;
    let config = store
        .get_typed(data_dir, None, LM_PROVIDER_CONFIG_KEY)
        .map_err(CliError::from)?;
    Ok((
        config,
        model_config_definition()
            .map_err(CliError::LanguageModel)?
            .local_path(data_dir),
    ))
}

pub(crate) enum ModelConfigClusterSnapshot {
    Authoritative(KvAuthoritativeRead<KvSnapshot>),
    #[cfg(test)]
    Unverified(KvSnapshot),
}

impl ModelConfigClusterSnapshot {
    fn into_snapshot(self) -> KvSnapshot {
        match self {
            Self::Authoritative(read) => read.value,
            #[cfg(test)]
            Self::Unverified(snapshot) => snapshot,
        }
    }
}

pub(crate) fn model_config_cluster_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    model_config_cluster_snapshot_read(paths).map(ModelConfigClusterSnapshot::into_snapshot)
}

/// Both chat settings must come from the same authoritative read. Fetching a
/// whole cluster snapshot also transfers unrelated history and receipts.
pub(crate) fn chat_model_config_entries(
    _paths: &ClusterPaths,
    keys: &[String],
    timeout: Duration,
) -> Result<Vec<sinomo_runtime::kv::KvEntry>, CliError> {
    if let Some(status) = default_main_daemon_status()? {
        let payload = sinomo_runtime::kv::cluster_kv_authoritative_entries_rpc_payload(keys)?;
        let request = network::ObjectRpcRequest::from_encoded_body(
            payload.route,
            payload.category,
            payload.body,
        );
        let options = network::ClusterObjectRpcOptions::new(
            network::DaemonRpcTimeouts::connect_and_idle(timeout),
        )
        .with_overall_timeout(timeout);
        let response = sinomo_runtime::daemon::send_daemon_rpc_with_options(
            &status.address,
            &request,
            options,
        )?;
        response.error_detail()?;
        let read = sinomo_runtime::kv::decode_cluster_kv_authoritative_entries_rpc_body(
            &response.body,
            keys,
        )?;
        return Ok(read.value.into_iter().flatten().collect());
    }
    #[cfg(test)]
    {
        let snapshot = sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&_paths.kv_paths())
            .map_err(network::NetworkError::directory)?;
        Ok(snapshot
            .entries
            .into_iter()
            .filter(|entry| keys.contains(&entry.key))
            .collect())
    }
    #[cfg(not(test))]
    Err(CliError::DaemonNotRunning {
        detail: "clustered model config requires the local daemon".to_string(),
    })
}

pub(crate) fn model_config_cluster_snapshot_read(
    paths: &ClusterPaths,
) -> Result<ModelConfigClusterSnapshot, CliError> {
    let _ = paths;
    if let Some(status) = default_main_daemon_status()? {
        match daemon_fetch_cluster_kv_authoritative_snapshot(
            &status.address,
            MODEL_CONFIG_CLUSTER_SNAPSHOT_TIMEOUT,
        ) {
            Ok(snapshot) => return Ok(ModelConfigClusterSnapshot::Authoritative(snapshot)),
            Err(error) => {
                #[cfg(not(test))]
                return Err(error.into());
                #[cfg(test)]
                let _ = error;
            }
        }
    }
    #[cfg(test)]
    {
        sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .map(ModelConfigClusterSnapshot::Unverified)
            .map_err(sinomo_runtime::network::NetworkError::directory)
            .map_err(Into::into)
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
        commit_model_config_cluster_txn(data_dir, txn)?;
        return Ok(paths.kv_paths().snapshot);
    }
    let store = model_config_store().map_err(CliError::LanguageModel)?;
    store
        .set_typed(
            data_dir,
            None,
            LM_PROVIDER_CONFIG_KEY,
            config,
            sinomo_runtime::config::ConfigWriteTarget::Local,
        )
        .map_err(CliError::from)?;
    Ok(model_config_definition()
        .map_err(CliError::LanguageModel)?
        .local_path(data_dir))
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
        return Ok((config, paths.kv_paths().snapshot));
    }
    load_local_model_runtime_config(data_dir)
}

pub(crate) fn write_model_runtime_config_for_node(
    data_dir: &SinomoDataDir,
    config: &ModelRuntimeConfig,
) -> Result<PathBuf, CliError> {
    if !model_config_uses_cluster(data_dir) {
        return write_local_model_runtime_config(data_dir, config);
    }
    let paths = ClusterPaths::new(data_dir);
    paths.ensure()?;
    let txn = model_runtime_config_cluster_kv_txn(config).map_err(CliError::LanguageModel)?;
    commit_model_config_cluster_txn(data_dir, txn)?;
    Ok(paths.kv_paths().snapshot)
}

fn local_model_runtime_config_path(data_dir: &SinomoDataDir) -> PathBuf {
    model_runtime_config_definition()
        .map(|definition| definition.local_path(data_dir))
        .unwrap_or_else(|_| {
            sinomo_runtime::config::configs_dir(data_dir)
                .join("lm")
                .join("runtime.json")
        })
}

fn load_local_model_runtime_config(
    data_dir: &SinomoDataDir,
) -> Result<(ModelRuntimeConfig, PathBuf), CliError> {
    let store = model_config_store().map_err(CliError::LanguageModel)?;
    let config = store
        .get_typed(data_dir, None, LM_RUNTIME_CONFIG_KEY)
        .map_err(CliError::from)?;
    Ok((config, local_model_runtime_config_path(data_dir)))
}

fn write_local_model_runtime_config(
    data_dir: &SinomoDataDir,
    config: &ModelRuntimeConfig,
) -> Result<PathBuf, CliError> {
    let store = model_config_store().map_err(CliError::LanguageModel)?;
    store
        .set_typed(
            data_dir,
            None,
            LM_RUNTIME_CONFIG_KEY,
            config,
            sinomo_runtime::config::ConfigWriteTarget::Local,
        )
        .map_err(CliError::from)?;
    Ok(local_model_runtime_config_path(data_dir))
}

#[cfg(test)]
fn commit_model_config_cluster_txn(data_dir: &SinomoDataDir, txn: KvTxn) -> Result<(), CliError> {
    let daemon_paths = DaemonPaths::new(data_dir);
    handle_daemon_cluster_kv_txn_request(&daemon_paths, txn)?;
    Ok(())
}

#[cfg(not(test))]
fn commit_model_config_cluster_txn(_data_dir: &SinomoDataDir, txn: KvTxn) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "clustered model config writes require the local daemon".to_string(),
        });
    };
    let txn_request = KvTxnRequest::generated(txn);
    let payload = sinomo_runtime::kv::cluster_kv_txn_rpc_payload(&txn_request)?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &request,
        DAEMON_CLUSTER_KV_TXN_TIMEOUT,
    )?;
    response.error_detail()?;
    let commit = sinomo_runtime::kv::decode_cluster_kv_txn_rpc_body(&response.body)?;
    let txn_response = commit.txn_response.ok_or_else(|| CliError::Parse {
        detail: "cluster KV returned a barrier result for a model config transaction".to_string(),
    })?;
    if !txn_response.succeeded {
        return Err(CliError::Parse {
            detail: format!(
                "cluster model config transaction did not commit at revision {:?}",
                txn_response.revision
            ),
        });
    }
    Ok(())
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
                "  pytorch loopback endpoint: {}",
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
                if let Some(endpoint) = config.pytorch_bridge.endpoint.as_deref()
                    && !is_loopback_http_endpoint(endpoint).map_err(|source| CliError::Parse {
                        detail: source.to_string(),
                    })?
                {
                    return Err(CliError::Parse {
                        detail: format!(
                            "pytorch endpoint `{endpoint}` is not loopback; cluster model traffic must go through sinomo-network"
                        ),
                    });
                }
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
            if model_config_uses_cluster(&data_dir) {
                println!("kv key: {LM_RUNTIME_CONFIG_KV_KEY}");
            } else {
                println!("mode: local");
            }
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
                .unwrap_or(serde_json::Value::Null)
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
