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
    Options(ModelOptionsCliCommand),
    Runtime(ModelRuntimeCliCommand),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ModelListOptions {
    pub(crate) provider: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ModelOptionsCliCommand {
    Show(ModelOptionsTarget),
    Set(ModelOptionsSetOptions),
    Clear(ModelOptionsTarget),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ModelOptionsTarget {
    pub(crate) provider: Option<String>,
    pub(crate) account: String,
    pub(crate) model: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ModelOptionsSetOptions {
    pub(crate) target: ModelOptionsTarget,
    pub(crate) reasoning_effort: Option<ModelOptionValueUpdate<ProviderReasoningEffort>>,
    pub(crate) reasoning_budget_tokens: Option<ModelOptionValueUpdate<u32>>,
    pub(crate) text_verbosity: Option<ModelOptionValueUpdate<ProviderTextVerbosity>>,
    pub(crate) vendor_options: Vec<(String, String)>,
    pub(crate) clear_vendor_options: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ModelOptionValueUpdate<T> {
    Set(T),
    Clear,
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
        Some((command, rest)) if command == "options" => Ok(ModelsCliCommand::Options(
            parse_model_options_command(rest)?,
        )),
        Some((command, rest)) if command == "runtime" => Ok(ModelsCliCommand::Runtime(
            parse_models_runtime_command(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_model_options_command(
    args: &[String],
) -> Result<ModelOptionsCliCommand, CliError> {
    match args.split_first() {
        None => Ok(ModelOptionsCliCommand::Show(parse_model_options_target(
            &[],
            false,
        )?)),
        Some((command, rest)) if matches!(command.as_str(), "show" | "help" | "--help" | "-h") => {
            Ok(ModelOptionsCliCommand::Show(parse_model_options_target(
                rest, false,
            )?))
        }
        Some((command, rest)) if command == "set" => Ok(ModelOptionsCliCommand::Set(
            parse_model_options_set_options(rest)?,
        )),
        Some((command, rest)) if command == "clear" => Ok(ModelOptionsCliCommand::Clear(
            parse_model_options_target(rest, true)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_model_options_target(
    args: &[String],
    require_provider: bool,
) -> Result<ModelOptionsTarget, CliError> {
    let mut provider = None;
    let mut account = DEFAULT_PROVIDER_ACCOUNT.to_string();
    let mut model = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                provider =
                    Some(
                        normalize_model_provider(value).ok_or_else(|| CliError::Parse {
                            detail: format!("unknown provider `{value}`"),
                        })?,
                    );
                index += 2;
            }
            "--account" => {
                account = normalize_provider_account_name(Some(
                    args.get(index + 1).ok_or(CliError::Usage)?.as_str(),
                ));
                index += 2;
            }
            "--model" => {
                model = Some(
                    args.get(index + 1)
                        .ok_or(CliError::Usage)?
                        .trim()
                        .to_string(),
                );
                index += 2;
            }
            _ => return Err(CliError::Usage),
        }
    }
    if require_provider && provider.is_none() {
        return Err(CliError::Parse {
            detail: "--provider is required".to_string(),
        });
    }
    let model = match (provider.as_deref(), model) {
        (Some(provider), Some(model)) => Some(normalize_model_id_for_provider(provider, &model)),
        (_, model) => model,
    };
    Ok(ModelOptionsTarget {
        provider,
        account,
        model,
    })
}

pub(crate) fn parse_model_options_set_options(
    args: &[String],
) -> Result<ModelOptionsSetOptions, CliError> {
    let mut options = ModelOptionsSetOptions {
        target: ModelOptionsTarget {
            account: DEFAULT_PROVIDER_ACCOUNT.to_string(),
            ..ModelOptionsTarget::default()
        },
        ..ModelOptionsSetOptions::default()
    };
    let mut raw_model = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                options.target.provider = Some(normalize_model_provider(value).ok_or_else(
                    || CliError::Parse {
                        detail: format!("unknown provider `{value}`"),
                    },
                )?);
                index += 2;
            }
            "--account" => {
                options.target.account = normalize_provider_account_name(Some(
                    args.get(index + 1).ok_or(CliError::Usage)?.as_str(),
                ));
                index += 2;
            }
            "--model" => {
                raw_model = Some(
                    args.get(index + 1)
                        .ok_or(CliError::Usage)?
                        .trim()
                        .to_string(),
                );
                index += 2;
            }
            "--reasoning-effort" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                options.reasoning_effort = Some(parse_option_update(value, |value| {
                    value
                        .parse::<ProviderReasoningEffort>()
                        .map_err(|detail| CliError::Parse { detail })
                })?);
                index += 2;
            }
            "--reasoning-budget-tokens" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                options.reasoning_budget_tokens = Some(parse_option_update(value, |value| {
                    value.parse::<u32>().map_err(|source| CliError::Parse {
                        detail: format!("invalid reasoning budget `{value}`: {source}"),
                    })
                })?);
                index += 2;
            }
            "--text-verbosity" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                options.text_verbosity = Some(parse_option_update(value, |value| {
                    value
                        .parse::<ProviderTextVerbosity>()
                        .map_err(|detail| CliError::Parse { detail })
                })?);
                index += 2;
            }
            "--vendor-option" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                let (key, value) = parse_vendor_option(value)?;
                options.vendor_options.push((key, value));
                index += 2;
            }
            "--clear-vendor-option" => {
                let value = args.get(index + 1).ok_or(CliError::Usage)?;
                let value = value.trim();
                if value.is_empty() {
                    return Err(CliError::Parse {
                        detail: "--clear-vendor-option requires a key".to_string(),
                    });
                }
                options.clear_vendor_options.push(value.to_string());
                index += 2;
            }
            _ => return Err(CliError::Usage),
        }
    }
    let provider = options
        .target
        .provider
        .as_deref()
        .ok_or_else(|| CliError::Parse {
            detail: "--provider is required".to_string(),
        })?;
    options.target.model = raw_model.map(|model| normalize_model_id_for_provider(provider, &model));
    if options.reasoning_effort.is_none()
        && options.reasoning_budget_tokens.is_none()
        && options.text_verbosity.is_none()
        && options.vendor_options.is_empty()
        && options.clear_vendor_options.is_empty()
    {
        return Err(CliError::Parse {
            detail: "no model options were provided".to_string(),
        });
    }
    Ok(options)
}

fn parse_option_update<T>(
    value: &str,
    parse: impl FnOnce(&str) -> Result<T, CliError>,
) -> Result<ModelOptionValueUpdate<T>, CliError> {
    match value.trim() {
        "clear" | "unset" | "default" => Ok(ModelOptionValueUpdate::Clear),
        value => parse(value).map(ModelOptionValueUpdate::Set),
    }
}

fn parse_vendor_option(value: &str) -> Result<(String, String), CliError> {
    let Some((key, value)) = value.split_once('=') else {
        return Err(CliError::Parse {
            detail: "--vendor-option requires key=value".to_string(),
        });
    };
    let key = key.trim();
    if key.is_empty() {
        return Err(CliError::Parse {
            detail: "--vendor-option requires a non-empty key".to_string(),
        });
    }
    Ok((key.to_string(), value.trim().to_string()))
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
        ModelsCliCommand::Options(command) => models_options(command),
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
        commit_model_config_cluster_txn(data_dir, txn)?;
        return Ok(paths.kv_snapshot);
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
        return Ok((config, paths.kv_snapshot));
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
    Ok(paths.kv_snapshot)
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
    daemon_handle_daemon_cluster_kv_txn_request(&daemon_paths, txn)?;
    Ok(())
}

#[cfg(not(test))]
fn commit_model_config_cluster_txn(_data_dir: &SinomoDataDir, txn: KvTxn) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "clustered model config writes require the local daemon".to_string(),
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

pub(crate) fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
}
