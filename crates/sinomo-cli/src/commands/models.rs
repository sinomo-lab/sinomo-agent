use super::*;
use sinomo_runtime::cli_registry::{
    CliAutocompleteChoice, CliCommand as RegisteredCommand, CliCommandError, CliCommandHandler,
    CliCommandHelp, CliCommandMatches, CliCommandParseOutcome, CliCommandPolicy, CliCommandSpec,
    CliOptionSpec, CliParseError, CliValueSpec,
};
#[allow(unused_imports)]
use sinomo_runtime::cluster;
use sinomo_runtime::cluster::ClusterKvPathsExt;

const MODEL_CONFIG_CLUSTER_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ModelDefaultOptions {
    pub(crate) provider: String,
    pub(crate) account: String,
    pub(crate) model: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ModelsCliCommand {
    Overview,
    Help(CliCommandHelp),
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

const MODELS_COMMAND_NAME: &str = "models";
const MODELS_POLICY: CliCommandPolicy = CliCommandPolicy::local_capable();

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ModelsCliCommandHandler;

pub(crate) fn models_cli_command_handler() -> ModelsCliCommandHandler {
    ModelsCliCommandHandler
}

impl CliCommandHandler for ModelsCliCommandHandler {
    fn metadata(&self) -> sinomo_runtime::cli_registry::CliCommandMetadata {
        models_cli_spec().metadata(MODELS_POLICY)
    }

    fn specification(&self) -> Option<CliCommandSpec> {
        Some(models_cli_spec())
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn RegisteredCommand>, CliParseError> {
        Ok(Box::new(ModelsCliCommandRunner {
            command: parse_models_command(args)?,
        }))
    }
}

#[derive(Debug)]
struct ModelsCliCommandRunner {
    command: ModelsCliCommand,
}

impl RegisteredCommand for ModelsCliCommandRunner {
    fn name(&self) -> &'static str {
        MODELS_COMMAND_NAME
    }

    fn policy(&self) -> CliCommandPolicy {
        MODELS_POLICY
    }

    fn is_help(&self) -> bool {
        matches!(self.command, ModelsCliCommand::Help(_))
    }

    fn help(&self) -> CliCommandHelp {
        models_cli_spec().help()
    }

    fn autocomplete(&self) -> sinomo_runtime::cli_registry::CliAutocompleteSpec {
        models_cli_spec().autocomplete()
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        run_models_cli_command(self.command, context)
    }
}

pub(crate) fn models_cli_spec() -> CliCommandSpec {
    let provider_value = || {
        CliValueSpec::new("provider", "model provider id").with_choices(
            default_model_provider_ids()
                .iter()
                .map(|provider| CliAutocompleteChoice::new(*provider, "available model provider")),
        )
    };
    let account_option = || {
        CliOptionSpec::value(
            "--account",
            "provider account",
            CliValueSpec::new("account", "provider account"),
        )
    };
    let model_option = || {
        CliOptionSpec::value(
            "--model",
            "provider model id",
            CliValueSpec::new("model", "provider model id"),
        )
    };
    let provider_option = || CliOptionSpec::value("--provider", "model provider", provider_value());
    let target_options = || [provider_option(), account_option(), model_option()];
    let choice =
        |value: &'static str, detail: &'static str| CliAutocompleteChoice::new(value, detail);

    let options = CliCommandSpec::new("options", "show provider-specific account or model options")
        .with_options(target_options())
        .with_children([
            CliCommandSpec::new("show", "show provider-specific account or model options")
                .with_options(target_options()),
            CliCommandSpec::new("set", "update provider-specific account or model options")
                .with_options([
                    provider_option().required(),
                    account_option(),
                    model_option(),
                    CliOptionSpec::value(
                        "--reasoning-effort",
                        "reasoning effort override",
                        CliValueSpec::new(
                            "none|minimal|low|medium|high|xhigh|max|clear",
                            "reasoning effort or clear",
                        )
                        .with_choices([
                            choice("none", "disable reasoning effort"),
                            choice("minimal", "minimal reasoning effort"),
                            choice("low", "low reasoning effort"),
                            choice("medium", "medium reasoning effort"),
                            choice("high", "high reasoning effort"),
                            choice("xhigh", "extra-high reasoning effort"),
                            choice("max", "maximum supported reasoning effort"),
                            choice("clear", "remove the configured override"),
                            choice("unset", "remove the configured override"),
                            choice("default", "remove the configured override"),
                        ])
                        .restricted(),
                    ),
                    CliOptionSpec::value(
                        "--reasoning-budget-tokens",
                        "reasoning token budget override",
                        CliValueSpec::new("n|clear", "token budget or clear"),
                    ),
                    CliOptionSpec::value(
                        "--text-verbosity",
                        "text verbosity override",
                        CliValueSpec::new("low|medium|high|clear", "text verbosity or clear")
                            .with_choices([
                                choice("low", "low response verbosity"),
                                choice("medium", "medium response verbosity"),
                                choice("high", "high response verbosity"),
                                choice("clear", "remove the configured override"),
                                choice("unset", "remove the configured override"),
                                choice("default", "remove the configured override"),
                            ])
                            .restricted(),
                    ),
                    CliOptionSpec::value(
                        "--vendor-option",
                        "vendor-specific key=value option",
                        CliValueSpec::new("key=value", "vendor option"),
                    )
                    .repeatable(),
                    CliOptionSpec::value(
                        "--clear-vendor-option",
                        "vendor-specific option key to clear",
                        CliValueSpec::new("key", "vendor option key"),
                    )
                    .repeatable(),
                ]),
            CliCommandSpec::new("clear", "clear provider-specific options").with_options([
                provider_option().required(),
                account_option(),
                model_option(),
            ]),
        ]);

    let runtime = CliCommandSpec::new("runtime", "show cluster-scoped model runtime settings")
        .with_children([
            CliCommandSpec::new("show", "show cluster-scoped model runtime settings"),
            CliCommandSpec::new("set", "update cluster-scoped model runtime settings")
                .with_options([
                    CliOptionSpec::value(
                        "--native-models-cache-dir",
                        "native model cache directory",
                        CliValueSpec::new("path", "cache directory path"),
                    ),
                    CliOptionSpec::value(
                        "--minicpm-v46-model-dir",
                        "MiniCPM-V 4.6 model directory",
                        CliValueSpec::new("path", "model directory path"),
                    ),
                    CliOptionSpec::value(
                        "--minicpm-v46-max-input-tokens",
                        "MiniCPM-V 4.6 maximum input tokens",
                        CliValueSpec::new("n", "input token limit"),
                    ),
                    CliOptionSpec::value(
                        "--minicpm-v46-max-output-tokens",
                        "MiniCPM-V 4.6 maximum output tokens",
                        CliValueSpec::new("n", "output token limit"),
                    ),
                    CliOptionSpec::value(
                        "--pytorch-endpoint",
                        "PyTorch bridge endpoint",
                        CliValueSpec::new("url", "bridge endpoint URL"),
                    ),
                    CliOptionSpec::value(
                        "--pytorch-python",
                        "PyTorch Python executable",
                        CliValueSpec::new("path", "Python executable path"),
                    ),
                    CliOptionSpec::value(
                        "--pytorch-device",
                        "PyTorch execution device",
                        CliValueSpec::new("auto|cpu|cuda", "execution device")
                            .with_choices([
                                choice("auto", "select the device automatically"),
                                choice("cpu", "run on CPU"),
                                choice("cuda", "run on CUDA"),
                            ])
                            .restricted(),
                    ),
                    CliOptionSpec::value(
                        "--pytorch-timeout-seconds",
                        "PyTorch request timeout",
                        CliValueSpec::new("n", "timeout in seconds"),
                    ),
                    CliOptionSpec::value(
                        "--pytorch-mock",
                        "enable or disable the mock PyTorch bridge",
                        CliValueSpec::new("true|false", "boolean value")
                            .with_choices([
                                choice("true", "enable mock mode"),
                                choice("false", "disable mock mode"),
                                choice("1", "enable mock mode"),
                                choice("0", "disable mock mode"),
                                choice("on", "enable mock mode"),
                                choice("off", "disable mock mode"),
                                choice("yes", "enable mock mode"),
                                choice("no", "disable mock mode"),
                            ])
                            .restricted(),
                    ),
                ]),
        ]);

    CliCommandSpec::new(
        MODELS_COMMAND_NAME,
        "show the current default model and model commands",
    )
    .with_title("Models:")
    .with_aliases(["model"])
    .with_children([
        CliCommandSpec::new("list", "list available or known models")
            .with_options([provider_option()]),
        CliCommandSpec::new("default", "set the default model for model-backed commands")
            .with_options([
                provider_option().required(),
                model_option().required(),
                account_option(),
            ]),
        options,
        runtime,
    ])
    .with_after_help(
        std::iter::once("Providers:".to_string()).chain(
            default_model_provider_ids()
                .iter()
                .map(|provider| format!("  {provider}")),
        ),
    )
}

pub(crate) fn parse_models_command(args: &[String]) -> Result<ModelsCliCommand, CliParseError> {
    let matches = match models_cli_spec().parse(args)? {
        CliCommandParseOutcome::Help(help) => return Ok(ModelsCliCommand::Help(help)),
        CliCommandParseOutcome::Matches(matches) => matches,
    };
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
        ["models"] => Ok(ModelsCliCommand::Overview),
        ["models", "list"] => Ok(ModelsCliCommand::List(parse_models_list_matches(&matches))),
        ["models", "default"] => Ok(ModelsCliCommand::Default(parse_model_default_matches(
            &matches,
        )?)),
        ["models", "options"] | ["models", "options", "show"] => Ok(ModelsCliCommand::Options(
            ModelOptionsCliCommand::Show(parse_model_options_target(&matches)?),
        )),
        ["models", "options", "set"] => Ok(ModelsCliCommand::Options(ModelOptionsCliCommand::Set(
            parse_model_options_set_matches(&matches)?,
        ))),
        ["models", "options", "clear"] => Ok(ModelsCliCommand::Options(
            ModelOptionsCliCommand::Clear(parse_model_options_target(&matches)?),
        )),
        ["models", "runtime"] | ["models", "runtime", "show"] => {
            Ok(ModelsCliCommand::Runtime(ModelRuntimeCliCommand::Show))
        }
        ["models", "runtime", "set"] => Ok(ModelsCliCommand::Runtime(ModelRuntimeCliCommand::Set(
            parse_model_runtime_set_matches(&matches)?,
        ))),
        _ => Err(CliParseError::Parse {
            detail: format!("unhandled models command path `{}`", path.join(" ")),
        }),
    }
}

fn parse_models_list_matches(matches: &CliCommandMatches) -> ModelListOptions {
    ModelListOptions {
        provider: matches.value("--provider").map(str::to_string),
    }
}

fn parse_model_default_matches(
    matches: &CliCommandMatches,
) -> Result<ModelDefaultOptions, CliParseError> {
    let raw_provider = required_match_value(matches, "--provider")?;
    let provider = normalize_model_provider(raw_provider).ok_or_else(|| CliParseError::Parse {
        detail: format!("unknown provider `{raw_provider}`"),
    })?;
    let model =
        normalize_model_id_for_provider(&provider, required_match_value(matches, "--model")?);
    let account = normalize_provider_account_name(matches.value("--account"));
    Ok(ModelDefaultOptions {
        provider,
        account,
        model,
    })
}

fn parse_model_options_target(
    matches: &CliCommandMatches,
) -> Result<ModelOptionsTarget, CliParseError> {
    let provider = matches
        .value("--provider")
        .map(|value| {
            normalize_model_provider(value).ok_or_else(|| CliParseError::Parse {
                detail: format!("unknown provider `{value}`"),
            })
        })
        .transpose()?;
    let account = normalize_provider_account_name(matches.value("--account"));
    let model = matches.value("--model").map(str::trim).map(str::to_string);
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

fn parse_model_options_set_matches(
    matches: &CliCommandMatches,
) -> Result<ModelOptionsSetOptions, CliParseError> {
    let mut options = ModelOptionsSetOptions {
        target: ModelOptionsTarget {
            account: DEFAULT_PROVIDER_ACCOUNT.to_string(),
            ..ModelOptionsTarget::default()
        },
        ..ModelOptionsSetOptions::default()
    };
    let raw_provider = required_match_value(matches, "--provider")?;
    let provider = normalize_model_provider(raw_provider).ok_or_else(|| CliParseError::Parse {
        detail: format!("unknown provider `{raw_provider}`"),
    })?;
    options.target.provider = Some(provider.clone());
    options.target.account = normalize_provider_account_name(matches.value("--account"));
    options.target.model = matches
        .value("--model")
        .map(|model| normalize_model_id_for_provider(&provider, model.trim()));
    options.reasoning_effort = matches
        .value("--reasoning-effort")
        .map(|value| {
            parse_option_update(value, |value| {
                value
                    .parse::<ProviderReasoningEffort>()
                    .map_err(|detail| CliParseError::Parse { detail })
            })
        })
        .transpose()?;
    options.reasoning_budget_tokens = matches
        .value("--reasoning-budget-tokens")
        .map(|value| {
            parse_option_update(value, |value| {
                value.parse::<u32>().map_err(|source| CliParseError::Parse {
                    detail: format!("invalid reasoning budget `{value}`: {source}"),
                })
            })
        })
        .transpose()?;
    options.text_verbosity = matches
        .value("--text-verbosity")
        .map(|value| {
            parse_option_update(value, |value| {
                value
                    .parse::<ProviderTextVerbosity>()
                    .map_err(|detail| CliParseError::Parse { detail })
            })
        })
        .transpose()?;
    options.vendor_options = matches
        .values("--vendor-option")
        .map(parse_vendor_option)
        .collect::<Result<Vec<_>, _>>()?;
    options.clear_vendor_options = matches
        .values("--clear-vendor-option")
        .map(|value| {
            let value = value.trim();
            if value.is_empty() {
                Err(CliParseError::Parse {
                    detail: "--clear-vendor-option requires a key".to_string(),
                })
            } else {
                Ok(value.to_string())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if options.reasoning_effort.is_none()
        && options.reasoning_budget_tokens.is_none()
        && options.text_verbosity.is_none()
        && options.vendor_options.is_empty()
        && options.clear_vendor_options.is_empty()
    {
        return Err(CliParseError::Parse {
            detail: "no model options were provided".to_string(),
        });
    }
    Ok(options)
}

fn parse_option_update<T>(
    value: &str,
    parse: impl FnOnce(&str) -> Result<T, CliParseError>,
) -> Result<ModelOptionValueUpdate<T>, CliParseError> {
    match value.trim() {
        "clear" | "unset" | "default" => Ok(ModelOptionValueUpdate::Clear),
        value => parse(value).map(ModelOptionValueUpdate::Set),
    }
}

fn parse_vendor_option(value: &str) -> Result<(String, String), CliParseError> {
    let Some((key, value)) = value.split_once('=') else {
        return Err(CliParseError::Parse {
            detail: "--vendor-option requires key=value".to_string(),
        });
    };
    let key = key.trim();
    if key.is_empty() {
        return Err(CliParseError::Parse {
            detail: "--vendor-option requires a non-empty key".to_string(),
        });
    }
    Ok((key.to_string(), value.trim().to_string()))
}

fn parse_model_runtime_set_matches(
    matches: &CliCommandMatches,
) -> Result<ModelRuntimeSetOptions, CliParseError> {
    let mut options = ModelRuntimeSetOptions::default();
    options.native_models_cache_dir = trimmed_match_value(matches, "--native-models-cache-dir");
    options.minicpm_v46_model_dir = trimmed_match_value(matches, "--minicpm-v46-model-dir");
    options.minicpm_v46_max_input_tokens = matches
        .value("--minicpm-v46-max-input-tokens")
        .map(|value| {
            value.parse().map_err(|source| CliParseError::Parse {
                detail: format!("invalid --minicpm-v46-max-input-tokens `{value}`: {source}"),
            })
        })
        .transpose()?;
    options.minicpm_v46_max_output_tokens = matches
        .value("--minicpm-v46-max-output-tokens")
        .map(|value| {
            value.parse().map_err(|source| CliParseError::Parse {
                detail: format!("invalid --minicpm-v46-max-output-tokens `{value}`: {source}"),
            })
        })
        .transpose()?;
    options.pytorch_endpoint = trimmed_match_value(matches, "--pytorch-endpoint");
    options.pytorch_python = trimmed_match_value(matches, "--pytorch-python");
    options.pytorch_device = matches.value("--pytorch-device").map(str::to_string);
    options.pytorch_timeout_seconds = matches
        .value("--pytorch-timeout-seconds")
        .map(|value| {
            value.parse().map_err(|source| CliParseError::Parse {
                detail: format!("invalid --pytorch-timeout-seconds `{value}`: {source}"),
            })
        })
        .transpose()?;
    options.pytorch_mock = matches
        .value("--pytorch-mock")
        .map(parse_bool_flag_value)
        .transpose()?;
    Ok(options)
}

fn parse_bool_flag_value(value: &str) -> Result<bool, CliParseError> {
    match value {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        _ => Err(CliParseError::Parse {
            detail: format!("invalid boolean value `{value}`"),
        }),
    }
}

fn required_match_value<'a>(
    matches: &'a CliCommandMatches,
    option: &str,
) -> Result<&'a str, CliParseError> {
    matches.value(option).ok_or_else(|| CliParseError::Parse {
        detail: format!("declarative parser omitted required option `{option}`"),
    })
}

fn trimmed_match_value(matches: &CliCommandMatches, option: &str) -> Option<String> {
    matches.value(option).map(str::trim).map(str::to_string)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UsageOptions {
    pub(crate) provider: Option<String>,
    pub(crate) json: bool,
}

pub(crate) fn usage_cli_command_handler() -> DeclarativeCommandHandler<UsageOptions> {
    DeclarativeCommandHandler::new(
        "usage",
        CliCommandPolicy::local_capable(),
        usage_cli_spec,
        parse_usage_matches,
        usage,
    )
}

fn usage_cli_spec() -> CliCommandSpec {
    CliCommandSpec::new("usage", "show provider subscription usage").with_options([
        CliOptionSpec::value(
            "--provider",
            "provider id",
            CliValueSpec::new("provider", "provider id").with_choices(
                subscription_usage_provider_ids()
                    .iter()
                    .map(|provider| CliAutocompleteChoice::new(*provider, "usage provider")),
            ),
        ),
        CliOptionSpec::flag("--json", "print JSON output"),
    ])
}

fn parse_usage_matches(matches: &CliCommandMatches) -> Result<UsageOptions, CliParseError> {
    Ok(UsageOptions {
        provider: matches.value("--provider").map(str::to_string),
        json: matches.contains("--json"),
    })
}

#[cfg(test)]
pub(crate) fn parse_usage_schema_for_test(args: &[String]) -> Result<UsageOptions, CliError> {
    parse_schema_command_for_test(usage_cli_spec(), args, parse_usage_matches)
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

fn run_models_cli_command(
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
    };
    result.map_err(|error| CliCommandError::new(error.to_string()))
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

pub(crate) fn model_config_cluster_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    let _ = paths;
    if let Some(status) = default_main_daemon_status()? {
        match daemon_fetch_cluster_kv_authoritative_snapshot(
            &status.address,
            MODEL_CONFIG_CLUSTER_SNAPSHOT_TIMEOUT,
        ) {
            Ok(snapshot) => return Ok(snapshot.value),
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
        return sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .map_err(sinomo_runtime::network::NetworkError::directory)
            .map_err(Into::into);
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
