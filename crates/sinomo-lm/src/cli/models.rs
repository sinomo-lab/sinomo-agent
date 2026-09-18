//! Model command grammar; output and backend execution belong to the caller.
use crate::{
    DEFAULT_PROVIDER_ACCOUNT, LM_STUDIO_PROVIDER_ID, OLLAMA_PROVIDER_ID, ProviderReasoningEffort,
    ProviderTextVerbosity, default_model_provider_ids, normalize_model_id_for_provider,
    normalize_model_provider, normalize_provider_account_name,
};
use sinomo_runtime::cli_registry::CliCommand as RegisteredCommand;
use sinomo_runtime::cli_registry::*;
pub type ModelCommandRunner =
    fn(ModelsCliCommand, &mut CliCommandContext<'_>) -> Result<(), CliCommandError>;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelDefaultOptions {
    pub provider: String,
    pub account: String,
    pub model: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ModelsCliCommand {
    Overview,
    Help(CliCommandHelp),
    List(ModelListOptions),
    Default(ModelDefaultOptions),
    Options(ModelOptionsCliCommand),
    Runtime(ModelRuntimeCliCommand),
    Pull(LocalModelActionOptions),
    Load(LocalModelActionOptions),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelListOptions {
    pub provider: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalModelActionOptions {
    pub provider: String,
    pub account: String,
    pub model: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ModelOptionsCliCommand {
    Show(ModelOptionsTarget),
    Set(ModelOptionsSetOptions),
    Clear(ModelOptionsTarget),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelOptionsTarget {
    pub provider: Option<String>,
    pub account: String,
    pub model: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelOptionsSetOptions {
    pub target: ModelOptionsTarget,
    pub reasoning_effort: Option<ModelOptionValueUpdate<ProviderReasoningEffort>>,
    pub reasoning_budget_tokens: Option<ModelOptionValueUpdate<u32>>,
    pub text_verbosity: Option<ModelOptionValueUpdate<ProviderTextVerbosity>>,
    pub max_context_tokens: Option<ModelOptionValueUpdate<u32>>,
    pub auto_compact_token_limit: Option<ModelOptionValueUpdate<u32>>,
    pub vendor_options: Vec<(String, String)>,
    pub clear_vendor_options: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelOptionValueUpdate<T> {
    Set(T),
    Clear,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ModelRuntimeCliCommand {
    Show,
    Set(ModelRuntimeSetOptions),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelRuntimeSetOptions {
    pub native_models_cache_dir: Option<String>,
    pub minicpm_v46_model_dir: Option<String>,
    pub minicpm_v46_max_input_tokens: Option<usize>,
    pub minicpm_v46_max_output_tokens: Option<usize>,
    pub pytorch_endpoint: Option<String>,
    pub pytorch_python: Option<String>,
    pub pytorch_device: Option<String>,
    pub pytorch_timeout_seconds: Option<u64>,
    pub pytorch_mock: Option<bool>,
}

const MODELS_COMMAND_NAME: &str = "models";

const MODELS_POLICY: CliCommandPolicy = CliCommandPolicy::local_capable();

#[derive(Clone, Copy, Debug)]
pub struct ModelsCliCommandHandler {
    run: ModelCommandRunner,
}

pub fn models_cli_command_handler(run: ModelCommandRunner) -> ModelsCliCommandHandler {
    ModelsCliCommandHandler { run }
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
            run: self.run,
        }))
    }
}

#[derive(Debug)]
struct ModelsCliCommandRunner {
    pub command: ModelsCliCommand,
    run: ModelCommandRunner,
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
        match self.command {
            ModelsCliCommand::Help(help) => help.write(context),
            command => (self.run)(command, context),
        }
    }
}

pub fn models_cli_spec() -> CliCommandSpec {
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
                            "none|minimal|low|medium|high|xhigh|max|ultra|clear",
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
                            choice("ultra", "Codex ultra reasoning selection"),
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
                        "--max-context-tokens",
                        "maximum model context token cap",
                        CliValueSpec::new("n|clear", "context limit or clear"),
                    ),
                    CliOptionSpec::value(
                        "--auto-compact-token-limit",
                        "automatic history compaction threshold",
                        CliValueSpec::new("n|clear", "compaction limit or clear"),
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

    let local_model_action_options = || {
        [
            CliOptionSpec::value(
                "--provider",
                "local model provider",
                CliValueSpec::new("ollama|lmstudio", "local model provider")
                    .with_choices([
                        choice(OLLAMA_PROVIDER_ID, "Ollama local runtime"),
                        choice(LM_STUDIO_PROVIDER_ID, "LM Studio local runtime"),
                        choice("lm-studio", "LM Studio local runtime alias"),
                    ])
                    .restricted(),
            )
            .required(),
            account_option(),
            model_option().required(),
        ]
    };

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
        CliCommandSpec::new("pull", "download a model into a managed local runtime")
            .with_options(local_model_action_options()),
        CliCommandSpec::new("load", "load or warm a model in a local runtime")
            .with_options(local_model_action_options()),
    ])
    .with_after_help(
        std::iter::once("Providers:".to_string()).chain(
            default_model_provider_ids()
                .iter()
                .map(|provider| format!("  {provider}")),
        ),
    )
}

pub fn parse_models_command(args: &[String]) -> Result<ModelsCliCommand, CliParseError> {
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
        ["models", "pull"] => Ok(ModelsCliCommand::Pull(parse_local_model_action_matches(
            &matches,
        )?)),
        ["models", "load"] => Ok(ModelsCliCommand::Load(parse_local_model_action_matches(
            &matches,
        )?)),
        _ => Err(CliParseError::Parse {
            detail: format!("unhandled models command path `{}`", path.join(" ")),
        }),
    }
}

fn parse_local_model_action_matches(
    matches: &CliCommandMatches,
) -> Result<LocalModelActionOptions, CliParseError> {
    let raw_provider = required_match_value(matches, "--provider")?;
    let provider = normalize_model_provider(raw_provider).ok_or_else(|| CliParseError::Parse {
        detail: format!("unknown provider `{raw_provider}`"),
    })?;
    if provider != OLLAMA_PROVIDER_ID && provider != LM_STUDIO_PROVIDER_ID {
        return Err(CliParseError::Parse {
            detail: format!("provider `{provider}` is not a local model runtime"),
        });
    }
    Ok(LocalModelActionOptions {
        provider,
        account: normalize_provider_account_name(matches.value("--account")),
        model: required_match_value(matches, "--model")?.trim().to_string(),
    })
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
    options.max_context_tokens = matches
        .value("--max-context-tokens")
        .map(|value| parse_u32_option_update(value, "maximum context tokens"))
        .transpose()?;
    options.auto_compact_token_limit = matches
        .value("--auto-compact-token-limit")
        .map(|value| parse_u32_option_update(value, "automatic compaction token limit"))
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
        && options.max_context_tokens.is_none()
        && options.auto_compact_token_limit.is_none()
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

fn parse_u32_option_update(
    value: &str,
    label: &str,
) -> Result<ModelOptionValueUpdate<u32>, CliParseError> {
    parse_option_update(value, |value| {
        let parsed = value
            .parse::<u32>()
            .map_err(|source| CliParseError::Parse {
                detail: format!("invalid {label} `{value}`: {source}"),
            })?;
        if parsed == 0 {
            return Err(CliParseError::Parse {
                detail: format!("{label} must be greater than zero"),
            });
        }
        Ok(parsed)
    })
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
    Ok(ModelRuntimeSetOptions {
        native_models_cache_dir: trimmed_match_value(matches, "--native-models-cache-dir"),
        minicpm_v46_model_dir: trimmed_match_value(matches, "--minicpm-v46-model-dir"),
        minicpm_v46_max_input_tokens: matches
            .value("--minicpm-v46-max-input-tokens")
            .map(|value| {
                value.parse().map_err(|source| CliParseError::Parse {
                    detail: format!("invalid --minicpm-v46-max-input-tokens `{value}`: {source}"),
                })
            })
            .transpose()?,
        minicpm_v46_max_output_tokens: matches
            .value("--minicpm-v46-max-output-tokens")
            .map(|value| {
                value.parse().map_err(|source| CliParseError::Parse {
                    detail: format!("invalid --minicpm-v46-max-output-tokens `{value}`: {source}"),
                })
            })
            .transpose()?,
        pytorch_endpoint: trimmed_match_value(matches, "--pytorch-endpoint"),
        pytorch_python: trimmed_match_value(matches, "--pytorch-python"),
        pytorch_device: matches.value("--pytorch-device").map(str::to_string),
        pytorch_timeout_seconds: matches
            .value("--pytorch-timeout-seconds")
            .map(|value| {
                value.parse().map_err(|source| CliParseError::Parse {
                    detail: format!("invalid --pytorch-timeout-seconds `{value}`: {source}"),
                })
            })
            .transpose()?,
        pytorch_mock: matches
            .value("--pytorch-mock")
            .map(parse_bool_flag_value)
            .transpose()?,
    })
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
