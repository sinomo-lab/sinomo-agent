//! Model, provider, usage, and realtime command registration.
pub mod models;
pub mod voice;
use crate::{normalize_provider_account_name, subscription_usage_provider_ids};
use sinomo_runtime::cli_registry::{
    CliAutocompleteChoice, CliCommandMatches, CliCommandParseOutcome, CliCommandPolicy,
    CliCommandRegistry, CliCommandSpec, CliOptionSpec, CliParseError, CliPositionalSpec,
    CliValueSpec, DeclarativeCommandHandler,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderName {
    GitHubCopilot,
    Codex,
    Claude,
    Anthropic,
    OpenAi,
    DeepSeek,
    Gemini,
    Nvidia,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderCliCommand {
    Auth(ProviderCommandOptions),
    Test(ProviderCommandOptions),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCommandOptions {
    pub provider: ProviderName,
    pub account: String,
}

fn provider_value() -> CliValueSpec {
    CliValueSpec::new("provider", "provider id")
        .with_choices([
            CliAutocompleteChoice::new("github-copilot", "GitHub Copilot provider"),
            CliAutocompleteChoice::new("codex", "Codex CLI provider"),
            CliAutocompleteChoice::new("claude", "Claude CLI provider"),
            CliAutocompleteChoice::new("anthropic", "Anthropic API provider"),
            CliAutocompleteChoice::new("openai", "OpenAI API provider"),
            CliAutocompleteChoice::new("deepseek", "DeepSeek API provider"),
            CliAutocompleteChoice::new("gemini", "Gemini API provider"),
            CliAutocompleteChoice::new("nvidia", "NVIDIA API provider"),
        ])
        .restricted()
}

fn providers_cli_spec() -> CliCommandSpec {
    let action = |name: &'static str, about: &'static str| {
        CliCommandSpec::new(name, about)
            .with_options([CliOptionSpec::value(
                "--account",
                "provider account",
                CliValueSpec::new("account", "provider account"),
            )])
            .with_positionals([CliPositionalSpec::required(provider_value())])
    };
    CliCommandSpec::new("providers", "manage model provider credentials")
        .help_on_empty()
        .with_children([
            action("auth", "store or refresh provider credentials"),
            action("test", "test configured provider credentials"),
        ])
}

fn parse_provider_matches(
    matches: &CliCommandMatches,
) -> Result<ProviderCliCommand, CliParseError> {
    let provider = match matches
        .positionals()
        .first()
        .map(String::as_str)
        .ok_or_else(|| CliParseError::Parse {
            detail: "provider is required".into(),
        })? {
        "github-copilot" => ProviderName::GitHubCopilot,
        "codex" => ProviderName::Codex,
        "claude" => ProviderName::Claude,
        "anthropic" => ProviderName::Anthropic,
        "openai" => ProviderName::OpenAi,
        "deepseek" => ProviderName::DeepSeek,
        "gemini" => ProviderName::Gemini,
        "nvidia" => ProviderName::Nvidia,
        value => {
            return Err(CliParseError::Parse {
                detail: format!("unknown provider `{value}`"),
            });
        }
    };
    let account = normalize_provider_account_name(matches.value("--account"));
    let options = ProviderCommandOptions { provider, account };
    match matches.command_name() {
        "auth" => Ok(ProviderCliCommand::Auth(options)),
        "test" => Ok(ProviderCliCommand::Test(options)),
        _ => Err(CliParseError::Parse {
            detail: "providers requires a subcommand".to_string(),
        }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsageOptions {
    pub provider: Option<String>,
    pub json: bool,
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
pub fn register_cli_commands<E: Into<sinomo_runtime::cli_registry::CliCommandError> + 'static>(
    registry: CliCommandRegistry,
    run_provider: fn(ProviderCliCommand) -> Result<(), E>,
    run_usage: fn(UsageOptions) -> Result<(), E>,
    run_models: models::ModelCommandRunner,
    run_voice: fn(voice::VoiceCliCommand) -> Result<(), E>,
) -> Result<CliCommandRegistry, String> {
    registry
        .try_register(DeclarativeCommandHandler::new(
            "providers",
            CliCommandPolicy::local_capable(),
            providers_cli_spec,
            parse_provider_matches,
            run_provider,
        ))?
        .try_register(DeclarativeCommandHandler::new(
            "usage",
            CliCommandPolicy::local_capable(),
            usage_cli_spec,
            parse_usage_matches,
            run_usage,
        ))?
        .try_register(models::models_cli_command_handler(run_models))?
        .try_register(voice::voice_cli_command_handler(run_voice))
}

pub fn parse_provider_command(args: &[String]) -> Result<ProviderCliCommand, CliParseError> {
    match providers_cli_spec().parse(args)? {
        CliCommandParseOutcome::Matches(matches) => parse_provider_matches(&matches),
        CliCommandParseOutcome::Help(_) => Err(CliParseError::Parse {
            detail: "help has no provider invocation".into(),
        }),
    }
}
pub fn parse_usage_command(args: &[String]) -> Result<UsageOptions, CliParseError> {
    match usage_cli_spec().parse(args)? {
        CliCommandParseOutcome::Matches(matches) => parse_usage_matches(&matches),
        CliCommandParseOutcome::Help(_) => Err(CliParseError::Parse {
            detail: "help has no usage invocation".into(),
        }),
    }
}
