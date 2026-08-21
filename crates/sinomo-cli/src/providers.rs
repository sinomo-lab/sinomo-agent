use super::*;
use sinomo_runtime::cli_registry::{
    CliAutocompleteChoice, CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliOptionSpec,
    CliParseError, CliPositionalSpec, CliValueSpec,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProviderName {
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
pub(super) enum ProviderCliCommand {
    Auth(ProviderCommandOptions),
    Test(ProviderCommandOptions),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProviderCommandOptions {
    pub(super) provider: ProviderName,
    pub(super) account: String,
}

pub(super) fn providers_cli_command_handler() -> DeclarativeCommandHandler<ProviderCliCommand> {
    DeclarativeCommandHandler::new(
        "providers",
        CliCommandPolicy::local_capable(),
        providers_cli_spec,
        parse_provider_matches,
        run_provider_command,
    )
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
    let provider = match required_positional(matches, 0)? {
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

#[cfg(test)]
pub(super) fn parse_provider_schema_for_test(
    args: &[String],
) -> Result<ProviderCliCommand, CliError> {
    parse_schema_command_for_test(providers_cli_spec(), args, parse_provider_matches)
}

pub(super) fn run_provider_command(command: ProviderCliCommand) -> Result<(), CliError> {
    match command {
        ProviderCliCommand::Auth(options) => match options.provider {
            ProviderName::GitHubCopilot => auth_github_copilot_account(&options.account),
            ProviderName::Codex => auth_codex_account(&options.account),
            ProviderName::Claude => auth_claude(),
            ProviderName::Anthropic => auth_api_key_provider(
                "Anthropic",
                AnthropicProvider::PROVIDER_ID,
                &options.account,
            ),
            ProviderName::OpenAi => {
                auth_api_key_provider("OpenAI", OpenAiProvider::PROVIDER_ID, &options.account)
            }
            ProviderName::DeepSeek => {
                auth_api_key_provider("DeepSeek", DeepSeekProvider::PROVIDER_ID, &options.account)
            }
            ProviderName::Gemini => {
                auth_api_key_provider("Gemini", GeminiProvider::PROVIDER_ID, &options.account)
            }
            ProviderName::Nvidia => {
                auth_api_key_provider("NVIDIA", NvidiaProvider::PROVIDER_ID, &options.account)
            }
        },
        ProviderCliCommand::Test(options) => match options.provider {
            ProviderName::GitHubCopilot => test_github_copilot(),
            ProviderName::Codex => test_codex(),
            ProviderName::Claude => test_claude(),
            ProviderName::Anthropic => test_api_key_provider(
                "Anthropic",
                AnthropicProvider::PROVIDER_ID,
                &options.account,
            ),
            ProviderName::OpenAi => {
                test_api_key_provider("OpenAI", OpenAiProvider::PROVIDER_ID, &options.account)
            }
            ProviderName::DeepSeek => {
                test_api_key_provider("DeepSeek", DeepSeekProvider::PROVIDER_ID, &options.account)
            }
            ProviderName::Gemini => {
                test_api_key_provider("Gemini", GeminiProvider::PROVIDER_ID, &options.account)
            }
            ProviderName::Nvidia => {
                test_api_key_provider("NVIDIA", NvidiaProvider::PROVIDER_ID, &options.account)
            }
        },
    }
}

/// The user-facing prompt for an OAuth device-login flow.
pub struct DeviceLoginPrompt {
    pub verification_uri: String,
    pub user_code: String,
}

/// Whether `provider` authenticates via the OAuth device-login flow rather than an API key.
pub fn provider_uses_device_login(provider: &str) -> bool {
    matches!(provider, "codex" | "github-copilot")
}

/// Whether a device-login provider has stored credentials (cheap local check; no network).
pub fn provider_signed_in(provider: &str) -> bool {
    let path = match provider {
        "codex" => codex_provider().ok().map(|(_, path)| path),
        "claude" => claude_provider().ok().map(|(_, path)| path),
        "github-copilot" => github_copilot_provider().ok().map(|(_, path)| path),
        _ => None,
    };
    match provider {
        "claude" => claude_provider()
            .ok()
            .map(|(provider, _)| provider.has_oauth_tokens())
            .unwrap_or(false),
        _ => path.is_some_and(|path| {
            std::fs::metadata(&path)
                .map(|meta| meta.len() > 0)
                .unwrap_or(false)
        }),
    }
}

/// Run a provider's OAuth device-login flow: start it (calling `on_prompt` with the URL + user
/// code the user must enter), then poll until authorized or expired, saving credentials on
/// success. Blocks while polling — run it off the UI thread.
pub fn run_device_login(
    provider: &str,
    on_prompt: impl FnOnce(DeviceLoginPrompt),
) -> Result<(), CliError> {
    match provider {
        "codex" => run_codex_device_login(on_prompt),
        "github-copilot" => run_github_copilot_device_login(on_prompt),
        other => Err(CliError::AuthProvider {
            detail: format!("provider `{other}` does not use device login"),
        }),
    }
}

fn run_codex_device_login(on_prompt: impl FnOnce(DeviceLoginPrompt)) -> Result<(), CliError> {
    let (provider, _auth_path) = codex_provider()?;
    let device = provider.device_login_start()?;
    on_prompt(DeviceLoginPrompt {
        verification_uri: device.verification_uri.clone(),
        user_code: device.user_code.clone(),
    });

    let started = Instant::now();
    let expires_in = Duration::from_secs(device.expires_in);
    let interval = Duration::from_secs(device.interval.max(5));
    loop {
        if started.elapsed() >= expires_in {
            return Err(CliError::AuthExpired);
        }
        thread::sleep(interval);
        let poll = provider.device_login_poll(&device)?;
        if poll.tokens.is_some() {
            test_provider_credentials(CODEX_PROVIDER_ID)?;
            return Ok(());
        }
    }
}

fn run_github_copilot_device_login(
    on_prompt: impl FnOnce(DeviceLoginPrompt),
) -> Result<(), CliError> {
    let (provider, _credentials_path) = github_copilot_provider()?;
    let device = provider.device_login_start()?;
    on_prompt(DeviceLoginPrompt {
        verification_uri: device.verification_uri.clone(),
        user_code: device.user_code.clone(),
    });

    let started = Instant::now();
    let expires_in = if device.expires_in == 0 {
        Duration::from_secs(900)
    } else {
        Duration::from_secs(device.expires_in)
    };
    let mut interval = Duration::from_secs(device.interval.max(5));
    loop {
        if started.elapsed() >= expires_in {
            return Err(CliError::AuthExpired);
        }
        thread::sleep(interval);
        let poll = provider.device_login_poll(&device.device_code)?;
        if poll.access_token.is_some() {
            test_provider_credentials(GitHubCopilotProvider::PROVIDER_ID)?;
            return Ok(());
        }
        match poll.error.as_deref() {
            Some("authorization_pending") | None => {}
            Some("slow_down") => {
                interval += Duration::from_secs(poll.interval.unwrap_or(5));
            }
            Some("expired_token") => return Err(CliError::AuthExpired),
            Some("access_denied") => return Err(CliError::AuthDenied),
            Some(error) => {
                return Err(CliError::AuthProvider {
                    detail: poll.error_description.unwrap_or_else(|| error.to_string()),
                });
            }
        }
    }
}

fn print_device_prompt(prompt: DeviceLoginPrompt) {
    println!("Open this URL in your browser:");
    println!("{}", prompt.verification_uri);
    println!();
    println!("Enter this code:");
    println!("{}", prompt.user_code);
    println!();
    println!("Waiting for authorization...");
}

pub fn auth_github_copilot() -> Result<(), CliError> {
    auth_github_copilot_account(DEFAULT_PROVIDER_ACCOUNT)
}

fn auth_github_copilot_account(account: &str) -> Result<(), CliError> {
    run_github_copilot_device_login(print_device_prompt)?;
    let config_path =
        register_device_login_provider_account(GitHubCopilotProvider::PROVIDER_ID, account)?;
    println!(
        "GitHub Copilot credentials saved; account `{account}` registered in {}.",
        config_path.display()
    );
    Ok(())
}

pub fn test_github_copilot() -> Result<(), CliError> {
    let result = test_provider_credentials(GitHubCopilotProvider::PROVIDER_ID)?;
    println!(
        "Testing GitHub Copilot credentials at {}",
        result.credentials_path.display()
    );
    println!("GitHub token: ok");
    println!("Copilot API models endpoint: ok");
    println!("GitHub Copilot provider connection: ok");
    Ok(())
}

pub fn auth_anthropic() -> Result<(), CliError> {
    auth_api_key_provider(
        "Anthropic",
        AnthropicProvider::PROVIDER_ID,
        DEFAULT_PROVIDER_ACCOUNT,
    )
}

pub fn test_anthropic() -> Result<(), CliError> {
    test_api_key_provider(
        "Anthropic",
        AnthropicProvider::PROVIDER_ID,
        DEFAULT_PROVIDER_ACCOUNT,
    )
}

pub fn auth_openai() -> Result<(), CliError> {
    auth_api_key_provider(
        "OpenAI",
        OpenAiProvider::PROVIDER_ID,
        DEFAULT_PROVIDER_ACCOUNT,
    )
}

pub fn test_openai() -> Result<(), CliError> {
    test_api_key_provider(
        "OpenAI",
        OpenAiProvider::PROVIDER_ID,
        DEFAULT_PROVIDER_ACCOUNT,
    )
}

pub fn auth_codex() -> Result<(), CliError> {
    auth_codex_account(DEFAULT_PROVIDER_ACCOUNT)
}

fn auth_codex_account(account: &str) -> Result<(), CliError> {
    run_codex_device_login(print_device_prompt)?;
    let config_path = register_device_login_provider_account(CODEX_PROVIDER_ID, account)?;
    println!(
        "Codex credentials saved; account `{account}` registered in {}.",
        config_path.display()
    );
    Ok(())
}

fn register_device_login_provider_account(
    provider: &str,
    account: &str,
) -> Result<PathBuf, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let account = normalize_provider_account_name(Some(account));
    let (mut config, config_path) = load_model_config_for_node(&data_dir)?;
    if config.accounts.iter().any(|candidate| {
        candidate.provider == provider
            && normalize_provider_account_name(Some(&candidate.account)) == account
    }) {
        return Ok(config_path);
    }
    upsert_provider_account_config(
        &mut config,
        ProviderAccountConfig {
            provider: provider.to_string(),
            account: account.clone(),
            default_model: None,
            api_base_url: None,
            api_key: None,
            enabled: true,
            options: LanguageModelProviderOptions::default(),
            model_options: Vec::new(),
        },
    )
    .map_err(CliError::LanguageModel)?;
    if config.default_provider.is_none() {
        config.default_provider = Some(provider.to_string());
        config.default_account = Some(account);
        config.default_model = None;
    }
    write_model_config_for_node(&data_dir, &config)
}

pub fn auth_claude() -> Result<(), CliError> {
    let (provider, _credentials_path) = claude_provider()?;
    let login = provider.begin_pkce_login()?;
    println!("Open this URL in your browser:");
    println!("{}", login.authorization_url);
    println!();
    println!("After authorizing, paste the returned code here.");
    println!("The code should include the state suffix after `#`.");
    print!("Authorization code: ");
    std::io::Write::flush(&mut std::io::stdout()).ok();

    let mut authorization_code = String::new();
    std::io::stdin()
        .read_line(&mut authorization_code)
        .map_err(|source| CliError::AuthProvider {
            detail: source.to_string(),
        })?;
    provider.complete_pkce_login(&login, &authorization_code)?;
    test_provider_credentials(ClaudeProvider::PROVIDER_ID)?;
    println!("Claude credentials saved.");
    Ok(())
}

pub fn test_codex() -> Result<(), CliError> {
    let result = test_provider_credentials(CODEX_PROVIDER_ID)?;
    println!(
        "Testing Codex credentials at {}",
        result.credentials_path.display()
    );
    println!("Codex ChatGPT auth: ok");
    println!("Codex provider connection: ok");
    Ok(())
}

pub fn test_claude() -> Result<(), CliError> {
    let result = test_provider_credentials(ClaudeProvider::PROVIDER_ID)?;
    println!(
        "Testing Claude credentials at {}",
        result.credentials_path.display()
    );
    println!("Claude subscription auth: ok");
    println!("Claude provider connection: ok");
    Ok(())
}

pub fn auth_gemini() -> Result<(), CliError> {
    auth_api_key_provider(
        "Gemini",
        GeminiProvider::PROVIDER_ID,
        DEFAULT_PROVIDER_ACCOUNT,
    )
}

pub fn test_gemini() -> Result<(), CliError> {
    test_api_key_provider(
        "Gemini",
        GeminiProvider::PROVIDER_ID,
        DEFAULT_PROVIDER_ACCOUNT,
    )
}

pub fn auth_nvidia() -> Result<(), CliError> {
    auth_api_key_provider(
        "NVIDIA",
        NvidiaProvider::PROVIDER_ID,
        DEFAULT_PROVIDER_ACCOUNT,
    )
}

pub fn test_nvidia() -> Result<(), CliError> {
    test_api_key_provider(
        "NVIDIA",
        NvidiaProvider::PROVIDER_ID,
        DEFAULT_PROVIDER_ACCOUNT,
    )
}

fn auth_api_key_provider(
    provider_name: &str,
    provider_id: &str,
    account: &str,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let (api_key, source) = api_key_from_env_or_stored_or_prompt(provider_name, provider_id, true)?;
    let (mut config, _) = load_model_config_for_node(&data_dir)?;
    let account_info =
        save_provider_api_key_in_model_config(&mut config, provider_id, Some(account), &api_key)?;
    println!("Testing {provider_name} provider connection...");
    let account_config = provider_account_config(&config, provider_id, Some(account));
    test_provider_account_credentials(provider_id, account_config)?;
    let config_path = write_model_config_for_node(&data_dir, &config)?;
    let source_label = if model_config_uses_cluster(&data_dir) {
        LM_PROVIDER_CONFIG_KV_KEY
    } else {
        LM_PROVIDER_CONFIG_KEY
    };
    println!(
        "{provider_name} account `{}` saved to {} ({source_label}; credential source: {source})",
        account_info.account,
        config_path.display(),
    );
    Ok(())
}

fn test_api_key_provider(
    provider_name: &str,
    provider_id: &str,
    account: &str,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let (config, config_path) = load_model_config_for_node(&data_dir)?;
    let account_config =
        provider_account_config(&config, provider_id, Some(account)).ok_or_else(|| {
            CliError::AuthProvider {
                detail: format!(
                    "{provider_name} account `{account}` is not configured in lm.providers"
                ),
            }
        })?;
    let source_label = if model_config_uses_cluster(&data_dir) {
        LM_PROVIDER_CONFIG_KV_KEY
    } else {
        LM_PROVIDER_CONFIG_KEY
    };
    println!(
        "Testing {provider_name} account `{account}` from {} ({source_label})",
        config_path.display(),
    );
    test_provider_account_credentials(provider_id, Some(account_config))?;
    println!("{provider_name} API key: ok");
    println!("{provider_name} provider connection: ok");
    Ok(())
}

fn api_key_from_env_or_stored_or_prompt(
    provider_name: &str,
    provider_id: &str,
    allow_stored: bool,
) -> Result<(String, String), CliError> {
    if let Some(api_key) = provider_api_key_from_env(provider_id) {
        return Ok(api_key);
    }
    if allow_stored && let Some(api_key) = stored_provider_api_key(provider_id)? {
        return Ok((api_key, "stored-local-credential".to_string()));
    }

    println!("No {provider_name} API key found in environment. Paste an API key to store locally.");
    println!("Input will be visible in this terminal.");
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|source| CliError::AuthProvider {
            detail: format!("failed to read API key: {source}"),
        })?;
    let api_key = line.trim();
    if api_key.is_empty() {
        return Err(CliError::AuthProvider {
            detail: format!("{provider_name} API key was empty"),
        });
    }
    Ok((api_key.to_string(), "prompt".to_string()))
}
