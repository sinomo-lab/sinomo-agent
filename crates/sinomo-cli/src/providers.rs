use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProviderName {
    GitHubCopilot,
    Codex,
    Anthropic,
    OpenAi,
    Gemini,
    Nvidia,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProviderCliCommand {
    Help,
    Auth(ProviderName),
    Test(ProviderName),
}

pub(super) fn parse_provider_command(args: &[String]) -> Result<ProviderCliCommand, CliError> {
    match args {
        [] => return Ok(ProviderCliCommand::Help),
        [command] if matches!(command.as_str(), "help" | "--help" | "-h") => {
            return Ok(ProviderCliCommand::Help);
        }
        _ => {}
    }

    let [action, provider] = args else {
        return Err(CliError::Usage);
    };

    if !matches!(action.as_str(), "auth" | "test") {
        return Err(CliError::Usage);
    }

    let provider = match provider.as_str() {
        "github-copilot" => ProviderName::GitHubCopilot,
        "codex" => ProviderName::Codex,
        "anthropic" => ProviderName::Anthropic,
        "openai" => ProviderName::OpenAi,
        "gemini" => ProviderName::Gemini,
        "nvidia" => ProviderName::Nvidia,
        _ => {
            return Err(CliError::Parse {
                detail: format!("unknown provider `{provider}`"),
            });
        }
    };

    match action.as_str() {
        "auth" => Ok(ProviderCliCommand::Auth(provider)),
        "test" => Ok(ProviderCliCommand::Test(provider)),
        _ => unreachable!("provider action was checked before provider parsing"),
    }
}

pub(super) fn run_provider_command(command: ProviderCliCommand) -> Result<(), CliError> {
    match command {
        ProviderCliCommand::Help => {
            print_providers_help();
            Ok(())
        }
        ProviderCliCommand::Auth(ProviderName::GitHubCopilot) => auth_github_copilot(),
        ProviderCliCommand::Test(ProviderName::GitHubCopilot) => test_github_copilot(),
        ProviderCliCommand::Auth(ProviderName::Codex) => auth_codex(),
        ProviderCliCommand::Test(ProviderName::Codex) => test_codex(),
        ProviderCliCommand::Auth(ProviderName::Anthropic) => auth_anthropic(),
        ProviderCliCommand::Test(ProviderName::Anthropic) => test_anthropic(),
        ProviderCliCommand::Auth(ProviderName::OpenAi) => auth_openai(),
        ProviderCliCommand::Test(ProviderName::OpenAi) => test_openai(),
        ProviderCliCommand::Auth(ProviderName::Gemini) => auth_gemini(),
        ProviderCliCommand::Test(ProviderName::Gemini) => test_gemini(),
        ProviderCliCommand::Auth(ProviderName::Nvidia) => auth_nvidia(),
        ProviderCliCommand::Test(ProviderName::Nvidia) => test_nvidia(),
    }
}

pub fn auth_github_copilot() -> Result<(), CliError> {
    let (provider, credentials_path) = github_copilot_provider()?;
    let device = provider.device_login_start()?;

    println!("Open this URL in your browser:");
    println!("{}", device.verification_uri);
    println!();
    println!("Enter this code:");
    println!("{}", device.user_code);
    println!();
    println!("Waiting for GitHub authorization...");

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
            println!(
                "GitHub Copilot credentials saved to {}",
                credentials_path.display()
            );
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
    let (api_key, source) =
        api_key_from_env_or_prompt("Anthropic", AnthropicProvider::PROVIDER_ID)?;
    let result = save_provider_api_key(AnthropicProvider::PROVIDER_ID, &api_key, &source)?;
    println!("Testing Anthropic provider connection...");
    test_provider_credentials(AnthropicProvider::PROVIDER_ID)?;
    println!(
        "Anthropic credentials saved to {}",
        result.credentials_path.display()
    );
    Ok(())
}

pub fn test_anthropic() -> Result<(), CliError> {
    let result = test_provider_credentials(AnthropicProvider::PROVIDER_ID)?;
    println!(
        "Testing Anthropic credentials at {}",
        result.credentials_path.display()
    );
    println!("Anthropic API key: ok");
    println!("Anthropic provider connection: ok");
    Ok(())
}

pub fn auth_openai() -> Result<(), CliError> {
    let (api_key, source) = api_key_from_env_or_prompt("OpenAI", OpenAiProvider::PROVIDER_ID)?;
    let result = save_provider_api_key(OpenAiProvider::PROVIDER_ID, &api_key, &source)?;
    println!("Testing OpenAI provider connection...");
    test_provider_credentials(OpenAiProvider::PROVIDER_ID)?;
    println!(
        "OpenAI credentials saved to {}",
        result.credentials_path.display()
    );
    Ok(())
}

pub fn test_openai() -> Result<(), CliError> {
    let result = test_provider_credentials(OpenAiProvider::PROVIDER_ID)?;
    println!(
        "Testing OpenAI credentials at {}",
        result.credentials_path.display()
    );
    println!("OpenAI API key: ok");
    println!("OpenAI provider connection: ok");
    Ok(())
}

pub fn auth_codex() -> Result<(), CliError> {
    let (provider, auth_path) = codex_provider()?;
    let device = provider.device_login_start()?;

    println!("Open this URL in your browser:");
    println!("{}", device.verification_uri);
    println!();
    println!("Enter this code:");
    println!("{}", device.user_code);
    println!();
    println!("Waiting for ChatGPT authorization...");

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
            println!("Testing Codex provider connection...");
            test_provider_credentials(CODEX_PROVIDER_ID)?;
            break;
        }
    }
    println!("Codex credentials saved to {}", auth_path.display());
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

pub fn auth_gemini() -> Result<(), CliError> {
    let (api_key, source) = api_key_from_env_or_prompt("Gemini", GeminiProvider::PROVIDER_ID)?;
    let result = save_provider_api_key(GeminiProvider::PROVIDER_ID, &api_key, &source)?;
    println!("Testing Gemini provider connection...");
    test_provider_credentials(GeminiProvider::PROVIDER_ID)?;
    println!(
        "Gemini credentials saved to {}",
        result.credentials_path.display()
    );
    Ok(())
}

pub fn test_gemini() -> Result<(), CliError> {
    let result = test_provider_credentials(GeminiProvider::PROVIDER_ID)?;
    println!(
        "Testing Gemini credentials at {}",
        result.credentials_path.display()
    );
    println!("Gemini API key: ok");
    println!("Gemini provider connection: ok");
    Ok(())
}

pub fn auth_nvidia() -> Result<(), CliError> {
    let (api_key, source) = api_key_from_env_or_prompt("NVIDIA", NvidiaProvider::PROVIDER_ID)?;
    let result = save_provider_api_key(NvidiaProvider::PROVIDER_ID, &api_key, &source)?;
    println!("Testing NVIDIA provider connection...");
    test_provider_credentials(NvidiaProvider::PROVIDER_ID)?;
    println!(
        "NVIDIA credentials saved to {}",
        result.credentials_path.display()
    );
    Ok(())
}

pub fn test_nvidia() -> Result<(), CliError> {
    let result = test_provider_credentials(NvidiaProvider::PROVIDER_ID)?;
    println!(
        "Testing NVIDIA credentials at {}",
        result.credentials_path.display()
    );
    println!("NVIDIA API key: ok");
    println!("NVIDIA provider connection: ok");
    Ok(())
}

fn api_key_from_env_or_prompt(
    provider_name: &str,
    provider_id: &str,
) -> Result<(String, String), CliError> {
    if let Some(api_key) = provider_api_key_from_env(provider_id) {
        return Ok(api_key);
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
