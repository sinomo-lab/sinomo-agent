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
            provider.test_connection()?;
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
    let (provider, credentials_path) = github_copilot_provider()?;
    println!(
        "Testing GitHub Copilot credentials at {}",
        credentials_path.display()
    );
    provider.github_token()?;
    println!("GitHub token: ok");
    provider.test_connection()?;
    println!("Copilot API models endpoint: ok");
    println!("GitHub Copilot provider connection: ok");
    Ok(())
}

pub fn auth_anthropic() -> Result<(), CliError> {
    let (provider, credentials_path) = anthropic_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "Anthropic",
        &[
            "SINOMO_ANTHROPIC_API_KEY",
            "ANTHROPIC_API_KEY",
            "CLAUDE_API_KEY",
            "SINOMO_CLAUDE_API_KEY",
        ],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing Anthropic provider connection...");
    provider.test_connection()?;
    println!(
        "Anthropic credentials saved to {}",
        credentials_path.display()
    );
    Ok(())
}

pub fn test_anthropic() -> Result<(), CliError> {
    let (provider, credentials_path) = anthropic_provider()?;
    println!(
        "Testing Anthropic credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("Anthropic API key: ok");
    provider.test_connection()?;
    println!("Anthropic provider connection: ok");
    Ok(())
}

pub fn auth_openai() -> Result<(), CliError> {
    let (provider, credentials_path) = openai_provider()?;
    let (api_key, source) =
        api_key_from_env_or_prompt("OpenAI", &["SINOMO_OPENAI_API_KEY", "OPENAI_API_KEY"])?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing OpenAI provider connection...");
    provider.test_connection()?;
    println!("OpenAI credentials saved to {}", credentials_path.display());
    Ok(())
}

pub fn test_openai() -> Result<(), CliError> {
    let (provider, credentials_path) = openai_provider()?;
    println!(
        "Testing OpenAI credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("OpenAI API key: ok");
    provider.test_connection()?;
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
            provider.test_connection()?;
            break;
        }
    }
    println!("Codex credentials saved to {}", auth_path.display());
    Ok(())
}

pub fn test_codex() -> Result<(), CliError> {
    let (provider, auth_path) = codex_provider()?;
    println!("Testing Codex credentials at {}", auth_path.display());
    provider.chatgpt_tokens()?;
    println!("Codex ChatGPT auth: ok");
    provider.test_connection()?;
    println!("Codex provider connection: ok");
    Ok(())
}

pub fn auth_gemini() -> Result<(), CliError> {
    let (provider, credentials_path) = gemini_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "Gemini",
        &[
            "SINOMO_GEMINI_API_KEY",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "GOOGLE_AI_API_KEY",
        ],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing Gemini provider connection...");
    provider.test_connection()?;
    println!("Gemini credentials saved to {}", credentials_path.display());
    Ok(())
}

pub fn test_gemini() -> Result<(), CliError> {
    let (provider, credentials_path) = gemini_provider()?;
    println!(
        "Testing Gemini credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("Gemini API key: ok");
    provider.test_connection()?;
    println!("Gemini provider connection: ok");
    Ok(())
}

pub fn auth_nvidia() -> Result<(), CliError> {
    let (provider, credentials_path) = nvidia_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "NVIDIA",
        &["SINOMO_NVIDIA_API_KEY", "NVIDIA_API_KEY", "NIM_API_KEY"],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing NVIDIA provider connection...");
    provider.test_connection()?;
    println!("NVIDIA credentials saved to {}", credentials_path.display());
    Ok(())
}

pub fn test_nvidia() -> Result<(), CliError> {
    let (provider, credentials_path) = nvidia_provider()?;
    println!(
        "Testing NVIDIA credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("NVIDIA API key: ok");
    provider.test_connection()?;
    println!("NVIDIA provider connection: ok");
    Ok(())
}

pub(super) fn codex_provider() -> Result<(CodexProvider, PathBuf), CliError> {
    let provider = codex_provider_for_model(PROVIDER_DEFAULT_MODEL);
    let auth_path = provider
        .credentials_path()
        .ok_or_else(|| CliError::AuthProvider {
            detail: "could not resolve Sinomo Codex credentials path; set SINOMO_DATA_DIR or SINOMO_CODEX_CREDENTIALS_FILE".to_string(),
        })?;
    Ok((provider, auth_path))
}

fn api_key_from_env_or_prompt(
    provider_name: &str,
    env_vars: &[&str],
) -> Result<(String, String), CliError> {
    for env_var in env_vars {
        if let Ok(api_key) = env::var(env_var) {
            let api_key = api_key.trim();
            if !api_key.is_empty() {
                return Ok((api_key.to_string(), format!("env:{env_var}")));
            }
        }
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
