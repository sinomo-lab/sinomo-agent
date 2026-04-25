//! Shared Sinomo command-line handlers.

use sinomo_lm::{
    AnthropicConfig, AnthropicProvider, GitHubCopilotConfig, GitHubCopilotProvider,
    LanguageModelError, OpenAiConfig, OpenAiProvider,
};
use sinomo_local::SinomoDataDir;
use std::env;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    match args.as_slice() {
        [providers, github_copilot, auth]
            if providers == "providers" && github_copilot == "github-copilot" && auth == "auth" =>
        {
            auth_github_copilot()
        }
        [providers, github_copilot, test]
            if providers == "providers" && github_copilot == "github-copilot" && test == "test" =>
        {
            test_github_copilot()
        }
        [providers, anthropic, auth]
            if providers == "providers" && anthropic == "anthropic" && auth == "auth" =>
        {
            auth_anthropic()
        }
        [providers, anthropic, test]
            if providers == "providers" && anthropic == "anthropic" && test == "test" =>
        {
            test_anthropic()
        }
        [providers, openai, auth]
            if providers == "providers" && openai == "openai" && auth == "auth" =>
        {
            auth_openai()
        }
        [providers, openai, test]
            if providers == "providers" && openai == "openai" && test == "test" =>
        {
            test_openai()
        }
        [help] if help == "help" || help == "--help" || help == "-h" => {
            print_usage();
            Ok(())
        }
        [] => {
            print_usage();
            Ok(())
        }
        _ => Err(CliError::Usage),
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
    let (api_key, source) = api_key_from_env_or_prompt(
        "OpenAI",
        &[
            "SINOMO_OPENAI_API_KEY",
            "OPENAI_API_KEY",
            "CODEX_API_KEY",
            "SINOMO_CODEX_API_KEY",
        ],
    )?;
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

fn github_copilot_provider() -> Result<(GitHubCopilotProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(GitHubCopilotProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
    let client_id = env::var("SINOMO_COPILOT_CLIENT_ID")
        .unwrap_or_else(|_| GitHubCopilotConfig::default().client_id);
    let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
        client_id,
        token_store_path: Some(credentials_path.clone()),
        ..GitHubCopilotConfig::default()
    });
    Ok((provider, credentials_path))
}

fn anthropic_provider() -> Result<(AnthropicProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(AnthropicProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(AnthropicProvider::PROVIDER_ID);
    let provider = AnthropicProvider::new(AnthropicConfig {
        api_base_url: env::var("SINOMO_ANTHROPIC_API_BASE_URL")
            .or_else(|_| env::var("SINOMO_CLAUDE_API_BASE_URL"))
            .unwrap_or_else(|_| AnthropicConfig::default().api_base_url),
        default_model: env::var("SINOMO_ANTHROPIC_MODEL")
            .or_else(|_| env::var("SINOMO_CLAUDE_MODEL"))
            .unwrap_or_else(|_| AnthropicConfig::default().default_model),
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn openai_provider() -> Result<(OpenAiProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(OpenAiProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(OpenAiProvider::PROVIDER_ID);
    let provider = OpenAiProvider::new(OpenAiConfig {
        api_base_url: env::var("SINOMO_OPENAI_API_BASE_URL")
            .or_else(|_| env::var("SINOMO_CODEX_API_BASE_URL"))
            .unwrap_or_else(|_| OpenAiConfig::default().api_base_url),
        default_model: env::var("SINOMO_OPENAI_MODEL")
            .or_else(|_| env::var("SINOMO_CODEX_MODEL"))
            .unwrap_or_else(|_| OpenAiConfig::default().default_model),
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
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

pub fn print_usage() {
    println!("Usage:");
    println!("  sinomo providers github-copilot auth");
    println!("  sinomo providers github-copilot test");
    println!("  sinomo providers anthropic auth");
    println!("  sinomo providers anthropic test");
    println!("  sinomo providers openai auth");
    println!("  sinomo providers openai test");
    println!("  sinomo-app providers github-copilot auth");
    println!("  sinomo-app providers github-copilot test");
    println!("  sinomo-app providers anthropic auth");
    println!("  sinomo-app providers anthropic test");
    println!("  sinomo-app providers openai auth");
    println!("  sinomo-app providers openai test");
    println!();
    println!("Environment:");
    println!("  SINOMO_DATA_DIR             Defaults to ~/.sinomo");
    println!("  ANTHROPIC_API_KEY           Used by `providers anthropic auth/test`");
    println!("  OPENAI_API_KEY              Used by `providers openai auth/test`");
    println!(
        "  SINOMO_ANTHROPIC_MODEL      Defaults to {}",
        AnthropicConfig::default().default_model
    );
    println!(
        "  SINOMO_OPENAI_MODEL         Defaults to {}",
        OpenAiConfig::default().default_model
    );
    println!(
        "  SINOMO_COPILOT_CLIENT_ID    Defaults to {}",
        GitHubCopilotConfig::default().client_id
    );
    println!(
        "                               GitHub App client IDs are not accepted by Copilot token exchange"
    );
}

#[derive(Debug)]
pub enum CliError {
    Usage,
    AuthDenied,
    AuthExpired,
    AuthProvider { detail: String },
    LocalData(sinomo_local::LocalDataError),
    LanguageModel(LanguageModelError),
}

impl From<sinomo_local::LocalDataError> for CliError {
    fn from(error: sinomo_local::LocalDataError) -> Self {
        Self::LocalData(error)
    }
}

impl From<LanguageModelError> for CliError {
    fn from(error: LanguageModelError) -> Self {
        Self::LanguageModel(error)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage => write!(formatter, "unknown command; run `sinomo help`"),
            Self::AuthDenied => write!(formatter, "GitHub device authorization was denied"),
            Self::AuthExpired => write!(formatter, "GitHub device authorization expired"),
            Self::AuthProvider { detail } => write!(formatter, "provider auth failed: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::LanguageModel(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for CliError {}
