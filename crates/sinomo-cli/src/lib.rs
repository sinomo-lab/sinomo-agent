//! Shared Sinomo command-line handlers.

use sinomo_lm::{GitHubCopilotConfig, GitHubCopilotProvider, LanguageModelError};
use sinomo_local::SinomoDataDir;
use std::env;
use std::thread;
use std::time::{Duration, Instant};

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    match args.as_slice() {
        [providers, github_copilot, auth]
            if providers == "providers" && github_copilot == "github-copilot" && auth == "auth" =>
        {
            auth_github_copilot()
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
            provider.copilot_api_token()?;
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

pub fn print_usage() {
    println!("Usage:");
    println!("  sinomo providers github-copilot auth");
    println!("  sinomo-app providers github-copilot auth");
    println!();
    println!("Environment:");
    println!("  SINOMO_DATA_DIR             Defaults to ~/.sinomo");
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
            Self::AuthProvider { detail } => write!(formatter, "GitHub auth failed: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::LanguageModel(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for CliError {}
