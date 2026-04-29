use crate::{LanguageModelError, unix_timestamp};
use serde::{Deserialize, Serialize};
use sinomo_local::SinomoDataDir;
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredApiKeyCredential {
    api_key: String,
    source: String,
    created_at_unix: u64,
}

pub(crate) fn resolve_api_key(
    provider: &str,
    credentials_path: &Option<PathBuf>,
    env_vars: &[&str],
    missing_detail: &str,
) -> Result<String, LanguageModelError> {
    if let Some(stored) = load_api_key_credential(credentials_path)?
        && !stored.api_key.trim().is_empty()
    {
        return Ok(stored.api_key);
    }

    for env_var in env_vars {
        if let Ok(api_key) = std::env::var(env_var) {
            let api_key = api_key.trim();
            if !api_key.is_empty() {
                return Ok(api_key.to_string());
            }
        }
    }

    Err(LanguageModelError::AuthenticationRequired {
        provider: provider.to_string(),
        detail: missing_detail.to_string(),
    })
}

pub(crate) fn save_api_key_credential(
    credentials_path: &Option<PathBuf>,
    api_key: &str,
    source: &str,
) -> Result<(), LanguageModelError> {
    let Some(path) = credentials_path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| LanguageModelError::TokenStore {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let payload = StoredApiKeyCredential {
        api_key: api_key.trim().to_string(),
        source: source.to_string(),
        created_at_unix: unix_timestamp(),
    };
    let content = serde_json::to_string_pretty(&payload).map_err(|source| {
        LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        }
    })?;
    fs::write(path, content).map_err(|source| LanguageModelError::TokenStore {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub(crate) fn default_api_key_credentials_path(
    env_var: &str,
    provider_id: &str,
) -> Option<PathBuf> {
    if let Ok(path) = std::env::var(env_var) {
        return Some(PathBuf::from(path));
    }
    SinomoDataDir::from_env_or_home()
        .ok()
        .map(|data| data.provider_credentials_file(provider_id))
}

fn load_api_key_credential(
    credentials_path: &Option<PathBuf>,
) -> Result<Option<StoredApiKeyCredential>, LanguageModelError> {
    let Some(path) = credentials_path else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path).map_err(|source| LanguageModelError::TokenStore {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content)
        .map(Some)
        .map_err(|source| LanguageModelError::TokenStore {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}
