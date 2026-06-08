use crate::{
    LanguageModelError, read_provider_credentials_json_value, unix_timestamp,
    write_provider_credentials_json_value,
};
use serde::{Deserialize, Serialize};
use sinomo_runtime::local::SinomoDataDir;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredApiKeyCredential {
    api_key: String,
    source: String,
    created_at_unix: u64,
}

pub(crate) fn resolve_api_key(
    provider: &str,
    configured_api_key: &Option<String>,
    credentials_path: &Option<PathBuf>,
    env_vars: &[&str],
    missing_detail: &str,
) -> Result<String, LanguageModelError> {
    if let Some(api_key) = configured_api_key
        && !api_key.trim().is_empty()
    {
        return Ok(api_key.trim().to_string());
    }

    if let Some(stored) = load_api_key_credential(provider, credentials_path)?
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
    provider: &str,
    credentials_path: &Option<PathBuf>,
    api_key: &str,
    source: &str,
) -> Result<(), LanguageModelError> {
    let payload = StoredApiKeyCredential {
        api_key: api_key.trim().to_string(),
        source: source.to_string(),
        created_at_unix: unix_timestamp(),
    };
    let value = serde_json::to_value(payload).map_err(|source| {
        LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        }
    })?;
    write_provider_credentials_json_value(provider, credentials_path, value)
}

/// Whether a non-empty stored API-key credential exists for `provider`.
pub(crate) fn stored_api_key_is_present(
    provider: &str,
    credentials_path: &Option<PathBuf>,
) -> bool {
    load_api_key_credential(provider, credentials_path)
        .ok()
        .flatten()
        .is_some_and(|credential| !credential.api_key.trim().is_empty())
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
    provider: &str,
    credentials_path: &Option<PathBuf>,
) -> Result<Option<StoredApiKeyCredential>, LanguageModelError> {
    read_provider_credentials_json_value(provider, credentials_path)?
        .map(serde_json::from_value)
        .transpose()
        .map_err(|source| LanguageModelError::TokenStore {
            path: credentials_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<provider-credentials>".to_string()),
            detail: source.to_string(),
        })
}
