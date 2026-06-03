//! Local filesystem layout for Sinomo state.

use std::env;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::StorageRing;

    layout.register_escape_path(
        "sinomo-local",
        StorageRing::Application,
        "config",
        "legacy human-readable local configuration files",
    )?;
    layout.register_escape_path(
        "sinomo-local",
        StorageRing::Application,
        "providers",
        "legacy external provider credential and configuration files",
    )?;
    layout.register_escape_path(
        "sinomo-local",
        StorageRing::R2,
        "workspace",
        "user-authored workspace files",
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinomoDataDir {
    root: PathBuf,
}

impl SinomoDataDir {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn from_env_or_home() -> Result<Self, LocalDataError> {
        if let Ok(path) = env::var("SINOMO_DATA_DIR") {
            let path = path.trim();
            if !path.is_empty() {
                return Ok(Self::new(path));
            }
        }

        let home = env::var("USERPROFILE")
            .or_else(|_| env::var("HOME"))
            .map_err(|_| LocalDataError::HomeUnavailable)?;
        Ok(Self::new(PathBuf::from(home).join(".sinomo")))
    }

    pub fn root(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn configs_dir(&self) -> PathBuf {
        self.root.join("config")
    }

    pub fn shared_configs_dir(&self) -> PathBuf {
        self.root.join("configs")
    }

    pub fn dfs_dir(&self) -> PathBuf {
        self.root.join("dfs")
    }

    pub fn files_dir(&self) -> PathBuf {
        self.root.join("files")
    }

    pub fn dfs_expose_config_file(&self) -> PathBuf {
        self.shared_configs_dir().join("dfs").join("expose.json")
    }

    pub fn providers_dir(&self) -> PathBuf {
        self.root.join("providers")
    }

    pub fn provider_dir(&self, provider: &str) -> PathBuf {
        self.providers_dir().join(provider)
    }

    pub fn provider_credentials_file(&self, provider: &str) -> PathBuf {
        self.shared_configs_dir()
            .join("lm")
            .join("credentials")
            .join(format!("{provider}.json"))
    }

    pub fn legacy_provider_credentials_file(&self, provider: &str) -> PathBuf {
        self.provider_dir(provider).join("credentials.json")
    }

    pub fn ensure_root_layout(&self) -> Result<(), LocalDataError> {
        for path in [
            self.root(),
            self.configs_dir(),
            self.shared_configs_dir(),
            self.dfs_dir(),
            self.files_dir(),
            self.providers_dir(),
        ] {
            fs::create_dir_all(&path).map_err(|source| LocalDataError::CreateDir {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(())
    }

    pub fn ensure_provider_dir(&self, provider: &str) -> Result<PathBuf, LocalDataError> {
        self.ensure_root_layout()?;
        let path = self.provider_dir(provider);
        fs::create_dir_all(&path).map_err(|source| LocalDataError::CreateDir {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(path)
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum LocalDataError {
    #[error("could not resolve a home directory; set SINOMO_DATA_DIR")]
    HomeUnavailable,
    #[error("failed to create directory {path}: {detail}")]
    CreateDir { path: String, detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_credentials_live_under_shared_configs_dir() {
        let data = SinomoDataDir::new("root");

        assert_eq!(
            data.provider_credentials_file("github-copilot"),
            PathBuf::from("root")
                .join("configs")
                .join("lm")
                .join("credentials")
                .join("github-copilot.json")
        );
    }

    #[test]
    fn legacy_provider_credentials_path_stays_available_for_migration() {
        let data = SinomoDataDir::new("root");

        assert_eq!(
            data.legacy_provider_credentials_file("github-copilot"),
            PathBuf::from("root")
                .join("providers")
                .join("github-copilot")
                .join("credentials.json")
        );
    }

    #[test]
    fn dfs_expose_config_lives_under_shared_configs_dir() {
        let data = SinomoDataDir::new("root");

        assert_eq!(
            data.dfs_expose_config_file(),
            PathBuf::from("root")
                .join("configs")
                .join("dfs")
                .join("expose.json")
        );
    }
}
