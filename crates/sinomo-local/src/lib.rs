//! Local filesystem layout for Sinomo state.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;
use thiserror::Error;

static PROCESS_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Install a process-local data directory for hosts that do not expose a home
/// directory, such as Android NativeActivity applications.
///
/// An explicit `SINOMO_DATA_DIR` environment variable still takes precedence.
/// The first installed path wins so every subsystem in the process observes a
/// stable storage root without mutating the process environment after startup.
pub fn install_process_data_dir(root: impl Into<PathBuf>) -> PathBuf {
    PROCESS_DATA_DIR.get_or_init(|| root.into()).clone()
}

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
        let explicit = env::var("SINOMO_DATA_DIR").ok();
        let home = env::var("USERPROFILE").or_else(|_| env::var("HOME")).ok();
        resolve_data_dir(
            explicit.as_deref(),
            PROCESS_DATA_DIR.get().map(PathBuf::as_path),
            home.as_deref(),
        )
        .map(Self::new)
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

    pub fn cluster_endpoint_config_file(&self) -> PathBuf {
        self.shared_configs_dir()
            .join("cluster")
            .join("endpoints.json")
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

fn resolve_data_dir(
    explicit: Option<&str>,
    process_default: Option<&std::path::Path>,
    home: Option<&str>,
) -> Result<PathBuf, LocalDataError> {
    if let Some(path) = explicit.map(str::trim).filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = process_default {
        return Ok(path.to_path_buf());
    }
    home.map(|path| PathBuf::from(path).join(".sinomo"))
        .ok_or(LocalDataError::HomeUnavailable)
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
    fn data_dir_resolution_prefers_explicit_then_process_then_home() {
        let process = PathBuf::from("process");

        assert_eq!(
            resolve_data_dir(Some(" explicit "), Some(&process), Some("home")),
            Ok(PathBuf::from("explicit"))
        );
        assert_eq!(
            resolve_data_dir(Some("  "), Some(&process), Some("home")),
            Ok(process)
        );
        assert_eq!(
            resolve_data_dir(None, None, Some("home")),
            Ok(PathBuf::from("home").join(".sinomo"))
        );
        assert_eq!(
            resolve_data_dir(None, None, None),
            Err(LocalDataError::HomeUnavailable)
        );
    }

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
