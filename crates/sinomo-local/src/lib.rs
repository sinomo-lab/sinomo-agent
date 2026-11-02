//! Local filesystem layout for Sinomo state.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;
use thiserror::Error;

static PROCESS_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Installer-owned, UTF-8 absolute data path beside the bootstrap executables.
pub const INSTALLED_DATA_DIR_FILE: &str = "sinomo-data-dir";

/// Windows canonical paths are valid filesystem identities, but tools such as
/// Git parse the extended drive prefix as a remote URL when it is an argument.
pub fn command_path(path: &std::path::Path) -> PathBuf {
    #[cfg(windows)]
    if let Some(text) = path.to_str() {
        if let Some(unc) = text.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{unc}"));
        }
        if let Some(local) = text.strip_prefix(r"\\?\") {
            return PathBuf::from(local);
        }
    }
    path.to_path_buf()
}

/// A durable, node-local removal fence. Keeping data does not authorize recovery
/// or a sign-in launcher to resurrect a deployment the user has uninstalled.
pub const REMOVAL_FENCE: &str = "config/uninstalled";

pub fn ensure_deployment_enabled(data: &std::path::Path) -> Result<(), String> {
    match fs::symlink_metadata(data.join(REMOVAL_FENCE)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err("this deployment is uninstalled; explicitly reinstall to enable it".into()),
    }
}

pub fn inhibit_deployment(data: &std::path::Path) -> std::io::Result<()> {
    fs::create_dir_all(data.join("config"))?;
    sinomo_storage::durable_file::write_file_atomic(
        &data.join(REMOVAL_FENCE),
        b"uninstalled\n",
        sinomo_storage::durable_file::DurableAtomicFileOptions::new("deployment", "removal")
            .sync_all(),
    )
    .map_err(std::io::Error::other)
}

/// Setup calls this only after validating the retained payload and taking the
/// same activation admission used by rollout and removal.
pub fn enable_deployment(data: &std::path::Path) -> std::io::Result<()> {
    sinomo_storage::durable_file::remove_file_if_exists_and_sync_parent(&data.join(REMOVAL_FENCE))
        .map(|_| ())
        .map_err(std::io::Error::other)
}

/// Read only an explicit installation binding; development binaries have none.
pub fn installed_data_dir(directory: &std::path::Path) -> Result<Option<PathBuf>, LocalDataError> {
    let file = directory.join(INSTALLED_DATA_DIR_FILE);
    let text = match fs::read_to_string(&file) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(LocalDataError::InstallationBinding(error.to_string())),
    };
    let path = PathBuf::from(&text);
    if !path.is_absolute() || text.contains(['\n', '\r', '\0']) {
        return Err(LocalDataError::InstallationBinding(format!(
            "invalid data path in {}",
            file.display()
        )));
    }
    Ok(Some(path))
}

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
        let installed = if explicit.as_deref().is_some_and(|p| !p.trim().is_empty())
            || PROCESS_DATA_DIR.get().is_some()
        {
            None
        } else {
            env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(PathBuf::from))
                .map(|directory| installed_data_dir(&directory))
                .transpose()?
                .flatten()
        };
        resolve_data_dir(
            explicit.as_deref(),
            PROCESS_DATA_DIR
                .get()
                .or(installed.as_ref())
                .map(PathBuf::as_path),
            home.as_deref(),
        )
        .map(Self::new)
    }

    pub fn root(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn bin_dir(&self) -> PathBuf {
        self.root.join("bin")
    }

    pub fn binary(&self, stem: &str) -> PathBuf {
        self.bin_dir()
            .join(format!("{stem}{}", env::consts::EXE_SUFFIX))
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

    /// Host filesystem projections must neither contain nor sit inside DATA.
    /// This sibling holds mount points only; durable state remains in DATA.
    /// Resolve an existing DATA alias so one node does not acquire two mount
    /// namespaces through different spellings of its state directory.
    pub fn external_mounts_dir(&self) -> PathBuf {
        let root = fs::canonicalize(&self.root).unwrap_or_else(|_| self.root.clone());
        let mut name = root.file_name().unwrap_or_default().to_os_string();
        name.push(".mounts");
        root.with_file_name(name)
    }

    /// Node-local runtime state owned by dynamically loaded Sinomo modules.
    ///
    /// Module implementations append their validated module id below this
    /// directory. Keeping the root in the shared data-dir contract avoids
    /// ad-hoc host paths while preserving node-local consent and recovery.
    pub fn module_runtime_dir(&self) -> PathBuf {
        self.root.join("module").join("runtime")
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
    #[error("invalid installation data-directory binding: {0}")]
    InstallationBinding(String),
    #[error("could not resolve a home directory; set SINOMO_DATA_DIR")]
    HomeUnavailable,
    #[error("failed to create directory {path}: {detail}")]
    CreateDir { path: String, detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installation_binding_is_absolute_and_missing_files_are_unbound() {
        let root = env::temp_dir().join(format!("sinomo-binding-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let file = root.join(INSTALLED_DATA_DIR_FILE);
        let _ = fs::remove_file(&file);
        assert_eq!(installed_data_dir(&root).unwrap(), None);
        fs::write(&file, "relative/data").unwrap();
        assert!(installed_data_dir(&root).is_err());
        let data = root.join("data with spaces");
        fs::write(&file, data.to_str().unwrap()).unwrap();
        assert_eq!(installed_data_dir(&root).unwrap(), Some(data));
        fs::write(&file, "bad\npath").unwrap();
        assert!(installed_data_dir(&root).is_err());
        fs::remove_file(file).unwrap();
        fs::remove_dir(root).unwrap();
    }

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
    fn module_runtime_state_stays_under_the_sinomo_data_directory() {
        let data = SinomoDataDir::new("root");

        assert_eq!(
            data.module_runtime_dir(),
            PathBuf::from("root").join("module").join("runtime")
        );
    }
}
