use crate::DaemonApiError;
use sinomo_core::local::SinomoDataDir;
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonPaths {
    pub root: PathBuf,
    pub lock: PathBuf,
    pub data_owner_lock: PathBuf,
    pub status: PathBuf,
}

impl DaemonPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        daemon_paths_from_data_dir(data_dir)
    }

    pub fn for_shadow(data_dir: &SinomoDataDir) -> Self {
        shadow_daemon_paths_from_data_dir(data_dir)
    }

    pub fn with_root(root: PathBuf) -> Self {
        Self {
            lock: root.join("daemon.lock"),
            data_owner_lock: root.join("data-owner.lock"),
            status: root.join("status.sobj"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), DaemonApiError> {
        fs::create_dir_all(&self.root).map_err(|source| DaemonApiError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }

    pub fn is_shadow(&self) -> bool {
        self.root
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "shadow-daemon")
    }
}

pub fn daemon_paths_from_data_dir(data_dir: &SinomoDataDir) -> DaemonPaths {
    DaemonPaths::with_root(data_dir.root().join("daemon"))
}

pub fn shadow_daemon_paths_from_data_dir(data_dir: &SinomoDataDir) -> DaemonPaths {
    DaemonPaths::with_root(data_dir.root().join("shadow-daemon"))
}
