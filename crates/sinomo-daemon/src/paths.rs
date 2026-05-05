use crate::DaemonError;
use sinomo_local::SinomoDataDir;
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonPaths {
    pub root: PathBuf,
    pub lock: PathBuf,
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
            status: root.join("status.json"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), DaemonError> {
        fs::create_dir_all(&self.root).map_err(|source| DaemonError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }
}

pub fn daemon_paths_from_data_dir(data_dir: &SinomoDataDir) -> DaemonPaths {
    DaemonPaths::with_root(data_dir.root().join("daemon"))
}

pub fn shadow_daemon_paths_from_data_dir(data_dir: &SinomoDataDir) -> DaemonPaths {
    DaemonPaths::with_root(data_dir.root().join("shadow-daemon"))
}
