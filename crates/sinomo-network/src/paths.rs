use crate::NetworkError;
use sinomo_local::SinomoDataDir;
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterPaths {
    pub root: PathBuf,
    pub state: PathBuf,
    pub connectivity_local: PathBuf,
}

impl ClusterPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("cluster");
        Self {
            state: root.join("state.sobj"),
            connectivity_local: root.join("connectivity-local.sobj"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), NetworkError> {
        fs::create_dir_all(&self.root).map_err(|source| NetworkError::StorageIo {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }
}
