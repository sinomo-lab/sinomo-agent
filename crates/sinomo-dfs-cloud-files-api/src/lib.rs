//! Typed SOBJ payloads for the Windows Cloud Files SIFS native module.
//!
//! The generic native module ABI lives in `sinomo-module-api`. This crate only
//! defines the backend-specific operation name and payloads used by the Windows
//! Cloud Files provider module.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const SINOMO_DFS_CLOUD_FILES_MODULE_ID: &str = "module.dfs.cloud-files";
pub const SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION: &str = "sinomo.dfs.cloud-files.v1";
pub const SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION: &str = "sifs.cloud-files.mount";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesModuleRequest {
    Mount(CloudFilesMountRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesMountRequest {
    pub sync_root: PathBuf,
    pub root_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir_root: Option<PathBuf>,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default = "default_provider_name")]
    pub provider_name: String,
    #[serde(default = "default_provider_version")]
    pub provider_version: String,
    #[serde(default = "default_prepopulate_depth")]
    pub prepopulate_depth: u32,
    #[serde(default = "default_connect")]
    pub connect: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_run_seconds: Option<u64>,
}

impl CloudFilesMountRequest {
    pub fn new(sync_root: impl Into<PathBuf>) -> Self {
        Self {
            sync_root: sync_root.into(),
            root_path: "/".to_string(),
            data_dir_root: None,
            read_only: false,
            provider_name: default_provider_name(),
            provider_version: default_provider_version(),
            prepopulate_depth: default_prepopulate_depth(),
            connect: default_connect(),
            max_run_seconds: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesModuleResponse {
    Mounted(CloudFilesMountResponse),
    Error(CloudFilesModuleError),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesMountResponse {
    pub sync_root: PathBuf,
    pub root_path: String,
    pub placeholders_created: u64,
    pub connected: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesModuleError {
    pub code: String,
    pub detail: String,
}

fn default_provider_name() -> String {
    "Sinomo SIFS".to_string()
}

fn default_provider_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn default_prepopulate_depth() -> u32 {
    1
}

fn default_connect() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_request_defaults_to_connected_root_exposure() {
        let request = CloudFilesMountRequest::new("C:\\SinomoSIFS");

        assert_eq!(request.root_path, "/");
        assert_eq!(request.provider_name, "Sinomo SIFS");
        assert_eq!(request.prepopulate_depth, 1);
        assert!(request.connect);
    }
}
