//! Provider-neutral host filesystem presentation contract.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{SifsMountConfig, SifsMountRuntimeStatus};

pub const HOST_FILESYSTEM_PROVIDER_INTERFACE_ID: &str = "sinomo.host-filesystem-provider/v1";
pub const HOST_FILESYSTEM_PROVIDER_INTERFACE_VERSION: &str = "1.0.0";
pub const HOST_FILESYSTEM_PROVIDER_OPERATION: &str = "host-filesystem.reconcile";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum HostFilesystemProviderRequest {
    Reconcile {
        data_dir_root: PathBuf,
        mount_point: PathBuf,
        config: SifsMountConfig,
    },
    Stop {
        data_dir_root: PathBuf,
        mount_point: PathBuf,
        config: SifsMountConfig,
        preserve_state: bool,
    },
    Status {
        data_dir_root: PathBuf,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum HostFilesystemProviderResponse {
    Status(SifsMountRuntimeStatus),
    Stopped(SifsMountRuntimeStatus),
    Error(HostFilesystemProviderError),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct HostFilesystemProviderError {
    pub code: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum HostFilesystemProviderEvent {
    Status(SifsMountRuntimeStatus),
}
