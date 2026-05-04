use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBinaryRequest {
    pub source: String,
    pub binary_base64: String,
    #[serde(default)]
    pub target_os: String,
    #[serde(default)]
    pub target_arch: String,
    pub host: String,
    pub port: u16,
    pub shadow_port: u16,
    pub requested_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBuildRequest {
    pub source: String,
    #[serde(default)]
    pub remote_repo: Option<PathBuf>,
    pub host: String,
    pub port: u16,
    pub shadow_port: u16,
    pub requested_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBinaryResponse {
    pub status: String,
    pub detail: String,
    pub main_binary: PathBuf,
    pub shadow_binary: PathBuf,
}
