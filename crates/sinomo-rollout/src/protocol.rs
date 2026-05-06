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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_bundle_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_head: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_head: Option<String>,
}
