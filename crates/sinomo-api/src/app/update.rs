//! Desktop release and restart contracts. Activation is owned by rollout, never the GUI.
use serde::{Deserialize, Serialize};

pub const APP_UPDATE_STATUS_ROUTE: &str = "/rollout/app/status";
pub const APP_UPDATE_STAGE_ROUTE: &str = "/rollout/app/stage";
pub const APP_UPDATE_RESTART_ROUTE: &str = "/rollout/app/restart";
pub const APP_UPDATE_READY_ROUTE: &str = "/rollout/app/ready";
pub const APP_UPDATE_UPLOAD_ROUTE: &str = "/rollout/app/upload";
pub const APP_UPDATE_ROUTES: &[&str] = &[
    APP_UPDATE_STATUS_ROUTE,
    APP_UPDATE_STAGE_ROUTE,
    APP_UPDATE_RESTART_ROUTE,
    APP_UPDATE_READY_ROUTE,
    APP_UPDATE_UPLOAD_ROUTE,
];
pub const APP_UPDATE_EVENT: &str = "app.update.changed";
pub const APP_BUNDLE_MANIFEST: &str = "desktop-release.json";
pub const APP_SUPERVISED_ENV: &str = "SINOMO_APP_SUPERVISED";
pub const APP_LAUNCH_TOKEN_ENV: &str = "SINOMO_APP_LAUNCH_TOKEN";
/// The graphical bootstrap asks the supervisor to report terminal failures to the user.
pub const APP_FATAL_DIALOG_ENV: &str = "SINOMO_APP_FATAL_DIALOG";
pub const APP_FATAL_DIALOG_ARG: &str = "--show-fatal-error";
/// The UI already presented its error. Preserve a failing exit without a second dialog.
pub const APP_REPORTED_ERROR_EXIT_CODE: u8 = 70;
pub const APP_RESTART_EXIT_CODE: i32 = 75;
pub const APP_WORKSPACE_SNAPSHOT_PATH: &str = "configs/app/workspace.json";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppBundleFile {
    pub path: String,
    pub content_hash: String,
    pub size: u64,
    pub executable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppBundleManifest {
    pub schema_version: u32,
    pub source_revision: String,
    pub target_os: String,
    pub target_arch: String,
    pub daemon_protocol: u32,
    pub restoration_schema: u32,
    pub files: Vec<AppBundleFile>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AppUpdatePhase {
    #[default]
    Current,
    Ready,
    RestartRequested,
    Starting,
    RolledBack,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppUpdateStatus {
    pub revision: u64,
    pub active_release: Option<String>,
    pub candidate_release: Option<String>,
    pub previous_release: Option<String>,
    pub phase: AppUpdatePhase,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppUpdateStatusRequest {
    pub after_revision: Option<u64>,
    pub wait_ms: u64,
}

/// Local-command-only: the operator explicitly selects a complete release directory.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppUpdateStageRequest {
    pub bundle_directory: std::path::PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppUpdateRestartRequest {
    pub candidate_release: String,
    pub launch_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppUpdateReadyRequest {
    pub launch_token: String,
    pub restoration_schema: u32,
}

/// Authorized cluster staging transfers one length/digest-bound file per streaming RPC.
/// Receiving all files only offers an update; restart consent remains node-local.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppUpdateUploadRequest {
    pub manifest: AppBundleManifest,
    pub file_index: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppUpdateUploadResponse {
    pub release_id: String,
    pub received_files: usize,
    pub expected_files: usize,
    pub status: Option<AppUpdateStatus>,
}
