use crate::{RolloutError, SelfRolloutPaths, executable_name};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutMainActivation {
    pub source: String,
    pub main_binary: PathBuf,
    pub shadow_binary: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_main_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_shadow_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_main_binary: Option<PathBuf>,
    #[serde(default = "default_rollout_main_host")]
    pub main_host: String,
    #[serde(default = "default_rollout_main_port")]
    pub main_port: u16,
    pub shadow_port: u16,
    pub requested_at: String,
    pub mode: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingShadowRollout {
    pub source: String,
    pub main_binary: PathBuf,
    pub shadow_binary: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_shadow_binary: Option<PathBuf>,
    pub previous_shadow_binary: PathBuf,
    pub main_host: String,
    pub main_port: u16,
    pub shadow_host: String,
    pub shadow_port: u16,
    pub requested_at: String,
    pub main_healthy_at: String,
    pub mode: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutFailureDiagnostics {
    pub diagnostics_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_log: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_log: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_tail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
}

pub trait PendingShadowRolloutContext {
    fn activation(&self) -> &RolloutMainActivation;
    fn main_binary(&self) -> &Path;
    fn main_host(&self) -> &str;
    fn main_port(&self) -> u16;
    fn previous_shadow_binary(&self) -> &Path;
}

pub fn rollout_main_activation_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("pending-main-rollout.json")
}

pub fn rollout_shadow_finalize_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("pending-shadow-rollout.json")
}

pub fn rollout_staging_dir(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("staged")
}

pub fn rollout_staged_main_binary(paths: &SelfRolloutPaths) -> PathBuf {
    rollout_staging_dir(paths).join(executable_name("sinomo"))
}

pub fn rollout_staged_shadow_binary(paths: &SelfRolloutPaths) -> PathBuf {
    rollout_staging_dir(paths).join(executable_name("sinomo-shadow"))
}

pub fn read_rollout_main_activation(
    paths: &SelfRolloutPaths,
) -> Result<Option<RolloutMainActivation>, RolloutError> {
    let path = rollout_main_activation_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content)
        .map(Some)
        .map_err(|source| RolloutError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

pub fn read_pending_shadow_rollout(
    paths: &SelfRolloutPaths,
) -> Result<Option<PendingShadowRollout>, RolloutError> {
    let path = rollout_shadow_finalize_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content)
        .map(Some)
        .map_err(|source| RolloutError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

pub fn stage_pending_shadow_rollout(
    rollout_paths: &SelfRolloutPaths,
    pending: &impl PendingShadowRolloutContext,
    shadow_host: &str,
    shadow_port: u16,
) -> Result<(), RolloutError> {
    let activation = pending.activation();
    let finalize = PendingShadowRollout {
        source: activation.source.clone(),
        main_binary: pending.main_binary().to_path_buf(),
        shadow_binary: activation.shadow_binary.clone(),
        staged_shadow_binary: activation.staged_shadow_binary.clone(),
        previous_shadow_binary: pending.previous_shadow_binary().to_path_buf(),
        main_host: pending.main_host().to_string(),
        main_port: pending.main_port(),
        shadow_host: shadow_host.to_string(),
        shadow_port,
        requested_at: activation.requested_at.clone(),
        main_healthy_at: format_now(),
        mode: activation.mode.clone(),
    };
    write_json(&rollout_shadow_finalize_path(rollout_paths), &finalize)
}

pub fn write_main_activation(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
) -> Result<(), RolloutError> {
    write_json(&rollout_main_activation_path(rollout_paths), activation)
}

pub fn record_main_activation_staged(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
) -> Result<(), RolloutError> {
    write_json(
        &rollout_paths.state,
        &serde_json::json!({
            "source": activation.source,
            "main_binary": activation.main_binary,
            "shadow_binary": activation.shadow_binary,
            "previous_main_binary": activation.previous_main_binary,
            "requested_at": activation.requested_at,
            "staged_at": format_now(),
            "status": "main-staged",
            "mode": activation.mode
        }),
    )
}

pub fn record_activation_failure(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
    previous_main_binary: &Path,
    detail: &str,
) -> Result<(), RolloutError> {
    write_json(
        &rollout_paths.state,
        &serde_json::json!({
            "source": activation.source,
            "main_binary": activation.main_binary,
            "shadow_binary": activation.shadow_binary,
            "previous_main_binary": previous_main_binary,
            "status": "failed",
            "detail": detail,
            "requested_at": activation.requested_at,
            "failed_at": format_now(),
            "mode": activation.mode
        }),
    )
}

pub fn record_main_rollout_failure(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
    previous_main_binary: &Path,
    previous_shadow_binary: &Path,
    detail: &str,
) -> Result<(), RolloutError> {
    record_main_rollout_failure_with_diagnostics(
        rollout_paths,
        activation,
        previous_main_binary,
        previous_shadow_binary,
        detail,
        None,
    )
}

pub fn record_main_rollout_failure_with_diagnostics(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
    previous_main_binary: &Path,
    previous_shadow_binary: &Path,
    detail: &str,
    diagnostics: Option<&RolloutFailureDiagnostics>,
) -> Result<(), RolloutError> {
    let mut value = serde_json::json!({
        "source": activation.source,
        "main_binary": activation.main_binary,
        "shadow_binary": activation.shadow_binary,
        "previous_main_binary": previous_main_binary,
        "previous_shadow_binary": previous_shadow_binary,
        "status": "failed",
        "detail": detail,
        "requested_at": activation.requested_at,
        "failed_at": format_now(),
        "mode": activation.mode
    });
    if let Some(diagnostics) = diagnostics {
        value["diagnostics"] = serde_json::json!(diagnostics.diagnostics_path);
        if let Some(stdout_log) = &diagnostics.stdout_log {
            value["stdout_log"] = serde_json::json!(stdout_log);
        }
        if let Some(stderr_log) = &diagnostics.stderr_log {
            value["stderr_log"] = serde_json::json!(stderr_log);
        }
        if let Some(stdout_tail) = &diagnostics.stdout_tail {
            value["stdout_tail"] = serde_json::json!(stdout_tail);
        }
        if let Some(stderr_tail) = &diagnostics.stderr_tail {
            value["stderr_tail"] = serde_json::json!(stderr_tail);
        }
    }
    write_json(&rollout_paths.state, &value)
}

pub fn record_shadow_rollout_finalized(
    rollout_paths: &SelfRolloutPaths,
    finalize: &PendingShadowRollout,
    status: &str,
    detail: &str,
) -> Result<(), RolloutError> {
    write_json(
        &rollout_paths.state,
        &serde_json::json!({
            "source": finalize.source,
            "main_binary": finalize.main_binary,
            "shadow_binary": finalize.shadow_binary,
            "previous_shadow_binary": finalize.previous_shadow_binary,
            "requested_at": finalize.requested_at,
            "main_healthy_at": finalize.main_healthy_at,
            "updated_at": format_now(),
            "status": status,
            "detail": detail,
            "mode": finalize.mode
        }),
    )
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), RolloutError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let file = fs::File::create(path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::to_writer_pretty(file, value).map_err(|source| RolloutError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn remove_main_activation(paths: &SelfRolloutPaths) {
    let _ = fs::remove_file(rollout_main_activation_path(paths));
}

pub fn remove_shadow_finalize(paths: &SelfRolloutPaths) {
    let _ = fs::remove_file(rollout_shadow_finalize_path(paths));
}

fn default_rollout_main_host() -> String {
    "127.0.0.1".to_string()
}

fn default_rollout_main_port() -> u16 {
    21_353
}

fn format_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}
