use crate::process::{MainExitKind, MainExitObservation};
use crate::{RolloutError, SelfRolloutPaths, executable_name};
use serde::{Deserialize, Serialize};
use sinomo_core::ClusterNodeRolloutStatus;
use sinomo_object::{LengthMode, serde_ext};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const ROLLOUT_DIAGNOSTIC_TAIL_BYTES: usize = 8 * 1024;

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutMainExitRecord {
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub requested_shutdown: bool,
    pub kind: MainExitKind,
    pub observed_at: String,
}

pub trait PendingShadowRolloutContext {
    fn activation(&self) -> &RolloutMainActivation;
    fn main_binary(&self) -> &Path;
    fn main_host(&self) -> &str;
    fn main_port(&self) -> u16;
    fn previous_shadow_binary(&self) -> &Path;
}

pub fn rollout_main_activation_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("pending-main-rollout.sobj")
}

pub fn rollout_shadow_finalize_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("pending-shadow-rollout.sobj")
}

pub fn rollout_main_exit_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("last-main-exit.sobj")
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

pub fn read_tail_lossy(path: &Path, max_bytes: usize) -> Result<Option<String>, RolloutError> {
    match fs::read(path) {
        Ok(bytes) => {
            let start = bytes.len().saturating_sub(max_bytes);
            Ok(Some(String::from_utf8_lossy(&bytes[start..]).to_string()))
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn rollout_diagnostics_report(
    rollout_paths: &SelfRolloutPaths,
) -> Result<String, RolloutError> {
    let mut output = String::new();
    output.push_str(&format!("rollout root: {}\n", rollout_paths.root.display()));
    append_rollout_status_diagnostic(&mut output, rollout_paths)?;
    append_rollout_diagnostic_file(&mut output, "build log", &rollout_paths.build_log)?;
    append_rollout_diagnostic_file(
        &mut output,
        "pending main",
        &rollout_main_activation_path(rollout_paths),
    )?;
    append_rollout_diagnostic_file(
        &mut output,
        "pending shadow",
        &rollout_shadow_finalize_path(rollout_paths),
    )?;
    append_rollout_main_exit_diagnostic(&mut output, rollout_paths)?;

    if let Ok(Some(status)) = read_rollout_status(rollout_paths) {
        if let Some(path) = status.diagnostics.as_deref() {
            append_rollout_diagnostic_file(&mut output, "failure diagnostics", Path::new(path))?;
        }
        if let Some(path) = status.stdout_log.as_deref() {
            append_rollout_diagnostic_file(&mut output, "candidate stdout", Path::new(path))?;
        }
        if let Some(path) = status.stderr_log.as_deref() {
            append_rollout_diagnostic_file(&mut output, "candidate stderr", Path::new(path))?;
        }
    }

    let logs_dir = rollout_paths.root.join("logs");
    if logs_dir.exists() {
        output.push_str("\n[rollout logs]\n");
        let mut entries = fs::read_dir(&logs_dir)
            .map_err(|source| RolloutError::Io {
                path: logs_dir.display().to_string(),
                detail: source.to_string(),
            })?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let metadata = entry.metadata().ok()?;
                Some((entry.path(), metadata.modified().ok(), metadata.len()))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(_, modified, _)| *modified);
        for (path, _, len) in entries.into_iter().rev().take(8) {
            output.push_str(&format!("{} ({} bytes)\n", path.display(), len));
        }
    }

    Ok(output)
}

pub fn append_rollout_status_diagnostic(
    output: &mut String,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(), RolloutError> {
    output.push_str(&format!("\n[state] {}\n", rollout_paths.state.display()));
    match read_rollout_status(rollout_paths)? {
        Some(status) => {
            output.push_str(&format!("status: {}\n", status.status));
            if let Some(mode) = status.mode {
                output.push_str(&format!("mode: {mode}\n"));
            }
            if let Some(detail) = status.detail {
                output.push_str(&format!("detail: {detail}\n"));
            }
            if let Some(source) = status.source {
                output.push_str(&format!("source: {source}\n"));
            }
            if let Some(requested_at) = status.requested_at {
                output.push_str(&format!("requested_at: {requested_at}\n"));
            }
            if let Some(staged_at) = status.staged_at {
                output.push_str(&format!("staged_at: {staged_at}\n"));
            }
            if let Some(updated_at) = status.updated_at {
                output.push_str(&format!("updated_at: {updated_at}\n"));
            }
            if let Some(failed_at) = status.failed_at {
                output.push_str(&format!("failed_at: {failed_at}\n"));
            }
        }
        None => output.push_str("<missing>\n"),
    }
    Ok(())
}

pub fn append_rollout_diagnostic_file(
    output: &mut String,
    label: &str,
    path: &Path,
) -> Result<(), RolloutError> {
    output.push_str(&format!("\n[{label}] {}\n", path.display()));
    match read_tail_lossy(path, ROLLOUT_DIAGNOSTIC_TAIL_BYTES)? {
        Some(content) if !content.is_empty() => output.push_str(&content),
        Some(_) => output.push_str("<empty>\n"),
        None => output.push_str("<missing>\n"),
    }
    if !output.ends_with('\n') {
        output.push('\n');
    }
    Ok(())
}

pub fn append_rollout_main_exit_diagnostic(
    output: &mut String,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(), RolloutError> {
    let path = rollout_main_exit_path(rollout_paths);
    output.push_str(&format!("\n[last main exit] {}\n", path.display()));
    match read_rollout_main_exit(rollout_paths)? {
        Some(record) => {
            output.push_str(&format!("kind: {:?}\n", record.kind));
            if let Some(exit_code) = record.exit_code {
                output.push_str(&format!("exit_code: {exit_code}\n"));
            }
            if let Some(signal) = record.signal {
                output.push_str(&format!("signal: {signal}\n"));
            }
            output.push_str(&format!(
                "requested_shutdown: {}\n",
                record.requested_shutdown
            ));
            output.push_str(&format!("observed_at: {}\n", record.observed_at));
        }
        None => output.push_str("<missing>\n"),
    }
    Ok(())
}

pub fn read_rollout_main_activation(
    paths: &SelfRolloutPaths,
) -> Result<Option<RolloutMainActivation>, RolloutError> {
    let path = rollout_main_activation_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn read_pending_shadow_rollout(
    paths: &SelfRolloutPaths,
) -> Result<Option<PendingShadowRollout>, RolloutError> {
    let path = rollout_shadow_finalize_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
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
    write_object(&rollout_shadow_finalize_path(rollout_paths), &finalize)
}

pub fn write_main_activation(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
) -> Result<(), RolloutError> {
    write_object(&rollout_main_activation_path(rollout_paths), activation)
}

pub fn record_main_activation_staged(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
) -> Result<(), RolloutError> {
    write_rollout_status(
        rollout_paths,
        &ClusterNodeRolloutStatus {
            status: "main-staged".to_string(),
            mode: Some(activation.mode.clone()),
            detail: None,
            source: Some(activation.source.clone()),
            requested_at: Some(activation.requested_at.clone()),
            staged_at: Some(format_now()),
            updated_at: None,
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        },
    )
}

pub fn record_activation_failure(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
    previous_main_binary: &Path,
    detail: &str,
) -> Result<(), RolloutError> {
    let _ = previous_main_binary;
    write_rollout_status(
        rollout_paths,
        &ClusterNodeRolloutStatus {
            status: "failed".to_string(),
            mode: Some(activation.mode.clone()),
            detail: Some(detail.to_string()),
            source: Some(activation.source.clone()),
            requested_at: Some(activation.requested_at.clone()),
            staged_at: None,
            updated_at: None,
            failed_at: Some(format_now()),
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        },
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
    let _ = (previous_main_binary, previous_shadow_binary);
    write_rollout_status(
        rollout_paths,
        &ClusterNodeRolloutStatus {
            status: "failed".to_string(),
            mode: Some(activation.mode.clone()),
            detail: Some(detail.to_string()),
            source: Some(activation.source.clone()),
            requested_at: Some(activation.requested_at.clone()),
            staged_at: None,
            updated_at: None,
            failed_at: Some(format_now()),
            diagnostics: diagnostics.map(|value| value.diagnostics_path.display().to_string()),
            stdout_log: diagnostics
                .and_then(|value| value.stdout_log.as_ref())
                .map(|path| path.display().to_string()),
            stderr_log: diagnostics
                .and_then(|value| value.stderr_log.as_ref())
                .map(|path| path.display().to_string()),
            stdout_tail: diagnostics.and_then(|value| value.stdout_tail.clone()),
            stderr_tail: diagnostics.and_then(|value| value.stderr_tail.clone()),
        },
    )
}

pub fn record_shadow_rollout_finalized(
    rollout_paths: &SelfRolloutPaths,
    finalize: &PendingShadowRollout,
    status: &str,
    detail: &str,
) -> Result<(), RolloutError> {
    write_rollout_status(
        rollout_paths,
        &ClusterNodeRolloutStatus {
            status: status.to_string(),
            mode: Some(finalize.mode.clone()),
            detail: Some(detail.to_string()),
            source: Some(finalize.source.clone()),
            requested_at: Some(finalize.requested_at.clone()),
            staged_at: None,
            updated_at: Some(format_now()),
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        },
    )
}

pub fn read_rollout_status(
    rollout_paths: &SelfRolloutPaths,
) -> Result<Option<ClusterNodeRolloutStatus>, RolloutError> {
    if !rollout_paths.state.exists() {
        return Ok(None);
    }
    read_object(&rollout_paths.state).map(Some)
}

pub fn write_rollout_status(
    rollout_paths: &SelfRolloutPaths,
    status: &ClusterNodeRolloutStatus,
) -> Result<(), RolloutError> {
    write_object(&rollout_paths.state, status)
}

pub fn read_rollout_main_exit(
    rollout_paths: &SelfRolloutPaths,
) -> Result<Option<RolloutMainExitRecord>, RolloutError> {
    let path = rollout_main_exit_path(rollout_paths);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn record_rollout_main_exit(
    rollout_paths: &SelfRolloutPaths,
    observation: MainExitObservation,
    kind: MainExitKind,
) -> Result<(), RolloutError> {
    write_object(
        &rollout_main_exit_path(rollout_paths),
        &RolloutMainExitRecord {
            exit_code: observation.exit_code,
            signal: observation.signal,
            requested_shutdown: observation.requested_shutdown,
            kind,
            observed_at: format_now(),
        },
    )
}

pub fn write_object<T: Serialize>(path: &Path, value: &T) -> Result<(), RolloutError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes =
        serde_ext::to_bytes(value, ROLLOUT_OBJECT_VERSION, LengthMode::U32).map_err(|source| {
            RolloutError::Object {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    fs::write(path, bytes).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn read_object<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, RolloutError> {
    let bytes = fs::read(path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_ext::from_bytes(&bytes).map_err(|source| RolloutError::Object {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
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
const ROLLOUT_OBJECT_VERSION: u16 = 1;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn read_tail_lossy_limits_bytes_and_handles_missing_file() {
        let root = unique_temp("sinomo-rollout-tail");
        fs::create_dir_all(&root).expect("root");
        let path = root.join("tail.log");
        fs::write(&path, "0123456789").expect("log");

        assert_eq!(
            read_tail_lossy(&path, 4).expect("tail"),
            Some("6789".to_string())
        );
        assert_eq!(
            read_tail_lossy(&root.join("missing.log"), 4).expect("missing"),
            None
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rollout_diagnostics_report_includes_state_and_tail_files() {
        let root = unique_temp("sinomo-rollout-diagnostics");
        let paths = SelfRolloutPaths::new(&root);
        fs::create_dir_all(&paths.root).expect("rollout root");
        fs::write(&paths.build_log, "build log\n").expect("build log");
        write_rollout_status(
            &paths,
            &ClusterNodeRolloutStatus {
                status: "main-staged".to_string(),
                mode: Some("cluster-remote-build".to_string()),
                detail: Some("ready".to_string()),
                source: Some("source-head".to_string()),
                requested_at: Some("2026-05-08T00:00:00Z".to_string()),
                staged_at: None,
                updated_at: None,
                failed_at: None,
                diagnostics: None,
                stdout_log: None,
                stderr_log: None,
                stdout_tail: None,
                stderr_tail: None,
            },
        )
        .expect("status");

        let report = rollout_diagnostics_report(&paths).expect("diagnostics");

        assert!(report.contains("rollout root:"));
        assert!(report.contains("status: main-staged"));
        assert!(report.contains("mode: cluster-remote-build"));
        assert!(report.contains("[build log]"));
        assert!(report.contains("build log"));
        assert!(report.contains("[last main exit]"));
        fs::remove_dir_all(root).ok();
    }

    fn unique_temp(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}",
            prefix,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }
}
