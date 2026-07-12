use crate::cluster::{CLUSTER_HEALTH_PROBE_TIMEOUT, start_shadow_daemon_status_publisher};
#[cfg(not(unix))]
use crate::spawn_shadow_rollout_completion_helper_process;
use crate::{
    CandidateMainDiagnostics, DaemonClusterStatus, DaemonError, DaemonLock, DaemonPaths,
    DaemonProbeStatus, DaemonStatus, daemon_connect_address, prepare_background_command,
    probe_daemon_status, read_daemon_status, send_daemon_shutdown_request,
    spawn_candidate_main_daemon_process, spawn_main_daemon_process,
    spawn_shadow_rollout_recovery_helper_process,
};
use fs2::FileExt;
#[cfg(test)]
use sinomo_rollout::read_rollout_status;
use sinomo_rollout::{
    CLUSTER_ROLLOUT_BUILD_HEARTBEAT_INTERVAL, CLUSTER_ROLLOUT_PROGRESS_TOTAL,
    ClusterRolloutBinaryManifest, ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse,
    ClusterRolloutBuildRequest, ClusterRolloutNativeDylibModuleRequest,
    ClusterRolloutNativeDylibModuleResponse, ClusterRolloutNativeDylibModuleUploadSpec,
    ClusterRolloutProgressUpdate, ClusterRolloutUploadArtifactSpec, LocalRolloutArtifacts,
    LocalRolloutNativeDylibModuleArtifact, MainExitObservation, ManagedSourceDeployOptions,
    PendingShadowRolloutContext, ROLLOUT_DIAGNOSTIC_TAIL_BYTES, RolloutActivationStrategy,
    RolloutFailureDiagnostics, RolloutMainActivation, SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV,
    SHADOW_MAIN_PORT_ENV, SHADOW_START_DELAY_MS_ENV, SelfRolloutPaths,
    activate_staged_rollout_binary, classify_main_exit, clear_rollout_status,
    cluster_rollout_progress_status, deploy_managed_source_bundle_with_options,
    ensure_rollout_layout, managed_source_status, read_object, read_pending_shadow_rollout,
    read_rollout_main_activation, read_tail_lossy, record_activation_failure,
    record_main_activation_staged, record_main_rollout_failure_with_diagnostics,
    record_rollout_main_exit, record_shadow_rollout_finalized, remove_main_activation,
    remove_shadow_finalize, restore_rollout_binary_from_backup, rollout_artifact_suffix,
    rollout_build_dev_artifacts, rollout_main_activation_path, rollout_previous_binary_path,
    rollout_remote_repo_path, rollout_shadow_finalize_path, rollout_staged_main_binary,
    rollout_staged_shadow_binary, rollout_timestamp_now, source_heads_match,
    stage_pending_shadow_rollout, stage_rollout_file, stage_rollout_file_pair, write_json,
    write_main_activation, write_object, write_rollout_status,
};
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
const MAIN_ACTIVATION_SHUTDOWN_GRACE: Duration = Duration::from_millis(750);
const STABLE_BINARY_COPY_TIMEOUT: Duration = Duration::from_secs(30);
const STABLE_BINARY_COPY_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const CLUSTER_ROLLOUT_BODY_BUFFER_BYTES: usize = 64 * 1024;
const SHADOW_MAIN_RESTART_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const SHADOW_MAIN_RESTART_MAX_BACKOFF: Duration = Duration::from_secs(30);
const SHADOW_MAIN_RESTART_STABLE_RESET_AFTER: Duration = Duration::from_secs(60);
const SUPERVISED_MAIN_RESTART_REQUEST_FILE: &str = "supervised-main-restart.request";
const SHADOW_RUNTIME_TASK_JOIN_TIMEOUT: Duration = Duration::from_millis(750);
const SHADOW_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
const SHADOW_STATUS_PUBLISHER_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
const SHADOW_RUNTIME_TASK_DRAIN_GRACE: Duration = Duration::from_millis(100);
const SHADOW_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const PENDING_MAIN_ACTIVATION_FORCE_TERMINATE_AFTER: Duration = Duration::from_secs(10);
const PENDING_MAIN_ACTIVATION_TERMINATE_WAIT: Duration = Duration::from_secs(5);
const ROLLOUT_DAEMON_TERMINATE_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RolloutTerminationMode {
    ProcessOnly,
    ProcessTree,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StagedMainActivationShutdown {
    Immediate,
    DeferredSelf,
}

#[allow(clippy::too_many_arguments)]
fn write_cluster_rollout_progress(
    daemon_paths: &DaemonPaths,
    rollout_paths: &SelfRolloutPaths,
    status: &str,
    mode: &str,
    detail: Option<String>,
    source: &str,
    requested_at: &str,
    phase: &str,
    progress_current: u32,
    progress_message: impl Into<String>,
) -> Result<(), DaemonError> {
    write_cluster_rollout_progress_inner(
        daemon_paths,
        rollout_paths,
        status,
        mode,
        detail,
        source,
        requested_at,
        phase,
        progress_current,
        progress_message,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_cluster_rollout_progress_local(
    daemon_paths: &DaemonPaths,
    rollout_paths: &SelfRolloutPaths,
    status: &str,
    mode: &str,
    detail: Option<String>,
    source: &str,
    requested_at: &str,
    phase: &str,
    progress_current: u32,
    progress_message: impl Into<String>,
) -> Result<(), DaemonError> {
    write_cluster_rollout_progress_inner(
        daemon_paths,
        rollout_paths,
        status,
        mode,
        detail,
        source,
        requested_at,
        phase,
        progress_current,
        progress_message,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_cluster_rollout_progress_inner(
    daemon_paths: &DaemonPaths,
    rollout_paths: &SelfRolloutPaths,
    status: &str,
    mode: &str,
    detail: Option<String>,
    source: &str,
    requested_at: &str,
    phase: &str,
    progress_current: u32,
    progress_message: impl Into<String>,
    publish_to_cluster: bool,
) -> Result<(), DaemonError> {
    let progress_message = progress_message.into();
    let status = cluster_rollout_progress_status(ClusterRolloutProgressUpdate::new(
        status,
        mode,
        detail,
        source,
        requested_at,
        phase,
        progress_current,
        progress_message.clone(),
    ));
    write_rollout_status(rollout_paths, &status)?;
    let status_label = status.status.clone();
    let kind = match status_label.as_str() {
        "accepted" => "rollout.stage.accepted",
        "failed" => "rollout.stage.failed",
        "staged" => "rollout.stage.staged",
        _ => "rollout.stage.progress",
    };
    let severity = if status_label == "failed" {
        crate::OperationalSeverity::Error
    } else {
        crate::OperationalSeverity::Info
    };
    crate::append_operational_event_best_effort(
        daemon_paths,
        severity,
        "rollout",
        kind,
        progress_message,
        Some(requested_at),
        serde_json::json!({
            "status": status_label,
            "mode": mode,
            "detail": status.detail,
            "source": source,
            "requested_at": requested_at,
            "phase": phase,
            "progress_current": progress_current,
            "progress_total": CLUSTER_ROLLOUT_PROGRESS_TOTAL,
        }),
    );
    if publish_to_cluster {
        publish_rollout_progress_to_cluster(daemon_paths);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn rejected_cluster_rollout_response(
    daemon_paths: &DaemonPaths,
    rollout_paths: &SelfRolloutPaths,
    mode: &str,
    source: &str,
    requested_at: &str,
    phase: &str,
    progress_message: &str,
    detail: String,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let _ = write_cluster_rollout_progress(
        daemon_paths,
        rollout_paths,
        "rejected",
        mode,
        Some(detail.clone()),
        source,
        requested_at,
        phase,
        CLUSTER_ROLLOUT_PROGRESS_TOTAL,
        progress_message,
    );
    Ok(ClusterRolloutBinaryResponse {
        status: "rejected".to_string(),
        detail,
        main_binary: PathBuf::new(),
        shadow_binary: PathBuf::new(),
        source_head: None,
    })
}

fn publish_rollout_progress_to_cluster(daemon_paths: &DaemonPaths) {
    let daemon_paths = daemon_paths.clone();
    if let Err(error) = thread::Builder::new()
        .name("sinomo-rollout-progress-publish".to_string())
        .spawn(move || {
            if let Err(error) = crate::cluster::refresh_daemon_node_status_in_kv(&daemon_paths) {
                tracing::warn!("sinomo daemon rollout progress publish failed: {error}");
            }
        })
    {
        tracing::warn!("sinomo daemon rollout progress publish spawn failed: {error}");
    }
}

#[allow(clippy::too_many_arguments)]
fn start_cluster_rollout_build_heartbeat(
    daemon_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    mode: &'static str,
    source: String,
    requested_at: String,
    phase: &'static str,
    progress_current: u32,
) -> (std::sync::mpsc::Sender<()>, thread::JoinHandle<()>) {
    let (stop_tx, stop_rx) = std::sync::mpsc::channel();
    let handle = thread::spawn(move || {
        let started = Instant::now();
        loop {
            match stop_rx.recv_timeout(CLUSTER_ROLLOUT_BUILD_HEARTBEAT_INTERVAL) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            let elapsed = started.elapsed().as_secs();
            let _ = write_cluster_rollout_progress_local(
                &daemon_paths,
                &rollout_paths,
                "in-progress",
                mode,
                Some(format!("building for {elapsed}s")),
                &source,
                &requested_at,
                phase,
                progress_current,
                format!("cargo build running for {elapsed}s"),
            );
        }
    });
    (stop_tx, handle)
}

#[derive(Clone, Debug)]
pub struct ShadowMainRolloutMonitor {
    pub activation: RolloutMainActivation,
    pub main_options: crate::DaemonRunOptions,
    pub previous_main_binary: PathBuf,
    pub previous_shadow_binary: PathBuf,
    pub diagnostics: CandidateMainDiagnostics,
    pub deadline: Instant,
}

#[derive(Clone, Debug)]
pub struct ShadowSupervisorDiagnostics {
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
}

pub enum ShadowMainStart {
    None,
    Main {
        child: Child,
    },
    StartFailed {
        detail: String,
    },
    Candidate {
        child: Child,
        monitor: ShadowMainRolloutMonitor,
        main_binary: PathBuf,
    },
    Recovered {
        child: Option<Child>,
        main_binary: PathBuf,
        main_options: crate::DaemonRunOptions,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShadowMainExit {
    PendingActivation,
    RestartRequested,
    Suppressed,
}

pub enum PendingShadowMainExit {
    Promoted {
        main_binary: PathBuf,
        main_options: crate::DaemonRunOptions,
        main_probe_status: DaemonProbeStatus,
    },
    RetryCandidate {
        child: Child,
        monitor: ShadowMainRolloutMonitor,
    },
    Recovered {
        child: Option<Child>,
        main_binary: PathBuf,
        main_options: crate::DaemonRunOptions,
    },
}

fn supervised_main_restart_request_path(rollout_paths: &SelfRolloutPaths) -> PathBuf {
    rollout_paths
        .root
        .join(SUPERVISED_MAIN_RESTART_REQUEST_FILE)
}

/// Mark a clean main-daemon exit as an intentional identity/runtime restart.
/// The requesting PID prevents a stale marker from changing the meaning of a
/// later operator-requested shutdown.
pub(crate) fn request_supervised_main_restart(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    fs::create_dir_all(&rollout_paths.root).map_err(|source| DaemonError::Io {
        path: rollout_paths.root.display().to_string(),
        detail: source.to_string(),
    })?;
    let marker = supervised_main_restart_request_path(&rollout_paths);
    fs::write(&marker, format!("{}\n", std::process::id())).map_err(|source| DaemonError::Io {
        path: marker.display().to_string(),
        detail: source.to_string(),
    })
}

fn take_supervised_main_restart_request(
    rollout_paths: &SelfRolloutPaths,
    exited_pid: u32,
) -> Result<bool, DaemonError> {
    let marker = supervised_main_restart_request_path(rollout_paths);
    let requested_pid = match fs::read_to_string(&marker) {
        Ok(value) => value.trim().parse::<u32>().ok(),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(DaemonError::Io {
                path: marker.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    match fs::remove_file(&marker) {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(DaemonError::Io {
                path: marker.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    Ok(requested_pid == Some(exited_pid))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalRolloutMainActivationPlan {
    pub activation: RolloutMainActivation,
    pub previous_main_status: Option<DaemonStatus>,
}

#[derive(Clone, Debug, Default)]
struct ShadowMainRestartBackoff {
    consecutive_failures: u32,
    next_restart_at: Option<Instant>,
}

impl ShadowMainRestartBackoff {
    fn can_restart(&self, now: Instant) -> bool {
        self.next_restart_at
            .is_none_or(|next_restart_at| now >= next_restart_at)
    }

    fn schedule_after_failure(&mut self, now: Instant) -> Duration {
        let shift = self.consecutive_failures.min(7);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let multiplier = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
        let delay = SHADOW_MAIN_RESTART_INITIAL_BACKOFF
            .saturating_mul(multiplier)
            .min(SHADOW_MAIN_RESTART_MAX_BACKOFF);
        self.next_restart_at = Some(now + delay);
        delay
    }

    fn clear_schedule(&mut self) {
        self.next_restart_at = None;
    }

    fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.next_restart_at = None;
    }

    fn reset_after_stable_start(&mut self, started_at: Option<Instant>, now: Instant) {
        let Some(started_at) = started_at else {
            return;
        };
        if now.duration_since(started_at) >= SHADOW_MAIN_RESTART_STABLE_RESET_AFTER {
            self.reset();
        }
    }
}

impl PendingShadowRolloutContext for ShadowMainRolloutMonitor {
    fn activation(&self) -> &RolloutMainActivation {
        &self.activation
    }

    fn main_binary(&self) -> &Path {
        &self.activation.main_binary
    }

    fn main_host(&self) -> &str {
        &self.main_options.host
    }

    fn main_port(&self) -> u16 {
        self.main_options.port
    }

    fn previous_main_binary(&self) -> Option<&Path> {
        Some(&self.previous_main_binary)
    }

    fn previous_shadow_binary(&self) -> &Path {
        &self.previous_shadow_binary
    }
}

pub fn prepare_local_rollout_main_activation(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    artifacts: &LocalRolloutArtifacts,
    fallback_host: &str,
    fallback_port: u16,
    shadow_port: u16,
    requested_at: String,
    probe_timeout: Duration,
) -> LocalRolloutMainActivationPlan {
    let plan = rollout_paths.plan();
    let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
    let previous_main_status = match probe_daemon_status(&main_paths, probe_timeout) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { status } => status,
        DaemonProbeStatus::NotRunning => None,
    };
    let previous_main_binary = previous_main_status
        .as_ref()
        .and_then(|status| status.executable.clone())
        .unwrap_or_else(|| plan.install_main_binary.clone());
    let main_host = previous_main_status
        .as_ref()
        .map(|status| status.host.clone())
        .unwrap_or_else(|| fallback_host.to_string());
    let main_port = previous_main_status
        .as_ref()
        .map(|status| status.port)
        .unwrap_or(fallback_port);

    LocalRolloutMainActivationPlan {
        activation: RolloutMainActivation {
            source: rollout_paths.source_repo.display().to_string(),
            main_binary: plan.install_main_binary,
            shadow_binary: plan.install_shadow_binary,
            staged_main_binary: Some(artifacts.staged_main_binary.clone()),
            staged_shadow_binary: Some(artifacts.staged_shadow_binary.clone()),
            previous_main_binary: Some(previous_main_binary),
            main_host,
            main_port,
            shadow_port,
            requested_at,
            mode: "local-apply".to_string(),
        },
        previous_main_status,
    }
}

fn install_local_rollout_native_dylib_modules(
    data_dir: &Path,
    artifacts: &[LocalRolloutNativeDylibModuleArtifact],
) -> Result<Vec<sinomo_module::NativeDylibModuleInstallResult>, DaemonError> {
    sinomo_rollout::install_local_rollout_native_dylib_modules(data_dir, artifacts)
        .map_err(Into::into)
}

fn shadow_reexec_request_path(rollout_paths: &SelfRolloutPaths) -> PathBuf {
    rollout_paths.root.join("shadow-reexec.request")
}

/// Consumes a pending shadow re-exec request, returning the target binary if set.
fn take_shadow_reexec_request(rollout_paths: &SelfRolloutPaths) -> Option<PathBuf> {
    let path = shadow_reexec_request_path(rollout_paths);
    let target = fs::read_to_string(&path).ok()?;
    let _ = fs::remove_file(&path);
    let target = target.trim();
    if target.is_empty() {
        None
    } else {
        Some(PathBuf::from(target))
    }
}

fn activate_staged_rollout_binary_with_retry(
    staged: &Path,
    destination: &Path,
    timeout: Duration,
    retry_interval: Duration,
) -> Result<Option<PathBuf>, DaemonError> {
    let deadline = Instant::now() + timeout;
    loop {
        match activate_staged_rollout_binary(staged, destination) {
            Ok(previous) => return Ok(previous),
            Err(_) if Instant::now() < deadline => thread::sleep(retry_interval),
            Err(error) => return Err(error.into()),
        }
    }
}

fn restore_rollout_binary_from_backup_with_retry(
    backup: &Path,
    destination: &Path,
    timeout: Duration,
    retry_interval: Duration,
) -> Result<(), DaemonError> {
    let deadline = Instant::now() + timeout;
    loop {
        match restore_rollout_binary_from_backup(backup, destination) {
            Ok(()) => return Ok(()),
            Err(_) if Instant::now() < deadline => thread::sleep(retry_interval),
            Err(error) => return Err(error.into()),
        }
    }
}

fn complete_pending_shadow_rollout_after_start(
    paths: &DaemonPaths,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(), DaemonError> {
    let Some(finalize) = read_pending_shadow_rollout(rollout_paths)? else {
        return Ok(());
    };
    let Ok(current) = env::current_exe() else {
        return Ok(());
    };
    if !sinomo_rollout::same_path_if_exists(&current, &finalize.shadow_binary) {
        return Ok(());
    }
    if rehydrate_main_activation_if_staged_main_is_pending(paths, rollout_paths, &finalize)? {
        return Ok(());
    }
    record_shadow_rollout_finalized(
        rollout_paths,
        &finalize,
        "accepted",
        "shadow rollout finalized",
    )?;
    remove_main_activation(rollout_paths);
    remove_shadow_finalize(rollout_paths);
    crate::append_operational_event_best_effort(
        paths,
        crate::OperationalSeverity::Info,
        "rollout",
        "rollout.stage.accepted",
        "shadow rollout finalized",
        Some(&finalize.requested_at),
        serde_json::json!({
            "source": finalize.source,
            "requested_at": finalize.requested_at,
            "shadow_binary": finalize.shadow_binary.display().to_string(),
        }),
    );
    publish_rollout_progress_to_cluster(paths);
    Ok(())
}

fn rehydrate_main_activation_if_staged_main_is_pending(
    paths: &DaemonPaths,
    rollout_paths: &SelfRolloutPaths,
    finalize: &sinomo_rollout::PendingShadowRollout,
) -> Result<bool, DaemonError> {
    let staged_main = finalize
        .staged_main_binary
        .clone()
        .or_else(|| {
            let default_staged = rollout_staged_main_binary(rollout_paths);
            default_staged.exists().then_some(default_staged)
        })
        .filter(|path| path.exists());
    let Some(staged_main) = staged_main else {
        return Ok(false);
    };
    let activation = RolloutMainActivation {
        source: finalize.source.clone(),
        main_binary: finalize.main_binary.clone(),
        shadow_binary: finalize.shadow_binary.clone(),
        staged_main_binary: Some(staged_main.clone()),
        staged_shadow_binary: finalize.staged_shadow_binary.clone(),
        previous_main_binary: finalize.previous_main_binary.clone(),
        main_host: finalize.main_host.clone(),
        main_port: finalize.main_port,
        shadow_port: finalize.shadow_port,
        requested_at: finalize.requested_at.clone(),
        mode: finalize.mode.clone(),
    };
    write_main_activation(rollout_paths, &activation)?;
    let detail = format!(
        "shadow finalization deferred because staged main binary {} is still pending",
        staged_main.display()
    );
    write_cluster_rollout_progress(
        paths,
        rollout_paths,
        "in-progress",
        &finalize.mode,
        Some(detail),
        &finalize.source,
        &finalize.requested_at,
        "main-promotion-pending",
        4,
        "rehydrated pending main activation",
    )?;
    Ok(true)
}

/// Re-executes the shadow supervisor in place from `target`, preserving the listen
/// options and pointing it at the (rolled-out) installed main binary. On success
/// this never returns (the process image is replaced); on failure it returns Err.
#[cfg(unix)]
fn reexec_shadow_supervisor(
    target: &Path,
    shadow_options: &crate::DaemonRunOptions,
    main_binary: &Path,
    main_options: &crate::DaemonRunOptions,
) -> DaemonError {
    use std::os::unix::process::CommandExt;
    let error = Command::new(target)
        .args([
            "daemon".to_string(),
            "shadow".to_string(),
            "run".to_string(),
            "--host".to_string(),
            shadow_options.host.clone(),
            "--port".to_string(),
            shadow_options.port.to_string(),
        ])
        .env(SHADOW_MAIN_BINARY_ENV, main_binary)
        .env(SHADOW_MAIN_HOST_ENV, &main_options.host)
        .env(SHADOW_MAIN_PORT_ENV, main_options.port.to_string())
        .env(SHADOW_START_DELAY_MS_ENV, "0")
        .exec();
    DaemonError::Io {
        path: target.display().to_string(),
        detail: format!("shadow supervisor re-exec failed: {error}"),
    }
}

fn join_shadow_runtime_task(
    runtime: &tokio::runtime::Runtime,
    label: &str,
    task: tokio::task::JoinHandle<()>,
    abort_first: bool,
) {
    let abort_handle = task.abort_handle();
    if abort_first {
        abort_handle.abort();
    }
    let join_timeout = shadow_runtime_task_join_timeout(label);
    let result = runtime.block_on(async { tokio::time::timeout(join_timeout, task).await });
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.is_cancelled() => {}
        Ok(Err(error)) => {
            tracing::warn!("sinomo shadow daemon {label} task stopped with error: {error}");
        }
        Err(_) => {
            abort_handle.abort();
            tracing::warn!(
                "sinomo shadow daemon {label} task did not stop within {}ms",
                join_timeout.as_millis()
            );
        }
    }
}

fn shadow_runtime_task_join_timeout(label: &str) -> Duration {
    match label {
        "network module context" => SHADOW_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT,
        "status publisher" => SHADOW_STATUS_PUBLISHER_TASK_JOIN_TIMEOUT,
        _ => SHADOW_RUNTIME_TASK_JOIN_TIMEOUT,
    }
}

fn drain_shadow_runtime_after_task_shutdown(runtime: &tokio::runtime::Runtime) {
    runtime.block_on(async {
        tokio::task::yield_now().await;
        tokio::time::sleep(SHADOW_RUNTIME_TASK_DRAIN_GRACE).await;
        tokio::task::yield_now().await;
    });
}

fn shutdown_shadow_runtime_after_task_shutdown(runtime: tokio::runtime::Runtime) {
    drain_shadow_runtime_after_task_shutdown(&runtime);
    runtime.shutdown_timeout(SHADOW_RUNTIME_SHUTDOWN_TIMEOUT);
}

pub fn run_shadow_daemon_supervision_loop(
    options: crate::DaemonRunOptions,
    shadow_paths: DaemonPaths,
    main_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    mut main_options: crate::DaemonRunOptions,
    mut main_binary: PathBuf,
    cluster: Option<DaemonClusterStatus>,
    finalize_timeout: Duration,
    probe_timeout: Duration,
) -> Result<(), DaemonError> {
    let _lock = DaemonLock::acquire(&shadow_paths, &options, cluster.clone())?;
    let bind_address = format!("{}:{}", options.host, options.port);
    let daemon_runtime = crate::build_daemon_async_runtime("shadow-daemon")?;
    let daemon_runtime_handle = daemon_runtime.handle().clone();
    let listener = daemon_runtime.block_on(
        sinomo_core::network::AsyncDaemonControlListener::bind(&bind_address),
    )?;
    let mut shadow_runtime_tasks = Vec::new();
    let (control_shutdown_tx, control_shutdown_rx) = tokio::sync::watch::channel(false);
    let control_shutdown_signal = control_shutdown_tx.clone();
    tracing::info!("sinomo shadow daemon listening on {bind_address}");
    crate::append_operational_event_best_effort(
        &shadow_paths,
        crate::OperationalSeverity::Info,
        "shadow-daemon",
        "daemon.lifecycle.started",
        format!("sinomo shadow daemon listening on {bind_address}"),
        None,
        serde_json::json!({
            "label": "shadow-daemon",
            "address": bind_address,
            "pid": std::process::id(),
            "cluster_node_id": cluster.as_ref().map(|status| status.node_id.to_string()),
            "main_binary": main_binary.display().to_string(),
        }),
    );
    if let Some(task) = crate::cluster::start_daemon_network_module_context_task(
        &shadow_paths,
        &options,
        cluster.as_ref(),
        daemon_runtime_handle.clone(),
        control_shutdown_rx.clone(),
        "shadow daemon",
    ) {
        shadow_runtime_tasks.push(("network module context", task));
    }
    match start_shadow_daemon_status_publisher(
        &shadow_paths,
        daemon_runtime_handle.clone(),
        control_shutdown_rx.clone(),
    ) {
        Ok(task) => shadow_runtime_tasks.push(("status publisher", task)),
        Err(error) => {
            tracing::warn!("sinomo shadow daemon status publisher disabled: {error}");
            crate::append_operational_log_best_effort(
                &shadow_paths,
                crate::OperationalSeverity::Warn,
                "shadow-daemon",
                "sinomo shadow daemon status publisher disabled",
                None,
                serde_json::json!({
                    "error": error.to_string(),
                }),
            );
        }
    }
    if let Err(error) = complete_pending_shadow_rollout_after_start(&shadow_paths, &rollout_paths) {
        tracing::warn!("sinomo shadow daemon rollout finalization publish failed: {error}");
        crate::append_operational_log_best_effort(
            &shadow_paths,
            crate::OperationalSeverity::Warn,
            "shadow-daemon",
            "sinomo shadow daemon rollout finalization publish failed",
            None,
            serde_json::json!({
                "error": error.to_string(),
            }),
        );
    }

    let mut supervised_main: Option<Child> = None;
    let mut supervised_main_started_at: Option<Instant> = None;
    let mut main_restart_suppressed = false;
    let mut main_restart_backoff = ShadowMainRestartBackoff::default();
    let mut pending_rollout: Option<ShadowMainRolloutMonitor> = None;
    let shadow_idle_interval = Duration::from_millis(100);
    let mut last_main_probe = Instant::now() - shadow_main_probe_interval(false);
    let mut next_pending_activation_shutdown_nudge = Instant::now();
    let mut pending_activation_shutdown_started_at: Option<Instant> = None;
    let mut main_probe_status = DaemonProbeStatus::NotRunning;
    let (control_loop_tx, control_loop_rx) = std::sync::mpsc::channel::<Result<(), DaemonError>>();
    let control_paths = shadow_paths.clone();
    let control_bind_address = bind_address.clone();
    let control_service_runtime = daemon_runtime_handle.clone();
    let shadow_control_accept_task = daemon_runtime_handle.spawn(async move {
        let result = match crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime(
            control_paths.clone(),
            control_service_runtime.clone(),
        )
        .await
        {
            Ok(service_host) => {
                crate::control::run_daemon_control_accept_loop(
                    listener,
                    control_paths,
                    "shadow daemon".to_string(),
                    control_bind_address,
                    service_host,
                    control_service_runtime,
                    control_shutdown_tx,
                    control_shutdown_rx,
                )
                .await
            }
            Err(error) => Err(error),
        };
        let _ = control_loop_tx.send(result);
    });

    loop {
        match control_loop_rx.try_recv() {
            Ok(Ok(())) => break,
            Ok(Err(error)) => {
                let _ = control_shutdown_signal.send(true);
                join_shadow_runtime_task(
                    &daemon_runtime,
                    "control accept",
                    shadow_control_accept_task,
                    false,
                );
                for (label, task) in shadow_runtime_tasks {
                    join_shadow_runtime_task(&daemon_runtime, label, task, false);
                }
                shutdown_shadow_runtime_after_task_shutdown(daemon_runtime);
                return Err(error);
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                let _ = control_shutdown_signal.send(true);
                join_shadow_runtime_task(
                    &daemon_runtime,
                    "control accept",
                    shadow_control_accept_task,
                    false,
                );
                for (label, task) in shadow_runtime_tasks {
                    join_shadow_runtime_task(&daemon_runtime, label, task, false);
                }
                shutdown_shadow_runtime_after_task_shutdown(daemon_runtime);
                return Err(DaemonError::Network {
                    address: bind_address.clone(),
                    detail: "shadow daemon control accept loop stopped before reporting shutdown"
                        .to_string(),
                });
            }
        }
        let now = Instant::now();

        let main_probe_interval = shadow_main_probe_interval(pending_rollout.is_some());
        if now.duration_since(last_main_probe) >= main_probe_interval {
            main_probe_status = probe_daemon_status(&main_paths, probe_timeout);
            last_main_probe = now;
            if matches!(main_probe_status, DaemonProbeStatus::Running(_))
                && pending_rollout.is_none()
            {
                main_restart_backoff.reset_after_stable_start(supervised_main_started_at, now);
            }
        }

        let pending_main_activation = rollout_main_activation_path(&rollout_paths).exists();
        if !pending_main_activation {
            pending_activation_shutdown_started_at = None;
        }
        if pending_rollout.is_none()
            && pending_main_activation_stop_target(&main_probe_status)
            && pending_main_activation
            && pending_main_activation_ready_for_shutdown(&rollout_paths)
            && Instant::now() >= next_pending_activation_shutdown_nudge
        {
            let shutdown_started_at =
                *pending_activation_shutdown_started_at.get_or_insert_with(Instant::now);
            match probe_daemon_status(&main_paths, probe_timeout) {
                DaemonProbeStatus::Running(status)
                | DaemonProbeStatus::Locked {
                    status: Some(status),
                } => {
                    let _ = send_daemon_shutdown_request(&status.address, probe_timeout);
                }
                DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {}
            }
            if shutdown_started_at.elapsed() >= PENDING_MAIN_ACTIVATION_FORCE_TERMINATE_AFTER {
                match terminate_pending_main_activation(
                    &main_paths,
                    PENDING_MAIN_ACTIVATION_TERMINATE_WAIT,
                    probe_timeout,
                ) {
                    Ok(Some(pid)) => {
                        main_probe_status = DaemonProbeStatus::NotRunning;
                        last_main_probe = Instant::now();
                        pending_activation_shutdown_started_at = None;
                        crate::append_operational_event_best_effort(
                            &shadow_paths,
                            crate::OperationalSeverity::Warn,
                            "shadow-daemon",
                            "rollout.stage.main_terminated",
                            "shadow daemon terminated unresponsive main for pending rollout activation",
                            None,
                            serde_json::json!({
                                "pid": pid,
                                "phase": "main-staged",
                            }),
                        );
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(
                            "sinomo shadow daemon could not terminate unresponsive main for pending activation: {error}"
                        );
                        crate::append_operational_event_best_effort(
                            &shadow_paths,
                            crate::OperationalSeverity::Warn,
                            "shadow-daemon",
                            "rollout.stage.main_terminate_failed",
                            "shadow daemon could not terminate unresponsive main for pending rollout activation",
                            None,
                            serde_json::json!({
                                "error": error.to_string(),
                                "phase": "main-staged",
                            }),
                        );
                    }
                }
            }
            next_pending_activation_shutdown_nudge = Instant::now() + Duration::from_secs(2);
        }

        if let Some(pending) = pending_rollout.as_ref() {
            if pending_rollout_probe_matches_candidate(
                &main_probe_status,
                supervised_main.as_ref().map(|child| child.id()),
            ) {
                let promoted_main_binary = pending.activation.main_binary.clone();
                let promoted_source = pending.activation.source.clone();
                let promoted_requested_at = pending.activation.requested_at.clone();
                let promoted_main_options = promote_shadow_main_rollout(
                    &rollout_paths,
                    pending,
                    &options.host,
                    options.port,
                )?;
                crate::append_operational_event_best_effort(
                    &shadow_paths,
                    crate::OperationalSeverity::Info,
                    "shadow-daemon",
                    "rollout.stage.accepted",
                    "candidate main became healthy; promoting shadow main rollout",
                    Some(&promoted_requested_at),
                    serde_json::json!({
                        "source": promoted_source,
                        "requested_at": promoted_requested_at,
                        "main_binary": promoted_main_binary.display().to_string(),
                    }),
                );
                main_binary = promoted_main_binary;
                main_options = promoted_main_options;
                main_probe_status = probe_daemon_status(&main_paths, probe_timeout);
                last_main_probe = Instant::now();
                pending_rollout = None;
                if shadow_reexec_request_path(&rollout_paths).exists() {
                    break;
                }
                thread::sleep(Duration::from_millis(200));
                continue;
            } else if Instant::now() >= pending.deadline {
                let detail = "candidate main did not become healthy before rollout timeout";
                if let Some(child) = supervised_main.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                main_binary =
                    recover_failed_shadow_main_rollout(&rollout_paths, pending, detail, None)?;
                main_options = pending.main_options.clone();
                supervised_main = spawn_main_daemon_process(&main_binary, &main_options).ok();
                crate::append_operational_event_best_effort(
                    &shadow_paths,
                    crate::OperationalSeverity::Error,
                    "shadow-daemon",
                    "rollout.stage.failed",
                    detail,
                    Some(&pending.activation.requested_at),
                    serde_json::json!({
                        "source": pending.activation.source,
                        "requested_at": pending.activation.requested_at,
                        "recovered_main_binary": main_binary.display().to_string(),
                    }),
                );
                pending_rollout = None;
            }
        }

        if should_shadow_start_main(main_restart_suppressed, pending_main_activation)
            && supervised_main.is_none()
            && matches!(main_probe_status, DaemonProbeStatus::NotRunning)
            && (!pending_main_activation || main_data_owner_lock_released(&main_paths))
            && (pending_main_activation || main_restart_backoff.can_restart(now))
        {
            if pending_main_activation {
                main_restart_suppressed = false;
                main_restart_backoff.clear_schedule();
            }
            match start_shadow_main_if_needed(
                &rollout_paths,
                &main_binary,
                &main_options,
                finalize_timeout,
            )? {
                ShadowMainStart::None => {}
                ShadowMainStart::Main { child } => {
                    supervised_main = Some(child);
                    supervised_main_started_at = Some(Instant::now());
                    main_restart_backoff.clear_schedule();
                    crate::append_operational_event_best_effort(
                        &shadow_paths,
                        crate::OperationalSeverity::Info,
                        "shadow-daemon",
                        "daemon.supervision.main_started",
                        "shadow daemon started supervised main",
                        None,
                        serde_json::json!({
                            "main_binary": main_binary.display().to_string(),
                            "main_host": main_options.host,
                            "main_port": main_options.port,
                        }),
                    );
                }
                ShadowMainStart::StartFailed { detail } => {
                    let delay = main_restart_backoff.schedule_after_failure(Instant::now());
                    tracing::warn!(
                        "sinomo shadow daemon could not start supervised main: {detail}; retrying in {}ms",
                        delay.as_millis()
                    );
                    crate::append_operational_event_best_effort(
                        &shadow_paths,
                        crate::OperationalSeverity::Warn,
                        "shadow-daemon",
                        "daemon.supervision.main_start_failed",
                        "shadow daemon could not start supervised main",
                        None,
                        serde_json::json!({
                            "main_binary": main_binary.display().to_string(),
                            "detail": detail,
                            "retry_delay_ms": delay.as_millis(),
                        }),
                    );
                }
                ShadowMainStart::Candidate {
                    child,
                    monitor,
                    main_binary: candidate_main_binary,
                } => {
                    main_binary = candidate_main_binary;
                    supervised_main = Some(child);
                    supervised_main_started_at = Some(Instant::now());
                    main_restart_backoff.clear_schedule();
                    pending_rollout = Some(monitor);
                    main_probe_status = DaemonProbeStatus::NotRunning;
                    last_main_probe = Instant::now() - shadow_main_probe_interval(true);
                    crate::append_operational_event_best_effort(
                        &shadow_paths,
                        crate::OperationalSeverity::Info,
                        "shadow-daemon",
                        "rollout.stage.progress",
                        "shadow daemon started candidate main",
                        pending_rollout
                            .as_ref()
                            .map(|pending| pending.activation.requested_at.as_str()),
                        serde_json::json!({
                            "phase": "candidate-main-started",
                            "main_binary": main_binary.display().to_string(),
                            "source": pending_rollout.as_ref().map(|pending| pending.activation.source.as_str()),
                        }),
                    );
                }
                ShadowMainStart::Recovered {
                    child,
                    main_binary: recovered_main_binary,
                    main_options: recovered_main_options,
                } => {
                    main_binary = recovered_main_binary;
                    main_options = recovered_main_options;
                    supervised_main = child;
                    supervised_main_started_at = supervised_main.as_ref().map(|_| Instant::now());
                    main_restart_backoff.clear_schedule();
                    crate::append_operational_event_best_effort(
                        &shadow_paths,
                        crate::OperationalSeverity::Warn,
                        "shadow-daemon",
                        "daemon.supervision.main_recovered",
                        "shadow daemon recovered supervised main after failed activation",
                        None,
                        serde_json::json!({
                            "main_binary": main_binary.display().to_string(),
                            "main_host": main_options.host,
                            "main_port": main_options.port,
                        }),
                    );
                }
            }
        }

        if let Some(child) = supervised_main.as_mut() {
            if let Some(status) = child.try_wait().map_err(|source| DaemonError::Io {
                path: main_binary.display().to_string(),
                detail: source.to_string(),
            })? {
                if let Some(pending) = pending_rollout.take() {
                    let candidate_pid = child.id();
                    match handle_pending_shadow_main_process_exit(
                        &rollout_paths,
                        &main_paths,
                        &pending,
                        candidate_pid,
                        status,
                        &options.host,
                        options.port,
                        probe_timeout,
                    )? {
                        PendingShadowMainExit::Promoted {
                            main_binary: promoted_main_binary,
                            main_options: promoted_main_options,
                            main_probe_status: promoted_probe_status,
                        } => {
                            main_binary = promoted_main_binary;
                            main_options = promoted_main_options;
                            main_probe_status = promoted_probe_status;
                            last_main_probe = Instant::now();
                            supervised_main = None;
                            supervised_main_started_at = None;
                            main_restart_backoff.reset();
                            crate::append_operational_event_best_effort(
                                &shadow_paths,
                                crate::OperationalSeverity::Info,
                                "shadow-daemon",
                                "rollout.stage.accepted",
                                "candidate main was promoted after exit observation",
                                Some(&pending.activation.requested_at),
                                serde_json::json!({
                                    "source": pending.activation.source,
                                    "requested_at": pending.activation.requested_at,
                                    "main_binary": main_binary.display().to_string(),
                                }),
                            );
                        }
                        PendingShadowMainExit::RetryCandidate { child, monitor } => {
                            supervised_main = Some(child);
                            supervised_main_started_at = Some(Instant::now());
                            pending_rollout = Some(monitor);
                            main_probe_status = DaemonProbeStatus::NotRunning;
                            last_main_probe = Instant::now() - shadow_main_probe_interval(true);
                            main_restart_backoff.clear_schedule();
                            crate::append_operational_event_best_effort(
                                &shadow_paths,
                                crate::OperationalSeverity::Warn,
                                "shadow-daemon",
                                "rollout.stage.retry",
                                "candidate main exited cleanly before health; retrying within activation deadline",
                                pending_rollout
                                    .as_ref()
                                    .map(|pending| pending.activation.requested_at.as_str()),
                                serde_json::json!({
                                    "source": pending_rollout.as_ref().map(|pending| pending.activation.source.as_str()),
                                    "main_binary": main_binary.display().to_string(),
                                }),
                            );
                        }
                        PendingShadowMainExit::Recovered {
                            child,
                            main_binary: recovered_main_binary,
                            main_options: recovered_main_options,
                        } => {
                            main_binary = recovered_main_binary;
                            main_options = recovered_main_options;
                            supervised_main = child;
                            supervised_main_started_at =
                                supervised_main.as_ref().map(|_| Instant::now());
                            main_restart_backoff.clear_schedule();
                            crate::append_operational_event_best_effort(
                                &shadow_paths,
                                crate::OperationalSeverity::Error,
                                "shadow-daemon",
                                "rollout.stage.failed",
                                "candidate main exited during shadow rollout; recovered previous main",
                                Some(&pending.activation.requested_at),
                                serde_json::json!({
                                    "source": pending.activation.source,
                                    "requested_at": pending.activation.requested_at,
                                    "recovered_main_binary": main_binary.display().to_string(),
                                }),
                            );
                        }
                    }
                    if shadow_reexec_request_path(&rollout_paths).exists() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }

                let exited_pid = child.id();
                match handle_shadow_main_process_exit(
                    &rollout_paths,
                    &main_binary,
                    &main_options,
                    exited_pid,
                    status,
                )? {
                    ShadowMainExit::PendingActivation => {
                        supervised_main = None;
                        supervised_main_started_at = None;
                        main_restart_suppressed = false;
                        main_restart_backoff.clear_schedule();
                        crate::append_operational_event_best_effort(
                            &shadow_paths,
                            crate::OperationalSeverity::Info,
                            "shadow-daemon",
                            "daemon.supervision.pending_activation",
                            "supervised main exited for pending activation",
                            None,
                            serde_json::json!({
                                "main_binary": main_binary.display().to_string(),
                            }),
                        );
                        thread::sleep(Duration::from_millis(200));
                        continue;
                    }
                    ShadowMainExit::RestartRequested => {
                        supervised_main = None;
                        supervised_main_started_at = None;
                        let delay = main_restart_backoff.schedule_after_failure(Instant::now());
                        tracing::warn!(
                            "sinomo shadow daemon supervised main exited with restart requested; retrying in {}ms",
                            delay.as_millis()
                        );
                        crate::append_operational_event_best_effort(
                            &shadow_paths,
                            crate::OperationalSeverity::Warn,
                            "shadow-daemon",
                            "daemon.supervision.restart_scheduled",
                            "supervised main exited; restart scheduled",
                            None,
                            serde_json::json!({
                                "main_binary": main_binary.display().to_string(),
                                "retry_delay_ms": delay.as_millis(),
                            }),
                        );
                    }
                    ShadowMainExit::Suppressed => {
                        supervised_main = None;
                        supervised_main_started_at = None;
                        main_restart_suppressed = true;
                        main_restart_backoff.reset();
                        crate::append_operational_event_best_effort(
                            &shadow_paths,
                            crate::OperationalSeverity::Error,
                            "shadow-daemon",
                            "daemon.supervision.restart_suppressed",
                            "supervised main exited too many times; restart suppressed",
                            None,
                            serde_json::json!({
                                "main_binary": main_binary.display().to_string(),
                            }),
                        );
                    }
                }
            }
        }

        thread::sleep(shadow_idle_interval);
    }
    let _ = control_shutdown_signal.send(true);
    join_shadow_runtime_task(
        &daemon_runtime,
        "control accept",
        shadow_control_accept_task,
        false,
    );
    for (label, task) in shadow_runtime_tasks {
        join_shadow_runtime_task(&daemon_runtime, label, task, false);
    }
    shutdown_shadow_runtime_after_task_shutdown(daemon_runtime);

    // Compatibility for old pending re-exec requests. New rollouts are finalized
    // by the main daemon from the stable installed shadow path.
    let reexec_target = take_shadow_reexec_request(&rollout_paths);
    let installed_main_binary = rollout_paths.main_binary.clone();
    let installed_shadow_binary = rollout_paths.shadow_binary.clone();
    drop(_lock);
    if reexec_target.is_some() {
        let target = if installed_shadow_binary.exists() {
            installed_shadow_binary
        } else {
            env::current_exe().unwrap_or(installed_shadow_binary)
        };
        tracing::warn!(
            "sinomo shadow daemon re-executing supervisor binary {}",
            target.display()
        );
        #[cfg(unix)]
        {
            return Err(reexec_shadow_supervisor(
                &target,
                &options,
                &installed_main_binary,
                &main_options,
            ));
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            spawn_shadow_rollout_completion_helper_process(&installed_main_binary)?;
        }
    }

    crate::append_operational_event_best_effort(
        &shadow_paths,
        crate::OperationalSeverity::Info,
        "shadow-daemon",
        "daemon.lifecycle.stopped",
        "sinomo shadow daemon stopped",
        None,
        serde_json::json!({
            "address": bind_address,
            "pid": std::process::id(),
            "main_binary": main_binary.display().to_string(),
        }),
    );
    Ok(())
}

pub fn sleep_for_shadow_start_delay() -> Result<(), DaemonError> {
    let Ok(delay) = env::var(SHADOW_START_DELAY_MS_ENV) else {
        return Ok(());
    };
    let delay = delay.parse::<u64>().map_err(|source| DaemonError::Parse {
        detail: format!("invalid {SHADOW_START_DELAY_MS_ENV} `{delay}`: {source}"),
    })?;
    if delay > 0 {
        thread::sleep(Duration::from_millis(delay));
    }
    Ok(())
}

pub fn shadow_main_options_from_env() -> Result<crate::DaemonRunOptions, DaemonError> {
    let mut options = crate::DaemonRunOptions::default();
    if let Ok(host) = env::var(SHADOW_MAIN_HOST_ENV) {
        options.host = host;
    }
    if let Ok(port) = env::var(SHADOW_MAIN_PORT_ENV) {
        options.port = port.parse::<u16>().map_err(|source| DaemonError::Parse {
            detail: format!("invalid {SHADOW_MAIN_PORT_ENV} `{port}`: {source}"),
        })?;
    }
    Ok(options)
}

pub fn shadow_main_binary_from_env(rollout_paths: &SelfRolloutPaths) -> PathBuf {
    env::var(SHADOW_MAIN_BINARY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| rollout_paths.main_binary.clone())
}

fn shadow_main_probe_interval(pending_rollout: bool) -> Duration {
    if pending_rollout {
        Duration::from_millis(250)
    } else {
        Duration::from_secs(10)
    }
}

fn should_shadow_start_main(main_restart_suppressed: bool, pending_main_activation: bool) -> bool {
    pending_main_activation || !main_restart_suppressed
}

fn pending_main_activation_stop_target(status: &DaemonProbeStatus) -> bool {
    matches!(
        status,
        DaemonProbeStatus::Running(_) | DaemonProbeStatus::Locked { status: Some(_) }
    )
}

fn pending_rollout_probe_matches_candidate(
    probe_status: &DaemonProbeStatus,
    candidate_pid: Option<u32>,
) -> bool {
    matches!(
        (probe_status, candidate_pid),
        (DaemonProbeStatus::Running(status), Some(pid)) if status.pid == pid
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingShadowCandidateExitAction {
    Promote,
    Retry,
    Recover,
}

fn pending_shadow_candidate_exit_action(
    probe_status: &DaemonProbeStatus,
    candidate_pid: Option<u32>,
    observation: MainExitObservation,
    now: Instant,
    deadline: Instant,
) -> PendingShadowCandidateExitAction {
    let clean_exit = observation.exit_code == Some(0) && observation.signal.is_none();
    if clean_exit && pending_rollout_probe_matches_candidate(probe_status, candidate_pid) {
        return PendingShadowCandidateExitAction::Promote;
    }
    if clean_exit && now < deadline {
        return PendingShadowCandidateExitAction::Retry;
    }
    PendingShadowCandidateExitAction::Recover
}

fn pending_main_activation_ready_for_shutdown(rollout_paths: &SelfRolloutPaths) -> bool {
    let path = rollout_main_activation_path(rollout_paths);
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified_at) = metadata.modified() else {
        return true;
    };
    modified_at
        .elapsed()
        .is_ok_and(|age| age >= MAIN_ACTIVATION_SHUTDOWN_GRACE)
}

fn main_data_owner_lock_released(main_paths: &DaemonPaths) -> bool {
    if !main_paths.data_owner_lock.exists() {
        return true;
    }
    let Ok(file) = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&main_paths.data_owner_lock)
    else {
        return true;
    };
    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = file.unlock();
            true
        }
        Err(_) => false,
    }
}

fn terminate_pending_main_activation(
    main_paths: &DaemonPaths,
    wait_timeout: Duration,
    probe_timeout: Duration,
) -> Result<Option<u32>, DaemonError> {
    let status = match read_daemon_status(main_paths) {
        Ok(status) if status_is_main_daemon(&status) => status,
        _ => return Ok(None),
    };
    terminate_process_for_rollout_stop(status.pid, RolloutTerminationMode::ProcessTree)?;
    let deadline = Instant::now() + wait_timeout;
    while Instant::now() < deadline {
        if matches!(
            probe_daemon_status(main_paths, probe_timeout),
            DaemonProbeStatus::NotRunning
        ) {
            return Ok(Some(status.pid));
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(None)
}

fn status_is_main_daemon(status: &DaemonStatus) -> bool {
    status
        .cluster
        .as_ref()
        .is_none_or(|cluster| cluster.process == sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE)
}

#[cfg(unix)]
fn rollout_process_is_running(pid: u32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe { kill(pid as i32, 0) == 0 }
}

#[cfg(windows)]
fn rollout_process_is_running(pid: u32) -> bool {
    use std::ffi::c_void;
    use std::ptr::null_mut;

    type Bool = i32;
    type Dword = u32;
    type Handle = *mut c_void;

    unsafe extern "system" {
        fn OpenProcess(dwDesiredAccess: Dword, bInheritHandle: Bool, dwProcessId: Dword) -> Handle;
        fn GetExitCodeProcess(hProcess: Handle, lpExitCode: *mut Dword) -> Bool;
        fn CloseHandle(hObject: Handle) -> Bool;
    }

    const PROCESS_QUERY_LIMITED_INFORMATION: Dword = 0x0000_1000;
    const STILL_ACTIVE: Dword = 259;

    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle == null_mut() {
        return false;
    }
    let mut exit_code = 0;
    let running =
        unsafe { GetExitCodeProcess(handle, &mut exit_code) != 0 && exit_code == STILL_ACTIVE };
    unsafe {
        let _ = CloseHandle(handle);
    }
    running
}

#[cfg(not(any(unix, windows)))]
fn rollout_process_is_running(_pid: u32) -> bool {
    false
}

#[cfg(unix)]
fn terminate_process_for_rollout_stop(
    pid: u32,
    _mode: RolloutTerminationMode,
) -> Result<(), DaemonError> {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const SIGTERM: i32 = 15;
    let result = unsafe { kill(pid as i32, SIGTERM) };
    if result == -1 {
        return Err(DaemonError::Io {
            path: format!("pid {pid}"),
            detail: std::io::Error::last_os_error().to_string(),
        });
    }
    Ok(())
}

#[cfg(windows)]
fn terminate_process_for_rollout_stop(
    pid: u32,
    mode: RolloutTerminationMode,
) -> Result<(), DaemonError> {
    let args = windows_taskkill_args_for_rollout_stop(pid, mode);
    let output = Command::new("taskkill")
        .args(args.iter().map(String::as_str))
        .output()
        .map_err(|source| DaemonError::Io {
            path: "taskkill".to_string(),
            detail: source.to_string(),
        })?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Err(DaemonError::Io {
        path: format!("pid {pid}"),
        detail: if stderr.is_empty() { stdout } else { stderr },
    })
}

#[cfg(any(test, windows))]
fn windows_taskkill_args_for_rollout_stop(pid: u32, mode: RolloutTerminationMode) -> Vec<String> {
    let mut args = vec!["/PID".to_string(), pid.to_string()];
    if mode == RolloutTerminationMode::ProcessTree {
        args.push("/T".to_string());
    }
    args.push("/F".to_string());
    args
}

#[cfg(not(any(unix, windows)))]
fn terminate_process_for_rollout_stop(
    _pid: u32,
    _mode: RolloutTerminationMode,
) -> Result<(), DaemonError> {
    Ok(())
}

pub async fn handle_daemon_rollout_binary_request<R>(
    paths: &DaemonPaths,
    request: ClusterRolloutBinaryRequest,
    body: &mut R,
) -> Result<ClusterRolloutBinaryResponse, DaemonError>
where
    R: AsyncRead + Unpin,
{
    validate_rollout_binary_manifest(&request.manifest)?;
    let artifact = request
        .manifest
        .artifact(&request.artifact_id)
        .cloned()
        .ok_or_else(|| DaemonError::Parse {
            detail: format!(
                "rollout manifest does not contain artifact {}",
                request.artifact_id
            ),
        })?;
    let data_dir = daemon_data_dir(paths)?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    ensure_rollout_upload_manifest(&rollout_paths, &request.manifest)?;
    receive_rollout_binary_artifact(&rollout_paths, &request.manifest, &artifact, body).await?;

    if let Some(receipt) = read_rollout_binary_receipt(&rollout_paths, &request.manifest)? {
        let _ = fs::remove_file(rollout_upload_artifact_path(
            &rollout_paths,
            &request.manifest.upload_id,
            &artifact.artifact_id,
        )?);
        return Ok(receipt.response);
    }

    if !request.activate {
        return Ok(ClusterRolloutBinaryResponse {
            status: "staged".to_string(),
            detail: format!(
                "received and verified rollout artifact {} ({} bytes)",
                artifact.artifact_id, artifact.total_bytes
            ),
            main_binary: rollout_paths.main_binary,
            shadow_binary: rollout_paths.shadow_binary,
            source_head: None,
        });
    }

    let paths = paths.clone();
    let manifest = request.manifest;
    let activation_manifest = manifest.clone();
    let response = tokio::task::spawn_blocking(move || {
        activate_daemon_rollout_binary_request(&paths, activation_manifest)
    })
    .await
    .map_err(|source| DaemonError::Io {
        path: "rollout binary activation task".to_string(),
        detail: source.to_string(),
    })??;
    write_rollout_binary_receipt(&rollout_paths, &manifest, &response)?;
    remove_rollout_uploaded_artifacts(&rollout_paths, &manifest);
    Ok(response)
}

pub fn handle_daemon_rollout_native_dylib_module_request(
    paths: &DaemonPaths,
    request: ClusterRolloutNativeDylibModuleRequest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let source = request.source.clone();
    let requested_at = request.requested_at.clone();
    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-module-native-dylib",
        Some(format!(
            "installing native module artifact {}",
            request.artifact.module_id
        )),
        &source,
        &requested_at,
        "staging-module",
        1,
        "installing native module artifact",
    )?;

    let main_binary = rollout_paths.main_binary.clone();
    let shadow_binary = rollout_paths.shadow_binary.clone();
    let staged_main_binary = rollout_staged_main_binary(&rollout_paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&rollout_paths);
    let staging_result = stage_rollout_file(&main_binary, &staged_main_binary).and_then(|()| {
        stage_rollout_file(
            if shadow_binary.exists() {
                &shadow_binary
            } else {
                &main_binary
            },
            &staged_shadow_binary,
        )
    });
    if let Err(error) = staging_result {
        let detail = format!("stage daemon refresh binaries failed: {error}");
        return rejected_cluster_rollout_response(
            paths,
            &rollout_paths,
            "cluster-module-native-dylib",
            &source,
            &requested_at,
            "staging-failed",
            "staging failed",
            detail,
        );
    }

    let install_result = match install_cluster_rollout_native_dylib_module(data_dir, &request) {
        Ok(result) => result,
        Err(error) => {
            let detail = format!("install native module artifact failed: {error}");
            let _ = write_cluster_rollout_progress(
                paths,
                &rollout_paths,
                "rejected",
                "cluster-module-native-dylib",
                Some(detail.clone()),
                &source,
                &requested_at,
                "module-install-failed",
                CLUSTER_ROLLOUT_PROGRESS_TOTAL,
                "module install failed",
            );
            return Ok(ClusterRolloutBinaryResponse {
                status: "rejected".to_string(),
                detail,
                main_binary: PathBuf::new(),
                shadow_binary: PathBuf::new(),
                source_head: None,
            });
        }
    };

    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-module-native-dylib",
        Some("preparing daemon refresh".to_string()),
        &source,
        &requested_at,
        "preparing-activation",
        4,
        "preparing daemon refresh",
    )?;
    let plan = prepare_cluster_rollout_main_activation(
        paths,
        data_dir,
        source,
        main_binary.clone(),
        shadow_binary.clone(),
        Some(staged_main_binary),
        Some(staged_shadow_binary),
        request.host,
        request.port,
        request.shadow_port,
        requested_at,
        "cluster-module-native-dylib",
    )?;
    let activation_strategy =
        stage_rollout_main_activation(paths, data_dir, &rollout_paths, plan.activation)?;
    publish_rollout_progress_to_cluster(paths);
    Ok(ClusterRolloutBinaryResponse {
        status: "accepted".to_string(),
        detail: format!(
            "{}; {}",
            module_rollout_response_detail(&install_result),
            rollout_staged_detail(
                "daemon refresh staged",
                plan.current_main_pid,
                activation_strategy
            )
        ),
        main_binary,
        shadow_binary,
        source_head: None,
    })
}

fn install_cluster_rollout_native_dylib_module(
    data_dir: &Path,
    request: &ClusterRolloutNativeDylibModuleRequest,
) -> Result<ClusterRolloutNativeDylibModuleResponse, DaemonError> {
    let mut installed = sinomo_rollout::install_cluster_rollout_native_dylib_modules(
        data_dir,
        std::slice::from_ref(&request.artifact),
    )?;
    let result = installed.pop().ok_or_else(|| DaemonError::Parse {
        detail: "native module install returned no result".to_string(),
    })?;
    Ok(ClusterRolloutNativeDylibModuleResponse {
        status: "accepted".to_string(),
        detail: "native module artifact installed".to_string(),
        module_id: result.state.module_id.to_string(),
        artifact_id: result.state.artifact_id.as_ref().map(ToString::to_string),
        interface_version: result.state.interface_version,
        cache_key: result.state.cache_key,
        content_hash: result.state.content_hash,
        cache_path: result.cache_path,
        state_path: result.state_path,
    })
}

fn module_rollout_response_detail(response: &ClusterRolloutNativeDylibModuleResponse) -> String {
    format!(
        "native module {} activated at {} ({})",
        response.module_id,
        response.cache_path.display(),
        response.content_hash
    )
}

fn activate_daemon_rollout_binary_request(
    paths: &DaemonPaths,
    manifest: ClusterRolloutBinaryManifest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    validate_rollout_upload_component("upload id", &manifest.upload_id)?;
    validate_rollout_upload_component("artifact id", &manifest.binary.artifact_id)?;
    let data_dir = daemon_data_dir(paths)?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let source = manifest.source.clone();
    let requested_at = manifest.requested_at.clone();
    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-binary-push",
        Some("verifying uploaded artifacts".to_string()),
        &source,
        &requested_at,
        "verifying-upload",
        1,
        "verifying uploaded artifacts",
    )?;
    if (!manifest.target_os.is_empty() && manifest.target_os != env::consts::OS)
        || (!manifest.target_arch.is_empty() && manifest.target_arch != env::consts::ARCH)
    {
        let detail = format!(
            "artifact platform {}/{} does not match target platform {}/{}",
            manifest.target_os,
            manifest.target_arch,
            env::consts::OS,
            env::consts::ARCH
        );
        write_cluster_rollout_progress(
            paths,
            &rollout_paths,
            "rejected",
            "cluster-binary-push",
            Some(detail.clone()),
            &source,
            &requested_at,
            "rejected",
            CLUSTER_ROLLOUT_PROGRESS_TOTAL,
            "platform rejected",
        )?;
        return Ok(ClusterRolloutBinaryResponse {
            status: "rejected".to_string(),
            detail,
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        });
    }

    let uploaded_binary =
        verify_rollout_uploaded_artifact(&rollout_paths, &manifest.upload_id, &manifest.binary)?;
    let mut uploaded_modules = Vec::new();
    for module in &manifest.native_dylib_modules {
        uploaded_modules.push(verify_rollout_uploaded_module(
            &rollout_paths,
            &manifest.upload_id,
            module,
        )?);
    }

    let main_binary = rollout_paths.main_binary.clone();
    let shadow_binary = rollout_paths.shadow_binary.clone();
    let staged_main_binary = rollout_staged_main_binary(&rollout_paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&rollout_paths);
    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-binary-push",
        Some("staging uploaded candidate binaries".to_string()),
        &source,
        &requested_at,
        "staging-binaries",
        2,
        "staging uploaded candidate binaries",
    )?;
    if let Err(error) =
        stage_rollout_file_pair(&uploaded_binary, &staged_main_binary, &staged_shadow_binary)
    {
        let detail = format!("stage uploaded candidate binaries failed: {error}");
        return rejected_cluster_rollout_response(
            paths,
            &rollout_paths,
            "cluster-binary-push",
            &source,
            &requested_at,
            "staging-failed",
            "staging failed",
            detail,
        );
    }
    if !uploaded_modules.is_empty() {
        write_cluster_rollout_progress(
            paths,
            &rollout_paths,
            "in-progress",
            "cluster-binary-push",
            Some(format!(
                "installing {} native module artifact(s)",
                uploaded_modules.len()
            )),
            &source,
            &requested_at,
            "staging-modules",
            3,
            "installing native module artifacts",
        )?;
        if let Err(error) = install_local_rollout_native_dylib_modules(data_dir, &uploaded_modules)
        {
            let detail = format!("install native module artifacts failed: {error}");
            let _ = write_cluster_rollout_progress(
                paths,
                &rollout_paths,
                "rejected",
                "cluster-binary-push",
                Some(detail.clone()),
                &source,
                &requested_at,
                "module-install-failed",
                CLUSTER_ROLLOUT_PROGRESS_TOTAL,
                "module install failed",
            );
            return Ok(ClusterRolloutBinaryResponse {
                status: "rejected".to_string(),
                detail,
                main_binary: PathBuf::new(),
                shadow_binary: PathBuf::new(),
                source_head: None,
            });
        }
    }

    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-binary-push",
        Some("preparing activation".to_string()),
        &source,
        &requested_at,
        "preparing-activation",
        4,
        "preparing activation",
    )?;
    let plan = prepare_cluster_rollout_main_activation(
        paths,
        data_dir,
        source,
        main_binary.clone(),
        shadow_binary.clone(),
        Some(staged_main_binary),
        Some(staged_shadow_binary),
        manifest.host,
        manifest.port,
        manifest.shadow_port,
        requested_at,
        "cluster-binary-push",
    )?;
    let activation_strategy =
        stage_rollout_main_activation(paths, data_dir, &rollout_paths, plan.activation)?;
    publish_rollout_progress_to_cluster(paths);
    Ok(ClusterRolloutBinaryResponse {
        status: "accepted".to_string(),
        detail: rollout_staged_detail(
            "uploaded candidate main staged",
            plan.current_main_pid,
            activation_strategy,
        ),
        main_binary,
        shadow_binary,
        source_head: None,
    })
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct ClusterRolloutBinaryReceipt {
    manifest: ClusterRolloutBinaryManifest,
    response: ClusterRolloutBinaryResponse,
}

fn validate_rollout_binary_manifest(
    manifest: &ClusterRolloutBinaryManifest,
) -> Result<(), DaemonError> {
    validate_rollout_upload_component("upload id", &manifest.upload_id)?;
    let mut artifact_ids = BTreeSet::new();
    for artifact in manifest.artifacts() {
        validate_rollout_upload_component("artifact id", &artifact.artifact_id)?;
        if artifact.total_bytes == 0 {
            return Err(DaemonError::Parse {
                detail: format!(
                    "rollout artifact {} must contain at least one byte",
                    artifact.artifact_id
                ),
            });
        }
        if artifact.blake3.len() != blake3::OUT_LEN {
            return Err(DaemonError::Parse {
                detail: format!(
                    "rollout artifact {} has a {}-byte blake3 digest; expected {}",
                    artifact.artifact_id,
                    artifact.blake3.len(),
                    blake3::OUT_LEN
                ),
            });
        }
        if !artifact_ids.insert(artifact.artifact_id.as_str()) {
            return Err(DaemonError::Parse {
                detail: format!(
                    "rollout manifest contains duplicate artifact id {}",
                    artifact.artifact_id
                ),
            });
        }
    }
    manifest.total_bytes().ok_or_else(|| DaemonError::Parse {
        detail: "rollout manifest total byte count overflowed u64".to_string(),
    })?;
    Ok(())
}

fn rollout_upload_manifest_path(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
) -> Result<PathBuf, DaemonError> {
    Ok(rollout_upload_dir(rollout_paths, upload_id)?.join("manifest.sobj"))
}

fn rollout_upload_receipt_path(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
) -> Result<PathBuf, DaemonError> {
    Ok(rollout_upload_dir(rollout_paths, upload_id)?.join("result.sobj"))
}

fn ensure_rollout_upload_manifest(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
) -> Result<(), DaemonError> {
    let path = rollout_upload_manifest_path(rollout_paths, &manifest.upload_id)?;
    if path.exists() {
        let existing: ClusterRolloutBinaryManifest = read_object(&path)?;
        if existing != *manifest {
            return Err(DaemonError::Parse {
                detail: format!(
                    "rollout upload id {} was already used with a different manifest",
                    manifest.upload_id
                ),
            });
        }
        return Ok(());
    }
    write_object(&path, manifest)?;
    sync_rollout_control_file(&path)
}

fn read_rollout_binary_receipt(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
) -> Result<Option<ClusterRolloutBinaryReceipt>, DaemonError> {
    let path = rollout_upload_receipt_path(rollout_paths, &manifest.upload_id)?;
    if !path.exists() {
        return Ok(None);
    }
    let receipt: ClusterRolloutBinaryReceipt = read_object(&path)?;
    if receipt.manifest != *manifest {
        return Err(DaemonError::Parse {
            detail: format!(
                "rollout upload id {} has a result for a different manifest",
                manifest.upload_id
            ),
        });
    }
    Ok(Some(receipt))
}

fn write_rollout_binary_receipt(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
    response: &ClusterRolloutBinaryResponse,
) -> Result<(), DaemonError> {
    let path = rollout_upload_receipt_path(rollout_paths, &manifest.upload_id)?;
    write_object(
        &path,
        &ClusterRolloutBinaryReceipt {
            manifest: manifest.clone(),
            response: response.clone(),
        },
    )?;
    sync_rollout_control_file(&path)
}

fn sync_rollout_control_file(path: &Path) -> Result<(), DaemonError> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

async fn receive_rollout_binary_artifact<R>(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
    artifact: &ClusterRolloutUploadArtifactSpec,
    body: &mut R,
) -> Result<(), DaemonError>
where
    R: AsyncRead + Unpin,
{
    let path =
        rollout_upload_artifact_path(rollout_paths, &manifest.upload_id, &artifact.artifact_id)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| DaemonError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
    }
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        let mut remaining = artifact.total_bytes;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; CLUSTER_ROLLOUT_BODY_BUFFER_BYTES];
        while remaining != 0 {
            let wanted = usize::try_from(remaining.min(buffer.len() as u64))
                .expect("bounded rollout read size fits usize");
            let read =
                body.read(&mut buffer[..wanted])
                    .await
                    .map_err(|source| DaemonError::Io {
                        path: format!("request body for {}", artifact.artifact_id),
                        detail: source.to_string(),
                    })?;
            if read == 0 {
                return Err(DaemonError::Parse {
                    detail: format!(
                        "rollout artifact {} ended {} bytes early",
                        artifact.artifact_id, remaining
                    ),
                });
            }
            file.write_all(&buffer[..read])
                .await
                .map_err(|source| DaemonError::Io {
                    path: path.display().to_string(),
                    detail: source.to_string(),
                })?;
            hasher.update(&buffer[..read]);
            remaining -= u64::try_from(read).expect("read length fits u64");
        }
        let mut extra = [0_u8; 1];
        if body
            .read(&mut extra)
            .await
            .map_err(|source| DaemonError::Io {
                path: format!("request body for {}", artifact.artifact_id),
                detail: source.to_string(),
            })?
            != 0
        {
            return Err(DaemonError::Parse {
                detail: format!(
                    "rollout artifact {} exceeded its declared {} bytes",
                    artifact.artifact_id, artifact.total_bytes
                ),
            });
        }
        file.flush().await.map_err(|source| DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        file.sync_all().await.map_err(|source| DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let actual = hasher.finalize();
        if actual.as_bytes().as_slice() != artifact.blake3.as_slice() {
            return Err(DaemonError::Parse {
                detail: format!("rollout artifact {} hash mismatch", artifact.artifact_id),
            });
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&path).await;
    }
    result
}

fn remove_rollout_uploaded_artifacts(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
) {
    for artifact in manifest.artifacts() {
        if let Ok(path) =
            rollout_upload_artifact_path(rollout_paths, &manifest.upload_id, &artifact.artifact_id)
        {
            let _ = fs::remove_file(path);
        }
    }
}

fn rollout_upload_dir(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
) -> Result<PathBuf, DaemonError> {
    validate_rollout_upload_component("upload id", upload_id)?;
    Ok(rollout_paths
        .root
        .join("staged")
        .join("uploads")
        .join(upload_id))
}

fn rollout_upload_artifact_path(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
    artifact_id: &str,
) -> Result<PathBuf, DaemonError> {
    validate_rollout_upload_component("artifact id", artifact_id)?;
    Ok(rollout_upload_dir(rollout_paths, upload_id)?.join(format!("{artifact_id}.part")))
}

fn validate_rollout_upload_component(label: &str, value: &str) -> Result<(), DaemonError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(DaemonError::Parse {
            detail: format!("invalid rollout upload {label}: `{value}`"),
        });
    }
    Ok(())
}

fn verify_rollout_uploaded_module(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
    module: &ClusterRolloutNativeDylibModuleUploadSpec,
) -> Result<LocalRolloutNativeDylibModuleArtifact, DaemonError> {
    let built_library =
        verify_rollout_uploaded_artifact(rollout_paths, upload_id, &module.artifact)?;
    Ok(LocalRolloutNativeDylibModuleArtifact {
        module_id: module.module_id.clone(),
        interface_version: module.interface_version.clone(),
        cache_key: module.cache_key.clone(),
        library_name: module.library_name.clone(),
        built_library,
        source: module.source.clone(),
    })
}

fn verify_rollout_uploaded_artifact(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
    spec: &ClusterRolloutUploadArtifactSpec,
) -> Result<PathBuf, DaemonError> {
    validate_rollout_upload_component("artifact id", &spec.artifact_id)?;
    let path = rollout_upload_artifact_path(rollout_paths, upload_id, &spec.artifact_id)?;
    let metadata = path.metadata().map_err(|source| DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    if metadata.len() != spec.total_bytes {
        return Err(DaemonError::Parse {
            detail: format!(
                "rollout upload artifact {} is {} bytes, expected {}",
                spec.artifact_id,
                metadata.len(),
                spec.total_bytes
            ),
        });
    }
    let actual = rollout_file_blake3(&path)?;
    if actual != spec.blake3 {
        return Err(DaemonError::Parse {
            detail: format!("rollout upload artifact {} hash mismatch", spec.artifact_id),
        });
    }
    Ok(path)
}

fn rollout_file_blake3(path: &Path) -> Result<Vec<u8>, DaemonError> {
    let mut file = fs::File::open(path).map_err(|source| DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().as_bytes().to_vec())
}

pub fn stage_rollout_main_activation(
    current_daemon_paths: &DaemonPaths,
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    activation: RolloutMainActivation,
) -> Result<RolloutActivationStrategy, DaemonError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let current_daemon_is_shadow = paths_equivalent(&current_daemon_paths.root, &shadow_paths.root);
    let shadow_running = current_daemon_is_shadow
        || daemon_probe_indicates_process_present(probe_daemon_status(
            &shadow_paths,
            CLUSTER_HEALTH_PROBE_TIMEOUT,
        ));
    if !shadow_running {
        stage_main_only_rollout_without_existing_shadow(data_dir, rollout_paths, activation)?;
        return Ok(RolloutActivationStrategy::MainOnlyShadowBootstrap);
    }
    write_main_activation(rollout_paths, &activation)?;
    record_main_activation_staged(rollout_paths, &activation)?;
    match staged_main_activation_shutdown_action(current_daemon_is_shadow) {
        StagedMainActivationShutdown::Immediate => {
            request_main_shutdown_for_shadow_rollout(data_dir, &activation)?;
        }
        StagedMainActivationShutdown::DeferredSelf => {
            request_main_shutdown_for_shadow_rollout_deferred(
                data_dir.to_path_buf(),
                rollout_paths.clone(),
                activation.clone(),
            );
        }
    }
    Ok(RolloutActivationStrategy::ShadowSupervised)
}

fn daemon_probe_indicates_process_present(status: DaemonProbeStatus) -> bool {
    matches!(
        status,
        DaemonProbeStatus::Running(_) | DaemonProbeStatus::Locked { .. }
    )
}

fn staged_main_activation_shutdown_action(
    current_daemon_is_shadow: bool,
) -> StagedMainActivationShutdown {
    if current_daemon_is_shadow {
        StagedMainActivationShutdown::Immediate
    } else {
        StagedMainActivationShutdown::DeferredSelf
    }
}

fn stage_main_only_rollout_without_existing_shadow(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    activation: RolloutMainActivation,
) -> Result<(), DaemonError> {
    let _staged_main = activation
        .staged_main_binary
        .as_ref()
        .filter(|path| path.exists())
        .ok_or_else(|| DaemonError::NotRunning {
            detail: "main-only rollout requires a staged main binary".to_string(),
        })?;
    let staged_shadow = activation
        .staged_shadow_binary
        .as_ref()
        .filter(|path| path.exists())
        .ok_or_else(|| DaemonError::NotRunning {
            detail: "main-only rollout requires a staged shadow binary".to_string(),
        })?;
    let previous_shadow_binary = activate_staged_rollout_binary_with_retry(
        staged_shadow,
        &activation.shadow_binary,
        STABLE_BINARY_COPY_TIMEOUT,
        STABLE_BINARY_COPY_RETRY_INTERVAL,
    )?;
    write_main_activation(rollout_paths, &activation)?;
    record_main_activation_staged(rollout_paths, &activation)?;
    // Bring up the bootstrap shadow with bounded, health-verified retries so a
    // transient bind/lock race during handoff does not leave the node without a
    // supervisor. In tests the spawn/probe is skipped.
    #[cfg(not(test))]
    let shadow_start = ensure_shadow_supervisor_running(
        data_dir,
        &activation.shadow_binary,
        &activation.main_binary,
        &activation.main_host,
        activation.main_port,
        &activation.main_host,
        activation.shadow_port,
        SHADOW_RECOVERY_DEADLINE,
    );
    #[cfg(test)]
    let shadow_start: Result<(), DaemonError> = {
        let _ = data_dir;
        Ok(())
    };
    if let Err(error) = shadow_start {
        let previous_main = activation
            .previous_main_binary
            .as_deref()
            .unwrap_or(&activation.main_binary);
        let _ = record_activation_failure(
            rollout_paths,
            &activation,
            previous_main,
            &format!("main-only rollout could not start stable shadow supervisor: {error}"),
        );
        if let Some(previous_shadow_binary) = previous_shadow_binary.as_ref() {
            let _ = restore_rollout_binary_from_backup_with_retry(
                previous_shadow_binary,
                &activation.shadow_binary,
                STABLE_BINARY_COPY_TIMEOUT,
                STABLE_BINARY_COPY_RETRY_INTERVAL,
            );
        }
        let _ = remove_main_activation(rollout_paths);
        return Err(error);
    }
    Ok(())
}

pub fn daemon_paths_are_shadow(paths: &DaemonPaths) -> bool {
    let Ok(data_dir) = daemon_data_dir(paths) else {
        return false;
    };
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    paths_equivalent(&paths.root, &shadow_paths.root)
}

fn request_main_shutdown_for_shadow_rollout(
    data_dir: &Path,
    activation: &RolloutMainActivation,
) -> Result<(), DaemonError> {
    let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
    let status = match probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { status } => status,
        DaemonProbeStatus::NotRunning => None,
    };
    let address = status
        .as_ref()
        .map(|status| status.address.clone())
        .unwrap_or_else(|| daemon_connect_address(&activation.main_host, activation.main_port));
    let _ = send_daemon_shutdown_request(&address, DAEMON_PROBE_TIMEOUT);
    Ok(())
}

fn request_main_shutdown_for_shadow_rollout_deferred(
    data_dir: PathBuf,
    rollout_paths: SelfRolloutPaths,
    activation: RolloutMainActivation,
) {
    #[cfg(test)]
    {
        let _ = (data_dir, rollout_paths, activation);
    }
    #[cfg(not(test))]
    {
        if let Err(error) = thread::Builder::new()
            .name("sinomo-rollout-main-yield".to_string())
            .spawn(move || {
                thread::sleep(MAIN_ACTIVATION_SHUTDOWN_GRACE);
                if !rollout_main_activation_path(&rollout_paths).exists() {
                    return;
                }
                if let Err(error) = request_main_shutdown_for_shadow_rollout(&data_dir, &activation)
                {
                    tracing::warn!("deferred rollout main shutdown request failed: {error}");
                }
            })
        {
            tracing::warn!("deferred rollout main shutdown task spawn failed: {error}");
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterRolloutMainActivationPlan {
    activation: RolloutMainActivation,
    current_main_pid: Option<u32>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_cluster_rollout_main_activation(
    current_daemon_paths: &DaemonPaths,
    data_dir: &Path,
    source: String,
    main_binary: PathBuf,
    shadow_binary: PathBuf,
    staged_main_binary: Option<PathBuf>,
    staged_shadow_binary: Option<PathBuf>,
    requested_host: String,
    requested_port: u16,
    shadow_port: u16,
    requested_at: String,
    mode: &str,
) -> Result<ClusterRolloutMainActivationPlan, DaemonError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let current_daemon_is_shadow = paths_equivalent(&current_daemon_paths.root, &shadow_paths.root);
    let main_status = if current_daemon_is_shadow {
        let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
        match probe_daemon_status(&main_paths, CLUSTER_HEALTH_PROBE_TIMEOUT) {
            DaemonProbeStatus::Running(status) => Some(status),
            DaemonProbeStatus::Locked { status } => status,
            DaemonProbeStatus::NotRunning => None,
        }
    } else {
        Some(read_daemon_status(current_daemon_paths)?)
    };
    let previous_main_binary = main_status
        .as_ref()
        .and_then(|status| status.executable.clone())
        .unwrap_or_else(|| main_binary.clone());
    let main_host = main_status
        .as_ref()
        .map(|status| status.host.clone())
        .or_else(|| env::var(SHADOW_MAIN_HOST_ENV).ok())
        .unwrap_or(requested_host);
    let main_port = main_status
        .as_ref()
        .map(|status| status.port)
        .or_else(|| {
            env::var(SHADOW_MAIN_PORT_ENV)
                .ok()
                .and_then(|value| value.parse::<u16>().ok())
        })
        .unwrap_or(requested_port);
    Ok(ClusterRolloutMainActivationPlan {
        activation: RolloutMainActivation {
            source,
            main_binary,
            shadow_binary,
            staged_main_binary,
            staged_shadow_binary,
            previous_main_binary: Some(previous_main_binary),
            main_host,
            main_port,
            shadow_port,
            requested_at,
            mode: mode.to_string(),
        },
        current_main_pid: main_status.as_ref().map(|status| status.pid),
    })
}

fn rollout_staged_detail(
    prefix: &str,
    current_main_pid: Option<u32>,
    strategy: RolloutActivationStrategy,
) -> String {
    match (strategy, current_main_pid) {
        (RolloutActivationStrategy::ShadowSupervised, Some(pid)) => format!(
            "{prefix}; existing shadow daemon will restart main, verify health, then update shadow (current main pid {pid} will stop)"
        ),
        (RolloutActivationStrategy::ShadowSupervised, None) => format!(
            "{prefix}; existing shadow daemon will start main, verify health, then update shadow"
        ),
        (RolloutActivationStrategy::MainOnlyShadowBootstrap, Some(pid)) => format!(
            "{prefix}; stable shadow supervisor will restart main from the stable binary path (current main pid {pid} will stop)"
        ),
        (RolloutActivationStrategy::MainOnlyShadowBootstrap, None) => format!(
            "{prefix}; stable shadow supervisor will start main from the stable binary path"
        ),
    }
}

fn daemon_control_port_released(host: &str, port: u16) -> bool {
    std::net::TcpListener::bind(daemon_connect_address(host, port)).is_ok()
}

fn rollout_stop_ready_for_binary_activation(
    control_listener_released: bool,
    previous_process_running: bool,
) -> bool {
    control_listener_released && !previous_process_running
}

fn daemon_released_for_rollout_stop(
    host: &str,
    port: u16,
    last_status: Option<&DaemonStatus>,
) -> bool {
    rollout_stop_ready_for_binary_activation(
        daemon_control_port_released(host, port),
        last_status.is_some_and(|status| rollout_process_is_running(status.pid)),
    )
}

fn stop_daemon_for_rollout(
    daemon_paths: &DaemonPaths,
    host: &str,
    port: u16,
    label: &str,
    termination_mode: RolloutTerminationMode,
    timeout: Duration,
    probe_timeout: Duration,
) -> Result<(), DaemonError> {
    let deadline = Instant::now() + timeout;
    let mut next_shutdown_nudge = Instant::now();
    let mut last_status: Option<DaemonStatus> = None;
    loop {
        match probe_daemon_status(daemon_paths, probe_timeout) {
            DaemonProbeStatus::Running(status) => {
                last_status = Some(status.clone());
                if Instant::now() >= next_shutdown_nudge {
                    let _ = send_daemon_shutdown_request(&status.address, probe_timeout);
                    next_shutdown_nudge = Instant::now() + Duration::from_secs(1);
                }
            }
            DaemonProbeStatus::Locked {
                status: Some(status),
            } => {
                last_status = Some(status.clone());
                if Instant::now() >= next_shutdown_nudge {
                    let _ = send_daemon_shutdown_request(&status.address, probe_timeout);
                    next_shutdown_nudge = Instant::now() + Duration::from_secs(1);
                }
            }
            DaemonProbeStatus::Locked { status: None } => {
                if daemon_released_for_rollout_stop(host, port, last_status.as_ref()) {
                    return Ok(());
                }
            }
            DaemonProbeStatus::NotRunning => {
                if daemon_released_for_rollout_stop(host, port, last_status.as_ref()) {
                    return Ok(());
                }
            }
        }
        if Instant::now() >= deadline {
            if let Some(status) = last_status.as_ref() {
                tracing::warn!(
                    "sinomo rollout terminating unresponsive {label} pid {} after {}ms graceful stop window",
                    status.pid,
                    timeout.as_millis()
                );
                terminate_process_for_rollout_stop(status.pid, termination_mode)?;
                let terminate_deadline = Instant::now() + ROLLOUT_DAEMON_TERMINATE_WAIT;
                while Instant::now() < terminate_deadline {
                    match probe_daemon_status(daemon_paths, probe_timeout) {
                        DaemonProbeStatus::Running(_)
                        | DaemonProbeStatus::Locked { status: Some(_) } => {}
                        DaemonProbeStatus::Locked { status: None }
                        | DaemonProbeStatus::NotRunning => {
                            if daemon_released_for_rollout_stop(host, port, last_status.as_ref()) {
                                return Ok(());
                            }
                        }
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
            return Err(DaemonError::Network {
                address: daemon_connect_address(host, port),
                detail: format!("{label} did not release control listener for rollout"),
            });
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn stop_shadow_supervisor_for_rollout(
    shadow_paths: &DaemonPaths,
    shadow_host: &str,
    shadow_port: u16,
    timeout: Duration,
    probe_timeout: Duration,
) -> Result<(), DaemonError> {
    stop_daemon_for_rollout(
        shadow_paths,
        shadow_host,
        shadow_port,
        "shadow supervisor",
        RolloutTerminationMode::ProcessOnly,
        timeout,
        probe_timeout,
    )
}

fn stop_main_daemon_for_rollout(
    main_paths: &DaemonPaths,
    main_host: &str,
    main_port: u16,
    timeout: Duration,
    probe_timeout: Duration,
) -> Result<(), DaemonError> {
    stop_daemon_for_rollout(
        main_paths,
        main_host,
        main_port,
        "main daemon",
        RolloutTerminationMode::ProcessTree,
        timeout,
        probe_timeout,
    )
}

#[allow(clippy::too_many_arguments)]
fn ensure_shadow_supervisor_running_from_installed_path(
    data_dir: &Path,
    shadow_binary: &Path,
    main_binary: &Path,
    main_host: &str,
    main_port: u16,
    shadow_host: &str,
    shadow_port: u16,
    deadline: Duration,
) -> Result<(), DaemonError> {
    ensure_shadow_supervisor_running(
        data_dir,
        shadow_binary,
        main_binary,
        main_host,
        main_port,
        shadow_host,
        shadow_port,
        deadline,
    )?;
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let running = match probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => status,
        DaemonProbeStatus::Locked {
            status: Some(status),
        } => status,
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            return Err(DaemonError::Network {
                address: daemon_connect_address(shadow_host, shadow_port),
                detail: "shadow supervisor did not report status after rollout start".to_string(),
            });
        }
    };
    if running
        .executable
        .as_ref()
        .is_some_and(|executable| sinomo_rollout::same_path_if_exists(executable, shadow_binary))
    {
        return Ok(());
    }
    stop_shadow_supervisor_for_rollout(
        &shadow_paths,
        shadow_host,
        shadow_port,
        Duration::from_secs(15),
        DAEMON_PROBE_TIMEOUT,
    )?;
    ensure_shadow_supervisor_running(
        data_dir,
        shadow_binary,
        main_binary,
        main_host,
        main_port,
        shadow_host,
        shadow_port,
        deadline,
    )?;
    let running = match probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => status,
        DaemonProbeStatus::Locked {
            status: Some(status),
        } => status,
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            return Err(DaemonError::Network {
                address: daemon_connect_address(shadow_host, shadow_port),
                detail: "shadow supervisor did not report status after stable-path restart"
                    .to_string(),
            });
        }
    };
    if running
        .executable
        .as_ref()
        .is_some_and(|executable| sinomo_rollout::same_path_if_exists(executable, shadow_binary))
    {
        Ok(())
    } else {
        Err(DaemonError::Network {
            address: daemon_connect_address(shadow_host, shadow_port),
            detail: format!(
                "shadow supervisor started from {}, expected {}",
                running
                    .executable
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "unknown executable".to_string()),
                shadow_binary.display()
            ),
        })
    }
}

pub fn handle_daemon_finalize_shadow_rollout(paths: &DaemonPaths) -> Result<String, DaemonError> {
    finalize_pending_shadow_rollout(paths, false, true)
}

pub fn handle_deferred_shadow_rollout_completion(
    paths: &DaemonPaths,
) -> Result<String, DaemonError> {
    finalize_pending_shadow_rollout(paths, true, false)
}

fn finalize_pending_shadow_rollout(
    paths: &DaemonPaths,
    stop_main_first: bool,
    defer_on_error: bool,
) -> Result<String, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let Some(finalize) = read_pending_shadow_rollout(&rollout_paths)? else {
        return Ok("no pending shadow rollout\n".to_string());
    };

    let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let mut detail = "shadow rollout finalized".to_string();
    let mut status = "accepted";
    let mut previous_shadow_binary = finalize.previous_shadow_binary.clone();

    let outcome = (|| -> Result<(), DaemonError> {
        if stop_main_first {
            stop_main_daemon_for_rollout(
                &main_paths,
                &finalize.main_host,
                finalize.main_port,
                Duration::from_secs(30),
                DAEMON_PROBE_TIMEOUT,
            )?;
        }
        stop_shadow_supervisor_for_rollout(
            &shadow_paths,
            &finalize.shadow_host,
            finalize.shadow_port,
            Duration::from_secs(15),
            DAEMON_PROBE_TIMEOUT,
        )?;
        if let Some(staged_shadow) = finalize
            .staged_shadow_binary
            .as_ref()
            .filter(|path| path.exists())
        {
            if let Some(backup) = activate_staged_rollout_binary_with_retry(
                staged_shadow,
                &finalize.shadow_binary,
                STABLE_BINARY_COPY_TIMEOUT,
                STABLE_BINARY_COPY_RETRY_INTERVAL,
            )? {
                previous_shadow_binary = backup;
            }
        }
        ensure_shadow_supervisor_running_from_installed_path(
            data_dir,
            &finalize.shadow_binary,
            &finalize.main_binary,
            &finalize.main_host,
            finalize.main_port,
            &finalize.shadow_host,
            finalize.shadow_port,
            SHADOW_RECOVERY_DEADLINE,
        )
    })();

    if let Err(error) = outcome {
        if defer_on_error {
            match spawn_shadow_rollout_recovery_helper_process() {
                Ok(()) => {
                    return Ok(format!(
                        "deferred: shadow rollout finalization needs main process handoff after {error}\n"
                    ));
                }
                Err(helper_error) => {
                    tracing::warn!(
                        "sinomo shadow rollout finalize helper spawn failed: {helper_error}; attempting inline rollback"
                    );
                }
            }
        }
        // The new supervisor did not become healthy. Restore the previous
        // known-good binaries and recover with verified, bounded retries so the
        // node is not left without a supervisor or on a half-activated build.
        let _ = take_shadow_reexec_request(&rollout_paths);
        let recovery_result = (|| -> Result<(), DaemonError> {
            stop_main_daemon_for_rollout(
                &main_paths,
                &finalize.main_host,
                finalize.main_port,
                Duration::from_secs(15),
                DAEMON_PROBE_TIMEOUT,
            )?;
            if let Some(previous_main_binary) = finalize.previous_main_binary.as_ref() {
                restore_rollout_binary_from_backup_with_retry(
                    previous_main_binary,
                    &finalize.main_binary,
                    STABLE_BINARY_COPY_TIMEOUT,
                    STABLE_BINARY_COPY_RETRY_INTERVAL,
                )?;
            }
            restore_rollout_binary_from_backup_with_retry(
                &previous_shadow_binary,
                &finalize.shadow_binary,
                STABLE_BINARY_COPY_TIMEOUT,
                STABLE_BINARY_COPY_RETRY_INTERVAL,
            )
        })();
        match recovery_result.and_then(|()| {
            ensure_shadow_supervisor_running_from_installed_path(
                data_dir,
                &finalize.shadow_binary,
                &finalize.main_binary,
                &finalize.main_host,
                finalize.main_port,
                &finalize.shadow_host,
                finalize.shadow_port,
                SHADOW_RECOVERY_DEADLINE,
            )
        }) {
            Ok(()) => {
                status = "rolled-back";
                detail = format!(
                    "candidate shadow did not become healthy ({error}); restored previous main and shadow binaries"
                );
            }
            Err(recovery_error) => {
                status = "failed";
                detail = format!(
                    "candidate shadow failed ({error}) and rollback supervisor did not become healthy: {recovery_error}"
                );
                tracing::warn!("sinomo shadow rollout finalize: {detail}");
            }
        }
    }

    record_shadow_rollout_finalized(&rollout_paths, &finalize, status, &detail)?;
    remove_shadow_finalize(&rollout_paths);
    Ok(format!("{status}: {detail}\n"))
}

pub fn handle_daemon_clear_rollout_status(paths: &DaemonPaths) -> Result<String, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let rollout_paths = SelfRolloutPaths::new(&data_dir);
    let cleared = clear_rollout_status(&rollout_paths)?;
    if let Err(error) = crate::cluster::refresh_daemon_node_status_in_kv(paths) {
        tracing::warn!("sinomo daemon rollout status clear publish failed: {error}");
    }
    if cleared {
        Ok(format!(
            "cleared rollout status at {}\n",
            rollout_paths.state.display()
        ))
    } else {
        Ok(format!(
            "no rollout status to clear at {}\n",
            rollout_paths.state.display()
        ))
    }
}

/// Maximum time to keep retrying the known-good supervisor binary during rollout
/// recovery before giving up. Generous because a node with no supervisor is a
/// hard failure that blocks the whole node.
const SHADOW_RECOVERY_DEADLINE: Duration = Duration::from_secs(60);

/// Brings a shadow supervisor back up using `shadow_binary`, retrying with backoff
/// until it reports healthy or `deadline` elapses. Used to guarantee a supervisor
/// is running after a failed rollout handoff. Each attempt re-checks health first,
/// so a supervisor that came up late (handoff grace) is accepted without respawning.
#[allow(clippy::too_many_arguments)]
fn ensure_shadow_supervisor_running(
    data_dir: &Path,
    shadow_binary: &Path,
    main_binary: &Path,
    main_host: &str,
    main_port: u16,
    shadow_host: &str,
    shadow_port: u16,
    deadline: Duration,
) -> Result<(), DaemonError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let overall_deadline = Instant::now() + deadline;
    loop {
        if matches!(
            probe_daemon_status(&shadow_paths, Duration::from_secs(2)),
            DaemonProbeStatus::Running(_)
        ) {
            return Ok(());
        }
        let attempt = spawn_shadow_for_cluster_rollout(
            shadow_binary,
            main_binary,
            main_host,
            main_port,
            shadow_host,
            shadow_port,
            &rollout_paths,
        )
        .and_then(|(child, diagnostics)| {
            wait_for_shadow_rollout_health(
                data_dir,
                shadow_host,
                shadow_port,
                Duration::from_secs(5),
                child,
                diagnostics,
            )
        });
        match attempt {
            Ok(()) => return Ok(()),
            // Keep retrying until the deadline; surface the most recent failure if we run out.
            Err(error) if Instant::now() >= overall_deadline => return Err(error),
            Err(_) => thread::sleep(Duration::from_millis(500)),
        }
    }
}

pub fn wait_for_main_daemon_health(
    data_dir: &Path,
    host: &str,
    port: u16,
    previous_pid: Option<u32>,
    wait_timeout: Duration,
    probe_timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    let paths = DaemonPaths::with_root(data_dir.join("daemon"));
    let deadline = Instant::now() + wait_timeout;
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) = probe_daemon_status(&paths, probe_timeout) {
            if previous_pid.is_none_or(|pid| status.pid != pid) {
                return Ok(status);
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(DaemonError::Network {
        address: daemon_connect_address(host, port),
        detail: "rolled out main daemon did not become ready".to_string(),
    })
}

pub fn wait_for_local_shadow_rollout_completion(
    data_dir: &Path,
    main_host: &str,
    main_port: u16,
    shadow_host: &str,
    shadow_port: u16,
    wait_timeout: Duration,
    probe_timeout: Duration,
) -> Result<(), DaemonError> {
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
    let deadline = Instant::now() + wait_timeout;
    let mut next_restart_nudge = Instant::now() + Duration::from_secs(2);
    let deferred_finalization_at = Instant::now() + Duration::from_secs(5);
    let mut attempted_deferred_finalization = false;
    while Instant::now() < deadline {
        let pending_main = rollout_main_activation_path(&rollout_paths).exists();
        let pending_shadow = rollout_shadow_finalize_path(&rollout_paths).exists();
        if !pending_main
            && !pending_shadow
            && matches!(
                probe_daemon_status(&shadow_paths, probe_timeout),
                DaemonProbeStatus::Running(_)
            )
        {
            return Ok(());
        }
        if pending_shadow
            && !pending_main
            && !attempted_deferred_finalization
            && Instant::now() >= deferred_finalization_at
        {
            attempted_deferred_finalization = true;
            match handle_deferred_shadow_rollout_completion(&main_paths) {
                Ok(response) => {
                    tracing::info!(
                        "sinomo rollout wait completed deferred shadow finalization: {response}"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        "sinomo rollout wait deferred shadow finalization failed: {error}"
                    );
                }
            }
        }
        if pending_main && Instant::now() >= next_restart_nudge {
            if let DaemonProbeStatus::Running(status) =
                probe_daemon_status(&main_paths, probe_timeout)
            {
                let _ = send_daemon_shutdown_request(&status.address, probe_timeout);
            }
            next_restart_nudge = Instant::now() + Duration::from_secs(2);
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(DaemonError::Network {
        address: daemon_connect_address(shadow_host, shadow_port),
        detail: format!(
            "rolled out shadow daemon did not complete local rollout finalization after main {main_host}:{main_port} handoff"
        ),
    })
}

pub fn write_rollout_main_failure_diagnostics(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
    previous_main_binary: &Path,
    diagnostics_paths: &CandidateMainDiagnostics,
    detail: &str,
    observation: Option<MainExitObservation>,
) -> Result<RolloutFailureDiagnostics, DaemonError> {
    let stdout_tail =
        read_tail_lossy(&diagnostics_paths.stdout_log, ROLLOUT_DIAGNOSTIC_TAIL_BYTES)?;
    let stderr_tail =
        read_tail_lossy(&diagnostics_paths.stderr_log, ROLLOUT_DIAGNOSTIC_TAIL_BYTES)?;
    let diagnostics = RolloutFailureDiagnostics {
        diagnostics_path: diagnostics_paths.diagnostics_path.clone(),
        stdout_log: Some(diagnostics_paths.stdout_log.clone()),
        stderr_log: Some(diagnostics_paths.stderr_log.clone()),
        stdout_tail: stdout_tail.clone().filter(|tail| !tail.trim().is_empty()),
        stderr_tail: stderr_tail.clone().filter(|tail| !tail.trim().is_empty()),
    };
    write_json(
        &diagnostics_paths.diagnostics_path,
        &serde_json::json!({
            "status": "failed",
            "detail": detail,
            "source": &activation.source,
            "mode": &activation.mode,
            "main_binary": &activation.main_binary,
            "previous_main_binary": previous_main_binary,
            "stdout_log": &diagnostics_paths.stdout_log,
            "stderr_log": &diagnostics_paths.stderr_log,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "exit_code": observation.and_then(|observation| observation.exit_code),
            "signal": observation.and_then(|observation| observation.signal),
            "requested_shutdown": observation.map(|observation| observation.requested_shutdown),
            "observed_at": rollout_timestamp_now(),
            "rollout_state": &rollout_paths.state
        }),
    )?;
    Ok(diagnostics)
}

pub fn promote_shadow_main_rollout(
    rollout_paths: &SelfRolloutPaths,
    pending: &ShadowMainRolloutMonitor,
    shadow_host: &str,
    shadow_port: u16,
) -> Result<crate::DaemonRunOptions, DaemonError> {
    stage_pending_shadow_rollout(rollout_paths, pending, shadow_host, shadow_port)?;
    remove_main_activation(rollout_paths);
    Ok(pending.main_options.clone())
}

pub fn recover_failed_shadow_main_rollout(
    rollout_paths: &SelfRolloutPaths,
    pending: &ShadowMainRolloutMonitor,
    detail: &str,
    observation: Option<MainExitObservation>,
) -> Result<PathBuf, DaemonError> {
    let diagnostics = write_rollout_main_failure_diagnostics(
        rollout_paths,
        &pending.activation,
        &pending.previous_main_binary,
        &pending.diagnostics,
        detail,
        observation,
    )?;
    record_main_rollout_failure_with_diagnostics(
        rollout_paths,
        &pending.activation,
        &pending.previous_main_binary,
        &pending.previous_shadow_binary,
        detail,
        Some(&diagnostics),
    )?;
    restore_rollout_binary_from_backup_with_retry(
        &pending.previous_main_binary,
        &pending.activation.main_binary,
        STABLE_BINARY_COPY_TIMEOUT,
        STABLE_BINARY_COPY_RETRY_INTERVAL,
    )?;
    let main_binary = pending.activation.main_binary.clone();
    remove_main_activation(rollout_paths);
    Ok(main_binary)
}

pub fn start_shadow_main_if_needed(
    rollout_paths: &SelfRolloutPaths,
    main_binary: &Path,
    main_options: &crate::DaemonRunOptions,
    candidate_timeout: Duration,
) -> Result<ShadowMainStart, DaemonError> {
    if let Some(activation) = read_rollout_main_activation(rollout_paths)? {
        let mut previous_main_binary = activation
            .previous_main_binary
            .clone()
            .unwrap_or_else(|| rollout_previous_binary_path(&activation.main_binary));
        let activation_main_options = crate::DaemonRunOptions {
            host: activation.main_host.clone(),
            port: activation.main_port,
        };
        if let Some(staged_main) = activation
            .staged_main_binary
            .as_ref()
            .filter(|path| path.exists())
        {
            if let Some(backup) = activate_staged_rollout_binary_with_retry(
                staged_main,
                &activation.main_binary,
                STABLE_BINARY_COPY_TIMEOUT,
                STABLE_BINARY_COPY_RETRY_INTERVAL,
            )? {
                previous_main_binary = backup;
            }
        }
        match spawn_candidate_main_daemon_process(
            &activation.main_binary,
            &activation_main_options,
            rollout_paths,
        ) {
            Ok((child, diagnostics)) => {
                let main_binary = activation.main_binary.clone();
                let previous_shadow_binary =
                    rollout_previous_binary_path(&activation.shadow_binary);
                Ok(ShadowMainStart::Candidate {
                    child,
                    monitor: ShadowMainRolloutMonitor {
                        activation,
                        main_options: activation_main_options,
                        previous_main_binary,
                        previous_shadow_binary,
                        diagnostics,
                        deadline: Instant::now() + candidate_timeout,
                    },
                    main_binary,
                })
            }
            Err(error) => {
                record_activation_failure(
                    rollout_paths,
                    &activation,
                    &previous_main_binary,
                    &format!("candidate main could not execute: {error}"),
                )?;
                restore_rollout_binary_from_backup_with_retry(
                    &previous_main_binary,
                    &activation.main_binary,
                    STABLE_BINARY_COPY_TIMEOUT,
                    STABLE_BINARY_COPY_RETRY_INTERVAL,
                )?;
                let main_binary = activation.main_binary.clone();
                remove_main_activation(rollout_paths);
                let child = spawn_main_daemon_process(&main_binary, &activation_main_options).ok();
                Ok(ShadowMainStart::Recovered {
                    child,
                    main_binary,
                    main_options: activation_main_options,
                })
            }
        }
    } else if main_binary.exists() {
        match spawn_main_daemon_process(main_binary, main_options) {
            Ok(child) => Ok(ShadowMainStart::Main { child }),
            Err(error) => Ok(ShadowMainStart::StartFailed {
                detail: error.to_string(),
            }),
        }
    } else {
        Ok(ShadowMainStart::None)
    }
}

pub fn handle_shadow_main_process_exit(
    rollout_paths: &SelfRolloutPaths,
    _main_binary: &Path,
    _main_options: &crate::DaemonRunOptions,
    exited_pid: u32,
    status: ExitStatus,
) -> Result<ShadowMainExit, DaemonError> {
    let supervisor_restart_requested =
        take_supervised_main_restart_request(rollout_paths, exited_pid)?;
    if read_rollout_main_activation(rollout_paths)?.is_some() {
        return Ok(ShadowMainExit::PendingActivation);
    }
    let observation = crate::main_exit_observation_from_status(status);
    let kind = classify_main_exit(observation);
    record_rollout_main_exit(rollout_paths, observation, kind)?;
    if supervisor_restart_requested || kind == sinomo_rollout::MainExitKind::Crash {
        Ok(ShadowMainExit::RestartRequested)
    } else {
        Ok(ShadowMainExit::Suppressed)
    }
}

pub fn handle_pending_shadow_main_process_exit(
    rollout_paths: &SelfRolloutPaths,
    main_paths: &DaemonPaths,
    pending: &ShadowMainRolloutMonitor,
    candidate_pid: u32,
    status: ExitStatus,
    shadow_host: &str,
    shadow_port: u16,
    probe_timeout: Duration,
) -> Result<PendingShadowMainExit, DaemonError> {
    let observation = crate::main_exit_observation_from_status(status);
    let probe_status = probe_daemon_status(main_paths, probe_timeout);
    match pending_shadow_candidate_exit_action(
        &probe_status,
        Some(candidate_pid),
        observation,
        Instant::now(),
        pending.deadline,
    ) {
        PendingShadowCandidateExitAction::Promote => {
            let main_options =
                promote_shadow_main_rollout(rollout_paths, pending, shadow_host, shadow_port)?;
            let main_probe_status = probe_daemon_status(main_paths, probe_timeout);
            return Ok(PendingShadowMainExit::Promoted {
                main_binary: pending.activation.main_binary.clone(),
                main_options,
                main_probe_status,
            });
        }
        PendingShadowCandidateExitAction::Retry => {
            match spawn_candidate_main_daemon_process(
                &pending.activation.main_binary,
                &pending.main_options,
                rollout_paths,
            ) {
                Ok((child, diagnostics)) => {
                    let mut monitor = pending.clone();
                    monitor.diagnostics = diagnostics;
                    return Ok(PendingShadowMainExit::RetryCandidate { child, monitor });
                }
                Err(error) => {
                    let detail = format!(
                        "candidate main exited before health check and retry spawn failed: {error}"
                    );
                    let main_binary = recover_failed_shadow_main_rollout(
                        rollout_paths,
                        pending,
                        &detail,
                        Some(observation),
                    )?;
                    let main_options = pending.main_options.clone();
                    let child = spawn_main_daemon_process(&main_binary, &main_options).ok();
                    return Ok(PendingShadowMainExit::Recovered {
                        child,
                        main_binary,
                        main_options,
                    });
                }
            }
        }
        PendingShadowCandidateExitAction::Recover => {}
    }

    let detail = format!(
        "candidate main exited before health check: exit_code={:?} signal={:?}",
        observation.exit_code, observation.signal
    );
    let main_binary =
        recover_failed_shadow_main_rollout(rollout_paths, pending, &detail, Some(observation))?;
    let main_options = pending.main_options.clone();
    let child = spawn_main_daemon_process(&main_binary, &main_options).ok();
    Ok(PendingShadowMainExit::Recovered {
        child,
        main_binary,
        main_options,
    })
}

pub fn handle_daemon_rollout_build_request(
    paths: &DaemonPaths,
    request: ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let source = request.source.clone();
    let requested_at = request.requested_at.clone();
    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-remote-build",
        Some("remote build request received".to_string()),
        &source,
        &requested_at,
        "received",
        1,
        "remote build request received",
    )?;
    let repo = if let Some(expected_head) = request.source_head.as_deref()
        && request.source_bundle.is_some()
        && managed_source_status(&rollout_paths)
            .ok()
            .and_then(|status| status.head)
            .as_deref()
            .is_some_and(|current_head| source_heads_match(Some(current_head), expected_head))
    {
        write_cluster_rollout_progress(
            paths,
            &rollout_paths,
            "in-progress",
            "cluster-remote-build",
            Some("managed source already matches requested head".to_string()),
            &source,
            &requested_at,
            "preparing-source",
            1,
            "managed source already matches requested head",
        )?;
        rollout_paths.source_repo.clone()
    } else if let Some(bundle_bytes) = request.source_bundle.as_deref() {
        write_cluster_rollout_progress(
            paths,
            &rollout_paths,
            "in-progress",
            "cluster-remote-build",
            Some("importing source bundle".to_string()),
            &source,
            &requested_at,
            "importing-source",
            1,
            "importing source bundle",
        )?;
        let bundle_path = rollout_paths
            .root
            .join("staged")
            .join(format!("source-{}.bundle", rollout_artifact_suffix()));
        if let Some(parent) = bundle_path.parent() {
            fs::create_dir_all(parent).map_err(|source| DaemonError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&bundle_path, bundle_bytes).map_err(|source| DaemonError::Io {
            path: bundle_path.display().to_string(),
            detail: source.to_string(),
        })?;
        deploy_managed_source_bundle_with_options(
            &rollout_paths,
            &bundle_path,
            ManagedSourceDeployOptions {
                force_overwrite: request.force_source_overwrite,
            },
        )?;
        if let Some(expected_head) = request.source_head.as_deref() {
            let status = managed_source_status(&rollout_paths)?;
            if !source_heads_match(status.head.as_deref(), expected_head) {
                let detail = format!(
                    "source bundle imported but managed source head is {}, expected {expected_head}",
                    status.head.unwrap_or_else(|| "unknown".to_string())
                );
                write_cluster_rollout_progress(
                    paths,
                    &rollout_paths,
                    "rejected",
                    "cluster-remote-build",
                    Some(detail.clone()),
                    &source,
                    &requested_at,
                    "source-rejected",
                    CLUSTER_ROLLOUT_PROGRESS_TOTAL,
                    "source head mismatch",
                )?;
                return Ok(ClusterRolloutBinaryResponse {
                    status: "rejected".to_string(),
                    detail,
                    main_binary: PathBuf::new(),
                    shadow_binary: PathBuf::new(),
                    source_head: None,
                });
            }
        }
        rollout_paths.source_repo.clone()
    } else {
        write_cluster_rollout_progress(
            paths,
            &rollout_paths,
            "in-progress",
            "cluster-remote-build",
            Some("using target source repository".to_string()),
            &source,
            &requested_at,
            "preparing-source",
            1,
            "using target source repository",
        )?;
        rollout_remote_repo_path(&rollout_paths, request.remote_repo.as_deref())
    };
    if !repo.join("Cargo.toml").exists() {
        let detail = format!(
            "remote source repo is not initialized at {}; run `sinomo rollout init-source --repo <path>` on this node or pass --remote-repo <path>",
            repo.display()
        );
        write_cluster_rollout_progress(
            paths,
            &rollout_paths,
            "rejected",
            "cluster-remote-build",
            Some(detail.clone()),
            &source,
            &requested_at,
            "source-rejected",
            CLUSTER_ROLLOUT_PROGRESS_TOTAL,
            "source repository missing",
        )?;
        return Ok(ClusterRolloutBinaryResponse {
            status: "rejected".to_string(),
            detail,
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        });
    }

    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-remote-build",
        Some("building target binary".to_string()),
        &source,
        &requested_at,
        "building",
        2,
        "cargo build running",
    )?;
    let (heartbeat_stop, heartbeat) = start_cluster_rollout_build_heartbeat(
        paths.clone(),
        rollout_paths.clone(),
        "cluster-remote-build",
        source.clone(),
        requested_at.clone(),
        "building",
        2,
    );
    let built_artifacts = match rollout_build_dev_artifacts(&repo, &rollout_paths.build_log) {
        Ok(artifacts) => {
            let _ = heartbeat_stop.send(());
            let _ = heartbeat.join();
            artifacts
        }
        Err(error) => {
            let _ = heartbeat_stop.send(());
            let _ = heartbeat.join();
            let detail = format!("remote build failed: {error}");
            let _ = write_cluster_rollout_progress(
                paths,
                &rollout_paths,
                "rejected",
                "cluster-remote-build",
                Some(detail.clone()),
                &source,
                &requested_at,
                "build-failed",
                CLUSTER_ROLLOUT_PROGRESS_TOTAL,
                "build failed",
            );
            return Ok(ClusterRolloutBinaryResponse {
                status: "rejected".to_string(),
                detail,
                main_binary: PathBuf::new(),
                shadow_binary: PathBuf::new(),
                source_head: None,
            });
        }
    };
    let main_binary = rollout_paths.main_binary.clone();
    let shadow_binary = rollout_paths.shadow_binary.clone();
    let staged_main_binary = rollout_staged_main_binary(&rollout_paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&rollout_paths);
    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-remote-build",
        Some("staging built binaries".to_string()),
        &source,
        &requested_at,
        "staging-binaries",
        3,
        "staging built binaries",
    )?;
    if let Err(error) = stage_rollout_file_pair(
        &built_artifacts.built_binary,
        &staged_main_binary,
        &staged_shadow_binary,
    ) {
        let detail = format!("stage remote-build binaries failed: {error}");
        return rejected_cluster_rollout_response(
            paths,
            &rollout_paths,
            "cluster-remote-build",
            &source,
            &requested_at,
            "staging-failed",
            "staging failed",
            detail,
        );
    }
    if !built_artifacts.native_dylib_modules.is_empty() {
        write_cluster_rollout_progress(
            paths,
            &rollout_paths,
            "in-progress",
            "cluster-remote-build",
            Some(format!(
                "installing {} native module artifact(s)",
                built_artifacts.native_dylib_modules.len()
            )),
            &source,
            &requested_at,
            "staging-modules",
            3,
            "installing native module artifacts",
        )?;
        if let Err(error) = install_local_rollout_native_dylib_modules(
            data_dir,
            &built_artifacts.native_dylib_modules,
        ) {
            let detail = format!("install native module artifacts failed: {error}");
            let _ = write_cluster_rollout_progress(
                paths,
                &rollout_paths,
                "rejected",
                "cluster-remote-build",
                Some(detail.clone()),
                &source,
                &requested_at,
                "module-install-failed",
                CLUSTER_ROLLOUT_PROGRESS_TOTAL,
                "module install failed",
            );
            return Ok(ClusterRolloutBinaryResponse {
                status: "rejected".to_string(),
                detail,
                main_binary: PathBuf::new(),
                shadow_binary: PathBuf::new(),
                source_head: None,
            });
        }
    }

    write_cluster_rollout_progress(
        paths,
        &rollout_paths,
        "in-progress",
        "cluster-remote-build",
        Some("preparing activation".to_string()),
        &source,
        &requested_at,
        "preparing-activation",
        4,
        "preparing activation",
    )?;
    let plan = prepare_cluster_rollout_main_activation(
        paths,
        data_dir,
        source.clone(),
        main_binary.clone(),
        shadow_binary.clone(),
        Some(staged_main_binary),
        Some(staged_shadow_binary),
        request.host.clone(),
        request.port,
        request.shadow_port,
        requested_at.clone(),
        "cluster-remote-build",
    )?;
    let activation_strategy =
        stage_rollout_main_activation(paths, data_dir, &rollout_paths, plan.activation)?;
    let _ = (repo, built_artifacts);
    publish_rollout_progress_to_cluster(paths);
    Ok(ClusterRolloutBinaryResponse {
        status: "accepted".to_string(),
        detail: rollout_staged_detail(
            "remote build staged",
            plan.current_main_pid,
            activation_strategy,
        ),
        main_binary,
        shadow_binary,
        source_head: managed_source_status(&rollout_paths)?.head,
    })
}

pub fn spawn_shadow_for_cluster_rollout(
    shadow_binary: &Path,
    main_binary: &Path,
    main_host: &str,
    main_port: u16,
    shadow_host: &str,
    shadow_port: u16,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(Child, ShadowSupervisorDiagnostics), DaemonError> {
    let logs_dir = rollout_paths.root.join("logs");
    fs::create_dir_all(&logs_dir).map_err(|source| DaemonError::Io {
        path: logs_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    let suffix = rollout_artifact_suffix();
    let stdout_log = logs_dir.join(format!("shadow-supervisor-{suffix}.stdout.log"));
    let stderr_log = logs_dir.join(format!("shadow-supervisor-{suffix}.stderr.log"));
    let stdout = File::create(&stdout_log).map_err(|source| DaemonError::Io {
        path: stdout_log.display().to_string(),
        detail: source.to_string(),
    })?;
    let stderr = File::create(&stderr_log).map_err(|source| DaemonError::Io {
        path: stderr_log.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut command = Command::new(shadow_binary);
    prepare_background_command(&mut command);
    let child = command
        .args(["daemon", "shadow", "run"])
        .arg("--host")
        .arg(shadow_host)
        .arg("--port")
        .arg(shadow_port.to_string())
        .env(SHADOW_MAIN_BINARY_ENV, main_binary)
        .env(SHADOW_MAIN_HOST_ENV, main_host)
        .env(SHADOW_MAIN_PORT_ENV, main_port.to_string())
        .env(SHADOW_START_DELAY_MS_ENV, "0")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|source| DaemonError::Io {
            path: shadow_binary.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok((
        child,
        ShadowSupervisorDiagnostics {
            stdout_log,
            stderr_log,
        },
    ))
}

pub fn wait_for_shadow_rollout_health(
    data_dir: &Path,
    shadow_host: &str,
    shadow_port: u16,
    timeout: Duration,
    mut child: Child,
    diagnostics: ShadowSupervisorDiagnostics,
) -> Result<(), DaemonError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if matches!(
            probe_daemon_status(&shadow_paths, Duration::from_secs(2)),
            DaemonProbeStatus::Running(_)
        ) {
            reap_detached_shadow_supervisor(child);
            return Ok(());
        }
        if let Some(status) = child.try_wait().map_err(|source| DaemonError::Io {
            path: diagnostics.stderr_log.display().to_string(),
            detail: format!("shadow supervisor exit probe failed: {source}"),
        })? {
            return Err(DaemonError::Network {
                address: daemon_connect_address(shadow_host, shadow_port),
                detail: shadow_supervisor_exit_detail(status, &diagnostics)?,
            });
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
    Err(DaemonError::Network {
        address: daemon_connect_address(shadow_host, shadow_port),
        detail: format!(
            "rolled out shadow daemon did not become ready; stdout_log={} stderr_log={}",
            diagnostics.stdout_log.display(),
            diagnostics.stderr_log.display()
        ),
    })
}

fn shadow_supervisor_exit_detail(
    status: ExitStatus,
    diagnostics: &ShadowSupervisorDiagnostics,
) -> Result<String, DaemonError> {
    let stderr_tail = read_tail_lossy(&diagnostics.stderr_log, ROLLOUT_DIAGNOSTIC_TAIL_BYTES)?;
    let stdout_tail = read_tail_lossy(&diagnostics.stdout_log, ROLLOUT_DIAGNOSTIC_TAIL_BYTES)?;
    Ok(format!(
        "shadow supervisor exited before reporting healthy: {}; stdout_log={} stderr_log={} stdout_tail={:?} stderr_tail={:?}",
        format_exit_status(status),
        diagnostics.stdout_log.display(),
        diagnostics.stderr_log.display(),
        stdout_tail,
        stderr_tail
    ))
}

fn format_exit_status(status: ExitStatus) -> String {
    #[cfg(unix)]
    {
        if let Some(signal) = status.signal() {
            return format!("signal={signal}");
        }
    }
    format!("exit_code={:?}", status.code())
}

fn reap_detached_shadow_supervisor(mut child: Child) {
    let _ = thread::Builder::new()
        .name("sinomo-shadow-reaper".to_string())
        .spawn(move || {
            let _ = child.wait();
        });
}

fn daemon_data_dir(paths: &DaemonPaths) -> Result<&Path, DaemonError> {
    paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })
}

fn paths_equivalent(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rollout_binary_test_manifest(bytes: &[u8]) -> ClusterRolloutBinaryManifest {
        ClusterRolloutBinaryManifest {
            upload_id: "upload-test".to_string(),
            source: "test".to_string(),
            target_os: env::consts::OS.to_string(),
            target_arch: env::consts::ARCH.to_string(),
            host: "127.0.0.1".to_string(),
            port: 21353,
            shadow_port: 21354,
            requested_at: "now".to_string(),
            binary: ClusterRolloutUploadArtifactSpec {
                artifact_id: "binary".to_string(),
                total_bytes: bytes.len() as u64,
                blake3: blake3::hash(bytes).as_bytes().to_vec(),
            },
            native_dylib_modules: Vec::new(),
        }
    }

    #[tokio::test]
    async fn rollout_binary_body_streams_to_a_durable_verified_artifact() {
        let root = temp_rollout_test_root("binary-body-stream");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        fs::create_dir_all(&paths.root).expect("daemon root");
        let bytes = b"hello world";
        let manifest = rollout_binary_test_manifest(bytes);
        let request = ClusterRolloutBinaryRequest {
            manifest,
            artifact_id: "binary".to_string(),
            activate: false,
        };
        let mut body = bytes.as_slice();

        let response = handle_daemon_rollout_binary_request(&paths, request, &mut body)
            .await
            .expect("streamed upload");
        assert_eq!(response.status, "staged");
        let rollout_paths = SelfRolloutPaths::new(&root);
        let uploaded = rollout_upload_artifact_path(&rollout_paths, "upload-test", "binary")
            .expect("upload path");
        assert_eq!(fs::read(uploaded).expect("uploaded bytes"), bytes);
        assert!(
            rollout_upload_manifest_path(&rollout_paths, "upload-test")
                .expect("manifest path")
                .exists()
        );
    }

    #[tokio::test]
    async fn rollout_binary_body_rejects_truncated_content_and_removes_partial_file() {
        let root = temp_rollout_test_root("binary-body-truncated");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        fs::create_dir_all(&paths.root).expect("daemon root");
        let expected = b"hello world";
        let manifest = rollout_binary_test_manifest(expected);
        let request = ClusterRolloutBinaryRequest {
            manifest,
            artifact_id: "binary".to_string(),
            activate: false,
        };
        let mut body = b"hello".as_slice();

        let error = handle_daemon_rollout_binary_request(&paths, request, &mut body)
            .await
            .expect_err("truncated upload");
        assert!(error.to_string().contains("ended 6 bytes early"));
        let rollout_paths = SelfRolloutPaths::new(&root);
        let uploaded = rollout_upload_artifact_path(&rollout_paths, "upload-test", "binary")
            .expect("upload path");
        assert!(!uploaded.exists());
    }

    #[tokio::test]
    async fn rollout_binary_body_rejects_hash_mismatch() {
        let root = temp_rollout_test_root("binary-body-hash-mismatch");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        fs::create_dir_all(&paths.root).expect("daemon root");
        let expected = b"hello world";
        let manifest = rollout_binary_test_manifest(expected);
        let request = ClusterRolloutBinaryRequest {
            manifest,
            artifact_id: "binary".to_string(),
            activate: false,
        };
        let mut body = b"HELLO WORLD".as_slice();

        let error = handle_daemon_rollout_binary_request(&paths, request, &mut body)
            .await
            .expect_err("hash mismatch");
        assert!(error.to_string().contains("hash mismatch"));
    }

    #[test]
    fn pending_shadow_rollout_uses_fast_main_health_probe() {
        assert!(shadow_main_probe_interval(true) < Duration::from_secs(1));
        assert!(shadow_main_probe_interval(false) >= Duration::from_secs(10));
    }

    #[test]
    fn shadow_network_module_context_runtime_task_gets_graceful_join_budget() {
        assert_eq!(
            shadow_runtime_task_join_timeout("network module context"),
            SHADOW_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT
        );
        assert_eq!(
            shadow_runtime_task_join_timeout("status publisher"),
            SHADOW_STATUS_PUBLISHER_TASK_JOIN_TIMEOUT
        );
        assert!(
            shadow_runtime_task_join_timeout("network module context")
                > SHADOW_RUNTIME_TASK_JOIN_TIMEOUT,
            "network module shutdown needs more time than generic helper task shutdown"
        );
        assert_eq!(
            shadow_runtime_task_join_timeout("control accept"),
            SHADOW_RUNTIME_TASK_JOIN_TIMEOUT
        );
    }

    #[test]
    fn pending_main_activation_overrides_suppressed_restart() {
        assert!(should_shadow_start_main(true, true));
        assert!(!should_shadow_start_main(true, false));
        assert!(should_shadow_start_main(false, false));
    }

    #[test]
    fn pending_main_activation_waits_before_shutdown_nudge() {
        let root = temp_rollout_test_root("activation-shutdown-grace");
        let rollout_paths = SelfRolloutPaths::new(&root);
        std::fs::create_dir_all(&rollout_paths.root).expect("rollout root");
        write_main_activation(&rollout_paths, &test_main_activation(&rollout_paths))
            .expect("write activation");

        assert!(!pending_main_activation_ready_for_shutdown(&rollout_paths));
        std::thread::sleep(MAIN_ACTIVATION_SHUTDOWN_GRACE + Duration::from_millis(100));
        assert!(pending_main_activation_ready_for_shutdown(&rollout_paths));
    }

    #[test]
    fn pending_main_activation_treats_locked_main_as_stop_target() {
        let root = temp_rollout_test_root("activation-locked-main");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        let status = DaemonStatus::new(&crate::DaemonRunOptions::default(), &paths, None);

        assert!(pending_main_activation_stop_target(
            &DaemonProbeStatus::Running(status.clone())
        ));
        assert!(pending_main_activation_stop_target(
            &DaemonProbeStatus::Locked {
                status: Some(status)
            }
        ));
        assert!(!pending_main_activation_stop_target(
            &DaemonProbeStatus::Locked { status: None }
        ));
        assert!(!pending_main_activation_stop_target(
            &DaemonProbeStatus::NotRunning
        ));
    }

    #[test]
    fn staged_main_activation_shutdown_is_deferred_when_main_handles_request() {
        assert_eq!(
            staged_main_activation_shutdown_action(false),
            StagedMainActivationShutdown::DeferredSelf
        );
        assert_eq!(
            staged_main_activation_shutdown_action(true),
            StagedMainActivationShutdown::Immediate
        );
    }

    #[test]
    fn pending_candidate_clean_exit_retries_before_activation_deadline() {
        let now = Instant::now();
        let observation = MainExitObservation::exit_code(0);

        assert_eq!(
            pending_shadow_candidate_exit_action(
                &DaemonProbeStatus::NotRunning,
                Some(42),
                observation,
                now,
                now + Duration::from_secs(30)
            ),
            PendingShadowCandidateExitAction::Retry
        );
        assert_eq!(
            pending_shadow_candidate_exit_action(
                &DaemonProbeStatus::NotRunning,
                Some(42),
                observation,
                now,
                now
            ),
            PendingShadowCandidateExitAction::Recover
        );
        assert_eq!(
            pending_shadow_candidate_exit_action(
                &DaemonProbeStatus::Running(test_daemon_status(42)),
                Some(42),
                observation,
                now,
                now
            ),
            PendingShadowCandidateExitAction::Promote
        );
    }

    #[test]
    fn pending_candidate_start_waits_for_main_data_owner_lock_release() {
        let root = temp_rollout_test_root("activation-data-owner-lock");
        let paths = DaemonPaths::with_root(root.join("daemon"));

        assert!(main_data_owner_lock_released(&paths));
        let owner = crate::DaemonDataOwnerLock::acquire(&paths).expect("owner lock");
        assert!(!main_data_owner_lock_released(&paths));
        drop(owner);
        assert!(main_data_owner_lock_released(&paths));
    }

    #[test]
    fn rollout_stop_force_termination_wait_is_bounded() {
        assert!(PENDING_MAIN_ACTIVATION_FORCE_TERMINATE_AFTER <= Duration::from_secs(10));
        assert!(PENDING_MAIN_ACTIVATION_TERMINATE_WAIT <= Duration::from_secs(5));
        assert!(ROLLOUT_DAEMON_TERMINATE_WAIT <= Duration::from_secs(5));
    }

    #[test]
    fn rollout_stop_waits_for_previous_pid_before_binary_activation() {
        assert!(rollout_stop_ready_for_binary_activation(true, false));
        assert!(!rollout_stop_ready_for_binary_activation(false, false));
        assert!(!rollout_stop_ready_for_binary_activation(true, true));
    }

    #[test]
    fn rollout_process_liveness_detects_current_process() {
        assert!(rollout_process_is_running(std::process::id()));
    }

    #[test]
    fn shadow_force_stop_does_not_terminate_supervised_main_tree() {
        assert_eq!(
            windows_taskkill_args_for_rollout_stop(42, RolloutTerminationMode::ProcessOnly),
            vec!["/PID", "42", "/F"]
        );
        assert_eq!(
            windows_taskkill_args_for_rollout_stop(42, RolloutTerminationMode::ProcessTree),
            vec!["/PID", "42", "/T", "/F"]
        );
    }

    #[test]
    fn shadow_main_restart_backoff_caps_and_resets_after_stable_runtime() {
        let mut backoff = ShadowMainRestartBackoff::default();
        let start = Instant::now();

        let first = backoff.schedule_after_failure(start);
        assert_eq!(first, SHADOW_MAIN_RESTART_INITIAL_BACKOFF);
        assert!(!backoff.can_restart(start));
        assert!(backoff.can_restart(start + first));

        let mut delay = first;
        let mut now = start;
        for _ in 0..16 {
            now = now + delay;
            delay = backoff.schedule_after_failure(now);
        }
        assert_eq!(delay, SHADOW_MAIN_RESTART_MAX_BACKOFF);

        backoff.reset_after_stable_start(Some(now), now + SHADOW_MAIN_RESTART_STABLE_RESET_AFTER);
        assert_eq!(backoff.consecutive_failures, 0);
        assert!(backoff.next_restart_at.is_none());
    }

    #[test]
    fn stage_rollout_main_activation_accepts_current_shadow_without_self_probe() {
        let root = temp_rollout_test_root("shadow-self-probe");
        std::fs::create_dir_all(root.join("shadow-daemon")).expect("shadow root");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let shadow_paths = DaemonPaths::with_root(root.join("shadow-daemon"));
        let activation = test_main_activation(&rollout_paths);

        let strategy =
            stage_rollout_main_activation(&shadow_paths, &root, &rollout_paths, activation.clone())
                .expect("current shadow can stage activation without recursive status probe");
        assert_eq!(strategy, RolloutActivationStrategy::ShadowSupervised);

        let staged = read_rollout_main_activation(&rollout_paths)
            .expect("read activation")
            .expect("activation staged");
        assert_eq!(staged, activation);
    }

    #[test]
    fn stage_rollout_main_activation_treats_locked_shadow_as_existing_shadow() {
        let root = temp_rollout_test_root("locked-shadow-existing");
        std::fs::create_dir_all(root.join("daemon")).expect("main root");
        std::fs::create_dir_all(root.join("shadow-daemon")).expect("shadow root");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let main_paths = DaemonPaths::with_root(root.join("daemon"));
        let shadow_paths = DaemonPaths::with_root(root.join("shadow-daemon"));
        let _shadow_lock = DaemonLock::acquire(
            &shadow_paths,
            &crate::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21354,
            },
            None,
        )
        .expect("shadow lock");
        let staged_main = rollout_staged_main_binary(&rollout_paths);
        let staged_shadow = rollout_staged_shadow_binary(&rollout_paths);
        std::fs::create_dir_all(staged_main.parent().expect("staged parent"))
            .expect("staged parent dir");
        std::fs::write(&staged_main, b"candidate-main").expect("staged main");
        std::fs::write(&staged_shadow, b"candidate-shadow").expect("staged shadow");
        let mut activation = test_main_activation(&rollout_paths);
        activation.staged_main_binary = Some(staged_main.clone());
        activation.staged_shadow_binary = Some(staged_shadow.clone());

        let strategy =
            stage_rollout_main_activation(&main_paths, &root, &rollout_paths, activation)
                .expect("locked shadow should be treated as existing shadow");

        assert_eq!(strategy, RolloutActivationStrategy::ShadowSupervised);
        assert_eq!(
            std::fs::read(&staged_shadow).expect("staged shadow preserved"),
            b"candidate-shadow"
        );
        assert!(!rollout_paths.shadow_binary.exists());
    }

    #[test]
    fn stage_rollout_main_activation_without_shadow_requires_staged_binary() {
        let root = temp_rollout_test_root("main-shadow-bootstrap-requires-staged");
        std::fs::create_dir_all(root.join("daemon")).expect("main root");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let main_paths = DaemonPaths::with_root(root.join("daemon"));
        let activation = test_main_activation(&rollout_paths);

        let error = stage_rollout_main_activation(&main_paths, &root, &rollout_paths, activation)
            .expect_err("main daemon needs staged binaries for main-only rollout");
        assert!(error.to_string().contains("main-only rollout requires"));
        assert!(!rollout_main_activation_path(&rollout_paths).exists());
    }

    #[test]
    fn stage_rollout_main_activation_without_shadow_bootstraps_stable_shadow() {
        let root = temp_rollout_test_root("main-shadow-bootstrap");
        std::fs::create_dir_all(root.join("daemon")).expect("main root");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let main_paths = DaemonPaths::with_root(root.join("daemon"));
        let staged_main = rollout_staged_main_binary(&rollout_paths);
        let staged_shadow = rollout_staged_shadow_binary(&rollout_paths);
        std::fs::create_dir_all(staged_main.parent().expect("staged parent"))
            .expect("staged parent dir");
        write_test_executable(&staged_main);
        write_test_executable(&staged_shadow);
        let mut activation = test_main_activation(&rollout_paths);
        activation.staged_main_binary = Some(staged_main);
        activation.staged_shadow_binary = Some(staged_shadow);

        let strategy =
            stage_rollout_main_activation(&main_paths, &root, &rollout_paths, activation)
                .expect("main daemon can bootstrap stable shadow without pre-existing shadow");
        assert_eq!(strategy, RolloutActivationStrategy::MainOnlyShadowBootstrap);

        let status = read_rollout_status(&rollout_paths)
            .expect("read status")
            .expect("status recorded");
        assert_eq!(status.status, "main-staged");
        assert!(rollout_main_activation_path(&rollout_paths).exists());
        assert!(rollout_paths.shadow_binary.exists());
        let mut staged_names: Vec<_> = std::fs::read_dir(rollout_paths.root.join("staged"))
            .expect("staged dir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect();
        staged_names.sort();
        assert_eq!(
            staged_names,
            vec![format!("sinomo{}", std::env::consts::EXE_SUFFIX)]
        );
    }

    #[test]
    fn shadow_targeted_cluster_rollout_uses_main_defaults_not_shadow_status() {
        let root = temp_rollout_test_root("shadow-target-main-defaults");
        std::fs::create_dir_all(root.join("shadow-daemon")).expect("shadow root");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let shadow_paths = DaemonPaths::with_root(root.join("shadow-daemon"));
        let main_binary = rollout_paths.main_binary.clone();
        let shadow_binary = rollout_paths.shadow_binary.clone();

        let plan = prepare_cluster_rollout_main_activation(
            &shadow_paths,
            &root,
            "test-source".to_string(),
            main_binary.clone(),
            shadow_binary,
            Some(rollout_staged_main_binary(&rollout_paths)),
            Some(rollout_staged_shadow_binary(&rollout_paths)),
            "0.0.0.0".to_string(),
            21353,
            21354,
            "2026-05-08T00:00:00Z".to_string(),
            "cluster-remote-build",
        )
        .expect("prepare activation");

        assert_eq!(plan.activation.main_binary, main_binary);
        assert_eq!(
            plan.activation.previous_main_binary.as_deref(),
            Some(main_binary.as_path())
        );
        assert_eq!(plan.activation.main_host, "0.0.0.0");
        assert_eq!(plan.activation.main_port, 21353);
        assert_eq!(plan.activation.shadow_port, 21354);
        assert_eq!(plan.current_main_pid, None);
    }

    #[test]
    fn local_rollout_activation_plan_uses_fallback_when_main_is_not_running() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-local-rollout");
        let rollout_paths = SelfRolloutPaths::new(&root);
        let artifacts = LocalRolloutArtifacts {
            built_binary: root.join("build").join("sinomo"),
            staged_main_binary: root.join("rollout").join("main-staged"),
            staged_shadow_binary: root.join("rollout").join("shadow-staged"),
            native_dylib_modules: Vec::new(),
        };

        let plan = prepare_local_rollout_main_activation(
            &root,
            &rollout_paths,
            &artifacts,
            "127.0.0.1",
            21353,
            21354,
            "2026-05-08T00:00:00Z".to_string(),
            Duration::from_millis(1),
        );

        assert!(plan.previous_main_status.is_none());
        assert_eq!(
            plan.activation.source,
            rollout_paths.source_repo.display().to_string()
        );
        assert_eq!(
            plan.activation.main_binary,
            rollout_paths.plan().install_main_binary
        );
        assert_eq!(
            plan.activation.shadow_binary,
            rollout_paths.plan().install_shadow_binary
        );
        assert_eq!(
            plan.activation.staged_main_binary.as_deref(),
            Some(artifacts.staged_main_binary.as_path())
        );
        assert_eq!(
            plan.activation.staged_shadow_binary.as_deref(),
            Some(artifacts.staged_shadow_binary.as_path())
        );
        assert_eq!(
            plan.activation.previous_main_binary.as_deref(),
            Some(rollout_paths.plan().install_main_binary.as_path())
        );
        assert_eq!(plan.activation.main_host, "127.0.0.1");
        assert_eq!(plan.activation.main_port, 21353);
        assert_eq!(plan.activation.shadow_port, 21354);
        assert_eq!(plan.activation.mode, "local-apply");
    }

    #[test]
    fn promote_shadow_main_rollout_writes_local_shadow_finalize_state() {
        let root = temp_rollout_test_root("promote-local-finalize");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let staged_shadow = rollout_staged_shadow_binary(&rollout_paths);
        write_test_executable(&staged_shadow);
        write_test_executable(&rollout_paths.shadow_binary);
        let previous_shadow_binary = root.join("previous-shadow");
        write_test_executable(&previous_shadow_binary);

        let activation = test_main_activation(&rollout_paths);
        write_main_activation(&rollout_paths, &activation).expect("main activation");
        let monitor = ShadowMainRolloutMonitor {
            activation: activation.clone(),
            main_options: crate::DaemonRunOptions {
                host: activation.main_host.clone(),
                port: activation.main_port,
            },
            previous_main_binary: activation.main_binary.clone(),
            previous_shadow_binary,
            diagnostics: CandidateMainDiagnostics {
                stdout_log: root.join("stdout.log"),
                stderr_log: root.join("stderr.log"),
                diagnostics_path: root.join("diagnostics.json"),
            },
            deadline: Instant::now() + Duration::from_secs(1),
        };

        let promoted = promote_shadow_main_rollout(&rollout_paths, &monitor, "127.0.0.1", 21354)
            .expect("promote");

        assert_eq!(promoted.port, activation.main_port);
        assert!(!rollout_main_activation_path(&rollout_paths).exists());
        assert!(rollout_shadow_finalize_path(&rollout_paths).exists());
        let pending_shadow = read_pending_shadow_rollout(&rollout_paths)
            .expect("read pending shadow")
            .expect("pending shadow");
        assert_eq!(
            pending_shadow.previous_main_binary.as_deref(),
            Some(activation.main_binary.as_path())
        );
        assert_eq!(
            pending_shadow.staged_main_binary.as_deref(),
            activation.staged_main_binary.as_deref()
        );
        assert!(!shadow_reexec_request_path(&rollout_paths).exists());
        assert!(rollout_paths.shadow_binary.exists());
    }

    #[test]
    fn shadow_start_completion_clears_pending_finalize_when_running_installed_shadow() {
        let root = temp_rollout_test_root("shadow-start-completes");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let current_exe = env::current_exe().expect("current exe");
        let mut activation = test_main_activation(&rollout_paths);
        activation.shadow_binary = current_exe.clone();
        let previous_shadow_binary = root.join("previous-shadow");
        write_test_executable(&previous_shadow_binary);
        let monitor = ShadowMainRolloutMonitor {
            activation,
            main_options: crate::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21353,
            },
            previous_main_binary: rollout_paths.main_binary.clone(),
            previous_shadow_binary,
            diagnostics: CandidateMainDiagnostics {
                stdout_log: root.join("stdout.log"),
                stderr_log: root.join("stderr.log"),
                diagnostics_path: root.join("diagnostics.json"),
            },
            deadline: Instant::now() + Duration::from_secs(1),
        };
        stage_pending_shadow_rollout(&rollout_paths, &monitor, "127.0.0.1", 21354)
            .expect("pending shadow");

        complete_pending_shadow_rollout_after_start(
            &DaemonPaths::with_root(root.join("shadow-daemon")),
            &rollout_paths,
        )
        .expect("complete pending shadow");

        assert!(!rollout_shadow_finalize_path(&rollout_paths).exists());
        let status = read_rollout_status(&rollout_paths)
            .expect("read status")
            .expect("status");
        assert_eq!(status.status, "accepted");
        assert_eq!(status.phase.as_deref(), Some("shadow-finalized"));
    }

    #[test]
    fn shadow_start_completion_rehydrates_pending_main_when_staged_main_remains() {
        let root = temp_rollout_test_root("shadow-start-rehydrates-main");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let current_exe = env::current_exe().expect("current exe");
        let staged_main = rollout_staged_main_binary(&rollout_paths);
        write_test_executable(&staged_main);
        let mut activation = test_main_activation(&rollout_paths);
        activation.shadow_binary = current_exe.clone();
        let previous_shadow_binary = root.join("previous-shadow");
        write_test_executable(&previous_shadow_binary);
        let monitor = ShadowMainRolloutMonitor {
            activation,
            main_options: crate::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21353,
            },
            previous_main_binary: rollout_paths.main_binary.clone(),
            previous_shadow_binary,
            diagnostics: CandidateMainDiagnostics {
                stdout_log: root.join("stdout.log"),
                stderr_log: root.join("stderr.log"),
                diagnostics_path: root.join("diagnostics.json"),
            },
            deadline: Instant::now() + Duration::from_secs(1),
        };
        stage_pending_shadow_rollout(&rollout_paths, &monitor, "127.0.0.1", 21354)
            .expect("pending shadow");
        remove_main_activation(&rollout_paths);

        complete_pending_shadow_rollout_after_start(
            &DaemonPaths::with_root(root.join("shadow-daemon")),
            &rollout_paths,
        )
        .expect("complete pending shadow");

        assert!(rollout_shadow_finalize_path(&rollout_paths).exists());
        let activation = read_rollout_main_activation(&rollout_paths)
            .expect("read main activation")
            .expect("main activation");
        assert_eq!(
            activation.staged_main_binary.as_deref(),
            Some(staged_main.as_path())
        );
        let status = read_rollout_status(&rollout_paths)
            .expect("read status")
            .expect("status");
        assert_eq!(status.status, "in-progress");
        assert_eq!(status.phase.as_deref(), Some("main-promotion-pending"));
    }

    #[test]
    fn pending_rollout_probe_requires_candidate_pid() {
        let mut status = test_daemon_status(42);
        assert!(pending_rollout_probe_matches_candidate(
            &DaemonProbeStatus::Running(status.clone()),
            Some(42)
        ));
        status.pid = 7;
        assert!(!pending_rollout_probe_matches_candidate(
            &DaemonProbeStatus::Running(status),
            Some(42)
        ));
        assert!(!pending_rollout_probe_matches_candidate(
            &DaemonProbeStatus::NotRunning,
            Some(42)
        ));
        assert!(!pending_rollout_probe_matches_candidate(
            &DaemonProbeStatus::Running(test_daemon_status(42)),
            None
        ));
    }

    #[test]
    fn daemon_control_port_released_requires_bindable_port() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test port");
        let port = listener.local_addr().expect("local addr").port();

        assert!(!daemon_control_port_released("127.0.0.1", port));
        drop(listener);
        assert!(daemon_control_port_released("127.0.0.1", port));
    }

    #[test]
    fn supervised_restart_request_is_pid_scoped_and_consumed() {
        let root = temp_rollout_test_root("supervised-main-restart-request");
        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
        let rollout_paths = SelfRolloutPaths::new(&root);
        let pid = std::process::id();

        request_supervised_main_restart(&daemon_paths).expect("write restart request");
        assert!(
            !take_supervised_main_restart_request(&rollout_paths, pid.saturating_add(1))
                .expect("consume stale restart request")
        );
        assert!(!supervised_main_restart_request_path(&rollout_paths).exists());

        request_supervised_main_restart(&daemon_paths).expect("rewrite restart request");
        assert!(
            take_supervised_main_restart_request(&rollout_paths, pid)
                .expect("consume matching restart request")
        );
        assert!(!supervised_main_restart_request_path(&rollout_paths).exists());
    }

    #[test]
    fn deferred_shadow_rollout_completion_ignores_missing_pending_state() {
        let root = temp_rollout_test_root("deferred-shadow-no-pending");
        let paths = DaemonPaths::with_root(root.join("daemon"));

        assert_eq!(
            handle_deferred_shadow_rollout_completion(&paths).expect("deferred helper"),
            "no pending shadow rollout\n"
        );
    }

    fn temp_rollout_test_root(label: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-daemon-rollout-{label}"))
    }

    fn write_test_executable(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("test executable parent");
        }
        fs::write(path, b"sinomo rollout test executable").expect("test executable");
    }

    fn test_daemon_status(pid: u32) -> DaemonStatus {
        DaemonStatus {
            pid,
            host: "127.0.0.1".to_string(),
            port: 21353,
            address: "127.0.0.1:21353".to_string(),
            version: "0.1.0".to_string(),
            commit: Some("test".to_string()),
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            hostname: None,
            executable: None,
            rollout: None,
            started_at: "2026-05-08T00:00:00Z".to_string(),
            data_dir: PathBuf::from("/tmp/sinomo-test"),
            lock_file: PathBuf::from("/tmp/sinomo-test/daemon.lock"),
            cluster: None,
            idle_stats: None,
        }
    }

    fn test_main_activation(rollout_paths: &SelfRolloutPaths) -> RolloutMainActivation {
        RolloutMainActivation {
            source: "test-source".to_string(),
            main_binary: rollout_paths.main_binary.clone(),
            shadow_binary: rollout_paths.shadow_binary.clone(),
            staged_main_binary: Some(rollout_staged_main_binary(rollout_paths)),
            staged_shadow_binary: Some(rollout_staged_shadow_binary(rollout_paths)),
            previous_main_binary: Some(rollout_paths.main_binary.clone()),
            main_host: "127.0.0.1".to_string(),
            main_port: 21353,
            shadow_port: 21354,
            requested_at: "2026-05-08T00:00:00Z".to_string(),
            mode: "cluster-remote-build".to_string(),
        }
    }
}
