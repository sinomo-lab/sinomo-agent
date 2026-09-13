use crate::RolloutHostError as DaemonError;
use fs2::FileExt;
use sinomo_daemon::CLUSTER_HEALTH_PROBE_TIMEOUT;
#[cfg(windows)]
use sinomo_daemon::prepare_noninteractive_command;
#[cfg(not(unix))]
use sinomo_daemon::spawn_shadow_rollout_completion_helper_process;
use sinomo_daemon::{
    CandidateMainDiagnostics, DaemonClusterStatus, DaemonLock, DaemonPaths, DaemonProbeStatus,
    DaemonStatus, daemon_connect_address, prepare_background_command, probe_daemon_status,
    read_daemon_endpoint_advertisement_config, read_daemon_status, send_daemon_shutdown_request,
    spawn_candidate_main_daemon_process, spawn_main_daemon_process,
    spawn_shadow_rollout_recovery_helper_process,
};
#[cfg(test)]
use sinomo_rollout::read_rollout_status;
use sinomo_rollout::{
    CLUSTER_ROLLOUT_BUILD_HEARTBEAT_INTERVAL, CLUSTER_ROLLOUT_PROGRESS_TOTAL,
    ClusterRolloutBinaryManifest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleRequest, ClusterRolloutNativeDylibModuleResponse,
    ClusterRolloutProgressUpdate, LocalRolloutArtifacts, LocalRolloutNativeDylibModuleArtifact,
    MainExitObservation, ManagedSourceDeployOptions, PendingShadowCandidateExitAction,
    PendingShadowRolloutContext, ROLLOUT_DIAGNOSTIC_TAIL_BYTES, RolloutActivationStrategy,
    RolloutFailureDiagnostics, RolloutMainActivation, SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV,
    SHADOW_MAIN_PORT_ENV, SHADOW_START_DELAY_MS_ENV, SelfRolloutPaths, ShadowMainRestartBackoff,
    SupervisedMainExitDecision, acquire_rollout_activation_guard, activate_staged_rollout_binary,
    classify_main_exit, classify_supervised_main_exit, clear_rollout_status,
    cluster_rollout_progress_status, deploy_managed_source_bundle_with_options,
    ensure_rollout_layout, managed_source_status, pending_rollout_probe_matches_candidate,
    pending_shadow_candidate_exit_action, prune_rollout_binary_recovery_backups,
    read_pending_shadow_rollout, read_rollout_main_activation, read_tail_lossy,
    record_activation_failure, record_main_activation_staged,
    record_main_rollout_failure_with_diagnostics, record_rollout_main_exit,
    record_shadow_rollout_finalized, recover_unresolved_native_dylib_loader_rollback_intents,
    remove_main_activation, remove_native_dylib_loader_state_rollback, remove_shadow_finalize,
    restore_native_dylib_loader_state_for_activation, restore_native_dylib_loader_state_rollback,
    restore_rollout_binary_from_backup, rollout_artifact_suffix,
    rollout_bind_covers_manual_tcp_endpoint, rollout_build_artifacts, rollout_main_activation_path,
    rollout_previous_binary_path, rollout_remote_repo_path, rollout_shadow_finalize_path,
    rollout_staged_main_binary, rollout_staged_shadow_binary, rollout_timestamp_now,
    select_rollout_main_host, select_rollout_shadow_host, shadow_main_should_start,
    source_heads_match, stage_native_dylib_loader_state_rollback, stage_pending_shadow_rollout,
    stage_rollout_file, stage_rollout_file_pair, validate_rollout_upload_component,
    verify_rollout_uploaded_artifact, verify_rollout_uploaded_module, write_json,
    write_main_activation, write_rollout_status,
};
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};
const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
const MAIN_ACTIVATION_SHUTDOWN_GRACE: Duration = Duration::from_millis(750);
const STABLE_BINARY_COPY_TIMEOUT: Duration = Duration::from_secs(30);
const STABLE_BINARY_COPY_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const SUPERVISED_MAIN_RESTART_REQUEST_FILE: &str = "supervised-main-restart.request";
const SHADOW_RUNTIME_TASK_JOIN_TIMEOUT: Duration = Duration::from_millis(750);
const SHADOW_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
const SHADOW_RUNTIME_TASK_DRAIN_GRACE: Duration = Duration::from_millis(100);
const SHADOW_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const PENDING_MAIN_ACTIVATION_FORCE_TERMINATE_AFTER: Duration = Duration::from_secs(10);
const PENDING_MAIN_ACTIVATION_TERMINATE_WAIT: Duration = Duration::from_secs(5);
const ROLLOUT_DAEMON_TERMINATE_WAIT: Duration = Duration::from_secs(5);

fn main_exit_observation_from_status(status: ExitStatus) -> MainExitObservation {
    #[cfg(unix)]
    {
        if let Some(signal) = status.signal() {
            return MainExitObservation::signal(signal);
        }
    }
    MainExitObservation::exit_code(status.code().unwrap_or(1))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RolloutTerminationMode {
    ProcessOnly,
    // Reserved for explicit orphan-descendant cleanup. A live daemon handoff
    // must not select this on Windows because the supervisor or recovery
    // helper can itself be a descendant of the target process.
    ProcessTree,
}

fn main_rollout_termination_mode() -> RolloutTerminationMode {
    main_rollout_termination_mode_for_platform(cfg!(windows))
}

fn main_rollout_termination_mode_for_platform(
    windows_uses_ancestry_scoped_tree_kill: bool,
) -> RolloutTerminationMode {
    if windows_uses_ancestry_scoped_tree_kill {
        // CREATE_NEW_PROCESS_GROUP and DETACHED_PROCESS do not break Windows
        // process ancestry. After one successful rollout the replacement
        // shadow is a child of main, and the deferred recovery helper is also
        // spawned by main. taskkill /T would therefore kill the process that
        // must finish the handoff.
        RolloutTerminationMode::ProcessOnly
    } else {
        // Preserve the historical non-Windows selection. The Unix termination
        // implementation is PID-scoped and ignores this mode.
        RolloutTerminationMode::ProcessTree
    }
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
        sinomo_daemon::OperationalSeverity::Error
    } else {
        sinomo_daemon::OperationalSeverity::Info
    };
    sinomo_daemon::append_operational_event_best_effort(
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
            if let Err(error) = sinomo_daemon::refresh_daemon_node_status_in_kv(&daemon_paths) {
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
                format!("build-service compiler running for {elapsed}s"),
            );
        }
    });
    (stop_tx, handle)
}

#[derive(Clone, Debug)]
pub struct ShadowMainRolloutMonitor {
    pub activation: RolloutMainActivation,
    pub main_options: sinomo_daemon::DaemonRunOptions,
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
        main_options: sinomo_daemon::DaemonRunOptions,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShadowMainExit {
    PendingActivation,
    ImmediateRestartRequested,
    RestartRequested,
    Suppressed,
}

pub enum PendingShadowMainExit {
    Promoted {
        main_binary: PathBuf,
        main_options: sinomo_daemon::DaemonRunOptions,
        main_probe_status: DaemonProbeStatus,
    },
    RetryCandidate {
        child: Child,
        monitor: ShadowMainRolloutMonitor,
    },
    Recovered {
        child: Option<Child>,
        main_binary: PathBuf,
        main_options: sinomo_daemon::DaemonRunOptions,
    },
}

fn supervised_main_restart_request_path(rollout_paths: &SelfRolloutPaths) -> PathBuf {
    rollout_paths
        .root
        .join(SUPERVISED_MAIN_RESTART_REQUEST_FILE)
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
    host_override: Option<&str>,
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
    let (main_host, shadow_host) = local_rollout_listener_hosts(
        previous_main_status
            .as_ref()
            .map(|status| status.host.as_str()),
        fallback_host,
        host_override,
    );
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
            native_dylib_loader_rollback_generation: None,
            main_host,
            main_port,
            shadow_host,
            shadow_port,
            requested_at,
            mode: "local-apply".to_string(),
        },
        previous_main_status,
    }
}

fn local_rollout_listener_hosts(
    current_main_host: Option<&str>,
    fallback_host: &str,
    host_override: Option<&str>,
) -> (String, Option<String>) {
    let main_host = host_override
        .or(current_main_host)
        .unwrap_or(fallback_host)
        .to_string();
    let shadow_host = host_override.map(str::to_string);
    (main_host, shadow_host)
}

fn install_local_rollout_native_dylib_modules(
    data_dir: &Path,
    artifacts: &[LocalRolloutNativeDylibModuleArtifact],
) -> Result<Vec<sinomo_module::NativeDylibModuleInstallResult>, DaemonError> {
    sinomo_rollout::install_local_rollout_native_dylib_modules(data_dir, artifacts)
        .map_err(Into::into)
}

/// Owns an unresolved native-loader intent until its rollout activation marker
/// is durable. Any early return in the pre-activation phase restores the loader
/// state and resolves the intent before rollout admission is released.
struct NativeLoaderPreActivationRollbackGuard {
    data_dir: PathBuf,
    rollout_paths: SelfRolloutPaths,
    activation: RolloutMainActivation,
    armed: bool,
}

impl NativeLoaderPreActivationRollbackGuard {
    fn new(
        data_dir: &Path,
        rollout_paths: &SelfRolloutPaths,
        activation: &RolloutMainActivation,
    ) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            rollout_paths: rollout_paths.clone(),
            activation: activation.clone(),
            armed: true,
        }
    }

    fn rollback(&mut self) -> Result<bool, sinomo_rollout::RolloutError> {
        if !self.armed {
            return Ok(false);
        }
        let restored = restore_native_dylib_loader_state_for_activation(
            &self.data_dir,
            &self.rollout_paths,
            &self.activation,
        )?;
        self.armed = false;
        Ok(restored)
    }

    fn rollback_with_detail(&mut self, detail: String) -> String {
        match self.rollback() {
            Ok(_) => detail,
            Err(error) => {
                format!("{detail}; restoring previous native loader state failed: {error}")
            }
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for NativeLoaderPreActivationRollbackGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Err(error) = self.rollback() {
            tracing::error!(
                error = %error,
                generation = ?self.activation.native_dylib_loader_rollback_generation,
                "failed to restore native loader state after pre-activation rollout failure"
            );
        }
    }
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
    shadow_options: &sinomo_daemon::DaemonRunOptions,
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
    if shadow_options.host != finalize.shadow_host || shadow_options.port != finalize.shadow_port {
        return Err(DaemonError::Network {
            address: daemon_connect_address(&finalize.shadow_host, finalize.shadow_port),
            detail: format!(
                "replacement shadow bound {}:{}, expected rollout listener {}:{}; refusing to finalize the rollout",
                shadow_options.host,
                shadow_options.port,
                finalize.shadow_host,
                finalize.shadow_port,
            ),
        });
    }
    if rehydrate_main_activation_if_staged_main_is_pending(paths, rollout_paths, &finalize)? {
        return Ok(());
    }
    if let Some(staged_shadow) = finalize
        .staged_shadow_binary
        .as_deref()
        .filter(|path| path.exists())
    {
        return Err(sinomo_rollout::RolloutError::InvalidTransition {
            detail: format!(
                "shadow finalization refused because staged shadow binary {} is still pending; keeping the rollout pending for deferred finalization",
                staged_shadow.display()
            ),
        }
        .into());
    }
    record_shadow_rollout_finalized(
        rollout_paths,
        &finalize,
        "accepted",
        "shadow rollout finalized",
    )?;
    prune_finalized_rollout_recovery_backups(&finalize.main_binary, &finalize.shadow_binary);
    if let Some(generation) = finalize.native_dylib_loader_rollback_generation.as_deref() {
        remove_native_dylib_loader_state_rollback(rollout_paths, generation)?;
    }
    remove_main_activation(rollout_paths);
    remove_shadow_finalize(rollout_paths);
    sinomo_daemon::append_operational_event_best_effort(
        paths,
        sinomo_daemon::OperationalSeverity::Info,
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

/// Complete an authorized staged shadow installation when Docker restarts PID 1.
pub fn resume_pending_pid1_shadow_boot(
    paths: &SelfRolloutPaths,
    options: &sinomo_daemon::DaemonRunOptions,
) -> Result<(), DaemonError> {
    #[cfg(unix)]
    if std::process::id() == 1 {
        let current = env::current_exe().map_err(|error| DaemonError::Io {
            path: "current shadow executable".into(),
            detail: error.to_string(),
        })?;
        if let Some(finalize) = install_pending_shadow_at_boot(paths, &current)? {
            let mut shadow_options = options.clone();
            shadow_options.host = finalize.shadow_host;
            shadow_options.port = finalize.shadow_port;
            let main_options = sinomo_daemon::DaemonRunOptions {
                host: finalize.main_host,
                port: finalize.main_port,
                ..Default::default()
            };
            // Re-exec preserves PID 1 and adopts the original operation's
            // stable binary paths and listener settings.
            return Err(reexec_shadow_supervisor(
                &finalize.shadow_binary,
                &shadow_options,
                &finalize.main_binary,
                &main_options,
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = (paths, options);
    Ok(())
}

#[cfg(any(unix, test))]
fn install_pending_shadow_at_boot(
    paths: &SelfRolloutPaths,
    current: &Path,
) -> Result<Option<sinomo_rollout::PendingShadowRollout>, DaemonError> {
    let Some(mut finalize) = read_pending_shadow_rollout(paths)? else {
        return Ok(None);
    };
    let Some(staged) = finalize
        .staged_shadow_binary
        .as_ref()
        .filter(|path| path.exists())
    else {
        return Ok(None);
    };
    if staged != &sinomo_rollout::rollout_staged_shadow_binary(paths)
        || finalize.shadow_binary != paths.shadow_binary
    {
        return Err(DaemonError::Parse {
            detail: "pending shadow boot has unexpected installation paths".into(),
        });
    }
    if !sinomo_rollout::same_path_if_exists(current, staged) {
        // Older entrypoints copied the candidate into place but left its stage.
        // Identical installed bytes are required before consuming that stage.
        if !sinomo_rollout::same_path_if_exists(current, &finalize.shadow_binary)
            || sinomo_rollout::binary_should_update(staged, &finalize.shadow_binary)?
        {
            return Ok(None);
        }
        fs::remove_file(staged).map_err(|error| DaemonError::Io {
            path: staged.display().to_string(),
            detail: error.to_string(),
        })?;
    } else if let Some(previous) =
        sinomo_rollout::activate_staged_rollout_binary(staged, &finalize.shadow_binary)?
    {
        finalize.previous_shadow_binary = previous;
        sinomo_rollout::write_object(
            &sinomo_rollout::rollout_shadow_finalize_path(paths),
            &finalize,
        )?;
    }
    Ok(Some(finalize))
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
        native_dylib_loader_rollback_generation: finalize
            .native_dylib_loader_rollback_generation
            .clone(),
        main_host: finalize.main_host.clone(),
        main_port: finalize.main_port,
        shadow_host: Some(finalize.shadow_host.clone()),
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
    shadow_options: &sinomo_daemon::DaemonRunOptions,
    main_binary: &Path,
    main_options: &sinomo_daemon::DaemonRunOptions,
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

/// Repairs an orphaned native-loader mutation before startup can read loader
/// state into either the network-module registry or the service module registry.
pub(crate) fn recover_native_loader_intents_before_registry_load(
    paths: &DaemonPaths,
) -> Result<Vec<String>, DaemonError> {
    let data_dir = sinomo_daemon::daemon_data_dir_path(paths)?;
    let rollout_paths = SelfRolloutPaths::new(&data_dir);
    let recovered =
        recover_unresolved_native_dylib_loader_rollback_intents(&data_dir, &rollout_paths)?;
    if !recovered.is_empty() {
        tracing::warn!(
            generations = %recovered.join(","),
            "recovered orphaned native loader pre-mutation intent before registry load"
        );
    }
    Ok(recovered)
}

pub fn run_shadow_daemon_supervision_loop(
    options: sinomo_daemon::DaemonRunOptions,
    component_catalog: sinomo_module::ComponentCatalog,
    shadow_paths: DaemonPaths,
    main_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    main_options: sinomo_daemon::DaemonRunOptions,
    main_binary: PathBuf,
    cluster: Option<DaemonClusterStatus>,
    finalize_timeout: Duration,
    probe_timeout: Duration,
) -> Result<(), DaemonError> {
    let listener = sinomo_daemon::prepare_daemon_control_listener(options)?;
    run_shadow_daemon_supervision_loop_with_prepared_listener(
        listener,
        component_catalog,
        shadow_paths,
        main_paths,
        rollout_paths,
        main_options,
        main_binary,
        cluster,
        finalize_timeout,
        probe_timeout,
    )
}

pub fn run_shadow_daemon_supervision_loop_with_prepared_listener(
    listener: sinomo_daemon::PreparedDaemonControlListener,
    component_catalog: sinomo_module::ComponentCatalog,
    shadow_paths: DaemonPaths,
    main_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    mut main_options: sinomo_daemon::DaemonRunOptions,
    mut main_binary: PathBuf,
    cluster: Option<DaemonClusterStatus>,
    finalize_timeout: Duration,
    probe_timeout: Duration,
) -> Result<(), DaemonError> {
    let (options, listener, fallback_from_port) = listener.into_parts();
    if let Some(requested_port) = fallback_from_port {
        eprintln!(
            "warning: sinomo shadow daemon port {requested_port} is unavailable; listening on automatic fallback port {}",
            options.port
        );
    }
    sinomo_daemon::deny_shadow_daemon_local_raft_access(&shadow_paths)?;
    let _lock = DaemonLock::acquire(&shadow_paths, &options, cluster.clone())?;
    recover_native_loader_intents_before_registry_load(&shadow_paths)?;
    let bind_address = format!("{}:{}", options.host, options.port);
    let daemon_runtime = sinomo_daemon::build_daemon_async_runtime("shadow-daemon")?;
    let daemon_runtime_handle = daemon_runtime.handle().clone();
    let listener = daemon_runtime.block_on(
        sinomo_core::network::AsyncDaemonControlListener::from_std(listener, &bind_address),
    )?;
    let mut shadow_runtime_tasks = Vec::new();
    let (control_shutdown_tx, control_shutdown_rx) = tokio::sync::watch::channel(false);
    let control_shutdown_signal = control_shutdown_tx.clone();
    tracing::info!("sinomo shadow daemon listening on {bind_address}");
    if let Some(requested_port) = fallback_from_port {
        tracing::warn!(
            requested_port,
            fallback_port = options.port,
            "sinomo shadow daemon preferred port unavailable; using automatic fallback"
        );
        sinomo_daemon::append_operational_event_best_effort(
            &shadow_paths,
            sinomo_daemon::OperationalSeverity::Warn,
            "shadow-daemon",
            "daemon.lifecycle.port-fallback",
            format!(
                "sinomo shadow daemon preferred port {requested_port} was unavailable; listening on fallback port {}",
                options.port
            ),
            None,
            serde_json::json!({
                "label": "shadow-daemon",
                "requested_port": requested_port,
                "fallback_port": options.port,
                "address": bind_address,
                "pid": std::process::id(),
            }),
        );
    }
    sinomo_daemon::append_operational_event_best_effort(
        &shadow_paths,
        sinomo_daemon::OperationalSeverity::Info,
        "shadow-daemon",
        "daemon.lifecycle.started",
        format!("sinomo shadow daemon listening on {bind_address}"),
        None,
        serde_json::json!({
            "label": "shadow-daemon",
            "address": bind_address,
            "fallback_from_port": fallback_from_port,
            "pid": std::process::id(),
            "cluster_node_id": cluster.as_ref().map(|status| status.node_id.to_string()),
            "main_binary": main_binary.display().to_string(),
        }),
    );
    if let Some(task) = sinomo_daemon::start_daemon_network_module_context_task(
        &shadow_paths,
        &options,
        cluster.as_ref(),
        daemon_runtime_handle.clone(),
        control_shutdown_rx.clone(),
        "shadow daemon",
    ) {
        shadow_runtime_tasks.push(("network module context", task));
    }
    // The shadow is an alternate supervision endpoint, not a cluster member.
    // The main daemon probes it and publishes its status; starting a publisher
    // here would let the shadow originate quorum writes and leader campaigns.
    if let Err(error) =
        complete_pending_shadow_rollout_after_start(&shadow_paths, &rollout_paths, &options)
    {
        tracing::warn!("sinomo shadow daemon rollout finalization publish failed: {error}");
        sinomo_daemon::append_operational_log_best_effort(
            &shadow_paths,
            sinomo_daemon::OperationalSeverity::Warn,
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
    let control_component_catalog = component_catalog;
    let shadow_control_accept_task = daemon_runtime_handle.spawn(async move {
        let result = match sinomo_daemon::service_adapter::DaemonServiceRegistryHost::new_on_runtime_with_catalog(
            control_paths.clone(),
            control_service_runtime.clone(),
            control_component_catalog,
        ).await
        {
            Ok(service_host) => {
                sinomo_daemon::run_daemon_control_accept_loop(
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
                .map_err(Into::into)
            }
            Err(error) => Err(error.into()),
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
                        sinomo_daemon::append_operational_event_best_effort(
                            &shadow_paths,
                            sinomo_daemon::OperationalSeverity::Warn,
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
                        sinomo_daemon::append_operational_event_best_effort(
                            &shadow_paths,
                            sinomo_daemon::OperationalSeverity::Warn,
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
                running_daemon_probe_pid(&main_probe_status),
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
                sinomo_daemon::append_operational_event_best_effort(
                    &shadow_paths,
                    sinomo_daemon::OperationalSeverity::Info,
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
                sinomo_daemon::append_operational_event_best_effort(
                    &shadow_paths,
                    sinomo_daemon::OperationalSeverity::Error,
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

        if shadow_main_should_start(main_restart_suppressed, pending_main_activation)
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
                    sinomo_daemon::append_operational_event_best_effort(
                        &shadow_paths,
                        sinomo_daemon::OperationalSeverity::Info,
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
                    sinomo_daemon::append_operational_event_best_effort(
                        &shadow_paths,
                        sinomo_daemon::OperationalSeverity::Warn,
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
                    sinomo_daemon::append_operational_event_best_effort(
                        &shadow_paths,
                        sinomo_daemon::OperationalSeverity::Info,
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
                    sinomo_daemon::append_operational_event_best_effort(
                        &shadow_paths,
                        sinomo_daemon::OperationalSeverity::Warn,
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

        if let Some(child) = supervised_main.as_mut()
            && let Some(status) = child.try_wait().map_err(|source| DaemonError::Io {
                path: main_binary.display().to_string(),
                detail: source.to_string(),
            })?
        {
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
                        sinomo_daemon::append_operational_event_best_effort(
                            &shadow_paths,
                            sinomo_daemon::OperationalSeverity::Info,
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
                        sinomo_daemon::append_operational_event_best_effort(
                            &shadow_paths,
                            sinomo_daemon::OperationalSeverity::Warn,
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
                        sinomo_daemon::append_operational_event_best_effort(
                            &shadow_paths,
                            sinomo_daemon::OperationalSeverity::Error,
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
                    sinomo_daemon::append_operational_event_best_effort(
                        &shadow_paths,
                        sinomo_daemon::OperationalSeverity::Info,
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
                ShadowMainExit::ImmediateRestartRequested => {
                    supervised_main = None;
                    supervised_main_started_at = None;
                    main_restart_suppressed = false;
                    main_restart_backoff.reset();
                    main_probe_status = DaemonProbeStatus::NotRunning;
                    last_main_probe = Instant::now();
                    sinomo_daemon::append_operational_event_best_effort(
                        &shadow_paths,
                        sinomo_daemon::OperationalSeverity::Info,
                        "shadow-daemon",
                        "daemon.supervision.restart_immediate",
                        "supervised main exited for an explicit clean restart",
                        None,
                        serde_json::json!({
                            "main_binary": main_binary.display().to_string(),
                        }),
                    );
                    // Re-enter the start decision immediately. This
                    // identity/runtime handoff is neither a crash nor a
                    // start failure, so it must not inherit their backoff
                    // or the supervision loop's idle sleep.
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
                    sinomo_daemon::append_operational_event_best_effort(
                        &shadow_paths,
                        sinomo_daemon::OperationalSeverity::Warn,
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
                    sinomo_daemon::append_operational_event_best_effort(
                        &shadow_paths,
                        sinomo_daemon::OperationalSeverity::Error,
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

    sinomo_daemon::append_operational_event_best_effort(
        &shadow_paths,
        sinomo_daemon::OperationalSeverity::Info,
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

pub fn shadow_main_options_from_env() -> Result<sinomo_daemon::DaemonRunOptions, DaemonError> {
    let mut options = sinomo_daemon::DaemonRunOptions::default();
    if let Ok(host) = env::var(SHADOW_MAIN_HOST_ENV) {
        options.host = host;
    }
    if let Ok(port) = env::var(SHADOW_MAIN_PORT_ENV) {
        options.port = port.parse::<u16>().map_err(|source| DaemonError::Parse {
            detail: format!("invalid {SHADOW_MAIN_PORT_ENV} `{port}`: {source}"),
        })?;
        options.port_fallback = false;
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

fn pending_main_activation_stop_target(status: &DaemonProbeStatus) -> bool {
    matches!(
        status,
        DaemonProbeStatus::Running(_) | DaemonProbeStatus::Locked { status: Some(_) }
    )
}

fn running_daemon_probe_pid(status: &DaemonProbeStatus) -> Option<u32> {
    match status {
        DaemonProbeStatus::Running(status) => Some(status.pid),
        DaemonProbeStatus::Locked { .. } | DaemonProbeStatus::NotRunning => None,
    }
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
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            let _ = FileExt::unlock(&file);
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
    terminate_process_for_rollout_stop(status.pid, main_rollout_termination_mode())?;
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
    let mut command = Command::new("taskkill");
    prepare_noninteractive_command(&mut command);
    let output = command
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

pub fn handle_daemon_rollout_native_dylib_module_request(
    paths: &DaemonPaths,
    request: ClusterRolloutNativeDylibModuleRequest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    preflight_cluster_rollout_main_bind(
        paths,
        data_dir,
        &request.host,
        request.host_override.as_deref(),
        request.port,
        request.shadow_port,
    )?;
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
    let activation_guard = acquire_rollout_activation_guard(&rollout_paths)?;
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

    let mut plan = prepare_cluster_rollout_main_activation(
        paths,
        data_dir,
        source.clone(),
        main_binary.clone(),
        shadow_binary.clone(),
        Some(staged_main_binary),
        Some(staged_shadow_binary),
        request.host.clone(),
        request.host_override.clone(),
        request.port,
        request.shadow_port,
        requested_at.clone(),
        "cluster-module-native-dylib",
    )?;
    stage_native_dylib_loader_state_rollback(data_dir, &rollout_paths, &mut plan.activation)?;
    let mut native_loader_rollback =
        NativeLoaderPreActivationRollbackGuard::new(data_dir, &rollout_paths, &plan.activation);

    let install_result = match install_cluster_rollout_native_dylib_module(data_dir, &request) {
        Ok(result) => result,
        Err(error) => {
            let detail = native_loader_rollback
                .rollback_with_detail(format!("install native module artifact failed: {error}"));
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
    let activation_strategy =
        stage_rollout_main_activation(paths, data_dir, &rollout_paths, plan.activation)?;
    native_loader_rollback.disarm();
    drop(activation_guard);
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

pub fn activate_daemon_rollout_binary_manifest(
    paths: &DaemonPaths,
    manifest: ClusterRolloutBinaryManifest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    validate_rollout_upload_component("upload id", &manifest.upload_id)?;
    validate_rollout_upload_component("artifact id", &manifest.binary.artifact_id)?;
    let data_dir = daemon_data_dir(paths)?;
    preflight_cluster_rollout_main_bind(
        paths,
        data_dir,
        &manifest.host,
        manifest.host_override.as_deref(),
        manifest.port,
        manifest.shadow_port,
    )?;
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
    let activation_guard = acquire_rollout_activation_guard(&rollout_paths)?;
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
    let mut plan = prepare_cluster_rollout_main_activation(
        paths,
        data_dir,
        source.clone(),
        main_binary.clone(),
        shadow_binary.clone(),
        Some(staged_main_binary),
        Some(staged_shadow_binary),
        manifest.host.clone(),
        manifest.host_override.clone(),
        manifest.port,
        manifest.shadow_port,
        requested_at.clone(),
        "cluster-binary-push",
    )?;
    let mut native_loader_rollback = None;
    if !uploaded_modules.is_empty() {
        stage_native_dylib_loader_state_rollback(data_dir, &rollout_paths, &mut plan.activation)?;
        native_loader_rollback = Some(NativeLoaderPreActivationRollbackGuard::new(
            data_dir,
            &rollout_paths,
            &plan.activation,
        ));
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
            let detail = native_loader_rollback
                .as_mut()
                .expect("native loader rollback guard staged with module artifacts")
                .rollback_with_detail(format!("install native module artifacts failed: {error}"));
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
    let activation_strategy =
        stage_rollout_main_activation(paths, data_dir, &rollout_paths, plan.activation)?;
    if let Some(rollback) = native_loader_rollback.as_mut() {
        rollback.disarm();
    }
    drop(activation_guard);
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

pub fn stage_rollout_main_activation(
    current_daemon_paths: &DaemonPaths,
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    activation: RolloutMainActivation,
) -> Result<RolloutActivationStrategy, DaemonError> {
    if rollout_main_activation_path(rollout_paths).exists()
        || rollout_shadow_finalize_path(rollout_paths).exists()
    {
        restore_native_dylib_loader_state_for_activation(data_dir, rollout_paths, &activation)?;
        return Err(sinomo_rollout::RolloutError::InvalidTransition {
            detail: "another rollout activation is already pending on this node".to_string(),
        }
        .into());
    }
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let current_daemon_is_shadow = paths_equivalent(&current_daemon_paths.root, &shadow_paths.root);
    let shadow_running = current_daemon_is_shadow
        || daemon_probe_indicates_process_present(probe_daemon_status(
            &shadow_paths,
            CLUSTER_HEALTH_PROBE_TIMEOUT,
        ));
    if !shadow_running {
        if let Err(error) = stage_main_only_rollout_without_existing_shadow(
            data_dir,
            rollout_paths,
            activation.clone(),
        ) {
            restore_native_dylib_loader_state_for_activation(data_dir, rollout_paths, &activation)?;
            return Err(error);
        }
        return Ok(RolloutActivationStrategy::MainOnlyShadowBootstrap);
    }
    if let Err(error) = write_main_activation(rollout_paths, &activation) {
        remove_main_activation(rollout_paths);
        restore_native_dylib_loader_state_for_activation(data_dir, rollout_paths, &activation)?;
        return Err(error.into());
    }
    if let Err(error) = record_main_activation_staged(rollout_paths, &activation) {
        // The activation record is the shadow supervisor's execution trigger.
        // Never leave it behind when the accompanying durable status could not
        // be recorded, or a restart may activate a rollout already reported as
        // rejected while its loader state has been restored.
        remove_main_activation(rollout_paths);
        restore_native_dylib_loader_state_for_activation(data_dir, rollout_paths, &activation)?;
        return Err(error.into());
    }
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
    if let Err(error) = write_main_activation(rollout_paths, &activation) {
        remove_main_activation(rollout_paths);
        rollback_main_only_shadow_binary(
            previous_shadow_binary.as_deref(),
            &activation.shadow_binary,
        )?;
        return Err(error.into());
    }
    if let Err(error) = record_main_activation_staged(rollout_paths, &activation) {
        remove_main_activation(rollout_paths);
        rollback_main_only_shadow_binary(
            previous_shadow_binary.as_deref(),
            &activation.shadow_binary,
        )?;
        return Err(error.into());
    }
    // Bring up the bootstrap shadow with bounded, health-verified retries so a
    // transient bind/lock race during handoff does not leave the node without a
    // supervisor. In tests the spawn/probe is skipped.
    #[cfg(not(test))]
    let desired_shadow_host = activation
        .shadow_host
        .as_deref()
        .unwrap_or(&activation.main_host);
    #[cfg(not(test))]
    let shadow_start = ensure_shadow_supervisor_running(
        data_dir,
        &activation.shadow_binary,
        &activation.main_binary,
        &activation.main_host,
        activation.main_port,
        desired_shadow_host,
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
        restore_native_dylib_loader_state_for_activation(data_dir, rollout_paths, &activation)?;
        let _ = rollback_main_only_shadow_binary(
            previous_shadow_binary.as_deref(),
            &activation.shadow_binary,
        );
        remove_main_activation(rollout_paths);
        return Err(error);
    }
    Ok(())
}

fn rollback_main_only_shadow_binary(
    previous_shadow_binary: Option<&Path>,
    shadow_binary: &Path,
) -> Result<(), DaemonError> {
    if let Some(previous_shadow_binary) = previous_shadow_binary {
        restore_rollout_binary_from_backup_with_retry(
            previous_shadow_binary,
            shadow_binary,
            STABLE_BINARY_COPY_TIMEOUT,
            STABLE_BINARY_COPY_RETRY_INTERVAL,
        )?;
        return Ok(());
    }

    // The activation created this destination from scratch. Removing it is the
    // inverse of the swap; leaving it behind would let a later bootstrap launch
    // a candidate whose activation metadata was never durably accepted.
    match fs::remove_file(shadow_binary) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(DaemonError::Io {
                path: shadow_binary.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    #[cfg(unix)]
    if let Some(parent) = shadow_binary.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| DaemonError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
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
    let Some(status) = (match probe_daemon_status(&main_paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { status } => status,
        DaemonProbeStatus::NotRunning => None,
    }) else {
        // Do not fall back to dialing the activation address. Without status
        // from this data directory we have no evidence that a listener at that
        // address is the main process owned by this shadow supervisor. The
        // supervision loop will retry once an owned status becomes available.
        return Ok(());
    };
    if status.host != activation.main_host || status.port != activation.main_port {
        return Ok(());
    }
    let _ = send_daemon_shutdown_request(&status.address, DAEMON_PROBE_TIMEOUT);
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

fn cluster_rollout_current_main_status(
    current_daemon_paths: &DaemonPaths,
    data_dir: &Path,
) -> Result<Option<DaemonStatus>, DaemonError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let current_daemon_is_shadow = paths_equivalent(&current_daemon_paths.root, &shadow_paths.root);
    if current_daemon_is_shadow {
        let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
        return Ok(
            match probe_daemon_status(&main_paths, CLUSTER_HEALTH_PROBE_TIMEOUT) {
                DaemonProbeStatus::Running(status) => Some(status),
                DaemonProbeStatus::Locked { status } => status,
                DaemonProbeStatus::NotRunning => None,
            },
        );
    }
    Ok(Some(read_daemon_status(current_daemon_paths)?))
}

fn cluster_rollout_current_shadow_status(data_dir: &Path) -> Option<DaemonStatus> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    match probe_daemon_status(&shadow_paths, CLUSTER_HEALTH_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { status } => status,
        DaemonProbeStatus::NotRunning => None,
    }
}

fn preflight_cluster_rollout_main_bind(
    current_daemon_paths: &DaemonPaths,
    data_dir: &Path,
    requested_host: &str,
    requested_host_override: Option<&str>,
    requested_port: u16,
    requested_shadow_port: u16,
) -> Result<(), DaemonError> {
    let main_status = cluster_rollout_current_main_status(current_daemon_paths, data_dir)?;
    let shadow_status = cluster_rollout_current_shadow_status(data_dir);
    let supervised_main_host = env::var(SHADOW_MAIN_HOST_ENV).ok();
    let candidate_host = select_rollout_main_host(
        main_status.as_ref().map(|status| status.host.as_str()),
        supervised_main_host.as_deref(),
        requested_host,
        requested_host_override,
    );
    let candidate_port = main_status
        .as_ref()
        .map(|status| status.port)
        .or_else(|| {
            env::var(SHADOW_MAIN_PORT_ENV)
                .ok()
                .and_then(|value| value.parse::<u16>().ok())
        })
        .unwrap_or(requested_port);
    let candidate_shadow_host = select_rollout_shadow_host(
        shadow_status.as_ref().map(|status| status.host.as_str()),
        &candidate_host,
        requested_host_override,
    );
    let candidate_shadow_port = shadow_status
        .as_ref()
        .map(|status| status.port)
        .unwrap_or(requested_shadow_port);
    let config = read_daemon_endpoint_advertisement_config(data_dir)?;
    for (role, host, port, endpoints) in [
        (
            "main",
            candidate_host.as_str(),
            candidate_port,
            config.main_endpoints.as_slice(),
        ),
        (
            "shadow",
            candidate_shadow_host.as_str(),
            candidate_shadow_port,
            config.shadow_endpoints.as_slice(),
        ),
    ] {
        for manual in endpoints {
            let Some(endpoint_address) =
                sinomo_api::normalize_manual_daemon_tcp_endpoint_address(&manual.address, port)
            else {
                continue;
            };
            if rollout_bind_covers_manual_tcp_endpoint(host, port, &endpoint_address) {
                continue;
            }
            return Err(DaemonError::Parse {
                detail: format!(
                    "cluster rollout candidate would bind {role} at {}, but this node manually advertises {role} endpoint {}; pass an explicit --host that serves the advertised endpoint",
                    daemon_connect_address(host, port),
                    manual.address
                ),
            });
        }
    }
    Ok(())
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
    requested_host_override: Option<String>,
    requested_port: u16,
    requested_shadow_port: u16,
    requested_at: String,
    mode: &str,
) -> Result<ClusterRolloutMainActivationPlan, DaemonError> {
    let main_status = cluster_rollout_current_main_status(current_daemon_paths, data_dir)?;
    let shadow_status = cluster_rollout_current_shadow_status(data_dir);
    let previous_main_binary = main_status
        .as_ref()
        .and_then(|status| status.executable.clone())
        .unwrap_or_else(|| main_binary.clone());
    let supervised_main_host = env::var(SHADOW_MAIN_HOST_ENV).ok();
    let main_host = select_rollout_main_host(
        main_status.as_ref().map(|status| status.host.as_str()),
        supervised_main_host.as_deref(),
        &requested_host,
        requested_host_override.as_deref(),
    );
    let main_port = main_status
        .as_ref()
        .map(|status| status.port)
        .or_else(|| {
            env::var(SHADOW_MAIN_PORT_ENV)
                .ok()
                .and_then(|value| value.parse::<u16>().ok())
        })
        .unwrap_or(requested_port);
    let shadow_host = select_rollout_shadow_host(
        shadow_status.as_ref().map(|status| status.host.as_str()),
        &main_host,
        requested_host_override.as_deref(),
    );
    let shadow_port = shadow_status
        .as_ref()
        .map(|status| status.port)
        .unwrap_or(requested_shadow_port);
    Ok(ClusterRolloutMainActivationPlan {
        activation: RolloutMainActivation {
            source,
            main_binary,
            shadow_binary,
            staged_main_binary,
            staged_shadow_binary,
            previous_main_binary: Some(previous_main_binary),
            native_dylib_loader_rollback_generation: None,
            main_host,
            main_port,
            shadow_host: Some(shadow_host),
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
        main_rollout_termination_mode(),
        timeout,
        probe_timeout,
    )
}

fn shadow_supervisor_status_matches_rollout(
    status: &DaemonStatus,
    shadow_binary: &Path,
    shadow_host: &str,
    shadow_port: u16,
    expected_pid: Option<u32>,
) -> bool {
    status.host == shadow_host
        && status.port == shadow_port
        && expected_pid.is_none_or(|pid| status.pid == pid)
        && status.executable.as_ref().is_some_and(|executable| {
            sinomo_rollout::same_path_if_exists(executable, shadow_binary)
        })
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
    if shadow_supervisor_status_matches_rollout(
        &running,
        shadow_binary,
        shadow_host,
        shadow_port,
        None,
    ) {
        return Ok(());
    }
    stop_shadow_supervisor_for_rollout(
        &shadow_paths,
        &running.host,
        running.port,
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
    if shadow_supervisor_status_matches_rollout(
        &running,
        shadow_binary,
        shadow_host,
        shadow_port,
        None,
    ) {
        Ok(())
    } else {
        Err(DaemonError::Network {
            address: daemon_connect_address(shadow_host, shadow_port),
            detail: format!(
                "shadow supervisor reported pid={} executable={} bind={}:{}, expected executable={} bind={}:{}",
                running.pid,
                running
                    .executable
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "unknown executable".to_string()),
                running.host,
                running.port,
                shadow_binary.display(),
                shadow_host,
                shadow_port,
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
            && let Some(backup) = activate_staged_rollout_binary_with_retry(
                staged_shadow,
                &finalize.shadow_binary,
                STABLE_BINARY_COPY_TIMEOUT,
                STABLE_BINARY_COPY_RETRY_INTERVAL,
            )?
        {
            previous_shadow_binary = backup;
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
            if let Some(generation) = finalize.native_dylib_loader_rollback_generation.as_deref() {
                restore_native_dylib_loader_state_rollback(data_dir, &rollout_paths, generation)?;
            }
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
    if status == "accepted" {
        prune_finalized_rollout_recovery_backups(&finalize.main_binary, &finalize.shadow_binary);
        if let Some(generation) = finalize.native_dylib_loader_rollback_generation.as_deref() {
            remove_native_dylib_loader_state_rollback(&rollout_paths, generation)?;
        }
    }
    remove_shadow_finalize(&rollout_paths);
    Ok(format!("{status}: {detail}\n"))
}

fn prune_finalized_rollout_recovery_backups(main_binary: &Path, shadow_binary: &Path) {
    let mut destinations = BTreeSet::new();
    destinations.insert(main_binary.to_path_buf());
    destinations.insert(shadow_binary.to_path_buf());
    for destination in destinations {
        match prune_rollout_binary_recovery_backups(&destination) {
            Ok(removed) if removed > 0 => tracing::info!(
                destination = %destination.display(),
                removed,
                "pruned finalized rollout recovery binaries"
            ),
            Ok(_) => {}
            Err(error) => tracing::warn!(
                destination = %destination.display(),
                "could not prune finalized rollout recovery binaries: {error}"
            ),
        }
    }
}

pub fn handle_daemon_clear_rollout_status(paths: &DaemonPaths) -> Result<String, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let cleared = clear_rollout_status(&rollout_paths)?;
    if let Err(error) = sinomo_daemon::refresh_daemon_node_status_in_kv(paths) {
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

fn shadow_rollout_health_attempt_timeout(now: Instant, overall_deadline: Instant) -> Duration {
    overall_deadline.saturating_duration_since(now)
}

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
        let remaining = overall_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(DaemonError::Network {
                address: daemon_connect_address(shadow_host, shadow_port),
                detail: format!(
                    "shadow supervisor did not converge to executable={} bind={}:{} before the rollout recovery deadline",
                    shadow_binary.display(),
                    shadow_host,
                    shadow_port,
                ),
            });
        }
        match probe_daemon_status(&shadow_paths, Duration::from_secs(2).min(remaining)) {
            DaemonProbeStatus::Running(status)
                if shadow_supervisor_status_matches_rollout(
                    &status,
                    shadow_binary,
                    shadow_host,
                    shadow_port,
                    None,
                ) =>
            {
                return Ok(());
            }
            DaemonProbeStatus::Running(status)
            | DaemonProbeStatus::Locked {
                status: Some(status),
            } if !shadow_supervisor_status_matches_rollout(
                &status,
                shadow_binary,
                shadow_host,
                shadow_port,
                None,
            ) =>
            {
                // A service manager can race the rollout handoff by restarting
                // the previous stable-path supervisor with its old listener.
                // It is live, but it is not the requested replacement and must
                // not satisfy readiness or block the correct spawn indefinitely.
                stop_shadow_supervisor_for_rollout(
                    &shadow_paths,
                    &status.host,
                    status.port,
                    remaining.min(Duration::from_secs(15)),
                    DAEMON_PROBE_TIMEOUT.min(remaining),
                )?;
                continue;
            }
            DaemonProbeStatus::Locked { .. } => {
                // A matching supervisor may still be publishing its control
                // endpoint. Wait for a live RPC response rather than accepting
                // durable status alone or racing it with another process.
                thread::sleep(Duration::from_millis(100).min(remaining));
                continue;
            }
            DaemonProbeStatus::NotRunning | DaemonProbeStatus::Running(_) => {}
        }
        // Registration happens before the shadow publishes its control status.
        // Let one live child use the remaining recovery budget; if it exits,
        // `wait_for_shadow_rollout_health` returns early and the loop respawns it.
        let attempt_timeout =
            shadow_rollout_health_attempt_timeout(Instant::now(), overall_deadline);
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
                shadow_binary,
                shadow_host,
                shadow_port,
                attempt_timeout,
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
        if let DaemonProbeStatus::Running(status) = probe_daemon_status(&paths, probe_timeout)
            && previous_pid.is_none_or(|pid| status.pid != pid)
        {
            return Ok(status);
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
) -> Result<sinomo_daemon::DaemonRunOptions, DaemonError> {
    let (desired_shadow_host, desired_shadow_port) = pending
        .activation
        .shadow_host
        .as_deref()
        .map(|host| (host, pending.activation.shadow_port))
        .unwrap_or((shadow_host, shadow_port));
    stage_pending_shadow_rollout(
        rollout_paths,
        pending,
        desired_shadow_host,
        desired_shadow_port,
    )?;
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
    let data_dir = rollout_paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: rollout_paths.root.display().to_string(),
        detail: "rollout path has no data directory parent".to_string(),
    })?;
    restore_native_dylib_loader_state_for_activation(data_dir, rollout_paths, &pending.activation)?;
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
    main_options: &sinomo_daemon::DaemonRunOptions,
    candidate_timeout: Duration,
) -> Result<ShadowMainStart, DaemonError> {
    if let Some(activation) = read_rollout_main_activation(rollout_paths)? {
        let mut previous_main_binary = activation
            .previous_main_binary
            .clone()
            .unwrap_or_else(|| rollout_previous_binary_path(&activation.main_binary));
        let activation_main_options = sinomo_daemon::DaemonRunOptions {
            host: activation.main_host.clone(),
            port: activation.main_port,
            port_fallback: false,
        };
        if let Some(staged_main) = activation
            .staged_main_binary
            .as_ref()
            .filter(|path| path.exists())
            && let Some(backup) = activate_staged_rollout_binary_with_retry(
                staged_main,
                &activation.main_binary,
                STABLE_BINARY_COPY_TIMEOUT,
                STABLE_BINARY_COPY_RETRY_INTERVAL,
            )?
        {
            previous_main_binary = backup;
        }
        match spawn_candidate_main_daemon_process(
            &activation.main_binary,
            &activation_main_options,
            &rollout_paths.root,
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
                let data_dir = rollout_paths.root.parent().ok_or_else(|| DaemonError::Io {
                    path: rollout_paths.root.display().to_string(),
                    detail: "rollout path has no data directory parent".to_string(),
                })?;
                restore_native_dylib_loader_state_for_activation(
                    data_dir,
                    rollout_paths,
                    &activation,
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
    _main_options: &sinomo_daemon::DaemonRunOptions,
    exited_pid: u32,
    status: ExitStatus,
) -> Result<ShadowMainExit, DaemonError> {
    let supervisor_restart_requested =
        take_supervised_main_restart_request(rollout_paths, exited_pid)?;
    if read_rollout_main_activation(rollout_paths)?.is_some() {
        return Ok(ShadowMainExit::PendingActivation);
    }
    let observation = main_exit_observation_from_status(status);
    let kind = classify_main_exit(observation);
    record_rollout_main_exit(rollout_paths, observation, kind)?;
    Ok(classify_shadow_main_exit(
        supervisor_restart_requested,
        kind,
    ))
}

fn classify_shadow_main_exit(
    supervisor_restart_requested: bool,
    kind: sinomo_rollout::MainExitKind,
) -> ShadowMainExit {
    match classify_supervised_main_exit(supervisor_restart_requested, kind) {
        SupervisedMainExitDecision::RestartImmediately => ShadowMainExit::ImmediateRestartRequested,
        SupervisedMainExitDecision::RestartWithBackoff => ShadowMainExit::RestartRequested,
        SupervisedMainExitDecision::SuppressRestart => ShadowMainExit::Suppressed,
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
    let observation = main_exit_observation_from_status(status);
    let probe_status = probe_daemon_status(main_paths, probe_timeout);
    match pending_shadow_candidate_exit_action(
        pending_rollout_probe_matches_candidate(
            running_daemon_probe_pid(&probe_status),
            Some(candidate_pid),
        ),
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
                &rollout_paths.root,
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
    preflight_cluster_rollout_main_bind(
        paths,
        data_dir,
        &request.host,
        request.host_override.as_deref(),
        request.port,
        request.shadow_port,
    )?;
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
        "build-service compiler running",
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
    let built_artifacts = match rollout_build_artifacts(&repo, &rollout_paths.build_log) {
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
    let activation_guard = acquire_rollout_activation_guard(&rollout_paths)?;
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
    let mut plan = prepare_cluster_rollout_main_activation(
        paths,
        data_dir,
        source.clone(),
        main_binary.clone(),
        shadow_binary.clone(),
        Some(staged_main_binary),
        Some(staged_shadow_binary),
        request.host.clone(),
        request.host_override.clone(),
        request.port,
        request.shadow_port,
        requested_at.clone(),
        "cluster-remote-build",
    )?;
    stage_native_dylib_loader_state_rollback(data_dir, &rollout_paths, &mut plan.activation)?;
    let mut native_loader_rollback =
        NativeLoaderPreActivationRollbackGuard::new(data_dir, &rollout_paths, &plan.activation);
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
            let detail = native_loader_rollback
                .rollback_with_detail(format!("install native module artifacts failed: {error}"));
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
    if let Err(error) = sinomo_rollout::reconcile_rollout_native_dylib_platforms(data_dir, &repo) {
        let detail = native_loader_rollback
            .rollback_with_detail(format!("reconcile native module platforms failed: {error}"));
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
            "module platform reconciliation failed",
        );
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
        Some("preparing activation".to_string()),
        &source,
        &requested_at,
        "preparing-activation",
        4,
        "preparing activation",
    )?;
    let activation_strategy =
        stage_rollout_main_activation(paths, data_dir, &rollout_paths, plan.activation)?;
    native_loader_rollback.disarm();
    drop(activation_guard);
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
    shadow_binary: &Path,
    shadow_host: &str,
    shadow_port: u16,
    timeout: Duration,
    mut child: Child,
    diagnostics: ShadowSupervisorDiagnostics,
) -> Result<(), DaemonError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let deadline = Instant::now() + timeout;
    let expected_pid = child.id();
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&shadow_paths, Duration::from_secs(2))
            && shadow_supervisor_status_matches_rollout(
                &status,
                shadow_binary,
                shadow_host,
                shadow_port,
                Some(expected_pid),
            )
        {
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
            "rolled out shadow daemon pid={expected_pid} did not report executable={} bind={}:{}; stdout_log={} stderr_log={}",
            shadow_binary.display(),
            shadow_host,
            shadow_port,
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
    fn pending_boot_fixture(
        root: &Path,
    ) -> (SelfRolloutPaths, sinomo_rollout::PendingShadowRollout) {
        let paths = SelfRolloutPaths::new(root);
        let staged = sinomo_rollout::rollout_staged_shadow_binary(&paths);
        fs::create_dir_all(staged.parent().unwrap()).unwrap();
        fs::create_dir_all(&paths.bin).unwrap();
        fs::write(&staged, b"new shadow").unwrap();
        fs::write(&paths.shadow_binary, b"old shadow").unwrap();
        let pending = sinomo_rollout::PendingShadowRollout {
            source: "committed source".into(),
            main_binary: paths.main_binary.clone(),
            shadow_binary: paths.shadow_binary.clone(),
            staged_main_binary: None,
            staged_shadow_binary: Some(staged),
            previous_main_binary: None,
            native_dylib_loader_rollback_generation: None,
            previous_shadow_binary: paths.shadow_binary.with_extension("previous"),
            main_host: "0.0.0.0".into(),
            main_port: 24502,
            shadow_host: "0.0.0.0".into(),
            shadow_port: 24503,
            requested_at: "request".into(),
            main_healthy_at: "healthy".into(),
            mode: "cluster-binary-push".into(),
        };
        sinomo_rollout::write_object(
            &sinomo_rollout::rollout_shadow_finalize_path(&paths),
            &pending,
        )
        .unwrap();
        (paths, pending)
    }

    #[test]
    fn pid1_shadow_boot_consumes_stage_and_retains_previous_binary() {
        let root = sinomo_testdir::TestDir::new("pid1-shadow-boot");
        let (paths, pending) = pending_boot_fixture(&root);
        let staged = pending.staged_shadow_binary.as_ref().unwrap();
        let installed = install_pending_shadow_at_boot(&paths, staged)
            .unwrap()
            .unwrap();
        assert!(!staged.exists());
        assert_eq!(fs::read(&paths.shadow_binary).unwrap(), b"new shadow");
        assert_eq!(
            fs::read(&installed.previous_shadow_binary).unwrap(),
            b"old shadow"
        );
        assert_eq!(installed.shadow_port, 24503);
        assert_eq!(
            read_pending_shadow_rollout(&paths).unwrap().unwrap(),
            installed
        );
        assert!(
            install_pending_shadow_at_boot(&paths, &paths.shadow_binary)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn pid1_shadow_boot_requires_identical_bytes_for_legacy_copied_stage() {
        let root = sinomo_testdir::TestDir::new("pid1-shadow-copied-stage");
        let (paths, pending) = pending_boot_fixture(&root);
        let staged = pending.staged_shadow_binary.as_ref().unwrap();
        assert!(
            install_pending_shadow_at_boot(&paths, &paths.shadow_binary)
                .unwrap()
                .is_none()
        );
        assert!(staged.exists());
        fs::copy(staged, &paths.shadow_binary).unwrap();
        let recovered = install_pending_shadow_at_boot(&paths, &paths.shadow_binary)
            .unwrap()
            .unwrap();
        assert!(!staged.exists());
        assert_eq!(
            recovered.previous_shadow_binary,
            pending.previous_shadow_binary
        );
    }
    use super::*;

    #[test]
    fn pending_shadow_rollout_uses_fast_main_health_probe() {
        assert!(shadow_main_probe_interval(true) < Duration::from_secs(1));
        assert!(shadow_main_probe_interval(false) >= Duration::from_secs(10));
    }

    #[test]
    fn shadow_rollout_health_attempt_allows_slow_cluster_registration() {
        let now = Instant::now();
        assert_eq!(
            shadow_rollout_health_attempt_timeout(now, now + SHADOW_RECOVERY_DEADLINE),
            SHADOW_RECOVERY_DEADLINE
        );
        assert_eq!(
            shadow_rollout_health_attempt_timeout(now, now),
            Duration::ZERO
        );
    }

    #[test]
    fn shadow_network_module_context_runtime_task_gets_graceful_join_budget() {
        assert_eq!(
            shadow_runtime_task_join_timeout("network module context"),
            SHADOW_NETWORK_MODULE_CONTEXT_TASK_JOIN_TIMEOUT
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
    fn shadow_rollout_shutdown_never_contacts_an_unowned_fallback_listener() {
        let root = temp_rollout_test_root("shadow-shutdown-owned-status");
        let rollout_paths = SelfRolloutPaths::new(&root);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("test listener");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let mut activation = test_main_activation(&rollout_paths);
        activation.main_port = listener.local_addr().expect("listener address").port();

        request_main_shutdown_for_shadow_rollout(&root, &activation)
            .expect("missing owned status is a no-op");

        let error = listener
            .accept()
            .expect_err("unowned fallback listener must not be contacted");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn pending_main_activation_treats_locked_main_as_stop_target() {
        let root = temp_rollout_test_root("activation-locked-main");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        let status = DaemonStatus::new(&sinomo_daemon::DaemonRunOptions::default(), &paths, None);

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
    fn pending_candidate_start_waits_for_main_data_owner_lock_release() {
        let root = temp_rollout_test_root("activation-data-owner-lock");
        let paths = DaemonPaths::with_root(root.join("daemon"));

        assert!(main_data_owner_lock_released(&paths));
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        let dfs_paths = sinomo_dfs::runtime_host::DfsRuntimePaths::new(&data_dir);
        let owner =
            sinomo_dfs::runtime_host::DfsDataOwnerLock::acquire(&dfs_paths).expect("owner lock");
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
    fn windows_main_force_stop_is_separate_from_descendant_cleanup() {
        let main_stop_mode = main_rollout_termination_mode_for_platform(true);
        assert_eq!(main_stop_mode, RolloutTerminationMode::ProcessOnly);
        assert_eq!(
            windows_taskkill_args_for_rollout_stop(42, main_stop_mode),
            vec!["/PID", "42", "/F"]
        );

        // Tree termination remains available as an explicit cleanup operation,
        // but live main/shadow handoff paths do not select it on Windows.
        assert_eq!(
            windows_taskkill_args_for_rollout_stop(42, RolloutTerminationMode::ProcessTree),
            vec!["/PID", "42", "/T", "/F"]
        );
        assert_eq!(
            main_rollout_termination_mode_for_platform(false),
            RolloutTerminationMode::ProcessTree
        );
    }

    #[cfg(windows)]
    const WINDOWS_ROLLOUT_ANCESTRY_FIXTURE_ROLE: &str =
        "SINOMO_WINDOWS_ROLLOUT_ANCESTRY_FIXTURE_ROLE";
    #[cfg(windows)]
    const WINDOWS_ROLLOUT_ANCESTRY_MAIN_PID: &str = "SINOMO_WINDOWS_ROLLOUT_ANCESTRY_MAIN_PID";
    #[cfg(windows)]
    const WINDOWS_ROLLOUT_ANCESTRY_MARKER: &str = "SINOMO_WINDOWS_ROLLOUT_ANCESTRY_MARKER";
    #[cfg(windows)]
    const WINDOWS_ROLLOUT_ANCESTRY_GATE: &str = "SINOMO_WINDOWS_ROLLOUT_ANCESTRY_GATE";
    #[cfg(windows)]
    const WINDOWS_ROLLOUT_ANCESTRY_SUPERVISOR_PID: &str =
        "SINOMO_WINDOWS_ROLLOUT_ANCESTRY_SUPERVISOR_PID";

    #[cfg(windows)]
    fn windows_rollout_ancestry_fixture_test_name() -> String {
        let module = module_path!();
        let module = module
            .strip_prefix(concat!(env!("CARGO_CRATE_NAME"), "::"))
            .unwrap_or(module);
        format!("{module}::windows_exact_main_stop_preserves_descendant_supervisor")
    }

    #[cfg(windows)]
    fn run_windows_rollout_main_ancestry_fixture() -> ! {
        let current_exe = env::current_exe().expect("current test executable");
        let marker = env::var_os(WINDOWS_ROLLOUT_ANCESTRY_MARKER)
            .map(PathBuf::from)
            .expect("survival marker path");
        let gate = env::var_os(WINDOWS_ROLLOUT_ANCESTRY_GATE)
            .map(PathBuf::from)
            .expect("supervisor gate path");
        let supervisor_pid_path = env::var_os(WINDOWS_ROLLOUT_ANCESTRY_SUPERVISOR_PID)
            .map(PathBuf::from)
            .expect("supervisor pid path");
        let test_name = windows_rollout_ancestry_fixture_test_name();
        let mut supervisor_command = Command::new(current_exe);
        prepare_background_command(&mut supervisor_command);
        let supervisor = supervisor_command
            .arg("--exact")
            .arg(test_name)
            .arg("--nocapture")
            .env(WINDOWS_ROLLOUT_ANCESTRY_FIXTURE_ROLE, "supervisor")
            .env(
                WINDOWS_ROLLOUT_ANCESTRY_MAIN_PID,
                std::process::id().to_string(),
            )
            .env(WINDOWS_ROLLOUT_ANCESTRY_MARKER, &marker)
            .env(WINDOWS_ROLLOUT_ANCESTRY_GATE, &gate)
            .env(
                WINDOWS_ROLLOUT_ANCESTRY_SUPERVISOR_PID,
                &supervisor_pid_path,
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn descendant supervisor fixture");
        fs::write(&supervisor_pid_path, supervisor.id().to_string())
            .expect("publish supervisor pid");
        fs::write(&gate, b"ready").expect("release descendant supervisor fixture");
        drop(supervisor);

        loop {
            thread::sleep(Duration::from_millis(100));
        }
    }

    #[cfg(windows)]
    fn run_windows_rollout_supervisor_ancestry_fixture() {
        let main_pid = env::var(WINDOWS_ROLLOUT_ANCESTRY_MAIN_PID)
            .expect("main fixture pid")
            .parse::<u32>()
            .expect("numeric main fixture pid");
        let marker = env::var_os(WINDOWS_ROLLOUT_ANCESTRY_MARKER)
            .map(PathBuf::from)
            .expect("survival marker path");
        let gate = env::var_os(WINDOWS_ROLLOUT_ANCESTRY_GATE)
            .map(PathBuf::from)
            .expect("supervisor gate path");
        let gate_deadline = Instant::now() + Duration::from_secs(10);
        while !gate.exists() && Instant::now() < gate_deadline {
            thread::sleep(Duration::from_millis(25));
        }
        assert!(
            gate.exists(),
            "main fixture did not publish the ancestry gate"
        );

        terminate_process_for_rollout_stop(main_pid, main_rollout_termination_mode())
            .expect("terminate exact main fixture pid");
        fs::write(marker, b"supervisor-survived").expect("write survival marker");
    }

    #[cfg(windows)]
    #[test]
    fn windows_exact_main_stop_preserves_descendant_supervisor() {
        match env::var(WINDOWS_ROLLOUT_ANCESTRY_FIXTURE_ROLE).as_deref() {
            Ok("main") => run_windows_rollout_main_ancestry_fixture(),
            Ok("supervisor") => {
                run_windows_rollout_supervisor_ancestry_fixture();
                return;
            }
            _ => {}
        }

        let root = temp_rollout_test_root("windows-main-stop-ancestry");
        let marker = root.join("supervisor-survived");
        let gate = root.join("supervisor-gate");
        let supervisor_pid_path = root.join("supervisor.pid");
        let current_exe = env::current_exe().expect("current test executable");
        let test_name = windows_rollout_ancestry_fixture_test_name();
        let mut main = Command::new(current_exe)
            .arg("--exact")
            .arg(test_name)
            .arg("--nocapture")
            .env(WINDOWS_ROLLOUT_ANCESTRY_FIXTURE_ROLE, "main")
            .env(WINDOWS_ROLLOUT_ANCESTRY_MARKER, &marker)
            .env(WINDOWS_ROLLOUT_ANCESTRY_GATE, &gate)
            .env(
                WINDOWS_ROLLOUT_ANCESTRY_SUPERVISOR_PID,
                &supervisor_pid_path,
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn main ancestry fixture");
        let main_pid = main.id();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !marker.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(25));
        }
        let supervisor_survived = marker.exists();
        let supervisor_pid = fs::read_to_string(&supervisor_pid_path)
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok());

        let main_stopped = !rollout_process_is_running(main_pid);
        if !main_stopped {
            let _ = main.kill();
        }
        let _ = main.wait();

        let supervisor_exited = supervisor_pid.is_some_and(|pid| {
            let deadline = Instant::now() + Duration::from_secs(5);
            while rollout_process_is_running(pid) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(25));
            }
            !rollout_process_is_running(pid)
        });
        if !supervisor_exited && let Some(pid) = supervisor_pid {
            let _ = terminate_process_for_rollout_stop(pid, RolloutTerminationMode::ProcessOnly);
        }

        assert!(main_stopped, "descendant supervisor did not terminate main");
        assert!(
            supervisor_survived,
            "exact main termination also killed its descendant supervisor"
        );
        assert!(
            supervisor_exited,
            "descendant supervisor did not exit after publishing survival"
        );
    }

    #[test]
    fn stage_rollout_main_activation_accepts_current_shadow_without_self_probe() {
        let root = temp_rollout_test_root("shadow-self-probe");
        std::fs::create_dir_all(root.join("shadow-daemon")).expect("shadow root");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let shadow_paths = DaemonPaths::with_root(root.join("shadow-daemon"));
        let activation = test_main_activation(&rollout_paths);
        // Top-level rollout paths hold the activation guard while this staging
        // primitive writes the durable pending record; it must not try to
        // reacquire the caller-owned lock.
        let _activation_guard =
            acquire_rollout_activation_guard(&rollout_paths).expect("activation guard");

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
    fn activation_status_failure_removes_shadow_execution_trigger() {
        let root = temp_rollout_test_root("activation-status-failure-cleans-trigger");
        std::fs::create_dir_all(root.join("shadow-daemon")).expect("shadow root");
        let mut rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let shadow_paths = DaemonPaths::with_root(root.join("shadow-daemon"));
        rollout_paths.state = rollout_paths.root.join("unwritable-status-directory");
        std::fs::create_dir_all(&rollout_paths.state).expect("status directory");

        let error = stage_rollout_main_activation(
            &shadow_paths,
            &root,
            &rollout_paths,
            test_main_activation(&rollout_paths),
        )
        .expect_err("status metadata write must fail");

        assert!(error.to_string().contains("filesystem error"));
        assert!(
            !rollout_main_activation_path(&rollout_paths).exists(),
            "failed status write must not leave an executable pending activation"
        );
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
            &sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21354,
                port_fallback: false,
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
    fn main_only_activation_status_failure_restores_swapped_shadow_before_returning() {
        let root = temp_rollout_test_root("main-shadow-bootstrap-status-rollback");
        std::fs::create_dir_all(root.join("daemon")).expect("main root");
        let mut rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let main_paths = DaemonPaths::with_root(root.join("daemon"));
        let staged_main = rollout_staged_main_binary(&rollout_paths);
        let staged_shadow = rollout_staged_shadow_binary(&rollout_paths);
        std::fs::create_dir_all(staged_main.parent().expect("staged parent"))
            .expect("staged parent dir");
        write_test_executable(&staged_main);
        std::fs::write(&staged_shadow, b"candidate-shadow").expect("candidate shadow");
        std::fs::create_dir_all(
            rollout_paths
                .shadow_binary
                .parent()
                .expect("installed shadow parent"),
        )
        .expect("installed shadow parent");
        std::fs::write(&rollout_paths.shadow_binary, b"stable-shadow").expect("stable shadow");
        let mut activation = test_main_activation(&rollout_paths);
        activation.staged_main_binary = Some(staged_main);
        activation.staged_shadow_binary = Some(staged_shadow);
        rollout_paths.state = rollout_paths.root.join("unwritable-status-directory");
        std::fs::create_dir_all(&rollout_paths.state).expect("status directory");

        stage_rollout_main_activation(&main_paths, &root, &rollout_paths, activation)
            .expect_err("status metadata write must fail");

        assert_eq!(
            std::fs::read(&rollout_paths.shadow_binary).expect("restored shadow"),
            b"stable-shadow"
        );
        assert!(
            !rollout_main_activation_path(&rollout_paths).exists(),
            "failed main-only activation must remove its pending record"
        );
    }

    #[test]
    fn main_only_activation_status_failure_removes_new_shadow_without_predecessor() {
        let root = temp_rollout_test_root("main-shadow-bootstrap-status-remove-new");
        std::fs::create_dir_all(root.join("daemon")).expect("main root");
        let mut rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let main_paths = DaemonPaths::with_root(root.join("daemon"));
        let staged_main = rollout_staged_main_binary(&rollout_paths);
        let staged_shadow = rollout_staged_shadow_binary(&rollout_paths);
        std::fs::create_dir_all(staged_main.parent().expect("staged parent"))
            .expect("staged parent dir");
        write_test_executable(&staged_main);
        std::fs::write(&staged_shadow, b"candidate-shadow").expect("candidate shadow");
        assert!(!rollout_paths.shadow_binary.exists());
        let mut activation = test_main_activation(&rollout_paths);
        activation.staged_main_binary = Some(staged_main);
        activation.staged_shadow_binary = Some(staged_shadow);
        rollout_paths.state = rollout_paths.root.join("unwritable-status-directory");
        std::fs::create_dir_all(&rollout_paths.state).expect("status directory");

        stage_rollout_main_activation(&main_paths, &root, &rollout_paths, activation)
            .expect_err("status metadata write must fail");

        assert!(
            !rollout_paths.shadow_binary.exists(),
            "candidate destination created without a predecessor must be removed"
        );
        assert!(!rollout_main_activation_path(&rollout_paths).exists());
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
            None,
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
        assert_eq!(plan.activation.shadow_host.as_deref(), Some("0.0.0.0"));
        assert_eq!(plan.activation.shadow_port, 21354);
        assert_eq!(plan.current_main_pid, None);
    }

    #[test]
    fn explicit_cluster_rollout_host_overrides_and_persists_running_listener_binds() {
        let root = temp_rollout_test_root("explicit-host-overrides-running-main");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let main_paths = DaemonPaths::with_root(root.join("daemon"));
        main_paths.ensure().expect("main paths");
        let mut status = test_daemon_status(42);
        status.data_dir = root.to_path_buf();
        status.lock_file = main_paths.lock.clone();
        status.executable = Some(rollout_paths.main_binary.clone());
        sinomo_daemon::write_daemon_status(&main_paths, &status).expect("main status");

        let plan = prepare_cluster_rollout_main_activation(
            &main_paths,
            &root,
            "test-source".to_string(),
            rollout_paths.main_binary.clone(),
            rollout_paths.shadow_binary.clone(),
            Some(rollout_staged_main_binary(&rollout_paths)),
            Some(rollout_staged_shadow_binary(&rollout_paths)),
            "127.0.0.1".to_string(),
            Some("0.0.0.0".to_string()),
            21353,
            21354,
            "2026-08-04T00:00:00Z".to_string(),
            "cluster-binary-push",
        )
        .expect("prepare activation");

        assert_eq!(plan.activation.main_host, "0.0.0.0");
        assert_eq!(plan.activation.shadow_host.as_deref(), Some("0.0.0.0"));
        assert_eq!(plan.current_main_pid, Some(42));
    }

    #[test]
    fn shadow_rollout_readiness_requires_replacement_pid_executable_and_bind() {
        let current_exe = env::current_exe().expect("current executable");
        let expected_pid = std::process::id();
        let mut status = test_daemon_status(expected_pid);
        status.host = "0.0.0.0".to_string();
        status.port = 21354;
        // Wildcard listeners intentionally publish a loopback control address
        // for same-node readiness probes.
        status.address = "127.0.0.1:21354".to_string();
        status.executable = Some(current_exe.clone());

        assert!(shadow_supervisor_status_matches_rollout(
            &status,
            &current_exe,
            "0.0.0.0",
            21354,
            Some(expected_pid),
        ));

        let mut wrong_host = status.clone();
        wrong_host.host = "127.0.0.1".to_string();
        assert!(!shadow_supervisor_status_matches_rollout(
            &wrong_host,
            &current_exe,
            "0.0.0.0",
            21354,
            Some(expected_pid),
        ));
        assert!(!shadow_supervisor_status_matches_rollout(
            &status,
            &current_exe,
            "0.0.0.0",
            21354,
            Some(expected_pid.wrapping_add(1)),
        ));
        assert!(!shadow_supervisor_status_matches_rollout(
            &status,
            &current_exe.with_extension("wrong-shadow"),
            "0.0.0.0",
            21354,
            Some(expected_pid),
        ));
    }

    #[test]
    fn cluster_rollout_preflight_rejects_loopback_binds_for_manual_lan_endpoints() {
        let root = temp_rollout_test_root("manual-endpoint-bind-preflight");
        let main_paths = DaemonPaths::with_root(root.join("daemon"));
        main_paths.ensure().expect("main paths");
        let mut status = test_daemon_status(42);
        status.data_dir = root.to_path_buf();
        status.lock_file = main_paths.lock.clone();
        sinomo_daemon::write_daemon_status(&main_paths, &status).expect("main status");
        let config_path = sinomo_daemon::daemon_endpoint_advertisement_config_file(&root);
        fs::create_dir_all(config_path.parent().expect("config parent")).expect("config parent");
        fs::write(
            &config_path,
            serde_json::to_vec(&sinomo_daemon::DaemonEndpointAdvertisementConfig {
                main_endpoints: vec![sinomo_daemon::DaemonManualEndpointConfig {
                    address: "tcp://10.14.1.11:21353".to_string(),
                    scope: None,
                    priority: None,
                }],
                shadow_endpoints: vec![sinomo_daemon::DaemonManualEndpointConfig {
                    address: "tcp://10.14.1.11:21354".to_string(),
                    scope: None,
                    priority: None,
                }],
            })
            .expect("endpoint config"),
        )
        .expect("write endpoint config");

        let error = preflight_cluster_rollout_main_bind(
            &main_paths,
            &root,
            "127.0.0.1",
            None,
            21353,
            21354,
        )
        .expect_err("loopback candidate cannot serve a LAN endpoint");
        assert!(error.to_string().contains("10.14.1.11:21353"));

        preflight_cluster_rollout_main_bind(
            &main_paths,
            &root,
            "127.0.0.1",
            Some("0.0.0.0"),
            21353,
            21354,
        )
        .expect("explicit wildcard serves both LAN endpoints");

        fs::write(
            &config_path,
            serde_json::to_vec(&sinomo_daemon::DaemonEndpointAdvertisementConfig {
                main_endpoints: Vec::new(),
                shadow_endpoints: vec![sinomo_daemon::DaemonManualEndpointConfig {
                    address: "tcp://10.14.1.11:21354".to_string(),
                    scope: None,
                    priority: None,
                }],
            })
            .expect("shadow endpoint config"),
        )
        .expect("write shadow endpoint config");
        let error = preflight_cluster_rollout_main_bind(
            &main_paths,
            &root,
            "127.0.0.1",
            None,
            21353,
            21354,
        )
        .expect_err("loopback shadow cannot serve a LAN endpoint");
        assert!(error.to_string().contains("shadow endpoint"));
        assert!(error.to_string().contains("10.14.1.11:21354"));
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
            None,
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
    fn explicit_local_rollout_host_replaces_main_and_shadow_listener_binds() {
        assert_eq!(
            local_rollout_listener_hosts(Some("127.0.0.1"), "127.0.0.1", Some("0.0.0.0"),),
            ("0.0.0.0".to_string(), Some("0.0.0.0".to_string()))
        );
    }

    #[test]
    fn default_local_rollout_host_preserves_the_running_listener() {
        assert_eq!(
            local_rollout_listener_hosts(Some("10.14.1.11"), "127.0.0.1", None),
            ("10.14.1.11".to_string(), None)
        );
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

        let mut activation = test_main_activation(&rollout_paths);
        activation.shadow_host = Some("0.0.0.0".to_string());
        write_main_activation(&rollout_paths, &activation).expect("main activation");
        let monitor = ShadowMainRolloutMonitor {
            activation: activation.clone(),
            main_options: sinomo_daemon::DaemonRunOptions {
                host: activation.main_host.clone(),
                port: activation.main_port,
                port_fallback: false,
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
        assert_eq!(pending_shadow.shadow_host, "0.0.0.0");
        assert_eq!(pending_shadow.shadow_port, 21354);
        assert!(!shadow_reexec_request_path(&rollout_paths).exists());
        assert!(rollout_paths.shadow_binary.exists());
    }

    #[test]
    fn shadow_start_completion_refuses_wrong_explicit_listener() {
        let root = temp_rollout_test_root("shadow-start-wrong-explicit-listener");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let current_exe = env::current_exe().expect("current executable");
        let mut activation = test_main_activation(&rollout_paths);
        activation.shadow_binary = current_exe;
        let monitor = ShadowMainRolloutMonitor {
            activation,
            main_options: sinomo_daemon::DaemonRunOptions {
                host: "0.0.0.0".to_string(),
                port: 21353,
                port_fallback: false,
            },
            previous_main_binary: rollout_paths.main_binary.clone(),
            previous_shadow_binary: root.join("previous-shadow"),
            diagnostics: CandidateMainDiagnostics {
                stdout_log: root.join("stdout.log"),
                stderr_log: root.join("stderr.log"),
                diagnostics_path: root.join("diagnostics.json"),
            },
            deadline: Instant::now() + Duration::from_secs(1),
        };
        stage_pending_shadow_rollout(&rollout_paths, &monitor, "0.0.0.0", 21354)
            .expect("pending shadow");

        let error = complete_pending_shadow_rollout_after_start(
            &DaemonPaths::with_root(root.join("shadow-daemon")),
            &rollout_paths,
            &sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21354,
                port_fallback: false,
            },
        )
        .expect_err("loopback shadow must not finalize an explicit wildcard rollout");

        assert!(error.to_string().contains("refusing to finalize"));
        assert!(rollout_shadow_finalize_path(&rollout_paths).exists());
        assert!(
            read_rollout_status(&rollout_paths)
                .expect("read rollout status")
                .is_none_or(|status| status.phase.as_deref() != Some("shadow-finalized"))
        );
    }

    #[test]
    fn shadow_start_completion_retains_pending_when_staged_shadow_remains() {
        let root = temp_rollout_test_root("shadow-start-staged-shadow-pending");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let main_name = rollout_paths
            .main_binary
            .file_name()
            .and_then(|name| name.to_str())
            .expect("main binary name");
        let recovery_backups = (1..=(sinomo_rollout::ROLLOUT_RECOVERY_BACKUP_RETENTION + 2))
            .map(|generation| {
                let path = rollout_paths
                    .bin
                    .join(format!("{main_name}.previous.8-{}-0", generation * 100));
                write_test_executable(&path);
                path
            })
            .collect::<Vec<_>>();
        let current_exe = env::current_exe().expect("current exe");
        let staged_shadow = rollout_staged_shadow_binary(&rollout_paths);
        write_test_executable(&staged_shadow);
        let mut activation = test_main_activation(&rollout_paths);
        activation.shadow_binary = current_exe;
        let previous_shadow_binary = root.join("previous-shadow");
        write_test_executable(&previous_shadow_binary);
        let monitor = ShadowMainRolloutMonitor {
            activation: activation.clone(),
            main_options: sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21353,
                port_fallback: false,
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
        record_main_activation_staged(&rollout_paths, &activation).expect("staged status");
        let status_before = read_rollout_status(&rollout_paths)
            .expect("read status")
            .expect("status");

        let error = complete_pending_shadow_rollout_after_start(
            &DaemonPaths::with_root(root.join("shadow-daemon")),
            &rollout_paths,
            &sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21354,
                port_fallback: false,
            },
        )
        .expect_err("old installed shadow must not finalize staged replacement");

        assert!(error.to_string().contains("staged shadow binary"));
        assert!(error.to_string().contains("deferred finalization"));
        assert!(staged_shadow.exists());
        assert!(rollout_shadow_finalize_path(&rollout_paths).exists());
        let pending = read_pending_shadow_rollout(&rollout_paths)
            .expect("read pending shadow")
            .expect("pending shadow");
        assert_eq!(
            pending.staged_shadow_binary.as_deref(),
            Some(staged_shadow.as_path())
        );
        assert_eq!(
            read_rollout_status(&rollout_paths)
                .expect("read status")
                .expect("status"),
            status_before
        );
        for path in recovery_backups {
            assert!(
                path.exists(),
                "deferred finalization must retain recovery backup: {path:?}"
            );
        }
    }

    #[test]
    fn shadow_start_completion_clears_pending_finalize_when_running_installed_shadow() {
        let root = temp_rollout_test_root("shadow-start-completes");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let main_name = rollout_paths
            .main_binary
            .file_name()
            .and_then(|name| name.to_str())
            .expect("main binary name");
        let recovery_backups = (1..=(sinomo_rollout::ROLLOUT_RECOVERY_BACKUP_RETENTION + 2))
            .map(|generation| {
                let path = rollout_paths
                    .bin
                    .join(format!("{main_name}.previous.7-{}-0", generation * 100));
                write_test_executable(&path);
                path
            })
            .collect::<Vec<_>>();
        let current_exe = env::current_exe().expect("current exe");
        let mut activation = test_main_activation(&rollout_paths);
        activation.shadow_binary = current_exe.clone();
        let previous_shadow_binary = root.join("previous-shadow");
        write_test_executable(&previous_shadow_binary);
        let monitor = ShadowMainRolloutMonitor {
            activation,
            main_options: sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21353,
                port_fallback: false,
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
            &sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21354,
                port_fallback: false,
            },
        )
        .expect("complete pending shadow");

        assert!(!rollout_shadow_finalize_path(&rollout_paths).exists());
        let status = read_rollout_status(&rollout_paths)
            .expect("read status")
            .expect("status");
        assert_eq!(status.status, "accepted");
        assert_eq!(status.phase.as_deref(), Some("shadow-finalized"));
        assert!(!recovery_backups[0].exists());
        assert!(!recovery_backups[1].exists());
        for path in &recovery_backups[2..] {
            assert!(
                path.exists(),
                "accepted finalization must retain the newest bounded recovery set: {path:?}"
            );
        }
    }

    #[test]
    fn shadow_start_completion_rehydrates_pending_main_when_staged_main_remains() {
        let root = temp_rollout_test_root("shadow-start-rehydrates-main");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let current_exe = env::current_exe().expect("current exe");
        let staged_main = rollout_staged_main_binary(&rollout_paths);
        write_test_executable(&staged_main);
        let staged_shadow = rollout_staged_shadow_binary(&rollout_paths);
        write_test_executable(&staged_shadow);
        let mut activation = test_main_activation(&rollout_paths);
        activation.shadow_binary = current_exe.clone();
        let previous_shadow_binary = root.join("previous-shadow");
        write_test_executable(&previous_shadow_binary);
        let monitor = ShadowMainRolloutMonitor {
            activation,
            main_options: sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21353,
                port_fallback: false,
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
            &sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21354,
                port_fallback: false,
            },
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
        assert!(staged_shadow.exists());
        let status = read_rollout_status(&rollout_paths)
            .expect("read status")
            .expect("status");
        assert_eq!(status.status, "in-progress");
        assert_eq!(status.phase.as_deref(), Some("main-promotion-pending"));
    }

    #[test]
    fn running_daemon_probe_pid_excludes_locked_and_stopped_daemons() {
        assert_eq!(
            running_daemon_probe_pid(&DaemonProbeStatus::Running(test_daemon_status(42))),
            Some(42)
        );
        assert_eq!(
            running_daemon_probe_pid(&DaemonProbeStatus::Locked {
                status: Some(test_daemon_status(42)),
            }),
            None
        );
        assert_eq!(
            running_daemon_probe_pid(&DaemonProbeStatus::NotRunning),
            None
        );
    }

    #[test]
    fn forced_candidate_failure_restores_previous_native_loader_state() {
        let root = temp_rollout_test_root("candidate-loader-rollback");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let module_id =
            sinomo_module::ModuleId::from_legacy_named("module.network.iroh").expect("module id");
        let previous_state = sinomo_module::NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: "blake3:previous".to_string(),
            replaces_content_hash: None,
            cache_key: "network/iroh".to_string(),
            library_name: "libsinomo_network_iroh_module.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 1,
            interface_version: "sinomo-network-module-v1".to_string(),
            activated_at: "2026-07-12T00:00:00Z".to_string(),
            source: "test:previous".to_string(),
        };
        let previous_cache = sinomo_module::native_module_cache_path(
            &root,
            &previous_state.cache_key,
            &previous_state.content_hash,
            &previous_state.library_name,
        );
        fs::create_dir_all(previous_cache.parent().expect("cache parent")).expect("cache dir");
        fs::write(&previous_cache, b"previous module").expect("previous cache");
        sinomo_module::write_native_dylib_module_loader_state(&root, &previous_state)
            .expect("previous loader state");

        let mut activation = test_main_activation(&rollout_paths);
        let generation =
            stage_native_dylib_loader_state_rollback(&root, &rollout_paths, &mut activation)
                .expect("stage native loader rollback");
        let mut candidate_state = previous_state.clone();
        candidate_state.content_hash = "blake3:candidate".to_string();
        candidate_state.native_abi_version = 2;
        candidate_state.interface_version = "sinomo-network-module-v2".to_string();
        let candidate_cache = sinomo_module::native_module_cache_path(
            &root,
            &candidate_state.cache_key,
            &candidate_state.content_hash,
            &candidate_state.library_name,
        );
        fs::create_dir_all(candidate_cache.parent().expect("candidate parent"))
            .expect("candidate cache dir");
        fs::write(&candidate_cache, b"candidate module").expect("candidate cache");
        sinomo_module::write_native_dylib_module_loader_state(&root, &candidate_state)
            .expect("candidate loader state");

        write_test_executable(&activation.main_binary);
        let previous_main_binary = root.join("bin/sinomo.previous");
        fs::write(&previous_main_binary, b"previous binary").expect("previous binary");
        let diagnostics = CandidateMainDiagnostics {
            stdout_log: root.join("rollout/logs/candidate.stdout.log"),
            stderr_log: root.join("rollout/logs/candidate.stderr.log"),
            diagnostics_path: root.join("rollout/logs/candidate.json"),
        };
        fs::create_dir_all(diagnostics.stdout_log.parent().expect("diagnostics parent"))
            .expect("diagnostics dir");
        fs::write(&diagnostics.stdout_log, b"").expect("stdout log");
        fs::write(&diagnostics.stderr_log, b"forced candidate failure").expect("stderr log");
        let monitor = ShadowMainRolloutMonitor {
            activation,
            main_options: sinomo_daemon::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port: 21353,
                port_fallback: false,
            },
            previous_main_binary,
            previous_shadow_binary: root.join("bin/shadow.previous"),
            diagnostics,
            deadline: Instant::now(),
        };

        recover_failed_shadow_main_rollout(
            &rollout_paths,
            &monitor,
            "forced candidate failure",
            None,
        )
        .expect("recover failed candidate");

        let restored = sinomo_module::read_native_dylib_module_loader_state(&root, &module_id)
            .expect("read restored loader state")
            .expect("restored loader state");
        assert_eq!(restored, previous_state);
        assert_eq!(
            fs::read(&monitor.activation.main_binary).expect("restored binary"),
            b"previous binary"
        );
        assert!(
            sinomo_rollout::rollout_native_dylib_loader_rollback_path(&rollout_paths, &generation,)
                .expect("rollback path")
                .exists(),
            "failed generation backup must remain available"
        );
    }

    #[test]
    fn pre_install_progress_failure_resolves_native_loader_intent() {
        let root = temp_rollout_test_root("pre-install-progress-loader-rollback");
        let mut rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let module_id =
            sinomo_module::ModuleId::from_legacy_named("module.network.progress-before-install")
                .expect("module id");
        let previous = test_native_loader_state(&module_id, "previous", "test:previous");
        install_test_native_loader_state(&root, &previous);
        let mut activation = test_main_activation(&rollout_paths);
        stage_native_dylib_loader_state_rollback(&root, &rollout_paths, &mut activation)
            .expect("pre-mutation intent");

        rollout_paths.state = rollout_paths.root.join("unwritable-progress-status");
        fs::create_dir_all(&rollout_paths.state).expect("status directory");
        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
        fs::create_dir_all(&daemon_paths.root).expect("daemon root");
        let error = {
            let _rollback =
                NativeLoaderPreActivationRollbackGuard::new(&root, &rollout_paths, &activation);
            write_cluster_rollout_progress(
                &daemon_paths,
                &rollout_paths,
                "in-progress",
                "test",
                None,
                "test-source",
                "now",
                "staging-modules",
                3,
                "installing native module artifacts",
            )
        }
        .expect_err("progress write must fail before install");

        assert!(error.to_string().contains("filesystem error"));
        assert_eq!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &module_id)
                .expect("read loader state"),
            Some(previous)
        );
        drop(
            acquire_rollout_activation_guard(&rollout_paths)
                .expect("resolved pre-install intent must not strand admission"),
        );
    }

    #[test]
    fn post_install_progress_failure_restores_native_loader_state() {
        let root = temp_rollout_test_root("post-install-progress-loader-rollback");
        let mut rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let module_id =
            sinomo_module::ModuleId::from_legacy_named("module.network.progress-after-install")
                .expect("module id");
        let previous = test_native_loader_state(&module_id, "previous", "test:previous");
        install_test_native_loader_state(&root, &previous);
        let mut activation = test_main_activation(&rollout_paths);
        stage_native_dylib_loader_state_rollback(&root, &rollout_paths, &mut activation)
            .expect("pre-mutation intent");

        rollout_paths.state = rollout_paths.root.join("unwritable-progress-status");
        fs::create_dir_all(&rollout_paths.state).expect("status directory");
        let daemon_paths = DaemonPaths::with_root(root.join("daemon"));
        fs::create_dir_all(&daemon_paths.root).expect("daemon root");
        let candidate = test_native_loader_state(&module_id, "candidate", "test:candidate");
        let error = {
            let _rollback =
                NativeLoaderPreActivationRollbackGuard::new(&root, &rollout_paths, &activation);
            install_test_native_loader_state(&root, &candidate);
            write_cluster_rollout_progress(
                &daemon_paths,
                &rollout_paths,
                "in-progress",
                "test",
                None,
                "test-source",
                "now",
                "preparing-activation",
                4,
                "preparing activation",
            )
        }
        .expect_err("progress write must fail after install");

        assert!(error.to_string().contains("filesystem error"));
        assert_eq!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &module_id)
                .expect("read restored loader state"),
            Some(previous)
        );
        drop(
            acquire_rollout_activation_guard(&rollout_paths)
                .expect("restored post-install intent must not strand admission"),
        );
    }

    #[test]
    fn daemon_startup_recovers_orphaned_loader_intent_before_registry_observes_state() {
        let root = temp_rollout_test_root("startup-loader-intent-ordering");
        let rollout_paths = SelfRolloutPaths::new(&root);
        ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let module_id = sinomo_module::ModuleId::from_legacy_named("module.network.startup-test")
            .expect("module id");
        let state = |hash: &str, source: &str| sinomo_module::NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: format!("blake3:{hash}"),
            replaces_content_hash: None,
            cache_key: "network/startup-test".to_string(),
            library_name: "libsinomo_network_startup_test.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 2,
            interface_version: "sinomo-network-module-v2".to_string(),
            activated_at: "2026-07-12T00:00:00Z".to_string(),
            source: source.to_string(),
        };
        let install_state = |state: &sinomo_module::NativeDylibModuleLoaderState| {
            let artifact = sinomo_module::native_module_cache_path(
                &root,
                &state.cache_key,
                &state.content_hash,
                &state.library_name,
            );
            fs::create_dir_all(artifact.parent().expect("cache parent")).expect("cache dir");
            fs::write(&artifact, state.content_hash.as_bytes()).expect("cache artifact");
            sinomo_module::write_native_dylib_module_loader_state(&root, state)
                .expect("loader state");
        };

        let previous = state("previous", "test:previous");
        install_state(&previous);
        let mut activation = test_main_activation(&rollout_paths);
        let generation =
            stage_native_dylib_loader_state_rollback(&root, &rollout_paths, &mut activation)
                .expect("pre-mutation intent");
        install_state(&state("candidate", "test:candidate"));

        let recovered = recover_native_loader_intents_before_registry_load(
            &DaemonPaths::with_root(root.join("daemon")),
        )
        .expect("startup recovery before registry load");
        assert_eq!(recovered, vec![generation]);
        let registry_visible_states = sinomo_module::read_native_dylib_module_loader_states(&root)
            .expect("registry loader-state read");
        assert_eq!(registry_visible_states, vec![previous]);
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

        sinomo_daemon::request_supervised_main_restart(&daemon_paths)
            .expect("write restart request");
        assert!(
            !take_supervised_main_restart_request(&rollout_paths, pid.saturating_add(1))
                .expect("consume stale restart request")
        );
        assert!(!supervised_main_restart_request_path(&rollout_paths).exists());

        sinomo_daemon::request_supervised_main_restart(&daemon_paths)
            .expect("rewrite restart request");
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

    fn test_native_loader_state(
        module_id: &sinomo_module::ModuleId,
        hash: &str,
        source: &str,
    ) -> sinomo_module::NativeDylibModuleLoaderState {
        sinomo_module::NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: format!("blake3:{hash}"),
            replaces_content_hash: None,
            cache_key: "network/progress-test".to_string(),
            library_name: "libsinomo_network_progress_test.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 2,
            interface_version: "sinomo-network-module-v2".to_string(),
            activated_at: "2026-07-12T00:00:00Z".to_string(),
            source: source.to_string(),
        }
    }

    fn install_test_native_loader_state(
        root: &Path,
        state: &sinomo_module::NativeDylibModuleLoaderState,
    ) {
        let artifact = sinomo_module::native_module_cache_path(
            root,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        fs::create_dir_all(artifact.parent().expect("cache parent")).expect("cache directory");
        fs::write(&artifact, state.content_hash.as_bytes()).expect("cache artifact");
        sinomo_module::write_native_dylib_module_loader_state(root, state).expect("loader state");
    }

    fn test_daemon_status(pid: u32) -> DaemonStatus {
        DaemonStatus {
            pid,
            host: "127.0.0.1".to_string(),
            port: 21353,
            address: "127.0.0.1:21353".to_string(),
            api_protocol_version: sinomo_api::SINOMO_DAEMON_API_PROTOCOL_VERSION,
            app_launch_id: None,
            version: "0.1.0".to_string(),
            commit: Some("test".to_string()),
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            hostname: None,
            executable: None,
            rollout: None,
            component_status: Default::default(),
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
            native_dylib_loader_rollback_generation: None,
            main_host: "127.0.0.1".to_string(),
            main_port: 21353,
            shadow_host: None,
            shadow_port: 21354,
            requested_at: "2026-05-08T00:00:00Z".to_string(),
            mode: "cluster-remote-build".to_string(),
        }
    }
}
