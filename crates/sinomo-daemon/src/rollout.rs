use crate::{
    CLUSTER_HEALTH_PROBE_TIMEOUT, CandidateMainDiagnostics, DaemonClusterStatus, DaemonError,
    DaemonLock, DaemonPaths, DaemonProbeStatus, DaemonStatus, daemon_connect_address,
    prepare_background_command, probe_daemon_status, read_daemon_status,
    send_daemon_finalize_shadow_rollout_request, send_daemon_shutdown_request,
    spawn_candidate_main_daemon_process, spawn_main_daemon_process,
    start_daemon_iroh_proxy_if_clustered, start_shadow_daemon_status_publisher,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sinomo_core::ClusterNodeRolloutStatus;
use sinomo_network::is_loopback_connect_address;
#[cfg(test)]
use sinomo_rollout::read_rollout_status;
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    LocalRolloutArtifacts, MainExitObservation, PendingShadowRolloutContext,
    ROLLOUT_DIAGNOSTIC_TAIL_BYTES, RolloutFailureDiagnostics, RolloutMainActivation,
    SelfRolloutPaths, classify_main_exit, deploy_managed_source_bundle, ensure_rollout_layout,
    managed_source_status, read_pending_shadow_rollout, read_rollout_main_activation,
    read_tail_lossy, record_activation_failure, record_main_activation_staged,
    record_main_rollout_failure_with_diagnostics, record_rollout_main_exit,
    record_shadow_rollout_finalized, remove_main_activation, remove_shadow_finalize,
    rollout_artifact_suffix, rollout_build_dev_binary, rollout_diagnostics_report,
    rollout_main_activation_path, rollout_remote_repo_path, rollout_shadow_finalize_path,
    rollout_staged_main_binary, rollout_staged_shadow_binary, source_heads_match,
    stage_pending_shadow_rollout, stage_rollout_bytes, stage_rollout_file,
    stage_rollout_file_with_retry, write_json, write_main_activation, write_rollout_status,
};
use std::env;
use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const SHADOW_MAIN_HOST_ENV: &str = "SINOMO_SHADOW_MAIN_HOST";
pub const SHADOW_MAIN_PORT_ENV: &str = "SINOMO_SHADOW_MAIN_PORT";
pub const SHADOW_MAIN_BINARY_ENV: &str = "SINOMO_SHADOW_MAIN_BINARY";
pub const SHADOW_START_DELAY_MS_ENV: &str = "SINOMO_SHADOW_START_DELAY_MS";
const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
const MAIN_ACTIVATION_SHUTDOWN_GRACE: Duration = Duration::from_millis(750);
const STABLE_BINARY_COPY_TIMEOUT: Duration = Duration::from_secs(30);
const STABLE_BINARY_COPY_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const SHADOW_MAIN_RESTART_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const SHADOW_MAIN_RESTART_MAX_BACKOFF: Duration = Duration::from_secs(30);
const SHADOW_MAIN_RESTART_STABLE_RESET_AFTER: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct ShadowMainRolloutMonitor {
    pub activation: RolloutMainActivation,
    pub main_options: crate::DaemonRunOptions,
    pub previous_main_binary: PathBuf,
    pub previous_main_backup: Option<PathBuf>,
    pub previous_shadow_binary: PathBuf,
    pub diagnostics: CandidateMainDiagnostics,
    pub deadline: Instant,
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
    Recovered {
        child: Option<Child>,
        main_binary: PathBuf,
        main_options: crate::DaemonRunOptions,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RolloutActivationStrategy {
    MainOnlyShadowBootstrap,
    ShadowSupervised,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalShadowRolloutRestartPlan {
    pub shadow_paths: DaemonPaths,
    pub shadow_options: crate::DaemonRunOptions,
    pub args: Vec<String>,
    pub extra_env: Vec<(&'static str, String)>,
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

pub fn prepare_local_shadow_rollout_restart(
    data_dir: &Path,
    main_binary: &Path,
    main_host: &str,
    main_port: u16,
    fallback_shadow_host: &str,
    fallback_shadow_port: u16,
    probe_timeout: Duration,
) -> LocalShadowRolloutRestartPlan {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let shadow_options = match probe_daemon_status(&shadow_paths, probe_timeout) {
        DaemonProbeStatus::Running(status) => crate::DaemonRunOptions {
            host: status.host,
            port: status.port,
        },
        DaemonProbeStatus::Locked {
            status: Some(status),
        } => crate::DaemonRunOptions {
            host: status.host,
            port: status.port,
        },
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            crate::DaemonRunOptions {
                host: fallback_shadow_host.to_string(),
                port: fallback_shadow_port,
            }
        }
    };
    let args = vec![
        "daemon".to_string(),
        "shadow".to_string(),
        "run".to_string(),
        "--host".to_string(),
        shadow_options.host.clone(),
        "--port".to_string(),
        shadow_options.port.to_string(),
    ];
    let extra_env = vec![
        (SHADOW_MAIN_BINARY_ENV, main_binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, main_host.to_string()),
        (SHADOW_MAIN_PORT_ENV, main_port.to_string()),
    ];
    LocalShadowRolloutRestartPlan {
        shadow_paths,
        shadow_options,
        args,
        extra_env,
    }
}

fn shadow_reexec_request_path(rollout_paths: &SelfRolloutPaths) -> PathBuf {
    rollout_paths.root.join("shadow-reexec.request")
}

/// Records that the running shadow supervisor should re-exec `target` in place the
/// next time its control loop exits. Rollout self-update uses this so the
/// supervisor adopts a new binary by replacing its own process image — never by
/// stopping and relying on an external restarter, which fails when the supervisor
/// is PID 1 (a container) or has no init supervising it (a bare dev host).
fn request_shadow_reexec(
    rollout_paths: &SelfRolloutPaths,
    target: &Path,
) -> Result<(), DaemonError> {
    let path = shadow_reexec_request_path(rollout_paths);
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    fs::write(&path, target.display().to_string()).map_err(|source| DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
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

/// Re-executes the shadow supervisor in place from `target`, preserving the listen
/// options and pointing it at the (rolled-out) installed main binary. On success
/// this never returns (the process image is replaced); on failure it returns Err.
#[cfg(unix)]
fn reexec_shadow_supervisor(
    target: &Path,
    options: &crate::DaemonRunOptions,
    main_binary: &Path,
) -> DaemonError {
    use std::os::unix::process::CommandExt;
    let error = Command::new(target)
        .args([
            "daemon".to_string(),
            "shadow".to_string(),
            "run".to_string(),
            "--host".to_string(),
            options.host.clone(),
            "--port".to_string(),
            options.port.to_string(),
        ])
        .env(SHADOW_MAIN_BINARY_ENV, main_binary)
        .env(SHADOW_MAIN_HOST_ENV, &options.host)
        .env(SHADOW_START_DELAY_MS_ENV, "0")
        .exec();
    DaemonError::Io {
        path: target.display().to_string(),
        detail: format!("shadow supervisor re-exec failed: {error}"),
    }
}

pub fn run_shadow_daemon_supervision_loop<E, F>(
    options: crate::DaemonRunOptions,
    shadow_paths: DaemonPaths,
    main_paths: DaemonPaths,
    rollout_paths: SelfRolloutPaths,
    mut main_options: crate::DaemonRunOptions,
    mut main_binary: PathBuf,
    cluster: Option<DaemonClusterStatus>,
    finalize_timeout: Duration,
    probe_timeout: Duration,
    handle_connection: F,
) -> Result<(), DaemonError>
where
    E: Display + Send + 'static,
    F: Fn(&mut sinomo_network::DaemonControlStream, &DaemonPaths) -> Result<bool, E>
        + Send
        + Sync
        + 'static,
{
    let _lock = DaemonLock::acquire(&shadow_paths, &options, cluster.clone())?;
    let bind_address = format!("{}:{}", options.host, options.port);
    let listener = sinomo_network::DaemonControlListener::bind(&bind_address)?;
    listener.set_nonblocking(true)?;
    println!("sinomo shadow daemon listening on {bind_address}");
    if let Err(error) = start_daemon_iroh_proxy_if_clustered(
        &shadow_paths,
        &options,
        cluster.as_ref(),
        "shadow daemon",
    ) {
        eprintln!("sinomo shadow daemon iroh proxy disabled: {error}");
    }
    if let Err(error) = start_shadow_daemon_status_publisher(&shadow_paths) {
        eprintln!("sinomo shadow daemon status publisher disabled: {error}");
    }

    let mut supervised_main: Option<Child> = None;
    let mut supervised_main_started_at: Option<Instant> = None;
    let mut main_restart_suppressed = false;
    let mut main_restart_backoff = ShadowMainRestartBackoff::default();
    let mut pending_rollout: Option<ShadowMainRolloutMonitor> = None;
    let shadow_idle_interval = Duration::from_millis(100);
    let mut last_main_probe = Instant::now() - shadow_main_probe_interval(false);
    let mut next_pending_activation_shutdown_nudge = Instant::now();
    let mut main_probe_status = DaemonProbeStatus::NotRunning;
    let handle_connection = Arc::new(handle_connection);
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();

    loop {
        if shutdown_rx.try_recv().is_ok() {
            break;
        }
        let now = Instant::now();
        match listener.accept() {
            Ok(Some(mut stream)) => {
                let client_paths = shadow_paths.clone();
                let client_handler = Arc::clone(&handle_connection);
                let client_shutdown_tx = shutdown_tx.clone();
                thread::Builder::new()
                    .name("sinomo-shadow-client".to_string())
                    .spawn(move || match client_handler(&mut stream, &client_paths) {
                        Ok(true) => {
                            let _ = client_shutdown_tx.send(());
                        }
                        Ok(false) => {}
                        Err(error) => eprintln!("sinomo shadow daemon client error: {error}"),
                    })
                    .map_err(|source| DaemonError::Io {
                        path: "spawn shadow daemon client handler".to_string(),
                        detail: source.to_string(),
                    })?;
            }
            Ok(None) => {}
            Err(source) => {
                return Err(DaemonError::Network {
                    address: bind_address.clone(),
                    detail: source.to_string(),
                });
            }
        }

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
        if pending_rollout.is_none()
            && matches!(main_probe_status, DaemonProbeStatus::Running(_))
            && pending_main_activation
            && pending_main_activation_ready_for_shutdown(&rollout_paths)
            && Instant::now() >= next_pending_activation_shutdown_nudge
        {
            if let DaemonProbeStatus::Running(status) =
                probe_daemon_status(&main_paths, probe_timeout)
            {
                let _ = send_daemon_shutdown_request(&status.address, probe_timeout);
            }
            next_pending_activation_shutdown_nudge = Instant::now() + Duration::from_secs(2);
        }

        if let Some(pending) = pending_rollout.as_ref() {
            if matches!(main_probe_status, DaemonProbeStatus::Running(_)) {
                let promoted_main_options = promote_shadow_main_rollout(
                    &rollout_paths,
                    pending,
                    &options.host,
                    options.port,
                    finalize_timeout,
                )?;
                pending_rollout = None;
                main_options = promoted_main_options;
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
                pending_rollout = None;
            }
        }

        if should_shadow_start_main(main_restart_suppressed, pending_main_activation)
            && supervised_main.is_none()
            && matches!(main_probe_status, DaemonProbeStatus::NotRunning)
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
                }
                ShadowMainStart::StartFailed { detail } => {
                    let delay = main_restart_backoff.schedule_after_failure(Instant::now());
                    eprintln!(
                        "sinomo shadow daemon could not start supervised main: {detail}; retrying in {}ms",
                        delay.as_millis()
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
                }
            }
        }

        if let Some(child) = supervised_main.as_mut() {
            if let Some(status) = child.try_wait().map_err(|source| DaemonError::Io {
                path: main_binary.display().to_string(),
                detail: source.to_string(),
            })? {
                if let Some(pending) = pending_rollout.take() {
                    match handle_pending_shadow_main_process_exit(
                        &rollout_paths,
                        &main_paths,
                        &pending,
                        status,
                        &options.host,
                        options.port,
                        finalize_timeout,
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
                        }
                    }
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }

                match handle_shadow_main_process_exit(
                    &rollout_paths,
                    &main_binary,
                    &main_options,
                    status,
                )? {
                    ShadowMainExit::PendingActivation => {
                        supervised_main = None;
                        supervised_main_started_at = None;
                        main_restart_suppressed = false;
                        main_restart_backoff.clear_schedule();
                        thread::sleep(Duration::from_millis(200));
                        continue;
                    }
                    ShadowMainExit::RestartRequested => {
                        supervised_main = None;
                        supervised_main_started_at = None;
                        let delay = main_restart_backoff.schedule_after_failure(Instant::now());
                        eprintln!(
                            "sinomo shadow daemon supervised main crashed; retrying in {}ms",
                            delay.as_millis()
                        );
                    }
                    ShadowMainExit::Suppressed => {
                        supervised_main = None;
                        supervised_main_started_at = None;
                        main_restart_suppressed = true;
                        main_restart_backoff.reset();
                    }
                }
            }
        }

        thread::sleep(shadow_idle_interval);
    }

    // If a rollout asked this supervisor to adopt a new binary, replace our process
    // image in place rather than exiting and hoping something restarts us. This is
    // what keeps a node supervised across a rollout even with no external init.
    let reexec_target = take_shadow_reexec_request(&rollout_paths);
    let installed_main_binary = rollout_paths.main_binary.clone();
    drop(_lock);
    if let Some(target) = reexec_target {
        let target = if target.exists() {
            target
        } else {
            env::current_exe().unwrap_or(target)
        };
        eprintln!(
            "sinomo shadow daemon re-executing supervisor binary {}",
            target.display()
        );
        #[cfg(unix)]
        {
            return Err(reexec_shadow_supervisor(
                &target,
                &options,
                &installed_main_binary,
            ));
        }
        #[cfg(not(unix))]
        {
            let _ = (&target, &installed_main_binary);
        }
    }

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

pub fn rollout_target_platform_matches_local(status: &DaemonStatus) -> Option<bool> {
    match (&status.target_os, &status.target_arch) {
        (Some(target_os), Some(target_arch)) => {
            Some(target_os == env::consts::OS && target_arch == env::consts::ARCH)
        }
        _ => None,
    }
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

pub fn cluster_rollout_should_use_remote_build(
    address: &str,
    remote_build_requested: bool,
    binary_provided: bool,
    platform_matches_local: Option<bool>,
) -> bool {
    remote_build_requested
        || (!is_loopback_connect_address(address)
            && !binary_provided
            && platform_matches_local != Some(true))
}

pub fn ensure_rollout_target_platform_matches_local(
    address: &str,
    status: &DaemonStatus,
) -> Result<(), DaemonError> {
    let local = is_loopback_connect_address(address);
    match (&status.target_os, &status.target_arch) {
        (Some(target_os), Some(target_arch))
            if target_os == env::consts::OS && target_arch == env::consts::ARCH =>
        {
            Ok(())
        }
        (Some(target_os), Some(target_arch)) => Err(DaemonError::Parse {
            detail: format!(
                "target platform {target_os}/{target_arch} does not match local artifact platform {}/{}",
                env::consts::OS,
                env::consts::ARCH
            ),
        }),
        _ if local => Ok(()),
        _ => Err(DaemonError::Parse {
            detail:
                "target daemon does not report platform metadata; update it locally before binary rollout"
                    .to_string(),
        }),
    }
}

pub fn handle_daemon_rollout_binary_request(
    paths: &DaemonPaths,
    request: ClusterRolloutBinaryRequest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    if (!request.target_os.is_empty() && request.target_os != env::consts::OS)
        || (!request.target_arch.is_empty() && request.target_arch != env::consts::ARCH)
    {
        return Ok(ClusterRolloutBinaryResponse {
            status: "rejected".to_string(),
            detail: format!(
                "artifact platform {}/{} does not match target platform {}/{}",
                request.target_os,
                request.target_arch,
                env::consts::OS,
                env::consts::ARCH
            ),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        });
    }

    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    let binary = URL_SAFE_NO_PAD
        .decode(request.binary_base64.as_bytes())
        .map_err(|source| DaemonError::Parse {
            detail: format!("invalid rollout binary encoding: {source}"),
        })?;
    let main_binary = rollout_paths.main_binary.clone();
    let shadow_binary = rollout_paths.shadow_binary.clone();
    let staged_main_binary = rollout_staged_main_binary(&rollout_paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(&rollout_paths);
    stage_rollout_bytes(&binary, &staged_main_binary)?;
    stage_rollout_bytes(&binary, &staged_shadow_binary)?;

    let plan = prepare_cluster_rollout_main_activation(
        paths,
        data_dir,
        request.source,
        main_binary.clone(),
        shadow_binary.clone(),
        Some(staged_main_binary),
        Some(staged_shadow_binary),
        request.host,
        request.port,
        request.shadow_port,
        request.requested_at,
        "cluster-binary-push",
    )?;
    let activation_strategy =
        stage_rollout_main_activation(paths, data_dir, &rollout_paths, plan.activation)?;
    Ok(ClusterRolloutBinaryResponse {
        status: "accepted".to_string(),
        detail: rollout_staged_detail(
            "candidate main staged",
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
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let current_daemon_is_shadow = paths_equivalent(&current_daemon_paths.root, &shadow_paths.root);
    let shadow_running = current_daemon_is_shadow
        || matches!(
            probe_daemon_status(&shadow_paths, CLUSTER_HEALTH_PROBE_TIMEOUT),
            DaemonProbeStatus::Running(_)
        );
    if !shadow_running {
        stage_main_only_rollout_without_existing_shadow(data_dir, rollout_paths, activation)?;
        return Ok(RolloutActivationStrategy::MainOnlyShadowBootstrap);
    }
    write_main_activation(rollout_paths, &activation)?;
    record_main_activation_staged(rollout_paths, &activation)?;
    if current_daemon_is_shadow {
        request_main_shutdown_for_shadow_rollout(data_dir, &activation)?;
    }
    Ok(RolloutActivationStrategy::ShadowSupervised)
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
    stage_rollout_file_with_retry(
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

pub fn handle_daemon_rollout_diagnostics(paths: &DaemonPaths) -> Result<String, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    rollout_diagnostics_report(&SelfRolloutPaths::new(data_dir)).map_err(Into::into)
}

pub fn handle_daemon_finalize_shadow_rollout(paths: &DaemonPaths) -> Result<String, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let Some(finalize) = read_pending_shadow_rollout(&rollout_paths)? else {
        return Ok("no pending shadow rollout\n".to_string());
    };

    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let mut detail = "shadow rollout finalized".to_string();
    let mut status = "accepted";

    // Stage the new supervisor binary into the installed path first so an in-place
    // re-exec (below) adopts it.
    if let Some(staged_shadow) = finalize
        .staged_shadow_binary
        .as_ref()
        .filter(|path| path.exists())
    {
        stage_rollout_file_with_retry(
            staged_shadow,
            &finalize.shadow_binary,
            STABLE_BINARY_COPY_TIMEOUT,
            STABLE_BINARY_COPY_RETRY_INTERVAL,
        )?;
    }

    let outcome = if matches!(
        probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT),
        DaemonProbeStatus::Running(_)
    ) {
        // Ask the running supervisor to re-exec the new binary in place. It replaces
        // its own process image rather than stopping, so the node is never left
        // unsupervised — this is what fixes the dead-node failure when the shadow is
        // PID 1 (container) or has no external init.
        request_shadow_reexec(&rollout_paths, &finalize.shadow_binary)?;
        if let DaemonProbeStatus::Running(running) =
            probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT)
        {
            let _ = send_daemon_shutdown_request(&running.address, DAEMON_PROBE_TIMEOUT);
        }
        // Wait past the brief re-exec gap, then confirm a supervisor is healthy. If
        // the re-exec failed the port stays dead and this errors into recovery.
        thread::sleep(Duration::from_secs(2));
        wait_for_shadow_rollout_health(
            data_dir,
            &finalize.shadow_host,
            finalize.shadow_port,
            Duration::from_secs(30),
        )
    } else {
        // No supervisor to re-exec (already down) — start a fresh one, verified.
        ensure_shadow_supervisor_running(
            data_dir,
            &finalize.shadow_binary,
            &finalize.main_binary,
            &finalize.main_host,
            finalize.main_port,
            &finalize.shadow_host,
            finalize.shadow_port,
            SHADOW_RECOVERY_DEADLINE,
        )
    };

    if let Err(error) = outcome {
        // The new supervisor did not become healthy. Clear the pending re-exec,
        // restore the previous (known-good) binary, and recover with verified,
        // bounded retries so the node is never left without a supervisor.
        let _ = take_shadow_reexec_request(&rollout_paths);
        let _ = stage_rollout_file_with_retry(
            &finalize.previous_shadow_binary,
            &finalize.shadow_binary,
            STABLE_BINARY_COPY_TIMEOUT,
            STABLE_BINARY_COPY_RETRY_INTERVAL,
        );
        match ensure_shadow_supervisor_running(
            data_dir,
            &finalize.shadow_binary,
            &finalize.main_binary,
            &finalize.main_host,
            finalize.main_port,
            &finalize.shadow_host,
            finalize.shadow_port,
            SHADOW_RECOVERY_DEADLINE,
        ) {
            Ok(()) => {
                status = "rolled-back";
                detail = format!(
                    "candidate shadow did not become healthy ({error}); restored previous supervisor binary"
                );
            }
            Err(recovery_error) => {
                status = "failed";
                detail = format!(
                    "candidate shadow failed ({error}) and recovery supervisor did not become healthy: {recovery_error}"
                );
                eprintln!("sinomo shadow rollout finalize: {detail}");
            }
        }
    }

    record_shadow_rollout_finalized(&rollout_paths, &finalize, status, &detail)?;
    remove_shadow_finalize(&rollout_paths);
    Ok(format!("{status}: {detail}\n"))
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
        )
        .and_then(|()| {
            wait_for_shadow_rollout_health(data_dir, shadow_host, shadow_port, Duration::from_secs(5))
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
            "observed_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_else(|_| "unknown".to_string()),
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
    finalize_timeout: Duration,
) -> Result<crate::DaemonRunOptions, DaemonError> {
    stage_pending_shadow_rollout(rollout_paths, pending, shadow_host, shadow_port)?;
    let main_address =
        daemon_connect_address(&pending.main_options.host, pending.main_options.port);
    thread::spawn(move || {
        let _ = send_daemon_finalize_shadow_rollout_request(&main_address, finalize_timeout);
    });
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
    let main_binary = pending.previous_main_binary.clone();
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
        let previous_main_binary = activation
            .previous_main_binary
            .clone()
            .unwrap_or_else(|| main_binary.to_path_buf());
        let activation_main_options = crate::DaemonRunOptions {
            host: activation.main_host.clone(),
            port: activation.main_port,
        };
        let previous_main_backup = None;
        if let Some(staged_main) = activation
            .staged_main_binary
            .as_ref()
            .filter(|path| path.exists())
        {
            stage_rollout_file_with_retry(
                staged_main,
                &activation.main_binary,
                STABLE_BINARY_COPY_TIMEOUT,
                STABLE_BINARY_COPY_RETRY_INTERVAL,
            )?;
        }
        match spawn_candidate_main_daemon_process(
            &activation.main_binary,
            &activation_main_options,
            rollout_paths,
        ) {
            Ok((child, diagnostics)) => {
                let main_binary = activation.main_binary.clone();
                let previous_shadow_binary = env::current_exe().unwrap_or_default();
                Ok(ShadowMainStart::Candidate {
                    child,
                    monitor: ShadowMainRolloutMonitor {
                        activation,
                        main_options: activation_main_options,
                        previous_main_binary,
                        previous_main_backup,
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
                let main_binary = previous_main_binary;
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
    status: ExitStatus,
) -> Result<ShadowMainExit, DaemonError> {
    if read_rollout_main_activation(rollout_paths)?.is_some() {
        return Ok(ShadowMainExit::PendingActivation);
    }
    let observation = crate::main_exit_observation_from_status(status);
    let kind = classify_main_exit(observation);
    record_rollout_main_exit(rollout_paths, observation, kind)?;
    if kind == sinomo_rollout::MainExitKind::Crash {
        Ok(ShadowMainExit::RestartRequested)
    } else {
        Ok(ShadowMainExit::Suppressed)
    }
}

pub fn handle_pending_shadow_main_process_exit(
    rollout_paths: &SelfRolloutPaths,
    main_paths: &DaemonPaths,
    pending: &ShadowMainRolloutMonitor,
    status: ExitStatus,
    shadow_host: &str,
    shadow_port: u16,
    finalize_timeout: Duration,
    probe_timeout: Duration,
) -> Result<PendingShadowMainExit, DaemonError> {
    let observation = crate::main_exit_observation_from_status(status);
    if observation.exit_code == Some(0)
        && observation.signal.is_none()
        && matches!(
            probe_daemon_status(main_paths, probe_timeout),
            DaemonProbeStatus::Running(_)
        )
    {
        let main_options = promote_shadow_main_rollout(
            rollout_paths,
            pending,
            shadow_host,
            shadow_port,
            finalize_timeout,
        )?;
        let main_probe_status = probe_daemon_status(main_paths, probe_timeout);
        return Ok(PendingShadowMainExit::Promoted {
            main_binary: pending.activation.main_binary.clone(),
            main_options,
            main_probe_status,
        });
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
    let repo = if let Some(bundle_base64) = request.source_bundle_base64.as_deref() {
        let bundle_bytes = URL_SAFE_NO_PAD
            .decode(bundle_base64.as_bytes())
            .map_err(|source| DaemonError::Parse {
                detail: format!("invalid rollout source bundle encoding: {source}"),
            })?;
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
        deploy_managed_source_bundle(&rollout_paths, &bundle_path)?;
        if let Some(expected_head) = request.source_head.as_deref() {
            let status = managed_source_status(&rollout_paths)?;
            if !source_heads_match(status.head.as_deref(), expected_head) {
                return Ok(ClusterRolloutBinaryResponse {
                    status: "rejected".to_string(),
                    detail: format!(
                        "source bundle imported but managed source head is {}, expected {expected_head}",
                        status.head.unwrap_or_else(|| "unknown".to_string())
                    ),
                    main_binary: PathBuf::new(),
                    shadow_binary: PathBuf::new(),
                    source_head: None,
                });
            }
        }
        rollout_paths.source_repo.clone()
    } else {
        rollout_remote_repo_path(&rollout_paths, request.remote_repo.as_deref())
    };
    if !repo.join("Cargo.toml").exists() {
        return Ok(ClusterRolloutBinaryResponse {
            status: "rejected".to_string(),
            detail: format!(
                "remote source repo is not initialized at {}; run `sinomo rollout init-source --repo <path>` on this node or pass --remote-repo <path>",
                repo.display()
            ),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        });
    }

    let built_binary = match rollout_build_dev_binary(&repo, &rollout_paths.build_log) {
        Ok(binary) => binary,
        Err(error) => {
            return Ok(ClusterRolloutBinaryResponse {
                status: "rejected".to_string(),
                detail: format!("remote build failed: {error}"),
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
    stage_rollout_file(&built_binary, &staged_main_binary)?;
    stage_rollout_file(&built_binary, &staged_shadow_binary)?;

    let source = request.source.clone();
    let requested_at = request.requested_at.clone();
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
    let _ = (repo, built_binary);
    write_rollout_status(
        &rollout_paths,
        &ClusterNodeRolloutStatus {
            status: "main-staged".to_string(),
            mode: Some("cluster-remote-build".to_string()),
            detail: None,
            source: Some(source),
            requested_at: Some(requested_at),
            staged_at: Some(
                OffsetDateTime::now_utc()
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string()),
            ),
            updated_at: None,
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        },
    )?;
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
) -> Result<(), DaemonError> {
    let mut command = Command::new(shadow_binary);
    prepare_background_command(&mut command);
    command
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
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| DaemonError::Io {
            path: shadow_binary.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(())
}

pub fn wait_for_shadow_rollout_health(
    data_dir: &Path,
    shadow_host: &str,
    shadow_port: u16,
    timeout: Duration,
) -> Result<(), DaemonError> {
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if matches!(
            probe_daemon_status(&shadow_paths, Duration::from_secs(2)),
            DaemonProbeStatus::Running(_)
        ) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(DaemonError::Network {
        address: daemon_connect_address(shadow_host, shadow_port),
        detail: "rolled out shadow daemon did not become ready".to_string(),
    })
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn cluster_rollout_remote_build_policy_uses_remote_targets_and_platform() {
        assert!(cluster_rollout_should_use_remote_build(
            "10.0.0.2:21353",
            false,
            false,
            Some(false)
        ));
        assert!(cluster_rollout_should_use_remote_build(
            "10.0.0.2:21353",
            false,
            false,
            None
        ));
        assert!(!cluster_rollout_should_use_remote_build(
            "10.0.0.2:21353",
            false,
            false,
            Some(true)
        ));
        assert!(!cluster_rollout_should_use_remote_build(
            "10.0.0.2:21353",
            false,
            true,
            Some(false)
        ));
        assert!(!cluster_rollout_should_use_remote_build(
            "127.0.0.1:21353",
            false,
            false,
            Some(false)
        ));
        assert!(cluster_rollout_should_use_remote_build(
            "127.0.0.1:21353",
            true,
            true,
            Some(true)
        ));
    }

    #[test]
    fn pending_shadow_rollout_uses_fast_main_health_probe() {
        assert!(shadow_main_probe_interval(true) < Duration::from_secs(1));
        assert!(shadow_main_probe_interval(false) >= Duration::from_secs(10));
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

        std::fs::remove_dir_all(root).ok();
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
        std::fs::remove_dir_all(root).ok();
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
        std::fs::remove_dir_all(root).ok();
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
        let current_exe = std::env::current_exe().expect("current exe");
        std::fs::copy(&current_exe, &staged_main).expect("stage main executable");
        std::fs::copy(&current_exe, &staged_shadow).expect("stage shadow executable");
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
            vec![
                format!("sinomo{}", std::env::consts::EXE_SUFFIX),
                format!("sinomo-shadow{}", std::env::consts::EXE_SUFFIX)
            ]
        );
        std::fs::remove_dir_all(root).ok();
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
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_rollout_activation_plan_uses_fallback_when_main_is_not_running() {
        let root = env::temp_dir().join(format!(
            "sinomo-daemon-local-rollout-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let rollout_paths = SelfRolloutPaths::new(&root);
        let artifacts = LocalRolloutArtifacts {
            built_binary: root.join("build").join("sinomo"),
            staged_main_binary: root.join("rollout").join("main-staged"),
            staged_shadow_binary: root.join("rollout").join("shadow-staged"),
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

    fn temp_rollout_test_root(label: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "sinomo-daemon-rollout-{label}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
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
