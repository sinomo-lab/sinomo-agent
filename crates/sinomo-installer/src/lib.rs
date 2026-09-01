//! Local runtime installation, managed-source deployment, and boot-entry orchestration.

use sinomo_api::daemon_status_matches_options;
use sinomo_core::local::SinomoDataDir;
use sinomo_daemon::{
    DaemonError, DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonStatus,
    daemon_connect_address, probe_daemon_status, probe_daemon_status_at_address,
    send_daemon_shutdown_request,
};
use sinomo_rollout::{
    SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV, SHADOW_MAIN_PORT_ENV, SelfRolloutPaths,
    current_executable_should_update, deploy_managed_source_repo, discover_current_git_repo,
    ensure_rollout_layout,
};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use std::time::Instant;

pub const INSTALL_MAIN_BOOT_NAME: &str = "Sinomo Daemon";
pub const INSTALL_SHADOW_BOOT_NAME: &str = "Sinomo Shadow Daemon";

fn rollout_error(error: sinomo_rollout::RolloutError) -> DaemonError {
    DaemonError::Io {
        path: "sinomo rollout".to_string(),
        detail: error.to_string(),
    }
}

#[cfg(not(windows))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalBinSymlinkStatus {
    HomeNotSet,
    AlreadyCurrent { link: PathBuf },
    Created { link: PathBuf },
    BlockedByNonSymlink { link: PathBuf },
}

#[cfg(windows)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UserPathUpdate {
    AlreadyPresent,
    Added,
}

#[cfg(not(windows))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemctlUserOutcome {
    Succeeded { args: Vec<String> },
    Failed { args: Vec<String>, stderr: String },
    CouldNotRun { args: Vec<String>, detail: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonStopIfRunningStatus {
    StopRequested,
    Locked,
    NotRunning,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DaemonStopBeforeInstallStart {
    StopRequested,
    ShutdownRequestFailed { detail: String },
    Locked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DaemonStopBeforeInstallStatus {
    NotRunning,
    Stopped {
        start: DaemonStopBeforeInstallStart,
    },
    Terminated {
        start: DaemonStopBeforeInstallStart,
        pid: u32,
    },
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedSourceRepoInstallStatus {
    Updated,
    CurrentDirectoryNotInGitCheckout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstalledDaemonStartStatus {
    Started(sinomo_daemon::DaemonStatus),
    ExitedBeforeReady { command: String, status: i32 },
    TimedOut { status_path: PathBuf, child_id: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallLayout {
    pub data_dir: PathBuf,
    pub bin_dir: PathBuf,
    pub binary: PathBuf,
    pub shadow_binary: PathBuf,
    pub boot_dir: PathBuf,
    pub main_boot_entry: PathBuf,
    pub shadow_boot_entry: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledDaemonStartSpec {
    pub binary: PathBuf,
    pub args: Vec<String>,
    pub paths: DaemonPaths,
    pub label: &'static str,
    pub extra_env: Vec<(&'static str, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledDaemonPaths {
    pub main: DaemonPaths,
    pub shadow: DaemonPaths,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledDaemonInstallPlanRequest {
    pub host: String,
    pub port: u16,
    pub shadow_port: u16,
    pub port_fallback: bool,
    pub shadow_port_fallback: bool,
    pub start: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledDaemonInstallPlan {
    pub existing_install: bool,
    pub daemon_paths: InstalledDaemonPaths,
    pub updating_binary: bool,
    pub restarting_for_options: bool,
    pub stop_reason: Option<&'static str>,
    pub start_specs: Vec<InstalledDaemonStartSpec>,
}

impl InstallLayout {
    pub fn new(data_dir: &SinomoDataDir, boot_dir: PathBuf) -> Self {
        let data_root = data_dir.root();
        let bin_dir = data_root.join("bin");
        let binary = bin_dir.join(format!("sinomo{}", env::consts::EXE_SUFFIX));
        let shadow_binary = bin_dir.join(format!("sinomo-shadow{}", env::consts::EXE_SUFFIX));
        let main_boot_entry = boot_entry_path(&boot_dir, INSTALL_MAIN_BOOT_NAME);
        let shadow_boot_entry = boot_entry_path(&boot_dir, INSTALL_SHADOW_BOOT_NAME);
        Self {
            data_dir: data_root,
            bin_dir,
            binary,
            shadow_binary,
            boot_dir,
            main_boot_entry,
            shadow_boot_entry,
        }
    }
}

pub fn installed_daemon_paths(layout: &InstallLayout) -> InstalledDaemonPaths {
    InstalledDaemonPaths {
        main: DaemonPaths::with_root(layout.data_dir.join("daemon")),
        shadow: DaemonPaths::with_root(layout.data_dir.join("shadow-daemon")),
    }
}

fn installed_daemon_start_specs(
    layout: &InstallLayout,
    host: &str,
    main_port: u16,
    shadow_port: u16,
    main_port_fallback: bool,
    shadow_port_fallback: bool,
) -> [InstalledDaemonStartSpec; 2] {
    let daemon_paths = installed_daemon_paths(layout);
    let mut main_args = vec![
        "daemon".to_string(),
        "run".to_string(),
        "--host".to_string(),
        host.to_string(),
    ];
    if !main_port_fallback {
        main_args.extend(["--port".to_string(), main_port.to_string()]);
    }
    let mut shadow_args = vec![
        "daemon".to_string(),
        "shadow".to_string(),
        "run".to_string(),
        "--host".to_string(),
        host.to_string(),
    ];
    if !shadow_port_fallback {
        shadow_args.extend(["--port".to_string(), shadow_port.to_string()]);
    }
    let mut shadow_env = vec![
        (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, host.to_string()),
    ];
    if !main_port_fallback {
        shadow_env.push((SHADOW_MAIN_PORT_ENV, main_port.to_string()));
    }
    [
        InstalledDaemonStartSpec {
            binary: layout.binary.clone(),
            args: main_args,
            paths: daemon_paths.main,
            label: "daemon",
            extra_env: Vec::new(),
        },
        InstalledDaemonStartSpec {
            binary: layout.shadow_binary.clone(),
            args: shadow_args,
            paths: daemon_paths.shadow,
            label: "shadow daemon",
            extra_env: shadow_env,
        },
    ]
}

pub fn prepare_installed_daemon_install_plan(
    layout: &InstallLayout,
    request: InstalledDaemonInstallPlanRequest,
    probe_timeout: Duration,
) -> Result<InstalledDaemonInstallPlan, DaemonError> {
    let daemon_paths = installed_daemon_paths(layout);
    let updating_binary = current_executable_should_update(&layout.binary)
        .map_err(rollout_error)?
        || current_executable_should_update(&layout.shadow_binary).map_err(rollout_error)?;
    let restarting_for_options = request.start
        && installed_daemons_need_restart(
            &request.host,
            request.port,
            request.shadow_port,
            request.port_fallback,
            request.shadow_port_fallback,
            &daemon_paths.main,
            &daemon_paths.shadow,
            probe_timeout,
        );
    let stop_reason = installed_daemon_install_stop_reason(updating_binary, restarting_for_options);
    let start_specs = if request.start {
        installed_daemon_start_specs(
            layout,
            &request.host,
            request.port,
            request.shadow_port,
            request.port_fallback,
            request.shadow_port_fallback,
        )
        .into_iter()
        .collect()
    } else {
        Vec::new()
    };

    Ok(InstalledDaemonInstallPlan {
        existing_install: install_layout_exists(layout),
        daemon_paths,
        updating_binary,
        restarting_for_options,
        stop_reason,
        start_specs,
    })
}

fn installed_daemon_install_stop_reason(
    updating_binary: bool,
    restarting_for_options: bool,
) -> Option<&'static str> {
    if updating_binary {
        Some("binary update")
    } else if restarting_for_options {
        Some("install option update")
    } else {
        None
    }
}

fn install_layout_exists(layout: &InstallLayout) -> bool {
    layout.binary.exists()
        || layout.shadow_binary.exists()
        || layout.main_boot_entry.exists()
        || layout.shadow_boot_entry.exists()
        || SelfRolloutPaths::new(&layout.data_dir).source_repo.exists()
}

pub fn ensure_install_layout_dirs(layout: &InstallLayout) -> Result<(), DaemonError> {
    fs::create_dir_all(&layout.bin_dir).map_err(|source| DaemonError::Io {
        path: layout.bin_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::create_dir_all(&layout.boot_dir).map_err(|source| DaemonError::Io {
        path: layout.boot_dir.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn install_managed_source_repo(
    layout: &InstallLayout,
) -> Result<ManagedSourceRepoInstallStatus, DaemonError> {
    let rollout_paths = SelfRolloutPaths::new(&layout.data_dir);
    ensure_rollout_layout(&rollout_paths).map_err(rollout_error)?;
    let Some(source_repo) = discover_current_git_repo().map_err(rollout_error)? else {
        return Ok(ManagedSourceRepoInstallStatus::CurrentDirectoryNotInGitCheckout);
    };
    deploy_managed_source_repo(&rollout_paths, &source_repo).map_err(rollout_error)?;
    Ok(ManagedSourceRepoInstallStatus::Updated)
}

fn install_child_command(program: impl AsRef<std::ffi::OsStr>) -> Command {
    let command = Command::new(program);
    #[cfg(windows)]
    {
        let mut command = command;
        sinomo_daemon::prepare_noninteractive_command(&mut command);
        command
    }
    #[cfg(not(windows))]
    command
}

pub fn git_is_available() -> bool {
    install_child_command("git")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

pub fn managed_source_git_status_lines(repo: &Path) -> Result<Vec<String>, DaemonError> {
    if !repo.join(".git").exists() {
        return Ok(Vec::new());
    }
    let output = install_child_command("git")
        .arg("-C")
        .arg(repo)
        .args(["status", "--short"])
        .output()
        .map_err(|source| DaemonError::Io {
            path: format!("git -C {} status --short", repo.display()),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(DaemonError::Io {
            path: format!("git -C {} status --short", repo.display()),
            detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

#[cfg(not(windows))]
pub fn ensure_local_bin_symlink(
    layout: &InstallLayout,
) -> Result<LocalBinSymlinkStatus, DaemonError> {
    let Some(home) = env::var_os("HOME") else {
        return Ok(LocalBinSymlinkStatus::HomeNotSet);
    };
    ensure_local_bin_symlink_in_home(layout, Path::new(&home))
}

#[cfg(not(windows))]
fn ensure_local_bin_symlink_in_home(
    layout: &InstallLayout,
    home: &Path,
) -> Result<LocalBinSymlinkStatus, DaemonError> {
    let user_bin = home.join(".local").join("bin");
    fs::create_dir_all(&user_bin).map_err(|source| DaemonError::Io {
        path: user_bin.display().to_string(),
        detail: source.to_string(),
    })?;
    let link = user_bin.join("sinomo");
    if local_bin_symlink_is_current(&link, &layout.binary) {
        return Ok(LocalBinSymlinkStatus::AlreadyCurrent { link });
    }
    match fs::symlink_metadata(&link) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            fs::remove_file(&link).map_err(|source| DaemonError::Io {
                path: link.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(_) => {
            return Ok(LocalBinSymlinkStatus::BlockedByNonSymlink { link });
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(DaemonError::Io {
                path: link.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    std::os::unix::fs::symlink(&layout.binary, &link).map_err(|source| DaemonError::Io {
        path: format!("{} -> {}", link.display(), layout.binary.display()),
        detail: source.to_string(),
    })?;
    Ok(LocalBinSymlinkStatus::Created { link })
}

#[cfg(not(windows))]
fn local_bin_symlink_is_current(link: &Path, target: &Path) -> bool {
    let Ok(existing_target) = fs::read_link(link) else {
        return false;
    };
    if existing_target == target {
        return true;
    }
    match (fs::canonicalize(&existing_target), fs::canonicalize(target)) {
        (Ok(existing_target), Ok(target)) => existing_target == target,
        _ => false,
    }
}

#[cfg(windows)]
pub fn ensure_windows_user_path_contains(bin_dir: &Path) -> Result<UserPathUpdate, DaemonError> {
    let script = r#"
$bin = [System.IO.Path]::GetFullPath($env:SINOMO_INSTALL_BIN_DIR)
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if ([string]::IsNullOrEmpty($userPath)) {
    $entries = @()
} else {
    $entries = $userPath -split ';'
}
$normalizedBin = $bin.TrimEnd('\')
$exists = $false
foreach ($entry in $entries) {
    if ($entry.Trim().TrimEnd('\') -ieq $normalizedBin) {
        $exists = $true
        break
    }
}
if ($exists) {
    Write-Output 'already'
} else {
    if ([string]::IsNullOrEmpty($userPath)) {
        $nextPath = $bin
    } else {
        $nextPath = $userPath.TrimEnd(';') + ';' + $bin
    }
    [Environment]::SetEnvironmentVariable('Path', $nextPath, 'User')
    Write-Output 'added'
}
"#;
    let output = install_child_command("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("SINOMO_INSTALL_BIN_DIR", bin_dir)
        .output()
        .map_err(|source| DaemonError::Io {
            path: "powershell user PATH update".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(DaemonError::Io {
            path: "powershell user PATH update".to_string(),
            detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    match String::from_utf8_lossy(&output.stdout).trim() {
        "already" => Ok(UserPathUpdate::AlreadyPresent),
        "added" => Ok(UserPathUpdate::Added),
        other => Err(DaemonError::Io {
            path: "powershell user PATH update".to_string(),
            detail: format!("unexpected output `{other}`"),
        }),
    }
}

#[cfg(windows)]
pub fn activate_boot_entries(_layout: &InstallLayout) -> Vec<()> {
    Vec::new()
}

#[cfg(not(windows))]
pub fn activate_boot_entries(layout: &InstallLayout) -> Vec<SystemctlUserOutcome> {
    let mut outcomes = Vec::new();
    outcomes.push(systemctl_user_best_effort(&["daemon-reload"]));
    for entry in [&layout.main_boot_entry, &layout.shadow_boot_entry] {
        let Some(unit) = entry.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        outcomes.push(systemctl_user_best_effort(&["enable", unit]));
    }
    outcomes
}

#[cfg(windows)]
pub fn deactivate_boot_entries(_layout: &InstallLayout) -> Vec<()> {
    Vec::new()
}

#[cfg(not(windows))]
pub fn deactivate_boot_entries(layout: &InstallLayout) -> Vec<SystemctlUserOutcome> {
    let mut outcomes = Vec::new();
    for entry in [&layout.main_boot_entry, &layout.shadow_boot_entry] {
        let Some(unit) = entry.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        outcomes.push(systemctl_user_best_effort(&["disable", unit]));
    }
    outcomes.push(systemctl_user_best_effort(&["daemon-reload"]));
    outcomes
}

#[cfg(not(windows))]
fn systemctl_user_best_effort(args: &[&str]) -> SystemctlUserOutcome {
    let output = Command::new("systemctl").arg("--user").args(args).output();
    let args = args.iter().map(|arg| (*arg).to_string()).collect();
    match output {
        Ok(output) if output.status.success() => SystemctlUserOutcome::Succeeded { args },
        Ok(output) => SystemctlUserOutcome::Failed {
            args,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        },
        Err(source) => SystemctlUserOutcome::CouldNotRun {
            args,
            detail: source.to_string(),
        },
    }
}

fn installed_daemons_need_restart(
    host: &str,
    main_port: u16,
    shadow_port: u16,
    main_port_fallback: bool,
    shadow_port_fallback: bool,
    main_paths: &DaemonPaths,
    shadow_paths: &DaemonPaths,
    timeout: Duration,
) -> bool {
    installed_daemon_probe_needs_restart(
        probe_daemon_status(main_paths, timeout),
        host,
        main_port,
        main_port_fallback,
    ) || installed_daemon_probe_needs_restart(
        probe_daemon_status(shadow_paths, timeout),
        host,
        shadow_port,
        shadow_port_fallback,
    )
}

fn installed_daemon_probe_needs_restart(
    status: DaemonProbeStatus,
    host: &str,
    port: u16,
    port_fallback: bool,
) -> bool {
    match status {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => status.host != host || !port_fallback && status.port != port,
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => false,
    }
}

pub fn stop_daemon_if_running(
    paths: &DaemonPaths,
    timeout: Duration,
) -> Result<DaemonStopIfRunningStatus, DaemonError> {
    match probe_daemon_status(paths, timeout) {
        sinomo_daemon::DaemonProbeStatus::Running(status) => {
            let _ = send_daemon_shutdown_request(&status.address, timeout)?;
            Ok(DaemonStopIfRunningStatus::StopRequested)
        }
        sinomo_daemon::DaemonProbeStatus::Locked { .. } => Ok(DaemonStopIfRunningStatus::Locked),
        sinomo_daemon::DaemonProbeStatus::NotRunning => Ok(DaemonStopIfRunningStatus::NotRunning),
    }
}

pub fn stop_daemon_before_install(
    paths: &DaemonPaths,
    wait_timeout: Duration,
    probe_timeout: Duration,
) -> Result<DaemonStopBeforeInstallStatus, DaemonError> {
    let mut next_shutdown_nudge = Instant::now();
    let start = match probe_daemon_status(paths, probe_timeout) {
        sinomo_daemon::DaemonProbeStatus::Running(status) => {
            match send_daemon_shutdown_request(&status.address, probe_timeout) {
                Ok(_) => DaemonStopBeforeInstallStart::StopRequested,
                Err(error) => {
                    if matches!(
                        probe_daemon_status(paths, probe_timeout),
                        sinomo_daemon::DaemonProbeStatus::NotRunning
                    ) {
                        return Ok(DaemonStopBeforeInstallStatus::NotRunning);
                    }
                    next_shutdown_nudge = Instant::now() + Duration::from_secs(1);
                    DaemonStopBeforeInstallStart::ShutdownRequestFailed {
                        detail: error.to_string(),
                    }
                }
            }
        }
        sinomo_daemon::DaemonProbeStatus::Locked { .. } => DaemonStopBeforeInstallStart::Locked,
        sinomo_daemon::DaemonProbeStatus::NotRunning => {
            return Ok(DaemonStopBeforeInstallStatus::NotRunning);
        }
    };

    let deadline = Instant::now() + wait_timeout;
    while Instant::now() < deadline {
        match probe_daemon_status(paths, probe_timeout) {
            sinomo_daemon::DaemonProbeStatus::NotRunning => {
                return Ok(DaemonStopBeforeInstallStatus::Stopped { start });
            }
            sinomo_daemon::DaemonProbeStatus::Running(status)
                if Instant::now() >= next_shutdown_nudge =>
            {
                let _ = send_daemon_shutdown_request(&status.address, probe_timeout);
                next_shutdown_nudge = Instant::now() + Duration::from_secs(1);
            }
            sinomo_daemon::DaemonProbeStatus::Running(_)
            | sinomo_daemon::DaemonProbeStatus::Locked { .. } => {}
        }
        thread::sleep(Duration::from_millis(100));
    }

    if let Some(pid) =
        terminate_daemon_from_status_file(paths, Duration::from_secs(10), probe_timeout)?
    {
        return Ok(DaemonStopBeforeInstallStatus::Terminated { start, pid });
    }

    Ok(DaemonStopBeforeInstallStatus::TimedOut)
}

fn terminate_daemon_from_status_file(
    paths: &DaemonPaths,
    wait_timeout: Duration,
    probe_timeout: Duration,
) -> Result<Option<u32>, DaemonError> {
    let Ok(status) = sinomo_daemon::read_daemon_status(paths) else {
        return Ok(None);
    };
    #[cfg(not(unix))]
    let _ = (wait_timeout, probe_timeout, &status);
    #[cfg(unix)]
    {
        unsafe extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        const SIGTERM: i32 = 15;
        let result = unsafe { kill(status.pid as i32, SIGTERM) };
        if result == -1 {
            return Err(DaemonError::Io {
                path: format!("pid {}", status.pid),
                detail: std::io::Error::last_os_error().to_string(),
            });
        }
        let deadline = Instant::now() + wait_timeout;
        while Instant::now() < deadline {
            if matches!(
                probe_daemon_status(paths, probe_timeout),
                sinomo_daemon::DaemonProbeStatus::NotRunning
            ) {
                return Ok(Some(status.pid));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    Ok(None)
}

pub fn start_installed_sinomo_daemon(
    binary: &Path,
    data_dir: &Path,
    args: &[String],
    paths: &DaemonPaths,
    extra_env: &[(&str, String)],
    ready_timeout: Duration,
    probe_timeout: Duration,
) -> Result<InstalledDaemonStartStatus, DaemonError> {
    if let Some(status) = probe_installed_daemon_start_status(paths, args, probe_timeout) {
        return Ok(InstalledDaemonStartStatus::Started(status));
    }
    terminate_matching_installed_daemon_process(binary, args, ready_timeout)?;

    let mut command = installed_daemon_command(binary, data_dir, args, extra_env);
    let mut child = command.spawn().map_err(|source| DaemonError::Io {
        path: binary.display().to_string(),
        detail: source.to_string(),
    })?;

    let deadline = Instant::now() + ready_timeout;
    let mut exited_before_ready: Option<(String, i32)> = None;
    while Instant::now() < deadline {
        if let Some(status) = probe_installed_daemon_start_status(paths, args, probe_timeout) {
            return Ok(InstalledDaemonStartStatus::Started(status));
        }

        if exited_before_ready.is_none()
            && let Some(status) = child.try_wait().map_err(|source| DaemonError::Io {
                path: binary.display().to_string(),
                detail: source.to_string(),
            })?
        {
            exited_before_ready = Some((
                format!("{} {}", binary.display(), args.join(" ")),
                status.code().unwrap_or(-1),
            ));
        }
        thread::sleep(Duration::from_millis(100));
    }

    if let Some((command, status)) = exited_before_ready {
        return Ok(InstalledDaemonStartStatus::ExitedBeforeReady { command, status });
    }

    Ok(InstalledDaemonStartStatus::TimedOut {
        status_path: paths.status.clone(),
        child_id: child.id(),
    })
}

fn installed_daemon_command(
    binary: &Path,
    data_dir: &Path,
    args: &[String],
    extra_env: &[(&str, String)],
) -> Command {
    let mut command = Command::new(binary);
    command
        .args(args)
        .env("SINOMO_DATA_DIR", data_dir)
        // Installed daemons must not retain the CLI invoker's working
        // directory. On Windows that inheritance keeps removable checkout
        // directories busy for the lifetime of the daemon process.
        .current_dir(data_dir);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    sinomo_daemon::prepare_background_command(&mut command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

#[cfg(target_os = "linux")]
fn terminate_matching_installed_daemon_process(
    binary: &Path,
    args: &[String],
    wait_timeout: Duration,
) -> Result<Option<u32>, DaemonError> {
    let Some(pid) = find_matching_installed_daemon_process(binary, args)? else {
        return Ok(None);
    };

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

    let proc_dir = PathBuf::from(format!("/proc/{pid}"));
    let deadline = Instant::now() + wait_timeout;
    while Instant::now() < deadline {
        if !proc_dir.exists() || process_cmdline(&proc_dir).is_none_or(|cmdline| cmdline.is_empty())
        {
            return Ok(Some(pid));
        }
        thread::sleep(Duration::from_millis(100));
    }

    Ok(Some(pid))
}

#[cfg(not(target_os = "linux"))]
fn terminate_matching_installed_daemon_process(
    _binary: &Path,
    _args: &[String],
    _wait_timeout: Duration,
) -> Result<Option<u32>, DaemonError> {
    Ok(None)
}

#[cfg(target_os = "linux")]
fn find_matching_installed_daemon_process(
    binary: &Path,
    args: &[String],
) -> Result<Option<u32>, DaemonError> {
    let current_pid = std::process::id();
    let entries = fs::read_dir("/proc").map_err(|source| DaemonError::Io {
        path: "/proc".to_string(),
        detail: source.to_string(),
    })?;

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Ok(pid) = file_name.parse::<u32>() else {
            continue;
        };
        if pid == current_pid {
            continue;
        }
        let Some(cmdline) = process_cmdline(&entry.path()) else {
            continue;
        };
        if installed_daemon_cmdline_matches(&cmdline, binary, args) {
            return Ok(Some(pid));
        }
    }

    Ok(None)
}

#[cfg(target_os = "linux")]
fn process_cmdline(proc_dir: &Path) -> Option<Vec<String>> {
    let bytes = fs::read(proc_dir.join("cmdline")).ok()?;
    if bytes.is_empty() {
        return Some(Vec::new());
    }
    Some(
        bytes
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part).into_owned())
            .collect(),
    )
}

#[cfg(target_os = "linux")]
fn installed_daemon_cmdline_matches(cmdline: &[String], binary: &Path, args: &[String]) -> bool {
    if cmdline.len() != args.len() + 1 {
        return false;
    }
    if Path::new(&cmdline[0]) != binary {
        return false;
    }
    cmdline[1..] == *args
}

fn probe_installed_daemon_start_status(
    paths: &DaemonPaths,
    args: &[String],
    timeout: Duration,
) -> Option<DaemonStatus> {
    match probe_daemon_status(paths, timeout) {
        sinomo_daemon::DaemonProbeStatus::Running(status)
        | sinomo_daemon::DaemonProbeStatus::Locked {
            status: Some(status),
        } => return Some(status),
        sinomo_daemon::DaemonProbeStatus::Locked { status: None }
        | sinomo_daemon::DaemonProbeStatus::NotRunning => {}
    }

    let options = daemon_run_options_from_args(args)?;
    let address = daemon_connect_address(&options.host, options.port);
    let status = probe_daemon_status_at_address(&address, timeout)?;
    if daemon_status_matches_options(&status, &options.host, options.port) {
        Some(status)
    } else {
        None
    }
}

fn daemon_run_options_from_args(args: &[String]) -> Option<DaemonRunOptions> {
    let mut host = None;
    let mut port = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--host" => host = iter.next().cloned(),
            "--port" => {
                port = iter.next().and_then(|value| value.parse::<u16>().ok());
            }
            _ => {}
        }
    }
    Some(DaemonRunOptions {
        host: host?,
        port: port?,
        port_fallback: false,
    })
}

pub fn default_boot_dir(data_dir: &SinomoDataDir) -> PathBuf {
    #[cfg(windows)]
    {
        if let Ok(appdata) = env::var("APPDATA") {
            let appdata = appdata.trim();
            if !appdata.is_empty() {
                return PathBuf::from(appdata)
                    .join("Microsoft")
                    .join("Windows")
                    .join("Start Menu")
                    .join("Programs")
                    .join("Startup");
            }
        }
        data_dir.root().join("startup")
    }
    #[cfg(not(windows))]
    {
        if let Ok(config_home) = env::var("XDG_CONFIG_HOME") {
            let config_home = config_home.trim();
            if !config_home.is_empty() {
                return PathBuf::from(config_home).join("systemd").join("user");
            }
        }
        if let Ok(home) = env::var("HOME") {
            let home = home.trim();
            if !home.is_empty() {
                return PathBuf::from(home)
                    .join(".config")
                    .join("systemd")
                    .join("user");
            }
        }
        data_dir.root().join("startup")
    }
}

fn boot_entry_path(boot_dir: &Path, name: &str) -> PathBuf {
    #[cfg(windows)]
    {
        boot_dir.join(format!("{name}.vbs"))
    }
    #[cfg(not(windows))]
    {
        let file_name = name.to_ascii_lowercase().replace(' ', "-");
        boot_dir.join(format!("{file_name}.service"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BootEntryChildProcessPolicy {
    SystemdDefault,
    PreserveDetachedChildren,
}

fn write_boot_entry(
    path: &Path,
    data_dir: &Path,
    binary: &Path,
    command_prefix: &[&str],
    host: &str,
    port: u16,
    port_fallback: bool,
    description: &str,
    child_process_policy: BootEntryChildProcessPolicy,
    extra_env: &[(&str, String)],
) -> Result<(), DaemonError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| DaemonError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = boot_entry_content(
        data_dir,
        binary,
        command_prefix,
        host,
        port,
        port_fallback,
        description,
        child_process_policy,
        extra_env,
    );
    fs::write(path, content).map_err(|source| DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn write_install_boot_entries(
    layout: &InstallLayout,
    host: &str,
    main_port: u16,
    shadow_port: u16,
    main_port_fallback: bool,
    shadow_port_fallback: bool,
) -> Result<(), DaemonError> {
    #[cfg(windows)]
    let main_command_prefix = ["daemon", "start"];
    #[cfg(not(windows))]
    let main_command_prefix = ["daemon", "run"];
    #[cfg(windows)]
    let shadow_command_prefix = ["daemon", "shadow", "start"];
    #[cfg(not(windows))]
    let shadow_command_prefix = ["daemon", "shadow", "run"];

    write_boot_entry(
        &layout.main_boot_entry,
        &layout.data_dir,
        &layout.binary,
        &main_command_prefix,
        host,
        main_port,
        main_port_fallback,
        "Sinomo daemon",
        BootEntryChildProcessPolicy::SystemdDefault,
        &[],
    )?;
    let mut shadow_env = vec![
        (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, host.to_string()),
    ];
    if !main_port_fallback {
        shadow_env.push((SHADOW_MAIN_PORT_ENV, main_port.to_string()));
    }
    write_boot_entry(
        &layout.shadow_boot_entry,
        &layout.data_dir,
        &layout.shadow_binary,
        &shadow_command_prefix,
        host,
        shadow_port,
        shadow_port_fallback,
        "Sinomo shadow daemon",
        BootEntryChildProcessPolicy::PreserveDetachedChildren,
        &shadow_env,
    )
}

pub fn remove_boot_entry(path: &Path) -> Result<(), DaemonError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn boot_entry_content(
    data_dir: &Path,
    binary: &Path,
    command_prefix: &[&str],
    host: &str,
    port: u16,
    port_fallback: bool,
    description: &str,
    child_process_policy: BootEntryChildProcessPolicy,
    extra_env: &[(&str, String)],
) -> String {
    let command = command_prefix.join(" ");
    #[cfg(windows)]
    {
        let _ = (description, child_process_policy);
        let extra_env = extra_env
            .iter()
            .map(|(key, value)| {
                format!(
                    "env(\"{}\") = \"{}\"\r\n",
                    vbs_string_literal(key),
                    vbs_string_literal(value)
                )
            })
            .collect::<String>();
        let port = if port_fallback {
            String::new()
        } else {
            format!(" --port {port}")
        };
        let command_line = format!(
            "\"{}\" {} --host {}{}",
            binary.display(),
            command,
            host,
            port
        );
        format!(
            "' Generated by sinomo install.\r\nDim shell\r\nSet shell = CreateObject(\"WScript.Shell\")\r\nDim env\r\nSet env = shell.Environment(\"Process\")\r\nenv(\"SINOMO_DATA_DIR\") = \"{}\"\r\n{}shell.Run \"{}\", 0, False\r\n",
            vbs_string_literal(&data_dir.display().to_string()),
            extra_env,
            vbs_string_literal(&command_line)
        )
    }
    #[cfg(not(windows))]
    {
        let kill_mode = match child_process_policy {
            BootEntryChildProcessPolicy::SystemdDefault => "",
            BootEntryChildProcessPolicy::PreserveDetachedChildren => "KillMode=process\n",
        };
        let extra_env = extra_env
            .iter()
            .map(|(key, value)| format!("Environment={key}={value}\n"))
            .collect::<String>();
        let port = if port_fallback {
            String::new()
        } else {
            format!(" --port {port}")
        };
        format!(
            "[Unit]\nDescription={description}\n\n[Service]\n{kill_mode}Environment=SINOMO_DATA_DIR={}\n{}ExecStart={} {} --host {}{}\nRestart=on-failure\n\n[Install]\nWantedBy=default.target\n",
            data_dir.display(),
            extra_env,
            binary.display(),
            command,
            host,
            port
        )
    }
}

#[cfg(windows)]
fn vbs_string_literal(value: &str) -> String {
    value.replace('"', "\"\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn windows_install_children_are_hidden_and_still_capture_output() {
        let output = install_child_command("cmd")
            .args(["/d", "/c", "echo", "sinomo-hidden-install-child"])
            .output()
            .expect("run hidden install child");

        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("sinomo-hidden-install-child"));
    }

    #[test]
    fn installed_daemon_start_specs_describe_main_and_shadow_daemons() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-install-specs");
        let data_dir = SinomoDataDir::new(&root);
        let layout = InstallLayout::new(&data_dir, root.join("startup"));

        let [main, shadow] =
            installed_daemon_start_specs(&layout, "127.0.0.1", 21400, 21401, false, false);

        assert_eq!(main.binary, layout.binary);
        assert_eq!(main.label, "daemon");
        assert_eq!(
            main.args,
            ["daemon", "run", "--host", "127.0.0.1", "--port", "21400"]
        );
        assert_eq!(main.paths, DaemonPaths::with_root(root.join("daemon")));
        assert!(main.extra_env.is_empty());

        assert_eq!(shadow.binary, layout.shadow_binary);
        assert_eq!(shadow.label, "shadow daemon");
        assert_eq!(
            shadow.args,
            [
                "daemon",
                "shadow",
                "run",
                "--host",
                "127.0.0.1",
                "--port",
                "21401",
            ]
        );
        assert_eq!(
            shadow.paths,
            DaemonPaths::with_root(root.join("shadow-daemon"))
        );
        assert_eq!(
            shadow.extra_env,
            vec![
                (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
                (SHADOW_MAIN_HOST_ENV, "127.0.0.1".to_string()),
                (SHADOW_MAIN_PORT_ENV, "21400".to_string()),
            ]
        );
    }

    #[test]
    fn installed_default_ports_remain_automatic_in_process_and_boot_specs() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-install-auto-ports");
        let data_dir = SinomoDataDir::new(&root);
        let layout = InstallLayout::new(&data_dir, root.join("startup"));

        let [main, shadow] =
            installed_daemon_start_specs(&layout, "0.0.0.0", 21353, 21354, true, true);
        assert_eq!(main.args, ["daemon", "run", "--host", "0.0.0.0"]);
        assert_eq!(
            shadow.args,
            ["daemon", "shadow", "run", "--host", "0.0.0.0"]
        );
        assert!(
            shadow
                .extra_env
                .iter()
                .all(|(name, _)| *name != SHADOW_MAIN_PORT_ENV)
        );

        write_install_boot_entries(&layout, "0.0.0.0", 21353, 21354, true, true)
            .expect("write automatic-port boot entries");
        let main = fs::read_to_string(&layout.main_boot_entry).expect("read main boot entry");
        let shadow = fs::read_to_string(&layout.shadow_boot_entry).expect("read shadow boot entry");
        assert!(!main.contains("--port"), "{main}");
        assert!(!shadow.contains("--port"), "{shadow}");
        assert!(!shadow.contains(SHADOW_MAIN_PORT_ENV), "{shadow}");
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_shadow_boot_entry_preserves_detached_rollout_children() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-shadow-kill-mode");
        let data_dir = SinomoDataDir::new(&root);
        let layout = InstallLayout::new(&data_dir, root.join("startup"));

        write_install_boot_entries(&layout, "0.0.0.0", 21353, 21354, false, false)
            .expect("write boot entries");

        let main = fs::read_to_string(&layout.main_boot_entry).expect("read main boot entry");
        let shadow = fs::read_to_string(&layout.shadow_boot_entry).expect("read shadow boot entry");
        assert!(main.contains("[Service]\nEnvironment=SINOMO_DATA_DIR="));
        assert!(!main.contains("KillMode="));
        assert!(shadow.contains("[Service]\nKillMode=process\nEnvironment=SINOMO_DATA_DIR="));
        assert_eq!(shadow.matches("KillMode=process").count(), 1);
    }

    #[test]
    fn windows_installed_daemon_does_not_inherit_invoker_cwd() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-installed-stable-cwd");
        let data_dir = root.join("data");
        let binary = root.join("bin").join("sinomo");
        let args = vec!["daemon".to_string(), "run".to_string()];

        let command = installed_daemon_command(&binary, &data_dir, &args, &[]);

        assert_eq!(command.get_current_dir(), Some(data_dir.as_path()));
    }

    #[test]
    fn installed_daemon_install_stop_reason_prioritizes_binary_updates() {
        assert_eq!(
            installed_daemon_install_stop_reason(true, true),
            Some("binary update")
        );
        assert_eq!(
            installed_daemon_install_stop_reason(false, true),
            Some("install option update")
        );
        assert_eq!(installed_daemon_install_stop_reason(false, false), None);
    }

    #[cfg(not(windows))]
    #[test]
    fn ensure_local_bin_symlink_creates_user_bin_link_to_installed_binary() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-local-bin-link");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let layout = InstallLayout::new(&data_dir, root.join("startup"));
        std::fs::create_dir_all(&layout.bin_dir).expect("create installed bin dir");
        std::fs::write(&layout.binary, b"installed sinomo").expect("write installed binary");

        let home = root.join("home");
        let status = ensure_local_bin_symlink_in_home(&layout, &home).expect("local bin symlink");
        let link = home.join(".local").join("bin").join("sinomo");

        assert_eq!(
            status,
            LocalBinSymlinkStatus::Created { link: link.clone() }
        );
        assert_eq!(std::fs::read_link(&link).expect("read link"), layout.binary);
    }

    #[cfg(not(windows))]
    #[test]
    fn ensure_local_bin_symlink_refreshes_stale_symlink() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-local-bin-refresh");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let layout = InstallLayout::new(&data_dir, root.join("startup"));
        std::fs::create_dir_all(&layout.bin_dir).expect("create installed bin dir");
        std::fs::write(&layout.binary, b"installed sinomo").expect("write installed binary");

        let home = root.join("home");
        let link = home.join(".local").join("bin").join("sinomo");
        std::fs::create_dir_all(link.parent().expect("link parent")).expect("create user bin");
        std::os::unix::fs::symlink(root.join("old").join("sinomo"), &link)
            .expect("stale local bin symlink");

        let status = ensure_local_bin_symlink_in_home(&layout, &home).expect("local bin symlink");

        assert_eq!(
            status,
            LocalBinSymlinkStatus::Created { link: link.clone() }
        );
        assert_eq!(std::fs::read_link(&link).expect("read link"), layout.binary);
    }

    #[test]
    fn daemon_run_options_from_args_parses_main_and_shadow_specs() {
        let main = daemon_run_options_from_args(&[
            "daemon".to_string(),
            "run".to_string(),
            "--host".to_string(),
            "0.0.0.0".to_string(),
            "--port".to_string(),
            "21353".to_string(),
        ])
        .expect("main options");
        assert_eq!(main.host, "0.0.0.0");
        assert_eq!(main.port, 21353);

        let shadow = daemon_run_options_from_args(&[
            "daemon".to_string(),
            "shadow".to_string(),
            "run".to_string(),
            "--host".to_string(),
            "127.0.0.1".to_string(),
            "--port".to_string(),
            "21354".to_string(),
        ])
        .expect("shadow options");
        assert_eq!(shadow.host, "127.0.0.1");
        assert_eq!(shadow.port, 21354);

        assert!(daemon_run_options_from_args(&["daemon".to_string(), "run".to_string()]).is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn installed_daemon_cmdline_matches_exact_installed_command() {
        let binary = Path::new("/home/test/.sinomo/bin/sinomo");
        let args = vec![
            "daemon".to_string(),
            "run".to_string(),
            "--host".to_string(),
            "0.0.0.0".to_string(),
            "--port".to_string(),
            "21353".to_string(),
        ];
        let mut cmdline = vec![binary.display().to_string()];
        cmdline.extend(args.clone());

        assert!(installed_daemon_cmdline_matches(&cmdline, binary, &args));

        let mut wrong_port = cmdline.clone();
        *wrong_port.last_mut().expect("port") = "21354".to_string();
        assert!(!installed_daemon_cmdline_matches(
            &wrong_port,
            binary,
            &args
        ));
    }

    #[test]
    fn installed_daemon_install_plan_describes_non_starting_install() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-install-plan");
        let data_dir = SinomoDataDir::new(&root);
        let layout = InstallLayout::new(&data_dir, root.join("startup"));

        let plan = prepare_installed_daemon_install_plan(
            &layout,
            InstalledDaemonInstallPlanRequest {
                host: "127.0.0.1".to_string(),
                port: 21400,
                shadow_port: 21401,
                port_fallback: false,
                shadow_port_fallback: false,
                start: false,
            },
            Duration::from_millis(1),
        )
        .expect("plan");

        assert!(!plan.existing_install);
        assert_eq!(
            plan.daemon_paths.main,
            DaemonPaths::with_root(root.join("daemon"))
        );
        assert_eq!(
            plan.daemon_paths.shadow,
            DaemonPaths::with_root(root.join("shadow-daemon"))
        );
        assert!(!plan.updating_binary);
        assert!(!plan.restarting_for_options);
        assert_eq!(plan.stop_reason, None);
        assert!(plan.start_specs.is_empty());
    }
}
