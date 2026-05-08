use crate::rollout::{SHADOW_MAIN_BINARY_ENV, SHADOW_MAIN_HOST_ENV, SHADOW_MAIN_PORT_ENV};
use crate::{
    DaemonError, DaemonPaths, daemon_probe_status_needs_restart_for_options, probe_daemon_status,
    send_daemon_shutdown_request,
};
use sinomo_local::SinomoDataDir;
use sinomo_rollout::{
    SelfRolloutPaths, current_executable_should_update, deploy_managed_source_repo,
    discover_current_git_repo, ensure_rollout_layout,
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
    Started(crate::DaemonStatus),
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

pub fn installed_daemon_start_specs(
    layout: &InstallLayout,
    host: &str,
    main_port: u16,
    shadow_port: u16,
) -> [InstalledDaemonStartSpec; 2] {
    let daemon_paths = installed_daemon_paths(layout);
    [
        InstalledDaemonStartSpec {
            binary: layout.binary.clone(),
            args: vec![
                "daemon".to_string(),
                "run".to_string(),
                "--host".to_string(),
                host.to_string(),
                "--port".to_string(),
                main_port.to_string(),
            ],
            paths: daemon_paths.main,
            label: "daemon",
            extra_env: Vec::new(),
        },
        InstalledDaemonStartSpec {
            binary: layout.shadow_binary.clone(),
            args: vec![
                "daemon".to_string(),
                "shadow".to_string(),
                "run".to_string(),
                "--host".to_string(),
                host.to_string(),
                "--port".to_string(),
                shadow_port.to_string(),
            ],
            paths: daemon_paths.shadow,
            label: "shadow daemon",
            extra_env: vec![
                (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
                (SHADOW_MAIN_HOST_ENV, host.to_string()),
                (SHADOW_MAIN_PORT_ENV, main_port.to_string()),
            ],
        },
    ]
}

pub fn prepare_installed_daemon_install_plan(
    layout: &InstallLayout,
    request: InstalledDaemonInstallPlanRequest,
    probe_timeout: Duration,
) -> Result<InstalledDaemonInstallPlan, DaemonError> {
    let daemon_paths = installed_daemon_paths(layout);
    let updating_binary = current_executable_should_update(&layout.binary)?
        || current_executable_should_update(&layout.shadow_binary)?;
    let restarting_for_options = request.start
        && installed_daemons_need_restart(
            &request.host,
            request.port,
            request.shadow_port,
            &daemon_paths.main,
            &daemon_paths.shadow,
            probe_timeout,
        );
    let stop_reason = installed_daemon_install_stop_reason(updating_binary, restarting_for_options);
    let start_specs = if request.start {
        installed_daemon_start_specs(layout, &request.host, request.port, request.shadow_port)
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

pub fn installed_daemon_install_stop_reason(
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

pub fn install_layout_exists(layout: &InstallLayout) -> bool {
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
    ensure_rollout_layout(&rollout_paths)?;
    let Some(source_repo) = discover_current_git_repo()? else {
        return Ok(ManagedSourceRepoInstallStatus::CurrentDirectoryNotInGitCheckout);
    };
    deploy_managed_source_repo(&rollout_paths, &source_repo)?;
    Ok(ManagedSourceRepoInstallStatus::Updated)
}

pub fn git_is_available() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

pub fn managed_source_git_status_lines(repo: &Path) -> Result<Vec<String>, DaemonError> {
    if !repo.join(".git").exists() {
        return Ok(Vec::new());
    }
    let output = Command::new("git")
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
    let user_bin = PathBuf::from(home).join(".local").join("bin");
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
pub fn local_bin_symlink_is_current(link: &Path, target: &Path) -> bool {
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
    let output = Command::new("powershell")
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
pub fn systemctl_user_best_effort(args: &[&str]) -> SystemctlUserOutcome {
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

pub fn installed_daemons_need_restart(
    host: &str,
    main_port: u16,
    shadow_port: u16,
    main_paths: &DaemonPaths,
    shadow_paths: &DaemonPaths,
    timeout: Duration,
) -> bool {
    daemon_probe_status_needs_restart_for_options(
        probe_daemon_status(main_paths, timeout),
        host,
        main_port,
    ) || daemon_probe_status_needs_restart_for_options(
        probe_daemon_status(shadow_paths, timeout),
        host,
        shadow_port,
    )
}

pub fn stop_daemon_if_running(
    paths: &DaemonPaths,
    timeout: Duration,
) -> Result<DaemonStopIfRunningStatus, DaemonError> {
    match probe_daemon_status(paths, timeout) {
        crate::DaemonProbeStatus::Running(status) => {
            let _ = send_daemon_shutdown_request(&status.address, timeout)?;
            Ok(DaemonStopIfRunningStatus::StopRequested)
        }
        crate::DaemonProbeStatus::Locked { .. } => Ok(DaemonStopIfRunningStatus::Locked),
        crate::DaemonProbeStatus::NotRunning => Ok(DaemonStopIfRunningStatus::NotRunning),
    }
}

pub fn stop_daemon_before_install(
    paths: &DaemonPaths,
    wait_timeout: Duration,
    probe_timeout: Duration,
) -> Result<DaemonStopBeforeInstallStatus, DaemonError> {
    let mut next_shutdown_nudge = Instant::now();
    let start = match probe_daemon_status(paths, probe_timeout) {
        crate::DaemonProbeStatus::Running(status) => {
            match send_daemon_shutdown_request(&status.address, probe_timeout) {
                Ok(_) => DaemonStopBeforeInstallStart::StopRequested,
                Err(error) => {
                    if matches!(
                        probe_daemon_status(paths, probe_timeout),
                        crate::DaemonProbeStatus::NotRunning
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
        crate::DaemonProbeStatus::Locked { .. } => DaemonStopBeforeInstallStart::Locked,
        crate::DaemonProbeStatus::NotRunning => {
            return Ok(DaemonStopBeforeInstallStatus::NotRunning);
        }
    };

    let deadline = Instant::now() + wait_timeout;
    while Instant::now() < deadline {
        match probe_daemon_status(paths, probe_timeout) {
            crate::DaemonProbeStatus::NotRunning => {
                return Ok(DaemonStopBeforeInstallStatus::Stopped { start });
            }
            crate::DaemonProbeStatus::Running(status) if Instant::now() >= next_shutdown_nudge => {
                let _ = send_daemon_shutdown_request(&status.address, probe_timeout);
                next_shutdown_nudge = Instant::now() + Duration::from_secs(1);
            }
            crate::DaemonProbeStatus::Running(_) | crate::DaemonProbeStatus::Locked { .. } => {}
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
    let Ok(status) = crate::read_daemon_status(paths) else {
        return Ok(None);
    };
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
                crate::DaemonProbeStatus::NotRunning
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
    let mut command = Command::new(binary);
    command.args(args).env("SINOMO_DATA_DIR", data_dir);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    crate::prepare_background_command(&mut command);
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| DaemonError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })?;

    let deadline = Instant::now() + ready_timeout;
    while Instant::now() < deadline {
        if let crate::DaemonProbeStatus::Running(status) = probe_daemon_status(paths, probe_timeout)
        {
            return Ok(InstalledDaemonStartStatus::Started(status));
        }
        if let Some(status) = child.try_wait().map_err(|source| DaemonError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })? {
            return Ok(InstalledDaemonStartStatus::ExitedBeforeReady {
                command: format!("{} {}", binary.display(), args.join(" ")),
                status: status.code().unwrap_or(-1),
            });
        }
        thread::sleep(Duration::from_millis(100));
    }

    Ok(InstalledDaemonStartStatus::TimedOut {
        status_path: paths.status.clone(),
        child_id: child.id(),
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

pub fn boot_entry_path(boot_dir: &Path, name: &str) -> PathBuf {
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

pub fn write_boot_entry(
    path: &Path,
    data_dir: &Path,
    binary: &Path,
    command_prefix: &[&str],
    host: &str,
    port: u16,
    description: &str,
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
        description,
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
        "Sinomo daemon",
        &[],
    )?;
    let shadow_env = vec![
        (SHADOW_MAIN_BINARY_ENV, layout.binary.display().to_string()),
        (SHADOW_MAIN_HOST_ENV, host.to_string()),
        (SHADOW_MAIN_PORT_ENV, main_port.to_string()),
    ];
    write_boot_entry(
        &layout.shadow_boot_entry,
        &layout.data_dir,
        &layout.shadow_binary,
        &shadow_command_prefix,
        host,
        shadow_port,
        "Sinomo shadow daemon",
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

pub fn boot_entry_content(
    data_dir: &Path,
    binary: &Path,
    command_prefix: &[&str],
    host: &str,
    port: u16,
    description: &str,
    extra_env: &[(&str, String)],
) -> String {
    let command = command_prefix.join(" ");
    #[cfg(windows)]
    {
        let _ = description;
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
        let command_line = format!(
            "\"{}\" {} --host {} --port {}",
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
        let extra_env = extra_env
            .iter()
            .map(|(key, value)| format!("Environment={key}={value}\n"))
            .collect::<String>();
        format!(
            "[Unit]\nDescription={description}\n\n[Service]\nEnvironment=SINOMO_DATA_DIR={}\n{}ExecStart={} {} --host {} --port {}\nRestart=on-failure\n\n[Install]\nWantedBy=default.target\n",
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn installed_daemon_start_specs_describe_main_and_shadow_daemons() {
        let root = env::temp_dir().join(format!(
            "sinomo-daemon-install-specs-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let layout = InstallLayout::new(&data_dir, root.join("startup"));

        let [main, shadow] = installed_daemon_start_specs(&layout, "127.0.0.1", 21400, 21401);

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

    #[test]
    fn installed_daemon_install_plan_describes_non_starting_install() {
        let root = env::temp_dir().join(format!(
            "sinomo-daemon-install-plan-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let layout = InstallLayout::new(&data_dir, root.join("startup"));

        let plan = prepare_installed_daemon_install_plan(
            &layout,
            InstalledDaemonInstallPlanRequest {
                host: "127.0.0.1".to_string(),
                port: 21400,
                shadow_port: 21401,
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
