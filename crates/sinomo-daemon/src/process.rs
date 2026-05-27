use crate::{
    DaemonError, DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonStatus,
    daemon_connect_address, probe_daemon_status,
};
use sinomo_rollout::{SelfRolloutPaths, rollout_artifact_suffix};
use std::env;
use std::fs::{self, File};
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct CandidateMainDiagnostics {
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
    pub diagnostics_path: PathBuf,
}

pub fn prepare_background_command(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        unsafe extern "C" {
            fn setsid() -> i32;
        }

        unsafe {
            command.pre_exec(|| {
                if setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW | DETACHED_PROCESS);
    }
}

pub fn spawn_main_daemon_process(
    binary: &Path,
    options: &DaemonRunOptions,
) -> Result<Child, DaemonError> {
    spawn_main_daemon_process_with_env(binary, options, &[])
}

pub fn spawn_main_daemon_process_with_env(
    binary: &Path,
    options: &DaemonRunOptions,
    extra_env: &[(&str, String)],
) -> Result<Child, DaemonError> {
    let mut command = Command::new(binary);
    prepare_background_command(&mut command);
    command
        .arg("daemon")
        .arg("run")
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(options.port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command.spawn().map_err(|source| DaemonError::Io {
        path: binary.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn spawn_candidate_main_daemon_process(
    binary: &Path,
    options: &DaemonRunOptions,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(Child, CandidateMainDiagnostics), DaemonError> {
    let logs_dir = rollout_paths.root.join("logs");
    fs::create_dir_all(&logs_dir).map_err(|source| DaemonError::Io {
        path: logs_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    let suffix = rollout_artifact_suffix();
    let stdout_log = logs_dir.join(format!("candidate-main-{suffix}.stdout.log"));
    let stderr_log = logs_dir.join(format!("candidate-main-{suffix}.stderr.log"));
    let diagnostics_path = logs_dir.join(format!("candidate-main-{suffix}.diagnostics.json"));
    let stdout = File::create(&stdout_log).map_err(|source| DaemonError::Io {
        path: stdout_log.display().to_string(),
        detail: source.to_string(),
    })?;
    let stderr = File::create(&stderr_log).map_err(|source| DaemonError::Io {
        path: stderr_log.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut command = Command::new(binary);
    prepare_background_command(&mut command);
    let child = command
        .arg("daemon")
        .arg("run")
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(options.port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|source| DaemonError::Io {
            path: binary.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok((
        child,
        CandidateMainDiagnostics {
            stdout_log,
            stderr_log,
            diagnostics_path,
        },
    ))
}

pub fn main_exit_observation_from_status(
    status: ExitStatus,
) -> sinomo_rollout::MainExitObservation {
    #[cfg(unix)]
    {
        if let Some(signal) = status.signal() {
            return sinomo_rollout::MainExitObservation::signal(signal);
        }
    }
    sinomo_rollout::MainExitObservation::exit_code(status.code().unwrap_or(1))
}

pub fn start_daemon_command_process(
    options: &DaemonRunOptions,
    paths: &DaemonPaths,
    command_prefix: &[&str],
    label: &str,
    extra_env: &[(&str, String)],
    ready_timeout: Duration,
    probe_timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    if matches!(
        probe_daemon_status(paths, probe_timeout),
        DaemonProbeStatus::Running(_) | DaemonProbeStatus::Locked { .. }
    ) {
        return Err(DaemonError::AlreadyRunning {
            lock_file: paths.lock.display().to_string(),
            detail: "daemon lock is already held".to_string(),
        });
    }

    let exe = env::current_exe().map_err(|source| DaemonError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    let mut command = Command::new(exe);
    command.args(command_prefix);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    prepare_background_command(&mut command);
    let child = command
        .arg("--host")
        .arg(&options.host)
        .arg("--port")
        .arg(options.port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| DaemonError::Io {
            path: format!("sinomo {label} start"),
            detail: source.to_string(),
        })?;

    let deadline = Instant::now() + ready_timeout;
    while Instant::now() < deadline {
        if let DaemonProbeStatus::Running(status) = probe_daemon_status(paths, probe_timeout) {
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(100));
    }

    Err(DaemonError::Network {
        address: daemon_connect_address(&options.host, options.port),
        detail: format!("daemon process {} did not become ready", child.id()),
    })
}
