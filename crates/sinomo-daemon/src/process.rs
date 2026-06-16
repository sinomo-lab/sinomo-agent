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

pub const SHADOW_ROLLOUT_RECOVERY_HELPER_ARGS: &[&str] = &["daemon", "__complete-shadow-rollout"];

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

pub(crate) fn build_daemon_async_runtime(
    label: &str,
) -> Result<tokio::runtime::Runtime, DaemonError> {
    let worker_threads = daemon_async_runtime_worker_threads();
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(format!("sinomo-{label}-async"))
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .map_err(|source| DaemonError::Io {
            path: format!("build sinomo {label} async runtime"),
            detail: source.to_string(),
        })
}

fn daemon_async_runtime_worker_threads() -> usize {
    const MAX_WORKER_THREADS: usize = 64;
    env::var("SINOMO_DAEMON_WORKER_THREADS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_WORKER_THREADS))
        .unwrap_or_else(default_daemon_async_runtime_worker_threads)
}

fn default_daemon_async_runtime_worker_threads() -> usize {
    const MIN_DEFAULT_WORKER_THREADS: usize = 4;
    const MAX_DEFAULT_WORKER_THREADS: usize = 16;
    std::thread::available_parallelism()
        .map(|count| {
            count
                .get()
                .clamp(MIN_DEFAULT_WORKER_THREADS, MAX_DEFAULT_WORKER_THREADS)
        })
        .unwrap_or(MIN_DEFAULT_WORKER_THREADS)
}

/// Stdio for the supervised main daemon's stdout/stderr. Captures to
/// `<SINOMO_DATA_DIR>/daemon/<name>` (appended) instead of discarding to `/dev/null`, so
/// panics and errors raised while handling requests (e.g. a rollout activation) survive
/// for diagnosis. Falls back to a null sink when no data dir is set or the file can't be
/// opened, so spawning never fails on account of logging.
fn supervised_main_daemon_log_stdio(name: &str) -> Stdio {
    let Some(data_dir) = env::var_os("SINOMO_DATA_DIR") else {
        return Stdio::null();
    };
    let dir = PathBuf::from(data_dir).join("daemon");
    if fs::create_dir_all(&dir).is_err() {
        return Stdio::null();
    }
    match File::options()
        .create(true)
        .append(true)
        .open(dir.join(name))
    {
        Ok(file) => Stdio::from(file),
        Err(_) => Stdio::null(),
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
        .stdout(supervised_main_daemon_log_stdio("main.stdout.log"))
        .stderr(supervised_main_daemon_log_stdio("main.stderr.log"));
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

pub fn spawn_shadow_rollout_recovery_helper_process() -> Result<(), DaemonError> {
    let exe = env::current_exe().map_err(|source| DaemonError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })?;
    spawn_detached_no_inherit(&exe, SHADOW_ROLLOUT_RECOVERY_HELPER_ARGS)
}

#[cfg(not(windows))]
fn spawn_detached_no_inherit(binary: &Path, args: &[&str]) -> Result<(), DaemonError> {
    let mut command = Command::new(binary);
    prepare_background_command(&mut command);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let _child = command.spawn().map_err(|source| DaemonError::Io {
        path: binary.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(())
}

#[cfg(windows)]
fn spawn_detached_no_inherit(binary: &Path, args: &[&str]) -> Result<(), DaemonError> {
    use std::ffi::c_void;
    use std::mem::{size_of, zeroed};
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::{null, null_mut};

    type Bool = i32;
    type Dword = u32;
    type Handle = *mut c_void;
    type Lpcwstr = *const u16;
    type Lpwstr = *mut u16;
    type Lpvoid = *mut c_void;

    #[repr(C)]
    struct StartupInfoW {
        cb: Dword,
        lp_reserved: Lpwstr,
        lp_desktop: Lpwstr,
        lp_title: Lpwstr,
        dw_x: Dword,
        dw_y: Dword,
        dw_x_size: Dword,
        dw_y_size: Dword,
        dw_x_count_chars: Dword,
        dw_y_count_chars: Dword,
        dw_fill_attribute: Dword,
        dw_flags: Dword,
        w_show_window: u16,
        cb_reserved2: u16,
        lp_reserved2: *mut u8,
        h_std_input: Handle,
        h_std_output: Handle,
        h_std_error: Handle,
    }

    #[repr(C)]
    struct ProcessInformation {
        h_process: Handle,
        h_thread: Handle,
        dw_process_id: Dword,
        dw_thread_id: Dword,
    }

    unsafe extern "system" {
        fn CreateProcessW(
            lpApplicationName: Lpcwstr,
            lpCommandLine: Lpwstr,
            lpProcessAttributes: Lpvoid,
            lpThreadAttributes: Lpvoid,
            bInheritHandles: Bool,
            dwCreationFlags: Dword,
            lpEnvironment: Lpvoid,
            lpCurrentDirectory: Lpcwstr,
            lpStartupInfo: *mut StartupInfoW,
            lpProcessInformation: *mut ProcessInformation,
        ) -> Bool;
        fn CloseHandle(hObject: Handle) -> Bool;
    }

    const CREATE_NEW_PROCESS_GROUP: Dword = 0x0000_0200;
    const DETACHED_PROCESS: Dword = 0x0000_0008;
    const CREATE_NO_WINDOW: Dword = 0x0800_0000;
    const CREATE_UNICODE_ENVIRONMENT: Dword = 0x0000_0400;

    let mut command_line = windows_command_line(binary, args);
    let mut startup: StartupInfoW = unsafe { zeroed() };
    startup.cb = size_of::<StartupInfoW>() as Dword;
    let mut process_info: ProcessInformation = unsafe { zeroed() };
    let flags =
        CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS | CREATE_NO_WINDOW | CREATE_UNICODE_ENVIRONMENT;
    let created = unsafe {
        CreateProcessW(
            null(),
            command_line.as_mut_ptr(),
            null_mut(),
            null_mut(),
            0,
            flags,
            null_mut(),
            null(),
            &mut startup,
            &mut process_info,
        )
    };
    if created == 0 {
        return Err(DaemonError::Io {
            path: binary.display().to_string(),
            detail: std::io::Error::last_os_error().to_string(),
        });
    }
    unsafe {
        let _ = CloseHandle(process_info.h_thread);
        let _ = CloseHandle(process_info.h_process);
    }
    Ok(())
}

#[cfg(windows)]
fn windows_command_line(binary: &Path, args: &[&str]) -> Vec<u16> {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;

    let mut command = windows_quote_arg(&binary.as_os_str().to_string_lossy());
    for arg in args {
        command.push(' ');
        command.push_str(&windows_quote_arg(arg));
    }
    OsStr::new(&command).encode_wide().chain([0]).collect()
}

#[cfg(windows)]
fn windows_quote_arg(arg: &str) -> String {
    if !arg.is_empty()
        && !arg
            .chars()
            .any(|ch| matches!(ch, ' ' | '\t' | '\n' | '\r' | '"'))
    {
        return arg.to_string();
    }
    let mut quoted = String::from("\"");
    let mut backslashes = 0usize;
    for ch in arg.chars() {
        match ch {
            '\\' => backslashes += 1,
            '"' => {
                quoted.extend(std::iter::repeat_n('\\', backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                quoted.extend(std::iter::repeat_n('\\', backslashes));
                backslashes = 0;
                quoted.push(ch);
            }
        }
    }
    quoted.extend(std::iter::repeat_n('\\', backslashes * 2));
    quoted.push('"');
    quoted
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
