use crate::{
    DaemonError, DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonStatus,
    daemon_connect_address, probe_daemon_status,
};
use sinomo_rollout::{SelfRolloutPaths, rollout_artifact_suffix};
use std::env;
use std::fs::{self, File};
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub const SHADOW_ROLLOUT_RECOVERY_HELPER_ARGS: &[&str] = &["daemon", "__complete-shadow-rollout"];

#[derive(Debug)]
pub struct PreparedDaemonControlListener {
    options: DaemonRunOptions,
    listener: TcpListener,
    fallback_from_port: Option<u16>,
}

impl PreparedDaemonControlListener {
    pub fn options(&self) -> &DaemonRunOptions {
        &self.options
    }

    pub fn fallback_from_port(&self) -> Option<u16> {
        self.fallback_from_port
    }

    pub(crate) fn into_parts(self) -> (DaemonRunOptions, TcpListener, Option<u16>) {
        (self.options, self.listener, self.fallback_from_port)
    }
}

pub fn prepare_daemon_control_listener(
    mut options: DaemonRunOptions,
) -> Result<PreparedDaemonControlListener, DaemonError> {
    let requested_port = options.port;
    let listener = match TcpListener::bind((options.host.as_str(), requested_port)) {
        Ok(listener) => listener,
        Err(source) if options.port_fallback && daemon_port_bind_conflict(&source) => {
            let listener = TcpListener::bind((options.host.as_str(), 0)).map_err(|fallback| {
                DaemonError::Network {
                    address: format!("{}:{requested_port}", options.host),
                    detail: format!(
                        "preferred daemon port is unavailable ({source}); automatic fallback also failed: {fallback}"
                    ),
                }
            })?;
            options.port = listener
                .local_addr()
                .map_err(|source| DaemonError::Network {
                    address: format!("{}:0", options.host),
                    detail: format!("read automatic daemon fallback port: {source}"),
                })?
                .port();
            listener
        }
        Err(source) => {
            return Err(DaemonError::Network {
                address: format!("{}:{requested_port}", options.host),
                detail: format!("bind daemon control listener: {source}"),
            });
        }
    };
    listener
        .set_nonblocking(true)
        .map_err(|source| DaemonError::Network {
            address: format!("{}:{}", options.host, options.port),
            detail: format!("prepare daemon control listener: {source}"),
        })?;
    Ok(PreparedDaemonControlListener {
        fallback_from_port: (options.port != requested_port).then_some(requested_port),
        options,
        listener,
    })
}

fn daemon_port_bind_conflict(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::AddrInUse
        || cfg!(windows) && error.kind() == std::io::ErrorKind::PermissionDenied
}

#[cfg(test)]
mod port_fallback_tests {
    use super::*;

    #[test]
    fn automatic_port_fallback_holds_an_os_selected_listener() {
        let occupied = TcpListener::bind(("127.0.0.1", 0)).expect("occupy preferred port");
        let preferred = occupied.local_addr().expect("occupied address").port();
        let prepared = prepare_daemon_control_listener(DaemonRunOptions {
            host: "127.0.0.1".to_string(),
            port: preferred,
            port_fallback: true,
        })
        .expect("fallback listener");

        assert_eq!(prepared.fallback_from_port(), Some(preferred));
        assert_ne!(prepared.options().port, preferred);
        assert!(TcpListener::bind(("127.0.0.1", prepared.options().port)).is_err());
    }

    #[test]
    fn strict_port_conflict_fails_without_fallback() {
        let occupied = TcpListener::bind(("127.0.0.1", 0)).expect("occupy strict port");
        let preferred = occupied.local_addr().expect("occupied address").port();
        let error = prepare_daemon_control_listener(DaemonRunOptions {
            host: "127.0.0.1".to_string(),
            port: preferred,
            port_fallback: false,
        })
        .expect_err("strict port must fail");

        assert!(error.to_string().contains("bind daemon control listener"));
    }
}

/// Resource shape for a daemon runtime. This is intentionally separate from
/// [`DaemonRunOptions`], which describes the network-visible listener and is
/// persisted in daemon status files.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DaemonExecutionProfile {
    /// Normal workstation/server daemon sizing.
    #[default]
    Standard,
    /// Lean embedded runtime for battery- and memory-constrained mobile apps.
    Mobile,
}

/// Process-local runtime inputs supplied by the daemon host.
///
/// Listener configuration remains in [`DaemonRunOptions`]. This value carries
/// only host resource policy and live, advisory node conditions; it is never
/// persisted or treated as cluster authority.
#[derive(Clone, Debug)]
pub struct DaemonRuntimeConfig {
    execution_profile: DaemonExecutionProfile,
    node_resources: sinomo_device::NodeResourceMonitor,
}

impl Default for DaemonRuntimeConfig {
    fn default() -> Self {
        Self::new(DaemonExecutionProfile::Standard)
    }
}

impl DaemonRuntimeConfig {
    pub fn new(execution_profile: DaemonExecutionProfile) -> Self {
        Self {
            execution_profile,
            node_resources: sinomo_device::NodeResourceMonitor::default(),
        }
    }

    pub fn with_node_resources(
        mut self,
        node_resources: sinomo_device::NodeResourceMonitor,
    ) -> Self {
        self.node_resources = node_resources;
        self
    }

    pub fn execution_profile(&self) -> DaemonExecutionProfile {
        self.execution_profile
    }

    pub fn node_resources(&self) -> sinomo_device::NodeResourceMonitor {
        self.node_resources.clone()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DaemonRuntimeLimits {
    worker_threads: usize,
    max_blocking_threads: Option<usize>,
}

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

/// Suppresses a transient Windows console for captured, non-interactive
/// subprocesses.
#[cfg(windows)]
pub fn prepare_noninteractive_command(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
}

pub(crate) fn build_daemon_async_runtime(
    label: &str,
) -> Result<tokio::runtime::Runtime, DaemonError> {
    build_daemon_async_runtime_with_profile(label, DaemonExecutionProfile::Standard)
}

pub(crate) fn build_daemon_async_runtime_with_profile(
    label: &str,
    profile: DaemonExecutionProfile,
) -> Result<tokio::runtime::Runtime, DaemonError> {
    let limits = daemon_async_runtime_limits(profile);
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .thread_name(format!("sinomo-{label}-async"))
        .worker_threads(limits.worker_threads)
        .enable_all();
    if let Some(max_blocking_threads) = limits.max_blocking_threads {
        builder.max_blocking_threads(max_blocking_threads);
    }
    builder.build().map_err(|source| DaemonError::Io {
        path: format!("build sinomo {label} async runtime"),
        detail: source.to_string(),
    })
}

fn daemon_async_runtime_limits(profile: DaemonExecutionProfile) -> DaemonRuntimeLimits {
    match profile {
        DaemonExecutionProfile::Standard => DaemonRuntimeLimits {
            worker_threads: daemon_async_runtime_worker_threads(),
            // Preserve Tokio's existing desktop/server blocking-pool default.
            max_blocking_threads: None,
        },
        DaemonExecutionProfile::Mobile => DaemonRuntimeLimits {
            worker_threads: 2,
            // The default Tokio blocking pool may grow to hundreds of threads.
            // Mobile work is expected to queue behind a small bounded pool.
            max_blocking_threads: Some(8),
        },
    }
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
    command.arg("daemon").arg("run");
    append_daemon_endpoint_args(&mut command, options);
    command
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
    command.arg("daemon").arg("run");
    append_daemon_endpoint_args(&mut command, options);
    let child = command
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
    spawn_shadow_rollout_completion_helper_process(&exe)
}

pub fn spawn_shadow_rollout_completion_helper_process(binary: &Path) -> Result<(), DaemonError> {
    spawn_detached_no_inherit(binary, SHADOW_ROLLOUT_RECOVERY_HELPER_ARGS)
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

pub fn daemon_main_binary_for_shadow_start(invoked_exe: &Path) -> PathBuf {
    if daemon_binary_stem(invoked_exe) == Some("sinomo-shadow") {
        sibling_daemon_binary(invoked_exe, "sinomo").unwrap_or_else(|| invoked_exe.to_path_buf())
    } else {
        invoked_exe.to_path_buf()
    }
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
    let exe = daemon_start_executable_for_command(&exe, command_prefix);
    let mut command = Command::new(exe);
    command.args(command_prefix);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    prepare_background_command(&mut command);
    append_daemon_endpoint_args(&mut command, options);
    let child = command
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

fn append_daemon_endpoint_args(command: &mut Command, options: &DaemonRunOptions) {
    command.arg("--host").arg(&options.host);
    if !options.port_fallback {
        command.arg("--port").arg(options.port.to_string());
    }
}

fn daemon_start_executable_for_command(invoked_exe: &Path, command_prefix: &[&str]) -> PathBuf {
    if matches!(command_prefix, ["daemon", "shadow", ..]) {
        if daemon_binary_stem(invoked_exe) == Some("sinomo-shadow") {
            return invoked_exe.to_path_buf();
        }
        if let Some(shadow_binary) = sibling_daemon_binary(invoked_exe, "sinomo-shadow") {
            return shadow_binary;
        }
    }
    invoked_exe.to_path_buf()
}

fn sibling_daemon_binary(invoked_exe: &Path, name: &str) -> Option<PathBuf> {
    let file_name = format!("{name}{}", env::consts::EXE_SUFFIX);
    let candidate = invoked_exe.with_file_name(file_name);
    candidate.exists().then_some(candidate)
}

fn daemon_binary_stem(path: &Path) -> Option<&str> {
    path.file_stem().and_then(|stem| stem.to_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_binary_path(root: &sinomo_testdir::TestDir, name: &str) -> PathBuf {
        let path = root.join(format!("{name}{}", env::consts::EXE_SUFFIX));
        File::create(&path).expect("test binary");
        path
    }

    #[test]
    fn shadow_start_uses_shadow_sibling_when_invoked_from_main_binary() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-shadow-start-exe");
        let main = test_binary_path(&root, "sinomo");
        let shadow = test_binary_path(&root, "sinomo-shadow");

        assert_eq!(
            daemon_start_executable_for_command(&main, &["daemon", "shadow", "run"]),
            shadow
        );
    }

    #[test]
    fn shadow_start_keeps_invoked_binary_when_shadow_sibling_is_absent() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-shadow-start-dev-exe");
        let main = test_binary_path(&root, "sinomo");

        assert_eq!(
            daemon_start_executable_for_command(&main, &["daemon", "shadow", "run"]),
            main
        );
    }

    #[test]
    fn shadow_start_main_binary_env_uses_main_sibling_when_invoked_from_shadow() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-shadow-main-exe");
        let main = test_binary_path(&root, "sinomo");
        let shadow = test_binary_path(&root, "sinomo-shadow");

        assert_eq!(daemon_main_binary_for_shadow_start(&shadow), main);
    }

    #[test]
    fn mobile_execution_profile_has_bounded_runtime_threads() {
        assert_eq!(
            daemon_async_runtime_limits(DaemonExecutionProfile::Mobile),
            DaemonRuntimeLimits {
                worker_threads: 2,
                max_blocking_threads: Some(8),
            }
        );
    }

    #[test]
    fn mobile_execution_profile_builds_a_two_worker_runtime() {
        let runtime = build_daemon_async_runtime_with_profile(
            "mobile-profile-test",
            DaemonExecutionProfile::Mobile,
        )
        .expect("mobile runtime");
        assert_eq!(runtime.metrics().num_workers(), 2);
    }
}
