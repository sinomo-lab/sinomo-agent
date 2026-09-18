//! Supervision for the full daemon hosted in the app's sibling process.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sinomo_runtime::api::{
    DaemonPaths, DaemonProbeStatus, DaemonRunOptions, DaemonStatus, SINOMO_APP_DAEMON_BINARY_ENV,
    SINOMO_APP_DAEMON_LAUNCH_ID_ENV, SINOMO_DAEMON_API_PROTOCOL_VERSION, probe_daemon_status,
};

const CHILD_PROBE_TIMEOUT: Duration = Duration::from_millis(300);
const CHILD_READY_TIMEOUT: Duration = Duration::from_secs(15);
const CHILD_STOP_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const BOOTSTRAP_LAUNCH_SUFFIX: &str = ":bootstrap";
static LAUNCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub enum StartAppDaemonOutcome {
    Started(DaemonStatus, OwnedDaemonHandle),
    Attached(DaemonStatus),
}

/// Owns the exact app-started daemon child, its lifetime lease, and the Windows process-tree job.
/// Despite the compatibility name used by the surrounding backend, no daemon implementation runs
/// in the GUI process.
pub struct OwnedDaemonHandle {
    child: Mutex<Child>,
    lease: Mutex<Option<ChildStdin>>,
    launch_id: String,
    #[cfg(windows)]
    _job: AppDaemonJob,
}

impl OwnedDaemonHandle {
    pub fn has_exited(&self) -> Result<bool, String> {
        self.child()
            .try_wait()
            .map(|status| status.is_some())
            .map_err(|error| format!("inspect app-owned daemon child: {error}"))
    }
    pub fn pid(&self) -> u32 {
        self.child().id()
    }

    pub fn owns_status(&self, status: &DaemonStatus) -> bool {
        status.pid == self.pid() && status.app_launch_id.as_deref() == Some(&self.launch_id)
    }

    pub fn wait_for_exit(&self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            if self
                .child()
                .try_wait()
                .map_err(|error| format!("inspect app-owned daemon child: {error}"))?
                .is_some()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "app-owned daemon did not stop within {} seconds",
                    timeout.as_secs_f32()
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn close_lease(&self) {
        self.lease().take();
    }

    fn force_stop_and_reap(&self) {
        let mut child = self.child();
        match child.try_wait() {
            Ok(Some(_)) => {
                let _ = child.wait();
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn child(&self) -> MutexGuard<'_, Child> {
        self.child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lease(&self) -> MutexGuard<'_, Option<ChildStdin>> {
        self.lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for OwnedDaemonHandle {
    fn drop(&mut self) {
        self.close_lease();
        if self.wait_for_exit(CHILD_STOP_TIMEOUT).is_err() {
            self.force_stop_and_reap();
        }
    }
}

pub fn start_app_owned_daemon(
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
) -> Result<StartAppDaemonOutcome, String> {
    start_app_owned_daemon_with_binary(paths, progress, None)
}

pub(crate) fn start_app_owned_daemon_with_binary(
    paths: &DaemonPaths,
    progress: &impl Fn(&str),
    binary: Option<&Path>,
) -> Result<StartAppDaemonOutcome, String> {
    let data_dir = paths.root.parent().ok_or_else(|| {
        format!(
            "app-owned daemon path {} has no data directory parent",
            paths.root.display()
        )
    })?;
    let binary = match binary {
        Some(path) => validate_app_daemon_binary(path.to_path_buf(), "desktop release")?,
        None => app_daemon_binary()?,
    };
    let mut launch_id = next_launch_id();
    // This is readiness evidence, not authority: the daemon still owns typed initialization.
    // It prevents a GUI from attaching to a bootstrap process while init is draining it.
    let cluster_paths = sinomo_runtime::network::ClusterPaths::new(
        &sinomo_runtime::local::SinomoDataDir::new(data_dir),
    );
    if !cluster_paths.state.exists() {
        launch_id.push_str(BOOTSTRAP_LAUNCH_SUFFIX);
    }
    let (stdout, stderr) = app_daemon_log_files(data_dir)?;
    let options = DaemonRunOptions::main_from_environment()?;
    let mut command = Command::new(&binary);
    prepare_app_daemon_command(&mut command);
    command.arg("--host").arg(&options.host);
    if !options.port_fallback {
        command.arg("--port").arg(options.port.to_string());
    }
    command
        .env("SINOMO_DATA_DIR", data_dir)
        .env(SINOMO_APP_DAEMON_LAUNCH_ID_ENV, &launch_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));

    let mut child = command.spawn().map_err(|error| {
        format!(
            "failed to start app daemon helper {}: {error}",
            binary.display()
        )
    })?;
    let lease = child
        .stdin
        .take()
        .ok_or_else(|| "app daemon helper did not expose its lifetime lease".to_string())?;

    #[cfg(windows)]
    let job = match AppDaemonJob::assign(&child) {
        Ok(job) => job,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };

    progress("Waiting for the daemon to come up…");
    let deadline = Instant::now() + CHILD_READY_TIMEOUT;
    loop {
        if let Some(exit) = child
            .try_wait()
            .map_err(|error| format!("inspect app daemon helper: {error}"))?
        {
            if let DaemonProbeStatus::Running(status) =
                probe_daemon_status(paths, CHILD_PROBE_TIMEOUT)
            {
                return Ok(StartAppDaemonOutcome::Attached(status));
            }
            return Err(format!(
                "app daemon helper exited before readiness with status {exit}; inspect {}",
                data_dir.join("daemon").join("main.stderr.log").display()
            ));
        }

        if let DaemonProbeStatus::Running(status) = probe_daemon_status(paths, CHILD_PROBE_TIMEOUT)
        {
            if status.pid == child.id() && status.app_launch_id.as_deref() == Some(&launch_id) {
                if status.api_protocol_version != SINOMO_DAEMON_API_PROTOCOL_VERSION {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "app daemon protocol {} is incompatible with client protocol {}",
                        status.api_protocol_version, SINOMO_DAEMON_API_PROTOCOL_VERSION
                    ));
                }
                return Ok(StartAppDaemonOutcome::Started(
                    status,
                    OwnedDaemonHandle {
                        child: Mutex::new(child),
                        lease: Mutex::new(Some(lease)),
                        launch_id,
                        #[cfg(windows)]
                        _job: job,
                    },
                ));
            }

            // Another process won the daemon lock. Stop and reap only our losing helper, then
            // attach to the winner without claiming lifecycle ownership.
            drop(lease);
            let _ = child.kill();
            let _ = child.wait();
            return Ok(StartAppDaemonOutcome::Attached(status));
        }

        if Instant::now() >= deadline {
            drop(lease);
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "app daemon helper did not become ready within {} seconds; inspect {}",
                CHILD_READY_TIMEOUT.as_secs(),
                data_dir.join("daemon").join("main.stderr.log").display()
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn app_daemon_binary() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os(SINOMO_APP_DAEMON_BINARY_ENV) {
        return validate_app_daemon_binary(PathBuf::from(path), SINOMO_APP_DAEMON_BINARY_ENV);
    }
    let current = std::env::current_exe()
        .map_err(|error| format!("resolve graphical app executable: {error}"))?;
    let file_name = format!("sinomo-app-daemon{}", std::env::consts::EXE_SUFFIX);
    let direct = current.with_file_name(&file_name);
    if direct.is_file() {
        return Ok(direct);
    }
    if current
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "deps")
        && let Some(debug_root) = current.parent().and_then(Path::parent)
    {
        let candidate = debug_root.join(&file_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(format!(
        "the required sibling daemon helper `{file_name}` was not found beside {}; build/package sinomo-app and sinomo-app-daemon together or set {SINOMO_APP_DAEMON_BINARY_ENV}",
        current.display()
    ))
}

fn validate_app_daemon_binary(path: PathBuf, source: &str) -> Result<PathBuf, String> {
    if path.is_file() {
        Ok(path)
    } else {
        Err(format!(
            "{source} points to missing daemon helper {}",
            path.display()
        ))
    }
}

fn app_daemon_log_files(data_dir: &Path) -> Result<(File, File), String> {
    let daemon_dir = data_dir.join("daemon");
    fs::create_dir_all(&daemon_dir).map_err(|error| {
        format!(
            "prepare app daemon log directory {}: {error}",
            daemon_dir.display()
        )
    })?;
    let open = |name: &str| {
        let path = daemon_dir.join(name);
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| format!("open app daemon log {}: {error}", path.display()))
    };
    Ok((open("main.stdout.log")?, open("main.stderr.log")?))
}

fn next_launch_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = LAUNCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("app-{}-{nanos:x}-{sequence:x}", std::process::id())
}

fn prepare_app_daemon_command(_command: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

        _command.creation_flags(CREATE_NO_WINDOW);
    }
}

#[cfg(windows)]
struct AppDaemonJob(windows_sys::Win32::Foundation::HANDLE);

// Windows kernel handles are process-global values. This guard performs no thread-affine API
// operation and closes the handle exactly once when the final owning backend is dropped.
#[cfg(windows)]
unsafe impl Send for AppDaemonJob {}
#[cfg(windows)]
unsafe impl Sync for AppDaemonJob {}

#[cfg(windows)]
impl AppDaemonJob {
    fn assign(child: &Child) -> Result<Self, String> {
        use std::mem;
        use std::os::windows::io::AsRawHandle;
        use std::ptr;
        use windows_sys::Win32::Foundation::GetLastError;
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };

        let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if handle.is_null() {
            return Err(format!(
                "create app daemon Windows job object: OS error {}",
                unsafe { GetLastError() }
            ));
        }
        let job = Self(handle);
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if unsafe {
            SetInformationJobObject(
                job.0,
                JobObjectExtendedLimitInformation,
                (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(format!(
                "configure app daemon Windows job object: OS error {}",
                unsafe { GetLastError() }
            ));
        }
        if unsafe { AssignProcessToJobObject(job.0, child.as_raw_handle().cast()) } == 0 {
            return Err(format!(
                "assign app daemon child to Windows job object: OS error {}",
                unsafe { GetLastError() }
            ));
        }
        Ok(job)
    }
}

#[cfg(windows)]
impl Drop for AppDaemonJob {
    fn drop(&mut self) {
        unsafe {
            let _ = windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_ids_are_process_scoped_and_unique() {
        let first = next_launch_id();
        let second = next_launch_id();
        assert!(first.starts_with(&format!("app-{}-", std::process::id())));
        assert_ne!(first, second);
    }

    #[test]
    fn helper_override_must_name_a_file() {
        let missing =
            std::env::temp_dir().join(format!("sinomo-app-daemon-missing-{}", next_launch_id()));
        let error = validate_app_daemon_binary(missing.clone(), "TEST_OVERRIDE")
            .expect_err("missing helper must fail");
        assert!(error.contains("TEST_OVERRIDE"));
        assert!(error.contains(&missing.display().to_string()));
    }
}
