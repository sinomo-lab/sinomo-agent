//! Simple native sandbox runtime.
//!
//! This runtime executes host binaries directly and exposes the host filesystem
//! layout as-is. On Linux, it uses Landlock as a best-effort filesystem
//! allowlist guard. This is intended to prevent accidental access to
//! unapproved paths; it is not a hostile-code security boundary.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use sinomo_node::NodeId;
use sinomo_sandbox::{
    LocalSandboxFileSystem, LocalSandboxInstanceLayout, LocalSandboxInstanceStore,
    SANDBOX_UNLIMITED_WALL_TIME_MS, SandboxCancellationToken, SandboxCommandExecutionError,
    SandboxCommandId, SandboxCommandRecord, SandboxCommandRecordStatus, SandboxCommandRequest,
    SandboxFsError, SandboxLocalCommandOutcome, SandboxOutputSink, SandboxOutputStream,
    SandboxPath, SandboxPolicy, SandboxProcessPlan, SandboxProcessPlanOutcome,
    SandboxResourceLimits, SandboxResourceUsage, SandboxStoreError, SandboxWasiError,
    effective_sandbox_timeout_ms, persist_command_outputs, record_local_command_history,
    sandbox_command_record_status_for_wasi_error, sandbox_command_stderr_path,
    sandbox_command_stdout_path, sandbox_output_chunks, validate_sandbox_process_plan,
    validate_sandbox_process_plan_working_directories, validate_sandbox_toolchain_argv,
    validate_toolchain_command_name, validate_workspace_usage, validate_written_bytes,
};
use time::OffsetDateTime;

pub const SINOMO_SANDBOX_SIMPLE_BACKEND: &str = "simple-native";
pub const SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED_ENV: &str =
    "SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED";
pub const SINOMO_SANDBOX_SIMPLE_READ_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_READ_PATHS";
pub const SINOMO_SANDBOX_SIMPLE_WRITE_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_WRITE_PATHS";
pub const SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS";

const SIMPLE_NATIVE_WAIT_POLL_MS: u64 = 50;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxSimpleNativeConfig {
    pub allow_unenforced: bool,
    pub extra_read_paths: Vec<PathBuf>,
    pub extra_write_paths: Vec<PathBuf>,
    pub extra_execute_paths: Vec<PathBuf>,
}

impl SandboxSimpleNativeConfig {
    pub fn from_env() -> Result<Self, SandboxCommandExecutionError> {
        Ok(Self {
            allow_unenforced: env_bool(SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED_ENV)?,
            extra_read_paths: env_path_list(SINOMO_SANDBOX_SIMPLE_READ_PATHS_ENV),
            extra_write_paths: env_path_list(SINOMO_SANDBOX_SIMPLE_WRITE_PATHS_ENV),
            extra_execute_paths: env_path_list(SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS_ENV),
        })
    }
}

impl Default for SandboxSimpleNativeConfig {
    fn default() -> Self {
        Self {
            allow_unenforced: false,
            extra_read_paths: Vec::new(),
            extra_write_paths: Vec::new(),
            extra_execute_paths: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SandboxSimpleNativeCommandRunner {
    config: SandboxSimpleNativeConfig,
}

impl SandboxSimpleNativeCommandRunner {
    pub fn new(config: SandboxSimpleNativeConfig) -> Self {
        Self { config }
    }

    pub fn from_env() -> Result<Self, SandboxCommandExecutionError> {
        Ok(Self::new(SandboxSimpleNativeConfig::from_env()?))
    }

    pub fn landlock_supported() -> bool {
        linux::landlock_abi_version().is_ok()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        _toolchain_store: &sinomo_sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_sandbox::SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        validate_toolchain_command_name(command_name)?;
        validate_sandbox_toolchain_argv(command_name, &argv).map_err(invalid_request)?;
        let (manifest, layout) = store.load(sandbox_id)?;
        policy
            .validate_manifest(&manifest, execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        if let Some(stdin) = &stdin {
            let limits = &manifest.limits;
            if stdin.len() as u64 > limits.max_input_bytes {
                return Err(SandboxWasiError::InputLimitExceeded {
                    limit: limits.max_input_bytes.min(usize::MAX as u64) as usize,
                }
                .into());
            }
        }
        let mut env = manifest.env.clone();
        if let Ok(path) = std::env::var("PATH") {
            env.entry("PATH".to_string()).or_insert(path);
        }
        let request = SandboxCommandRequest {
            sandbox_id: sandbox_id.clone(),
            argv,
            cwd: manifest.cwd.clone(),
            stdin,
            env,
            timeout_ms,
        };
        self.run_native_command(store, &manifest, &layout, request, output_sink)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_process_plan_with_policy_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        _toolchain_store: &sinomo_sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_sandbox::SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        validate_sandbox_process_plan(plan)?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let fs = LocalSandboxFileSystem::new(layout.clone());
        validate_sandbox_process_plan_working_directories(plan, &fs, &manifest.cwd)?;
        policy
            .validate_manifest(&manifest, execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;

        let mut step_outcomes = Vec::with_capacity(plan.steps.len());
        let mut piped_stdin: Option<Vec<u8>> = None;
        for (index, step) in plan.steps.iter().enumerate() {
            validate_toolchain_command_name(&step.command_name)?;
            let stdin = if index > 0 && plan.steps[index - 1].pipe_stdout_to_next {
                piped_stdin.take()
            } else {
                step.stdin.clone()
            };
            let mut env = manifest.env.clone();
            if let Ok(path) = std::env::var("PATH") {
                env.entry("PATH".to_string()).or_insert(path);
            }
            env.extend(step.env.clone());
            let request = SandboxCommandRequest {
                sandbox_id: sandbox_id.clone(),
                argv: step.argv.clone(),
                cwd: step.cwd.clone().unwrap_or_else(|| manifest.cwd.clone()),
                stdin,
                env,
                timeout_ms,
            };
            let outcome =
                self.run_native_command(store, &manifest, &layout, request, output_sink.clone())?;
            if step.pipe_stdout_to_next {
                piped_stdin = Some(outcome.stdout.clone());
            }
            let exit_status = outcome.response.exit_status;
            let stdout = outcome.stdout.clone();
            let stderr = outcome.stderr.clone();
            step_outcomes.push(outcome);
            if exit_status != 0 {
                return Ok(SandboxProcessPlanOutcome {
                    step_outcomes,
                    exit_status,
                    stdout,
                    stderr,
                });
            }
        }

        let last = step_outcomes
            .last()
            .expect("validated process plan has at least one step");
        Ok(SandboxProcessPlanOutcome {
            exit_status: last.response.exit_status,
            stdout: last.stdout.clone(),
            stderr: last.stderr.clone(),
            step_outcomes,
        })
    }

    fn run_native_command(
        &self,
        store: &LocalSandboxInstanceStore,
        manifest: &sinomo_sandbox::SandboxInstanceManifest,
        layout: &LocalSandboxInstanceLayout,
        request: SandboxCommandRequest,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let Some(argv0) = request.argv.first() else {
            return Err(invalid_request(
                "simple-native command argv must include argv[0]",
            ));
        };
        validate_sandbox_toolchain_argv(argv0, &request.argv).map_err(invalid_request)?;
        ensure_layout_dirs(layout)?;
        let command_id = SandboxCommandId::generate();
        let started_at = OffsetDateTime::now_utc();
        let timeout_ms =
            effective_sandbox_timeout_ms(request.timeout_ms, manifest.limits.wall_time_ms);

        if let Some(stdin) = &request.stdin {
            if stdin.len() as u64 > manifest.limits.max_input_bytes {
                let error = SandboxWasiError::InputLimitExceeded {
                    limit: manifest.limits.max_input_bytes.min(usize::MAX as u64) as usize,
                };
                persist_failed_command(
                    store,
                    manifest,
                    layout,
                    &command_id,
                    &request,
                    started_at,
                    None,
                    Vec::new(),
                    Vec::new(),
                    sandbox_command_record_status_for_wasi_error(&error),
                    error.to_string(),
                )?;
                return Err(error.into());
            }
        }

        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        let cwd = resolve_native_cwd(&sandbox_fs, layout, &request.cwd)?;
        let before = sandbox_fs.snapshot()?;
        let policy = SandboxSimpleNativePolicy::for_layout(layout, &self.config);
        policy.validate_cwd(&cwd)?;
        let started = Instant::now();
        let output = match execute_native_command(
            &request,
            layout,
            &cwd,
            &policy,
            timeout_ms,
            &self.config,
            &manifest.limits,
        ) {
            Ok(output) => output,
            Err(error) => {
                persist_failed_command(
                    store,
                    manifest,
                    layout,
                    &command_id,
                    &request,
                    started_at,
                    None,
                    Vec::new(),
                    Vec::new(),
                    sandbox_command_record_status_for_wasi_error(&error),
                    error.to_string(),
                )?;
                return Err(error.into());
            }
        };

        let wall_time_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let exit_status = exit_status_code(&output);
        let stdout = output.stdout;
        let stderr = output.stderr;
        let output_bytes = stdout.len().saturating_add(stderr.len()) as u64;
        let mut output_chunks = sandbox_output_chunks(
            &manifest.sandbox_id,
            &command_id,
            SandboxOutputStream::Stdout,
            &stdout,
        );
        output_chunks.extend(sandbox_output_chunks(
            &manifest.sandbox_id,
            &command_id,
            SandboxOutputStream::Stderr,
            &stderr,
        ));
        if let Some(sink) = output_sink.as_ref() {
            for chunk in &output_chunks {
                sink.output_chunk(chunk.clone());
            }
        }

        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        let changed_paths = sandbox_fs.changed_paths_since(&before)?;
        let after = sandbox_fs.snapshot()?;
        let written_bytes = after.written_bytes_since(&before);
        let response = sinomo_sandbox::SandboxCommandResponse {
            command_id: command_id.clone(),
            exit_status,
            stdout_ref: sandbox_command_stdout_path(&manifest.sandbox_id, &command_id),
            stderr_ref: sandbox_command_stderr_path(&manifest.sandbox_id, &command_id),
            changed_paths,
            resource_usage: SandboxResourceUsage {
                wall_time_ms,
                peak_memory_bytes: 0,
                stdout_bytes: stdout.len() as u64,
                stderr_bytes: stderr.len() as u64,
                written_bytes,
            },
        };

        let limit_error = if output_bytes > manifest.limits.max_output_bytes {
            Some(SandboxCommandExecutionError::Wasi(
                SandboxWasiError::OutputLimitExceeded {
                    limit: manifest.limits.max_output_bytes.min(usize::MAX as u64) as usize,
                },
            ))
        } else if let Err(error) =
            validate_workspace_usage(&after.workspace_usage(), &manifest.limits)
        {
            Some(SandboxCommandExecutionError::Fs(error))
        } else if let Err(error) = validate_written_bytes(written_bytes, &manifest.limits) {
            Some(SandboxCommandExecutionError::Fs(error))
        } else {
            None
        };

        let completed_at = OffsetDateTime::now_utc();
        let status = if limit_error.is_some() {
            SandboxCommandRecordStatus::ResourceExhausted
        } else {
            SandboxCommandRecordStatus::Completed
        };
        let error = limit_error.as_ref().map(ToString::to_string);
        let record = SandboxCommandRecord {
            sandbox_id: manifest.sandbox_id.clone(),
            command_id: command_id.clone(),
            status,
            workspace_root: manifest.workspace_root.clone(),
            toolchain_id: manifest.toolchain_id.clone(),
            argv: request.argv.clone(),
            cwd: request.cwd.clone(),
            env: request.env.clone(),
            started_at,
            completed_at: Some(completed_at),
            response: Some(response.clone()),
            error,
        };
        let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        persist_command_outputs(&mut sandbox_fs, &record, &stdout, &stderr)?;
        record_local_command_history(store, &manifest.sandbox_id, &command_id, completed_at)?;
        if let Some(error) = limit_error {
            return Err(error);
        }
        Ok(SandboxLocalCommandOutcome {
            response,
            stdout,
            stderr,
            output_chunks,
        })
    }
}

#[derive(Clone, Debug)]
struct SandboxSimpleNativePolicy {
    read_paths: Vec<PathBuf>,
    write_paths: Vec<PathBuf>,
    execute_paths: Vec<PathBuf>,
}

impl SandboxSimpleNativePolicy {
    fn for_layout(layout: &LocalSandboxInstanceLayout, config: &SandboxSimpleNativeConfig) -> Self {
        let mut read_paths = existing_paths([
            PathBuf::from("/bin"),
            PathBuf::from("/usr"),
            PathBuf::from("/lib"),
            PathBuf::from("/lib64"),
            PathBuf::from("/etc"),
            PathBuf::from("/proc"),
            PathBuf::from("/dev/null"),
            PathBuf::from("/dev/urandom"),
        ]);
        read_paths.extend(config.extra_read_paths.iter().cloned());

        let mut write_paths = existing_paths([
            layout.root_dir.clone(),
            layout.home.clone(),
            layout.tmp.clone(),
            layout.work.clone(),
            layout.var.clone(),
            layout.logs.clone(),
            layout.artifacts.clone(),
            PathBuf::from("/dev/null"),
        ]);
        write_paths.extend(config.extra_write_paths.iter().cloned());

        let mut execute_paths = existing_paths([
            PathBuf::from("/bin"),
            PathBuf::from("/usr/bin"),
            PathBuf::from("/usr/local/bin"),
            PathBuf::from("/lib"),
            PathBuf::from("/lib64"),
            PathBuf::from("/usr/lib"),
            PathBuf::from("/usr/lib64"),
            layout.work.clone(),
            layout.artifacts.clone(),
        ]);
        execute_paths.extend(config.extra_execute_paths.iter().cloned());

        Self {
            read_paths: dedup_paths(read_paths),
            write_paths: dedup_paths(write_paths),
            execute_paths: dedup_paths(execute_paths),
        }
    }

    fn validate_cwd(&self, cwd: &Path) -> Result<(), SandboxCommandExecutionError> {
        if self.path_is_allowed(cwd) {
            Ok(())
        } else {
            Err(invalid_request(format!(
                "simple-native cwd {} is outside allowed read/write paths",
                cwd.display()
            )))
        }
    }

    fn path_is_allowed(&self, path: &Path) -> bool {
        self.read_paths
            .iter()
            .chain(self.write_paths.iter())
            .any(|allowed| path_has_prefix(path, allowed))
    }
}

fn execute_native_command(
    request: &SandboxCommandRequest,
    layout: &LocalSandboxInstanceLayout,
    cwd: &Path,
    policy: &SandboxSimpleNativePolicy,
    timeout_ms: u64,
    config: &SandboxSimpleNativeConfig,
    limits: &SandboxResourceLimits,
) -> Result<Output, SandboxWasiError> {
    let argv0 = request
        .argv
        .first()
        .ok_or_else(|| SandboxWasiError::InvalidRequest {
            detail: "simple-native command argv must include argv[0]".to_string(),
        })?;
    linux::ensure_landlock_or_allowed(config.allow_unenforced)?;

    let mut command = Command::new(argv0);
    command
        .args(request.argv.iter().skip(1))
        .current_dir(cwd)
        .stdin(if request.stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_clear();
    for (name, value) in native_environment(request, layout, cwd) {
        command.env(name, value);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        let policy = policy.clone();
        let allow_unenforced = config.allow_unenforced;
        let memory_mb = limits.memory_mb;
        // SAFETY: pre_exec only calls async-signal-safe libc/syscall operations
        // and returns an io::Error on failure before exec.
        unsafe {
            command.pre_exec(move || {
                linux::prepare_child_process(&policy, allow_unenforced, memory_mb)
            });
        }
    }

    let mut child = command
        .spawn()
        .map_err(|source| SandboxWasiError::Runtime {
            detail: format!("failed to start simple-native command `{argv0}`: {source}"),
        })?;
    let child_pid = child.id() as i32;
    if let Some(stdin) = &request.stdin {
        let mut child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| SandboxWasiError::Runtime {
                detail: "simple-native stdin pipe was not available".to_string(),
            })?;
        child_stdin
            .write_all(stdin)
            .map_err(|source| SandboxWasiError::Runtime {
                detail: format!("failed to write simple-native stdin: {source}"),
            })?;
    }
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    wait_for_native_output(child_pid, rx, timeout_ms, &SandboxCancellationToken::new())
}

fn wait_for_native_output(
    child_pid: i32,
    rx: mpsc::Receiver<std::io::Result<Output>>,
    timeout_ms: u64,
    cancellation_token: &SandboxCancellationToken,
) -> Result<Output, SandboxWasiError> {
    let started = Instant::now();
    loop {
        if cancellation_token.is_cancelled() {
            linux::kill_process_group(child_pid);
            return Err(SandboxWasiError::Cancelled);
        }
        let wait = match native_wait_poll_duration(started, timeout_ms) {
            Ok(wait) => wait,
            Err(SandboxWasiError::TimedOut { timeout_ms }) => {
                linux::kill_process_group(child_pid);
                return Err(SandboxWasiError::TimedOut { timeout_ms });
            }
            Err(error) => return Err(error),
        };
        match rx.recv_timeout(wait) {
            Ok(output) => {
                return output.map_err(|source| SandboxWasiError::Runtime {
                    detail: format!("simple-native command wait failed: {source}"),
                });
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(SandboxWasiError::Runtime {
                    detail: "simple-native wait channel disconnected".to_string(),
                });
            }
        }
    }
}

fn native_wait_poll_duration(
    started: Instant,
    timeout_ms: u64,
) -> Result<Duration, SandboxWasiError> {
    if timeout_ms == SANDBOX_UNLIMITED_WALL_TIME_MS {
        return Ok(Duration::from_millis(SIMPLE_NATIVE_WAIT_POLL_MS));
    }
    let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    if elapsed_ms >= timeout_ms {
        return Err(SandboxWasiError::TimedOut { timeout_ms });
    }
    Ok(Duration::from_millis(
        timeout_ms
            .saturating_sub(elapsed_ms)
            .min(SIMPLE_NATIVE_WAIT_POLL_MS),
    ))
}

fn resolve_native_cwd(
    fs: &LocalSandboxFileSystem,
    layout: &LocalSandboxInstanceLayout,
    cwd: &SandboxPath,
) -> Result<PathBuf, SandboxCommandExecutionError> {
    match fs.host_path(cwd) {
        Ok(path) => Ok(path),
        Err(SandboxFsError::NotFound { .. }) => {
            let path = PathBuf::from(cwd.as_str());
            if path.exists() {
                Ok(path)
            } else if cwd.as_str() == "/" {
                Ok(layout.root.clone())
            } else {
                Err(SandboxCommandExecutionError::Fs(SandboxFsError::NotFound {
                    path: cwd.clone(),
                }))
            }
        }
        Err(error) => Err(SandboxCommandExecutionError::Fs(error)),
    }
}

fn native_environment(
    request: &SandboxCommandRequest,
    layout: &LocalSandboxInstanceLayout,
    cwd: &Path,
) -> Vec<(String, String)> {
    let mut envs = Vec::new();
    let path = request
        .env
        .get("PATH")
        .cloned()
        .unwrap_or_else(|| "/usr/local/bin:/usr/bin:/bin".to_string());
    envs.push(("PATH".to_string(), path));
    envs.push(("HOME".to_string(), layout.home.display().to_string()));
    envs.push(("TMPDIR".to_string(), layout.tmp.display().to_string()));
    envs.push(("PWD".to_string(), cwd.display().to_string()));
    envs.push((
        "SINOMO_SANDBOX_ROOT".to_string(),
        layout.root.display().to_string(),
    ));
    envs.push(("SINOMO_WORK".to_string(), layout.work.display().to_string()));
    envs.push((
        "SINOMO_SANDBOX_ID".to_string(),
        request.sandbox_id.to_string(),
    ));
    for (name, value) in &request.env {
        if name != "PATH" {
            envs.push((name.clone(), value.clone()));
        }
    }
    envs
}

#[allow(clippy::too_many_arguments)]
fn persist_failed_command(
    store: &LocalSandboxInstanceStore,
    manifest: &sinomo_sandbox::SandboxInstanceManifest,
    layout: &LocalSandboxInstanceLayout,
    command_id: &SandboxCommandId,
    request: &SandboxCommandRequest,
    started_at: OffsetDateTime,
    response: Option<sinomo_sandbox::SandboxCommandResponse>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    status: SandboxCommandRecordStatus,
    error: String,
) -> Result<(), SandboxCommandExecutionError> {
    let completed_at = OffsetDateTime::now_utc();
    let record = SandboxCommandRecord {
        sandbox_id: manifest.sandbox_id.clone(),
        command_id: command_id.clone(),
        status,
        workspace_root: manifest.workspace_root.clone(),
        toolchain_id: manifest.toolchain_id.clone(),
        argv: request.argv.clone(),
        cwd: request.cwd.clone(),
        env: request.env.clone(),
        started_at,
        completed_at: Some(completed_at),
        response,
        error: Some(error),
    };
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
    persist_command_outputs(&mut sandbox_fs, &record, &stdout, &stderr)?;
    record_local_command_history(store, &manifest.sandbox_id, command_id, completed_at)?;
    Ok(())
}

fn ensure_layout_dirs(layout: &LocalSandboxInstanceLayout) -> Result<(), SandboxStoreError> {
    for dir in layout.required_dirs() {
        fs::create_dir_all(dir).map_err(|source| SandboxStoreError::Io {
            path: dir.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(())
}

fn existing_paths(paths: impl IntoIterator<Item = PathBuf>) -> Vec<PathBuf> {
    paths
        .into_iter()
        .filter(|path| path.exists())
        .collect::<Vec<_>>()
}

fn dedup_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for path in paths {
        let key = fs::canonicalize(&path).unwrap_or(path.clone());
        if seen.insert(key.clone()) {
            result.push(key);
        }
    }
    result
}

fn path_has_prefix(path: &Path, prefix: &Path) -> bool {
    let path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let prefix = fs::canonicalize(prefix).unwrap_or_else(|_| prefix.to_path_buf());
    path == prefix || path.starts_with(prefix)
}

fn env_path_list(name: &str) -> Vec<PathBuf> {
    env::var_os(name)
        .map(|value| env::split_paths(&value).collect())
        .unwrap_or_default()
}

fn env_bool(name: &str) -> Result<bool, SandboxCommandExecutionError> {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "" | "0" | "false" | "no" | "off" => Ok(false),
            "1" | "true" | "yes" | "on" => Ok(true),
            _ => Err(invalid_request(format!(
                "invalid simple-native boolean `{name}={value}`; expected true/false/1/0/yes/no/on/off"
            ))),
        },
        Err(env::VarError::NotPresent) => Ok(false),
        Err(env::VarError::NotUnicode(_)) => Err(invalid_request(format!(
            "invalid non-unicode simple-native boolean `{name}`"
        ))),
    }
}

fn invalid_request(detail: impl Into<String>) -> SandboxCommandExecutionError {
    SandboxWasiError::InvalidRequest {
        detail: detail.into(),
    }
    .into()
}

#[cfg(unix)]
fn exit_status_code(output: &Output) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    output
        .status
        .code()
        .or_else(|| output.status.signal().map(|signal| 128 + signal))
        .unwrap_or(128)
}

#[cfg(not(unix))]
fn exit_status_code(output: &Output) -> i32 {
    output.status.code().unwrap_or(128)
}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs;
    use std::io;
    use std::mem;
    use std::os::fd::AsRawFd;
    use std::path::Path;

    use libc::{c_int, c_void};
    use sinomo_sandbox::{SANDBOX_UNLIMITED_SIZE_LIMIT, SandboxWasiError};

    use super::SandboxSimpleNativePolicy;

    const LANDLOCK_CREATE_RULESET_VERSION: u32 = 1;
    const LANDLOCK_RULE_PATH_BENEATH: u32 = 1;

    const LANDLOCK_ACCESS_FS_EXECUTE: u64 = 1 << 0;
    const LANDLOCK_ACCESS_FS_WRITE_FILE: u64 = 1 << 1;
    const LANDLOCK_ACCESS_FS_READ_FILE: u64 = 1 << 2;
    const LANDLOCK_ACCESS_FS_READ_DIR: u64 = 1 << 3;
    const LANDLOCK_ACCESS_FS_REMOVE_DIR: u64 = 1 << 4;
    const LANDLOCK_ACCESS_FS_REMOVE_FILE: u64 = 1 << 5;
    const LANDLOCK_ACCESS_FS_MAKE_CHAR: u64 = 1 << 6;
    const LANDLOCK_ACCESS_FS_MAKE_DIR: u64 = 1 << 7;
    const LANDLOCK_ACCESS_FS_MAKE_REG: u64 = 1 << 8;
    const LANDLOCK_ACCESS_FS_MAKE_SOCK: u64 = 1 << 9;
    const LANDLOCK_ACCESS_FS_MAKE_FIFO: u64 = 1 << 10;
    const LANDLOCK_ACCESS_FS_MAKE_BLOCK: u64 = 1 << 11;
    const LANDLOCK_ACCESS_FS_MAKE_SYM: u64 = 1 << 12;
    const LANDLOCK_ACCESS_FS_REFER: u64 = 1 << 13;
    const LANDLOCK_ACCESS_FS_TRUNCATE: u64 = 1 << 14;
    const LANDLOCK_ACCESS_FS_IOCTL_DEV: u64 = 1 << 15;

    #[repr(C)]
    struct LandlockRulesetAttr {
        handled_access_fs: u64,
    }

    #[repr(C)]
    struct LandlockPathBeneathAttr {
        allowed_access: u64,
        parent_fd: i32,
    }

    pub(super) fn ensure_landlock_or_allowed(
        allow_unenforced: bool,
    ) -> Result<(), SandboxWasiError> {
        if landlock_abi_version().is_ok() || allow_unenforced {
            Ok(())
        } else {
            Err(SandboxWasiError::Runtime {
                detail: format!(
                    "simple-native requires Linux Landlock; set {}=1 for explicit unenforced development runs",
                    super::SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED_ENV
                ),
            })
        }
    }

    pub(super) fn prepare_child_process(
        policy: &SandboxSimpleNativePolicy,
        allow_unenforced: bool,
        memory_mb: u64,
    ) -> io::Result<()> {
        // New process group lets the parent cancel a whole simple-native run tree.
        unsafe {
            libc::setpgid(0, 0);
        }
        apply_memory_limit(memory_mb)?;
        match apply_landlock(policy) {
            Ok(()) => Ok(()),
            Err(error) if allow_unenforced => {
                let _ = error;
                Ok(())
            }
            Err(error) => Err(io::Error::new(io::ErrorKind::Other, error.to_string())),
        }
    }

    pub(super) fn landlock_abi_version() -> io::Result<i32> {
        let result = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<c_void>(),
                0usize,
                LANDLOCK_CREATE_RULESET_VERSION,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as i32)
        }
    }

    pub(super) fn kill_process_group(child_pid: i32) {
        unsafe {
            let _ = libc::kill(-child_pid, libc::SIGKILL);
            let _ = libc::kill(child_pid, libc::SIGKILL);
        }
    }

    fn apply_memory_limit(memory_mb: u64) -> io::Result<()> {
        if memory_mb == SANDBOX_UNLIMITED_SIZE_LIMIT {
            return Ok(());
        }
        let bytes = memory_mb.saturating_mul(1024 * 1024);
        let limit = libc::rlimit {
            rlim_cur: bytes as libc::rlim_t,
            rlim_max: bytes as libc::rlim_t,
        };
        let result = unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn apply_landlock(policy: &SandboxSimpleNativePolicy) -> io::Result<()> {
        let abi = landlock_abi_version()?;
        let handled = handled_access_for_abi(abi);
        let ruleset_attr = LandlockRulesetAttr {
            handled_access_fs: handled,
        };
        let ruleset_fd = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                &ruleset_attr as *const LandlockRulesetAttr,
                mem::size_of::<LandlockRulesetAttr>(),
                0u32,
            )
        };
        if ruleset_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let ruleset_fd = ruleset_fd as c_int;
        let result = (|| {
            for path in &policy.read_paths {
                add_path_rule(ruleset_fd, path, read_access(), handled)?;
            }
            for path in &policy.execute_paths {
                add_path_rule(ruleset_fd, path, read_execute_access(), handled)?;
            }
            for path in &policy.write_paths {
                add_path_rule(
                    ruleset_fd,
                    path,
                    read_write_execute_access(handled),
                    handled,
                )?;
            }
            let nnp = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
            if nnp != 0 {
                return Err(io::Error::last_os_error());
            }
            let restricted =
                unsafe { libc::syscall(libc::SYS_landlock_restrict_self, ruleset_fd, 0u32) };
            if restricted != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        })();
        unsafe {
            libc::close(ruleset_fd);
        }
        result
    }

    fn add_path_rule(
        ruleset_fd: c_int,
        path: &Path,
        allowed_access: u64,
        handled: u64,
    ) -> io::Result<()> {
        let allowed_access = allowed_access_for_path(path, allowed_access, handled)?;
        if allowed_access == 0 {
            return Ok(());
        }
        let file = fs::File::open(path)?;
        let attr = LandlockPathBeneathAttr {
            allowed_access,
            parent_fd: file.as_raw_fd(),
        };
        let result = unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                ruleset_fd,
                LANDLOCK_RULE_PATH_BENEATH,
                &attr as *const LandlockPathBeneathAttr,
                0u32,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn allowed_access_for_path(path: &Path, allowed_access: u64, handled: u64) -> io::Result<u64> {
        let metadata = fs::metadata(path)?;
        let compatible = if metadata.is_dir() {
            handled & !LANDLOCK_ACCESS_FS_REFER
        } else {
            LANDLOCK_ACCESS_FS_EXECUTE
                | LANDLOCK_ACCESS_FS_WRITE_FILE
                | LANDLOCK_ACCESS_FS_READ_FILE
                | (handled & LANDLOCK_ACCESS_FS_TRUNCATE)
                | (handled & LANDLOCK_ACCESS_FS_IOCTL_DEV)
        };
        Ok(allowed_access & handled & compatible)
    }

    fn handled_access_for_abi(abi: i32) -> u64 {
        let mut handled = LANDLOCK_ACCESS_FS_EXECUTE
            | LANDLOCK_ACCESS_FS_WRITE_FILE
            | LANDLOCK_ACCESS_FS_READ_FILE
            | LANDLOCK_ACCESS_FS_READ_DIR
            | LANDLOCK_ACCESS_FS_REMOVE_DIR
            | LANDLOCK_ACCESS_FS_REMOVE_FILE
            | LANDLOCK_ACCESS_FS_MAKE_CHAR
            | LANDLOCK_ACCESS_FS_MAKE_DIR
            | LANDLOCK_ACCESS_FS_MAKE_REG
            | LANDLOCK_ACCESS_FS_MAKE_SOCK
            | LANDLOCK_ACCESS_FS_MAKE_FIFO
            | LANDLOCK_ACCESS_FS_MAKE_BLOCK
            | LANDLOCK_ACCESS_FS_MAKE_SYM;
        if abi >= 2 {
            handled |= LANDLOCK_ACCESS_FS_REFER;
        }
        if abi >= 3 {
            handled |= LANDLOCK_ACCESS_FS_TRUNCATE;
        }
        if abi >= 5 {
            handled |= LANDLOCK_ACCESS_FS_IOCTL_DEV;
        }
        handled
    }

    fn read_access() -> u64 {
        LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR
    }

    fn read_execute_access() -> u64 {
        read_access() | LANDLOCK_ACCESS_FS_EXECUTE
    }

    fn read_write_execute_access(handled: u64) -> u64 {
        handled & !LANDLOCK_ACCESS_FS_REFER
    }
}

#[cfg(not(target_os = "linux"))]
mod linux {
    use std::io;

    use sinomo_sandbox::SandboxWasiError;

    use super::SandboxSimpleNativePolicy;

    pub(super) fn ensure_landlock_or_allowed(
        allow_unenforced: bool,
    ) -> Result<(), SandboxWasiError> {
        if allow_unenforced {
            Ok(())
        } else {
            Err(SandboxWasiError::Runtime {
                detail:
                    "simple-native filesystem enforcement is currently implemented only on Linux"
                        .to_string(),
            })
        }
    }

    pub(super) fn prepare_child_process(
        _policy: &SandboxSimpleNativePolicy,
        _allow_unenforced: bool,
        _memory_mb: u64,
    ) -> io::Result<()> {
        Ok(())
    }

    pub(super) fn landlock_abi_version() -> io::Result<i32> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Landlock is Linux-only",
        ))
    }

    pub(super) fn kill_process_group(child_pid: i32) {
        let _ = child_pid;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_sandbox::{SandboxId, SandboxToolchainId};

    fn create_store() -> (
        sinomo_testdir::TestDir,
        LocalSandboxInstanceStore,
        SandboxId,
    ) {
        let dir = sinomo_testdir::TestDir::new("sinomo-sandbox-simple");
        let store = LocalSandboxInstanceStore::new(dir.path().join("runtime"));
        let sandbox_id = SandboxId::generate();
        let toolchain_id =
            SandboxToolchainId::from_legacy_named("simple-native").expect("toolchain id");
        store
            .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
            .expect("create sandbox");
        (dir, store, sandbox_id)
    }

    fn runner() -> SandboxSimpleNativeCommandRunner {
        SandboxSimpleNativeCommandRunner::new(SandboxSimpleNativeConfig {
            allow_unenforced: false,
            extra_read_paths: Vec::new(),
            extra_write_paths: Vec::new(),
            extra_execute_paths: Vec::new(),
        })
    }

    fn run_shell(
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        script: &str,
        timeout_ms: u64,
    ) -> SandboxLocalCommandOutcome {
        let node_id = NodeId::from_legacy_named("sandbox_simple_test").expect("node");
        let toolchain_store = sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());
        runner()
            .run_toolchain_command_with_stdin_policy_and_output_sink(
                store,
                &toolchain_store,
                sandbox_id,
                "sh",
                vec!["sh".to_string(), "-c".to_string(), script.to_string()],
                None,
                timeout_ms,
                &node_id,
                &SandboxPolicy::default(),
                None,
            )
            .expect("simple-native command")
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_can_read_system_files() {
        if !SandboxSimpleNativeCommandRunner::landlock_supported() {
            return;
        }
        let (_dir, store, sandbox_id) = create_store();
        let outcome = run_shell(&store, &sandbox_id, "cat /etc/passwd >/dev/null", 0);
        assert_eq!(outcome.response.exit_status, 0);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_blocks_unapproved_host_reads() {
        if !SandboxSimpleNativeCommandRunner::landlock_supported() {
            return;
        }
        let (dir, store, sandbox_id) = create_store();
        let secret = dir.path().join("secret.txt");
        fs::write(&secret, b"secret").expect("secret");
        let outcome = run_shell(
            &store,
            &sandbox_id,
            &format!("cat {} >/dev/null", secret.display()),
            0,
        );
        assert_ne!(outcome.response.exit_status, 0);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_allows_sandbox_work_writes() {
        if !SandboxSimpleNativeCommandRunner::landlock_supported() {
            return;
        }
        let (_dir, store, sandbox_id) = create_store();
        let (_manifest, layout) = store.load(&sandbox_id).expect("load");
        let outcome = run_shell(
            &store,
            &sandbox_id,
            &format!("printf ok > {}/out.txt", layout.work.display()),
            0,
        );
        assert_eq!(outcome.response.exit_status, 0);
        assert_eq!(
            fs::read_to_string(layout.work.join("out.txt")).expect("out"),
            "ok"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_blocks_unapproved_host_writes() {
        if !SandboxSimpleNativeCommandRunner::landlock_supported() {
            return;
        }
        let (dir, store, sandbox_id) = create_store();
        let denied = dir.path().join("denied.txt");
        let outcome = run_shell(
            &store,
            &sandbox_id,
            &format!("printf no > {}", denied.display()),
            0,
        );
        assert_ne!(outcome.response.exit_status, 0);
        assert!(!denied.exists());
    }
}
