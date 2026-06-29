//! Simple native sandbox runtime.
//!
//! This runtime executes host binaries directly and exposes the host filesystem
//! layout as-is. On Linux, it uses Landlock as a best-effort filesystem
//! allowlist guard. On Windows, it uses a restricted low-integrity token and
//! labels configured write paths for low-integrity writes. This is intended to
//! prevent accidental access to unapproved paths; it is not a hostile-code
//! security boundary.

use std::collections::BTreeSet;
use std::env;
use std::fs;
#[cfg(not(windows))]
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(not(windows))]
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
#[cfg(not(windows))]
use std::sync::mpsc;
use std::time::{Duration, Instant};

use sinomo_node::NodeId;
#[cfg(not(windows))]
use sinomo_sandbox::SandboxCancellationToken;
use sinomo_sandbox::{
    LocalSandboxFileSystem, LocalSandboxInstanceLayout, LocalSandboxInstanceStore,
    SANDBOX_UNLIMITED_WALL_TIME_MS, SandboxCommandExecutionError, SandboxCommandId,
    SandboxCommandRecord, SandboxCommandRecordStatus, SandboxCommandRequest, SandboxFsError,
    SandboxLocalCommandOutcome, SandboxOutputSink, SandboxOutputStream, SandboxPath, SandboxPolicy,
    SandboxProcessPlan, SandboxProcessPlanOutcome, SandboxResourceLimits, SandboxResourceUsage,
    SandboxStoreError, SandboxWasiError, effective_sandbox_timeout_ms, persist_command_outputs,
    record_local_command_history, sandbox_command_record_status_for_wasi_error,
    sandbox_command_stderr_path, sandbox_command_stdout_path, sandbox_output_chunks,
    validate_sandbox_process_plan, validate_sandbox_process_plan_working_directories,
    validate_sandbox_toolchain_argv, validate_toolchain_command_name, validate_workspace_usage,
    validate_written_bytes,
};
use time::OffsetDateTime;

pub const SINOMO_SANDBOX_SIMPLE_BACKEND: &str = "simple-native";
pub const SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED_ENV: &str =
    "SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED";
pub const SINOMO_SANDBOX_SIMPLE_READ_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_READ_PATHS";
pub const SINOMO_SANDBOX_SIMPLE_WRITE_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_WRITE_PATHS";
pub const SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS";

const SIMPLE_NATIVE_WAIT_POLL_MS: u64 = 50;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
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

    #[cfg(target_os = "linux")]
    pub fn landlock_supported() -> bool {
        linux::landlock_abi_version().is_ok()
    }

    #[cfg(not(target_os = "linux"))]
    pub fn landlock_supported() -> bool {
        false
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

        if let Some(stdin) = &request.stdin
            && stdin.len() as u64 > manifest.limits.max_input_bytes
        {
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
        let exit_status = output.exit_status;
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
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    execute_paths: Vec<PathBuf>,
}

#[derive(Debug)]
struct NativeCommandOutput {
    exit_status: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
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
) -> Result<NativeCommandOutput, SandboxWasiError> {
    #[cfg(windows)]
    {
        let _ = policy;
        return windows::execute_native_command(request, layout, cwd, timeout_ms, config, limits);
    }

    #[cfg(not(windows))]
    {
        execute_native_command_std(request, layout, cwd, policy, timeout_ms, config, limits)
    }
}

#[cfg(not(windows))]
fn execute_native_command_std(
    request: &SandboxCommandRequest,
    layout: &LocalSandboxInstanceLayout,
    cwd: &Path,
    policy: &SandboxSimpleNativePolicy,
    timeout_ms: u64,
    config: &SandboxSimpleNativeConfig,
    limits: &SandboxResourceLimits,
) -> Result<NativeCommandOutput, SandboxWasiError> {
    let argv0 = request
        .argv
        .first()
        .ok_or_else(|| SandboxWasiError::InvalidRequest {
            detail: "simple-native command argv must include argv[0]".to_string(),
        })?;
    linux::ensure_landlock_or_allowed(config.allow_unenforced)?;

    let env = native_environment(request, layout, cwd);
    let program = resolve_native_program(argv0, &env, cwd)?;
    let mut command = Command::new(&program);
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
    for (name, value) in env {
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
            detail: if program == Path::new(argv0) {
                format!("failed to start simple-native command `{argv0}`: {source}")
            } else {
                format!(
                    "failed to start simple-native command `{argv0}` resolved to `{}`: {source}",
                    program.display()
                )
            },
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
    let output =
        wait_for_native_output(child_pid, rx, timeout_ms, &SandboxCancellationToken::new())?;
    Ok(NativeCommandOutput {
        exit_status: exit_status_code(&output),
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

#[cfg(not(windows))]
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
        Ok(path) if path.exists() => Ok(path),
        Ok(_) => resolve_host_or_root_cwd(layout, cwd),
        Err(SandboxFsError::NotFound { .. }) => resolve_host_or_root_cwd(layout, cwd),
        Err(error) => Err(SandboxCommandExecutionError::Fs(error)),
    }
}

fn resolve_host_or_root_cwd(
    layout: &LocalSandboxInstanceLayout,
    cwd: &SandboxPath,
) -> Result<PathBuf, SandboxCommandExecutionError> {
    #[cfg(windows)]
    if let Some(path) = windows_host_path_from_sandbox_path(cwd)
        && path.exists()
    {
        return Ok(path);
    }
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

#[cfg(windows)]
fn windows_host_path_from_sandbox_path(cwd: &SandboxPath) -> Option<PathBuf> {
    let encoded = cwd.as_str().strip_prefix('/')?;
    let bytes = encoded.as_bytes();
    if bytes.len() < 2 || bytes[1] != b':' || !bytes[0].is_ascii_alphabetic() {
        return None;
    }
    let mut host_path = String::with_capacity(encoded.len());
    host_path.push(bytes[0] as char);
    host_path.push(':');
    host_path.push_str(&encoded[2..].replace('/', "\\"));
    Some(PathBuf::from(host_path))
}

#[cfg(not(windows))]
fn resolve_native_program(
    argv0: &str,
    envs: &[(String, String)],
    cwd: &Path,
) -> Result<PathBuf, SandboxWasiError> {
    if argv0.contains('/') || argv0.contains('\\') {
        return Ok(PathBuf::from(argv0));
    }
    let path = envs
        .iter()
        .find_map(|(name, value)| (name == "PATH").then_some(value.as_str()))
        .unwrap_or("/usr/local/bin:/usr/bin:/bin");
    for dir in env::split_paths(path) {
        let candidate = if dir.as_os_str().is_empty() {
            cwd.join(argv0)
        } else {
            dir.join(argv0)
        };
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(SandboxWasiError::Runtime {
        detail: format!("failed to resolve simple-native command `{argv0}` on PATH"),
    })
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
    envs.push(("USERPROFILE".to_string(), layout.home.display().to_string()));
    envs.push(("TMPDIR".to_string(), layout.tmp.display().to_string()));
    envs.push(("TEMP".to_string(), layout.tmp.display().to_string()));
    envs.push(("TMP".to_string(), layout.tmp.display().to_string()));
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

#[cfg(all(not(windows), unix))]
fn exit_status_code(output: &Output) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    output
        .status
        .code()
        .or_else(|| output.status.signal().map(|signal| 128 + signal))
        .unwrap_or(128)
}

#[cfg(all(not(windows), not(unix)))]
fn exit_status_code(output: &Output) -> i32 {
    output.status.code().unwrap_or(128)
}

#[cfg(windows)]
mod windows {
    use std::collections::BTreeMap;
    use std::ffi::OsStr;
    use std::mem;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use std::ptr;
    use std::thread;
    use std::time::Instant;

    use sinomo_sandbox::SANDBOX_UNLIMITED_SIZE_LIMIT;
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_BROKEN_PIPE, ERROR_HANDLE_EOF, ERROR_NO_DATA, GetLastError, HANDLE,
        HANDLE_FLAG_INHERIT, HLOCAL, INVALID_HANDLE_VALUE, LocalFree, STILL_ACTIVE, WAIT_FAILED,
        WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1, SE_FILE_OBJECT,
        SetNamedSecurityInfoW,
    };
    use windows_sys::Win32::Security::{
        ACL, CreateRestrictedToken, CreateWellKnownSid, DISABLE_MAX_PRIVILEGE,
        GetSecurityDescriptorSacl, LABEL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
        SECURITY_ATTRIBUTES, SID_AND_ATTRIBUTES, SetTokenInformation, TOKEN_ADJUST_DEFAULT,
        TOKEN_ASSIGN_PRIMARY, TOKEN_DUPLICATE, TOKEN_MANDATORY_LABEL, TOKEN_QUERY,
        TokenIntegrityLevel, WinLowLabelSid,
    };
    use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_JOB_MEMORY,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectExtendedLimitInformation, SetInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Pipes::CreatePipe;
    use windows_sys::Win32::System::SystemServices::{
        SE_GROUP_INTEGRITY, SE_GROUP_INTEGRITY_ENABLED,
    };
    use windows_sys::Win32::System::Threading::{
        CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessAsUserW,
        CreateProcessW, GetCurrentProcess, GetExitCodeProcess, INFINITE, OpenProcessToken,
        PROCESS_INFORMATION, ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOW, TerminateProcess,
        WaitForSingleObject,
    };

    use super::{
        LocalSandboxInstanceLayout, NativeCommandOutput, SandboxCommandRequest,
        SandboxResourceLimits, SandboxSimpleNativeConfig, SandboxWasiError, native_environment,
        native_wait_poll_duration,
    };

    const READ_BUFFER_SIZE: usize = 64 * 1024;
    const SECURITY_MAX_SID_SIZE: usize = 68;

    pub(super) fn execute_native_command(
        request: &SandboxCommandRequest,
        layout: &LocalSandboxInstanceLayout,
        cwd: &Path,
        timeout_ms: u64,
        config: &SandboxSimpleNativeConfig,
        limits: &SandboxResourceLimits,
    ) -> Result<NativeCommandOutput, SandboxWasiError> {
        let low_integrity = match prepare_low_integrity_layout(layout, config) {
            Ok(()) => true,
            Err(error) if config.allow_unenforced => {
                let _ = error;
                false
            }
            Err(error) => return Err(error),
        };
        let token = match create_restricted_token(low_integrity) {
            Ok(token) => Some(token),
            Err(error) if config.allow_unenforced => {
                let _ = error;
                None
            }
            Err(error) => return Err(error),
        };
        match execute_with_windows_process(
            request,
            layout,
            cwd,
            timeout_ms,
            limits,
            token.as_ref(),
        ) {
            Ok(output) => Ok(output),
            Err(error) if token.is_some() && config.allow_unenforced => {
                execute_with_windows_process(request, layout, cwd, timeout_ms, limits, None)
                    .map_err(|fallback_error| SandboxWasiError::Runtime {
                        detail: format!(
                            "simple-native restricted Windows process failed ({error}); unrestricted fallback failed ({fallback_error})"
                        ),
                    })
            }
            Err(error) => Err(error),
        }
    }

    fn execute_with_windows_process(
        request: &SandboxCommandRequest,
        layout: &LocalSandboxInstanceLayout,
        cwd: &Path,
        timeout_ms: u64,
        limits: &SandboxResourceLimits,
        token: Option<&OwnedHandle>,
    ) -> Result<NativeCommandOutput, SandboxWasiError> {
        let argv0 = request
            .argv
            .first()
            .ok_or_else(|| SandboxWasiError::InvalidRequest {
                detail: "simple-native command argv must include argv[0]".to_string(),
            })?;
        let job = create_job(limits)?;
        let (stdin_read, stdin_write) = create_pipe_pair("stdin")?;
        let (stdout_read, stdout_write) = create_pipe_pair("stdout")?;
        let (stderr_read, stderr_write) = create_pipe_pair("stderr")?;
        set_handle_non_inheritable(stdin_write.raw(), "stdin write pipe")?;
        set_handle_non_inheritable(stdout_read.raw(), "stdout read pipe")?;
        set_handle_non_inheritable(stderr_read.raw(), "stderr read pipe")?;

        let startup = startup_info(stdin_read.raw(), stdout_write.raw(), stderr_write.raw());
        let mut process_info: PROCESS_INFORMATION = unsafe { mem::zeroed() };
        let mut command_line = wide_command_line(&request.argv)?;
        let environment = wide_environment(request, layout, cwd)?;
        let current_dir = wide_path(cwd)?;
        let creation_flags = CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT | CREATE_NO_WINDOW;

        let created = unsafe {
            match token {
                Some(token) => CreateProcessAsUserW(
                    token.raw(),
                    ptr::null(),
                    command_line.as_mut_ptr(),
                    ptr::null(),
                    ptr::null(),
                    1,
                    creation_flags,
                    environment.as_ptr().cast(),
                    current_dir.as_ptr(),
                    &startup,
                    &mut process_info,
                ),
                None => CreateProcessW(
                    ptr::null(),
                    command_line.as_mut_ptr(),
                    ptr::null(),
                    ptr::null(),
                    1,
                    creation_flags,
                    environment.as_ptr().cast(),
                    current_dir.as_ptr(),
                    &startup,
                    &mut process_info,
                ),
            }
        };
        if created == 0 {
            return Err(last_error(format!(
                "failed to start simple-native Windows command `{argv0}`"
            )));
        }

        let process = OwnedHandle::new(process_info.hProcess, "process handle")?;
        let thread = OwnedHandle::new(process_info.hThread, "thread handle")?;
        if unsafe { AssignProcessToJobObject(job.raw(), process.raw()) } == 0 {
            unsafe {
                let _ = TerminateProcess(process.raw(), 1);
            }
            return Err(last_error("failed to assign simple-native process to job"));
        }
        if unsafe { ResumeThread(thread.raw()) } == u32::MAX {
            unsafe {
                let _ = TerminateJobObject(job.raw(), 1);
            }
            return Err(last_error("failed to resume simple-native Windows thread"));
        }

        drop(stdin_read);
        drop(stdout_write);
        drop(stderr_write);
        let stdout_reader = spawn_pipe_reader(stdout_read, "stdout");
        let stderr_reader = spawn_pipe_reader(stderr_read, "stderr");

        let stdin_result = if let Some(stdin) = &request.stdin {
            write_pipe_all(&stdin_write, stdin)
        } else {
            Ok(())
        };
        drop(stdin_write);
        if let Err(error) = stdin_result {
            terminate_job_and_wait(&job, &process);
            let _ = join_pipe_reader(stdout_reader, "stdout");
            let _ = join_pipe_reader(stderr_reader, "stderr");
            return Err(error);
        }

        if let Err(error) = wait_for_process(&job, &process, timeout_ms) {
            let _ = join_pipe_reader(stdout_reader, "stdout");
            let _ = join_pipe_reader(stderr_reader, "stderr");
            return Err(error);
        }
        let exit_status = process_exit_code(&process);
        let stdout = join_pipe_reader(stdout_reader, "stdout");
        let stderr = join_pipe_reader(stderr_reader, "stderr");
        let exit_status = exit_status?;
        let stdout = stdout?;
        let stderr = stderr?;
        Ok(NativeCommandOutput {
            exit_status,
            stdout,
            stderr,
        })
    }

    fn startup_info(stdin: HANDLE, stdout: HANDLE, stderr: HANDLE) -> STARTUPINFOW {
        let mut startup: STARTUPINFOW = unsafe { mem::zeroed() };
        startup.cb = mem::size_of::<STARTUPINFOW>() as u32;
        startup.dwFlags = STARTF_USESTDHANDLES;
        startup.hStdInput = stdin;
        startup.hStdOutput = stdout;
        startup.hStdError = stderr;
        startup
    }

    fn create_job(limits: &SandboxResourceLimits) -> Result<OwnedHandle, SandboxWasiError> {
        let job = OwnedHandle::new(
            unsafe { CreateJobObjectW(ptr::null(), ptr::null()) },
            "job object",
        )?;
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if limits.memory_mb != SANDBOX_UNLIMITED_SIZE_LIMIT {
            info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY;
            info.JobMemoryLimit = limits
                .memory_mb
                .saturating_mul(1024 * 1024)
                .min(usize::MAX as u64) as usize;
        }
        if unsafe {
            SetInformationJobObject(
                job.raw(),
                JobObjectExtendedLimitInformation,
                (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(last_error("failed to configure simple-native job object"));
        }
        Ok(job)
    }

    fn create_restricted_token(low_integrity: bool) -> Result<OwnedHandle, SandboxWasiError> {
        let mut current_token = ptr::null_mut();
        let desired_access =
            TOKEN_ASSIGN_PRIMARY | TOKEN_DUPLICATE | TOKEN_ADJUST_DEFAULT | TOKEN_QUERY;
        if unsafe { OpenProcessToken(GetCurrentProcess(), desired_access, &mut current_token) } == 0
        {
            return Err(last_error("failed to open current process token"));
        }
        let current_token = OwnedHandle::new(current_token, "current process token")?;
        let mut restricted_token = ptr::null_mut();
        if unsafe {
            CreateRestrictedToken(
                current_token.raw(),
                DISABLE_MAX_PRIVILEGE,
                0,
                ptr::null(),
                0,
                ptr::null(),
                0,
                ptr::null(),
                &mut restricted_token,
            )
        } == 0
        {
            return Err(last_error(
                "failed to create restricted simple-native token",
            ));
        }
        let restricted_token = OwnedHandle::new(restricted_token, "restricted token")?;
        if low_integrity {
            set_low_integrity_token(restricted_token.raw())?;
        }
        Ok(restricted_token)
    }

    fn set_low_integrity_token(token: HANDLE) -> Result<(), SandboxWasiError> {
        let mut sid = [0u8; SECURITY_MAX_SID_SIZE];
        let mut sid_size = sid.len() as u32;
        if unsafe {
            CreateWellKnownSid(
                WinLowLabelSid,
                ptr::null_mut(),
                sid.as_mut_ptr().cast(),
                &mut sid_size,
            )
        } == 0
        {
            return Err(last_error("failed to create low-integrity SID"));
        }
        let label = TOKEN_MANDATORY_LABEL {
            Label: SID_AND_ATTRIBUTES {
                Sid: sid.as_mut_ptr().cast(),
                Attributes: (SE_GROUP_INTEGRITY | SE_GROUP_INTEGRITY_ENABLED) as u32,
            },
        };
        let length = mem::size_of::<TOKEN_MANDATORY_LABEL>() as u32 + sid_size;
        if unsafe {
            SetTokenInformation(
                token,
                TokenIntegrityLevel,
                (&label as *const TOKEN_MANDATORY_LABEL).cast(),
                length,
            )
        } == 0
        {
            return Err(last_error("failed to lower simple-native token integrity"));
        }
        Ok(())
    }

    fn prepare_low_integrity_layout(
        layout: &LocalSandboxInstanceLayout,
        config: &SandboxSimpleNativeConfig,
    ) -> Result<(), SandboxWasiError> {
        for dir in layout.required_dirs() {
            set_low_integrity_directory(dir)?;
        }
        for dir in &config.extra_write_paths {
            if dir.exists() {
                set_low_integrity_directory(dir)?;
            }
        }
        Ok(())
    }

    fn set_low_integrity_directory(path: &Path) -> Result<(), SandboxWasiError> {
        let sddl = wide_null("S:(ML;OICI;NW;;;LW)");
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        } == 0
        {
            return Err(last_error(format!(
                "failed to create low-integrity security descriptor for {}",
                path.display()
            )));
        }
        let _descriptor = LocalSecurityDescriptor(descriptor);
        let mut sacl_present = 0;
        let mut sacl_defaulted = 0;
        let mut sacl: *mut ACL = ptr::null_mut();
        if unsafe {
            GetSecurityDescriptorSacl(
                descriptor,
                &mut sacl_present,
                &mut sacl,
                &mut sacl_defaulted,
            )
        } == 0
        {
            return Err(last_error(format!(
                "failed to read low-integrity SACL for {}",
                path.display()
            )));
        }
        if sacl_present == 0 || sacl.is_null() {
            return Err(SandboxWasiError::Runtime {
                detail: format!(
                    "low-integrity security descriptor did not contain a SACL for {}",
                    path.display()
                ),
            });
        }
        let path = wide_path(path)?;
        let error = unsafe {
            SetNamedSecurityInfoW(
                path.as_ptr(),
                SE_FILE_OBJECT,
                LABEL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null(),
                sacl.cast_const(),
            )
        };
        if error != 0 {
            return Err(SandboxWasiError::Runtime {
                detail: format!(
                    "failed to mark sandbox directory low-integrity: Windows error {error}"
                ),
            });
        }
        Ok(())
    }

    fn create_pipe_pair(name: &str) -> Result<(OwnedHandle, OwnedHandle), SandboxWasiError> {
        let mut security = SECURITY_ATTRIBUTES {
            nLength: mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: ptr::null_mut(),
            bInheritHandle: 1,
        };
        let mut read = ptr::null_mut();
        let mut write = ptr::null_mut();
        if unsafe { CreatePipe(&mut read, &mut write, &mut security, 0) } == 0 {
            return Err(last_error(format!("failed to create {name} pipe")));
        }
        Ok((
            OwnedHandle::new(read, format!("{name} read pipe"))?,
            OwnedHandle::new(write, format!("{name} write pipe"))?,
        ))
    }

    fn set_handle_non_inheritable(
        handle: HANDLE,
        name: impl AsRef<str>,
    ) -> Result<(), SandboxWasiError> {
        if unsafe {
            windows_sys::Win32::Foundation::SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0)
        } == 0
        {
            Err(last_error(format!(
                "failed to clear inheritance on {}",
                name.as_ref()
            )))
        } else {
            Ok(())
        }
    }

    fn wait_for_process(
        job: &OwnedHandle,
        process: &OwnedHandle,
        timeout_ms: u64,
    ) -> Result<(), SandboxWasiError> {
        let started = Instant::now();
        loop {
            let wait = match native_wait_poll_duration(started, timeout_ms) {
                Ok(wait) => wait,
                Err(SandboxWasiError::TimedOut { timeout_ms }) => {
                    terminate_job_and_wait(job, process);
                    return Err(SandboxWasiError::TimedOut { timeout_ms });
                }
                Err(error) => return Err(error),
            };
            let wait_ms = wait.as_millis().min(u128::from(u32::MAX)) as u32;
            match unsafe { WaitForSingleObject(process.raw(), wait_ms) } {
                WAIT_OBJECT_0 => return Ok(()),
                WAIT_TIMEOUT => continue,
                WAIT_FAILED => return Err(last_error("failed to wait for simple-native process")),
                other => {
                    return Err(SandboxWasiError::Runtime {
                        detail: format!("unexpected Windows wait result {other}"),
                    });
                }
            }
        }
    }

    fn terminate_job_and_wait(job: &OwnedHandle, process: &OwnedHandle) {
        unsafe {
            let _ = TerminateJobObject(job.raw(), 1);
            let _ = WaitForSingleObject(process.raw(), INFINITE);
        }
    }

    fn process_exit_code(process: &OwnedHandle) -> Result<i32, SandboxWasiError> {
        let mut exit_code = 0;
        if unsafe { GetExitCodeProcess(process.raw(), &mut exit_code) } == 0 {
            return Err(last_error("failed to get simple-native process exit code"));
        }
        if exit_code == STILL_ACTIVE as u32 {
            return Err(SandboxWasiError::Runtime {
                detail: "simple-native process is still active after wait".to_string(),
            });
        }
        Ok(exit_code as i32)
    }

    fn spawn_pipe_reader(
        handle: OwnedHandle,
        stream: &'static str,
    ) -> thread::JoinHandle<Result<Vec<u8>, String>> {
        thread::spawn(move || {
            read_pipe_to_end(handle)
                .map_err(|error| format!("failed to read simple-native {stream}: {error}"))
        })
    }

    fn join_pipe_reader(
        reader: thread::JoinHandle<Result<Vec<u8>, String>>,
        stream: &str,
    ) -> Result<Vec<u8>, SandboxWasiError> {
        reader
            .join()
            .map_err(|_| SandboxWasiError::Runtime {
                detail: format!("simple-native {stream} reader panicked"),
            })?
            .map_err(|detail| SandboxWasiError::Runtime { detail })
    }

    fn read_pipe_to_end(handle: OwnedHandle) -> Result<Vec<u8>, String> {
        let mut output = Vec::new();
        let mut buffer = [0u8; READ_BUFFER_SIZE];
        loop {
            let mut bytes_read = 0;
            let ok = unsafe {
                ReadFile(
                    handle.raw(),
                    buffer.as_mut_ptr(),
                    buffer.len() as u32,
                    &mut bytes_read,
                    ptr::null_mut(),
                )
            };
            if ok == 0 {
                let error = unsafe { GetLastError() };
                if matches!(error, ERROR_BROKEN_PIPE | ERROR_HANDLE_EOF | ERROR_NO_DATA) {
                    break;
                }
                return Err(format!("Windows error {error}"));
            }
            if bytes_read == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..bytes_read as usize]);
        }
        Ok(output)
    }

    fn write_pipe_all(handle: &OwnedHandle, mut bytes: &[u8]) -> Result<(), SandboxWasiError> {
        while !bytes.is_empty() {
            let chunk_len = bytes.len().min(READ_BUFFER_SIZE);
            let mut bytes_written = 0;
            let ok = unsafe {
                WriteFile(
                    handle.raw(),
                    bytes.as_ptr(),
                    chunk_len as u32,
                    &mut bytes_written,
                    ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(last_error("failed to write simple-native stdin"));
            }
            if bytes_written == 0 {
                return Err(SandboxWasiError::Runtime {
                    detail: "failed to write simple-native stdin: zero-byte pipe write".to_string(),
                });
            }
            bytes = &bytes[bytes_written as usize..];
        }
        Ok(())
    }

    fn wide_command_line(argv: &[String]) -> Result<Vec<u16>, SandboxWasiError> {
        if argv.is_empty() {
            return Err(SandboxWasiError::InvalidRequest {
                detail: "simple-native command argv must include argv[0]".to_string(),
            });
        }
        let mut pieces = Vec::with_capacity(argv.len());
        for arg in argv {
            if arg.contains('\0') {
                return Err(SandboxWasiError::InvalidRequest {
                    detail: "simple-native Windows argv contains NUL byte".to_string(),
                });
            }
            pieces.push(quote_windows_arg(arg));
        }
        Ok(wide_null(&pieces.join(" ")))
    }

    fn quote_windows_arg(arg: &str) -> String {
        if !arg.is_empty() && !arg.chars().any(|ch| ch.is_whitespace() || ch == '"') {
            return arg.to_string();
        }
        let mut quoted = String::from("\"");
        let mut backslashes = 0usize;
        for ch in arg.chars() {
            match ch {
                '\\' => backslashes += 1,
                '"' => {
                    quoted.extend(std::iter::repeat('\\').take(backslashes * 2 + 1));
                    quoted.push('"');
                    backslashes = 0;
                }
                _ => {
                    quoted.extend(std::iter::repeat('\\').take(backslashes));
                    backslashes = 0;
                    quoted.push(ch);
                }
            }
        }
        quoted.extend(std::iter::repeat('\\').take(backslashes * 2));
        quoted.push('"');
        quoted
    }

    fn wide_environment(
        request: &SandboxCommandRequest,
        layout: &LocalSandboxInstanceLayout,
        cwd: &Path,
    ) -> Result<Vec<u16>, SandboxWasiError> {
        let mut envs = BTreeMap::new();
        for (name, value) in native_environment(request, layout, cwd) {
            if name.is_empty() || name.contains('=') || name.contains('\0') || value.contains('\0')
            {
                return Err(SandboxWasiError::InvalidRequest {
                    detail: format!("invalid simple-native Windows environment variable `{name}`"),
                });
            }
            envs.insert(name.to_ascii_uppercase(), (name, value));
        }
        let mut block = Vec::new();
        for (_key, (name, value)) in envs {
            block.extend(format!("{name}={value}").encode_utf16());
            block.push(0);
        }
        block.push(0);
        Ok(block)
    }

    fn wide_path(path: &Path) -> Result<Vec<u16>, SandboxWasiError> {
        let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if wide.contains(&0) {
            return Err(SandboxWasiError::InvalidRequest {
                detail: format!(
                    "simple-native Windows path contains NUL byte: {}",
                    path.display()
                ),
            });
        }
        wide.push(0);
        Ok(wide)
    }

    fn wide_null(value: &str) -> Vec<u16> {
        OsStr::new(value).encode_wide().chain(Some(0)).collect()
    }

    fn last_error(action: impl Into<String>) -> SandboxWasiError {
        SandboxWasiError::Runtime {
            detail: format!("{}: Windows error {}", action.into(), unsafe {
                GetLastError()
            }),
        }
    }

    #[derive(Debug)]
    struct OwnedHandle(HANDLE);

    impl OwnedHandle {
        fn new(handle: HANDLE, name: impl AsRef<str>) -> Result<Self, SandboxWasiError> {
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                Err(last_error(format!("failed to create {}", name.as_ref())))
            } else {
                Ok(Self(handle))
            }
        }

        fn raw(&self) -> HANDLE {
            self.0
        }
    }

    unsafe impl Send for OwnedHandle {}

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
                unsafe {
                    let _ = CloseHandle(self.0);
                }
            }
        }
    }

    struct LocalSecurityDescriptor(PSECURITY_DESCRIPTOR);

    impl Drop for LocalSecurityDescriptor {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    let _ = LocalFree(self.0 as HLOCAL);
                }
            }
        }
    }
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
            Err(error) => Err(io::Error::other(error.to_string())),
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

#[cfg(all(not(target_os = "linux"), not(windows)))]
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
    fn simple_native_can_execute_from_extra_host_workspace_cwd() {
        if !SandboxSimpleNativeCommandRunner::landlock_supported() {
            return;
        }
        let (dir, store, sandbox_id) = create_store();
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let (mut manifest, _layout) = store.load(&sandbox_id).expect("load");
        manifest.cwd =
            SandboxPath::new(workspace.display().to_string()).expect("host workspace cwd");
        store.save_manifest(&manifest).expect("save manifest");
        let node_id = NodeId::from_legacy_named("sandbox_simple_test").expect("node");
        let toolchain_store = sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());
        let outcome = SandboxSimpleNativeCommandRunner::new(SandboxSimpleNativeConfig {
            allow_unenforced: false,
            extra_read_paths: vec![workspace.clone()],
            extra_write_paths: vec![workspace],
            extra_execute_paths: Vec::new(),
        })
        .run_toolchain_command_with_stdin_policy_and_output_sink(
            &store,
            &toolchain_store,
            &sandbox_id,
            "true",
            vec!["true".to_string()],
            None,
            0,
            &node_id,
            &SandboxPolicy::default(),
            None,
        )
        .expect("simple-native host workspace cwd command");

        assert_eq!(outcome.response.exit_status, 0);
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

    #[test]
    #[cfg(windows)]
    fn windows_sandbox_drive_path_decodes_to_host_path() {
        let cwd = SandboxPath::new("/D:/Workspace/sinomo").expect("sandbox path");

        let path = windows_host_path_from_sandbox_path(&cwd).expect("host path");

        assert_eq!(path, PathBuf::from(r"D:\Workspace\sinomo"));
    }

    #[test]
    #[cfg(windows)]
    fn simple_native_windows_runs_restricted_cmd() {
        let (_dir, store, sandbox_id) = create_store();
        let node_id = NodeId::from_legacy_named("sandbox_simple_windows_test").expect("node");
        let toolchain_store = sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());

        let outcome = runner()
            .run_toolchain_command_with_stdin_policy_and_output_sink(
                &store,
                &toolchain_store,
                &sandbox_id,
                "cmd.exe",
                vec![
                    "cmd.exe".to_string(),
                    "/C".to_string(),
                    "echo ok".to_string(),
                ],
                None,
                5000,
                &node_id,
                &SandboxPolicy::default(),
                None,
            )
            .expect("simple-native Windows command");

        assert_eq!(outcome.response.exit_status, 0);
        assert!(
            String::from_utf8_lossy(&outcome.stdout)
                .to_ascii_lowercase()
                .contains("ok")
        );
    }

    #[test]
    #[cfg(windows)]
    fn simple_native_windows_allows_extra_write_paths() {
        let (dir, store, sandbox_id) = create_store();
        let allowed = dir.path().join("allowed-write");
        fs::create_dir_all(&allowed).expect("allowed write dir");
        let node_id = NodeId::from_legacy_named("sandbox_simple_windows_write_test").expect("node");
        let toolchain_store = sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());

        let outcome = SandboxSimpleNativeCommandRunner::new(SandboxSimpleNativeConfig {
            allow_unenforced: false,
            extra_read_paths: vec![allowed.clone()],
            extra_write_paths: vec![allowed.clone()],
            extra_execute_paths: Vec::new(),
        })
        .run_toolchain_command_with_stdin_policy_and_output_sink(
            &store,
            &toolchain_store,
            &sandbox_id,
            "cmd.exe",
            vec![
                "cmd.exe".to_string(),
                "/C".to_string(),
                format!(
                    "echo ok>{}\\out.txt && type {}\\out.txt",
                    allowed.display(),
                    allowed.display()
                ),
            ],
            None,
            5000,
            &node_id,
            &SandboxPolicy::default(),
            None,
        )
        .expect("simple-native Windows command");

        assert_eq!(
            outcome.response.exit_status,
            0,
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&outcome.stdout),
            String::from_utf8_lossy(&outcome.stderr)
        );
        assert_eq!(
            fs::read_to_string(allowed.join("out.txt")).expect("written file"),
            "ok \r\n"
        );
    }

    #[test]
    #[cfg(windows)]
    fn simple_native_windows_blocks_unlabeled_host_read() {
        let (dir, store, sandbox_id) = create_store();
        let secret = dir.path().join("outside-sandbox-secret.txt");
        fs::write(&secret, "outside-secret").expect("secret");
        let node_id = NodeId::from_legacy_named("sandbox_simple_windows_read_gap").expect("node");
        let toolchain_store = sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());

        let outcome = runner()
            .run_toolchain_command_with_stdin_policy_and_output_sink(
                &store,
                &toolchain_store,
                &sandbox_id,
                "cmd.exe",
                vec![
                    "cmd.exe".to_string(),
                    "/C".to_string(),
                    format!("type \"{}\"", secret.display()),
                ],
                None,
                5000,
                &node_id,
                &SandboxPolicy::default(),
                None,
            )
            .expect("simple-native Windows host read command");

        assert_ne!(outcome.response.exit_status, 0);
        assert!(!String::from_utf8_lossy(&outcome.stdout).contains("outside-secret"));
    }

    #[test]
    #[cfg(windows)]
    fn simple_native_windows_blocks_unapproved_host_write() {
        let (dir, store, sandbox_id) = create_store();
        let denied = dir.path().join("denied.txt");
        let node_id = NodeId::from_legacy_named("sandbox_simple_windows_write_gap").expect("node");
        let toolchain_store = sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());

        let outcome = runner()
            .run_toolchain_command_with_stdin_policy_and_output_sink(
                &store,
                &toolchain_store,
                &sandbox_id,
                "cmd.exe",
                vec![
                    "cmd.exe".to_string(),
                    "/C".to_string(),
                    format!("echo no>{}", denied.display()),
                ],
                None,
                5000,
                &node_id,
                &SandboxPolicy::default(),
                None,
            )
            .expect("simple-native Windows host write command");

        assert_ne!(outcome.response.exit_status, 0);
        assert!(!denied.exists());
    }
}
