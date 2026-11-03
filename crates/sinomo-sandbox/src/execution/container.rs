use crate::internal::*;

use std::ffi::OsStr;
use std::io::{Read, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const SANDBOX_CONTAINER_WAIT_POLL_MS: u64 = 50;
const SANDBOX_CONTAINER_TERMINATION_WAIT_MS: u64 = 5_000;
const SANDBOX_CONTAINER_MAX_CAPTURE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
struct ContainerCommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    terminal_error: Option<SandboxWasiError>,
}

#[derive(Debug)]
struct ContainerOutputBudget {
    limit: u64,
    consumed: AtomicU64,
    exceeded: AtomicBool,
}

impl ContainerOutputBudget {
    fn new(configured: u64) -> Self {
        Self {
            limit: configured.min(SANDBOX_CONTAINER_MAX_CAPTURE_BYTES),
            consumed: AtomicU64::new(0),
            exceeded: AtomicBool::new(false),
        }
    }

    fn reserve(&self, requested: usize) -> usize {
        let requested = requested as u64;
        let start = self.consumed.fetch_add(requested, Ordering::AcqRel);
        let allowed = self.limit.saturating_sub(start).min(requested) as usize;
        if allowed < requested as usize {
            self.exceeded.store(true, Ordering::Release);
        }
        allowed
    }

    fn exceeded(&self) -> bool {
        self.exceeded.load(Ordering::Acquire)
    }
}

pub const SINOMO_SANDBOX_BACKEND_ENV: &str = "SINOMO_SANDBOX_BACKEND";
pub const SINOMO_SANDBOX_CONTAINER_RUNTIME_ENV: &str = "SINOMO_SANDBOX_CONTAINER_RUNTIME";
pub const SINOMO_SANDBOX_CONTAINER_IMAGE_ENV: &str = "SINOMO_SANDBOX_CONTAINER_IMAGE";
pub const SINOMO_SANDBOX_CONTAINER_ALLOW_NETWORK_ENV: &str =
    "SINOMO_SANDBOX_CONTAINER_ALLOW_NETWORK";

#[derive(Debug, Error)]
pub enum SandboxContainerConfigError {
    #[error(
        "invalid sandbox container runtime `{value}`; expected docker, podman, or an executable path"
    )]
    InvalidRuntime { value: String },
    #[error("sandbox container image is required; set {SINOMO_SANDBOX_CONTAINER_IMAGE_ENV}")]
    MissingImage,
    #[error("invalid sandbox container image `{value}`")]
    InvalidImage { value: String },
    #[error(
        "invalid sandbox container boolean `{name}={value}`; expected true/false/1/0/yes/no/on/off"
    )]
    InvalidBool { name: &'static str, value: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SandboxContainerRuntime {
    Docker,
    Podman,
    Command(PathBuf),
}

impl SandboxContainerRuntime {
    pub fn parse(value: &str) -> Result<Self, SandboxContainerConfigError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(SandboxContainerConfigError::InvalidRuntime {
                value: value.to_string(),
            });
        }
        match trimmed {
            "docker" => Ok(Self::Docker),
            "podman" => Ok(Self::Podman),
            other => {
                let path = PathBuf::from(other);
                if path.components().count() > 1 || path.is_absolute() {
                    Ok(Self::Command(path))
                } else {
                    Err(SandboxContainerConfigError::InvalidRuntime {
                        value: value.to_string(),
                    })
                }
            }
        }
    }

    fn command(&self) -> &OsStr {
        match self {
            Self::Docker => OsStr::new("docker"),
            Self::Podman => OsStr::new("podman"),
            Self::Command(path) => path.as_os_str(),
        }
    }

    fn default_for_backend(value: Option<&str>) -> Result<Self, SandboxContainerConfigError> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            Some("docker") => Ok(Self::Docker),
            Some("podman") => Ok(Self::Podman),
            Some("container") => Ok(Self::Docker),
            Some(other) => Self::parse(other),
            None => Ok(Self::Docker),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxContainerBackendConfig {
    pub runtime: SandboxContainerRuntime,
    pub image: String,
    pub allow_network: bool,
    pub read_only_rootfs: bool,
    pub pull_never: bool,
}

impl SandboxContainerBackendConfig {
    pub fn new(
        runtime: SandboxContainerRuntime,
        image: impl Into<String>,
    ) -> Result<Self, SandboxContainerConfigError> {
        let image = image.into();
        validate_container_image(&image)?;
        Ok(Self {
            runtime,
            image,
            allow_network: false,
            read_only_rootfs: true,
            pull_never: true,
        })
    }

    pub fn from_env() -> Result<Option<Self>, SandboxContainerConfigError> {
        Self::from_env_vars(|name| std::env::var(name).ok())
    }

    pub fn from_env_vars<F>(get: F) -> Result<Option<Self>, SandboxContainerConfigError>
    where
        F: Fn(&'static str) -> Option<String>,
    {
        let backend = get(SINOMO_SANDBOX_BACKEND_ENV);
        let runtime = get(SINOMO_SANDBOX_CONTAINER_RUNTIME_ENV);
        let image = get(SINOMO_SANDBOX_CONTAINER_IMAGE_ENV);
        let backend_requests_container = backend
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| matches!(value, "docker" | "podman" | "container"));
        if !backend_requests_container && runtime.is_none() && image.is_none() {
            return Ok(None);
        }
        let runtime = match runtime
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(value) => SandboxContainerRuntime::parse(value)?,
            None => SandboxContainerRuntime::default_for_backend(backend.as_deref())?,
        };
        let Some(image) = image
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        else {
            return Err(SandboxContainerConfigError::MissingImage);
        };
        validate_container_image(&image)?;
        let allow_network = get(SINOMO_SANDBOX_CONTAINER_ALLOW_NETWORK_ENV)
            .map(|value| parse_container_bool(SINOMO_SANDBOX_CONTAINER_ALLOW_NETWORK_ENV, &value))
            .transpose()?
            .unwrap_or(false);
        Ok(Some(Self {
            runtime,
            image,
            allow_network,
            read_only_rootfs: true,
            pull_never: true,
        }))
    }
}

#[derive(Clone, Debug)]
pub struct SandboxContainerCommandRunner {
    config: SandboxContainerBackendConfig,
}

impl SandboxContainerCommandRunner {
    pub fn new(config: SandboxContainerBackendConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &SandboxContainerBackendConfig {
        &self.config
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_and_output_sink_and_cancellation(
            store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            output_sink,
            SandboxCancellationToken::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_and_output_sink_and_cancellation(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        validate_toolchain_command_name(command_name)?;
        validate_sandbox_toolchain_argv(command_name, &argv).map_err(|error| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { detail: error })
        })?;
        let local_execution_lease = try_acquire_sandbox_local_execution_lease(store, sandbox_id)?;
        recover_sandbox_local_execution_journal_with_lease(
            store,
            &local_execution_lease,
            sandbox_id,
            OffsetDateTime::now_utc(),
        )?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let mut fs = LocalSandboxFileSystem::new(layout.clone());
        validate_sandbox_working_directory(&mut fs, &manifest.cwd)?;
        let request = SandboxCommandRequest {
            sandbox_id: sandbox_id.clone(),
            argv,
            cwd: manifest.cwd.clone(),
            stdin,
            env: manifest.env.clone(),
            timeout_ms,
        };
        self.run_container_command(
            store,
            &manifest,
            &layout,
            request,
            output_sink,
            &cancellation_token,
            &local_execution_lease,
        )
    }

    pub fn run_process_plan_with_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        self.run_process_plan_with_output_sink_and_cancellation(
            store,
            sandbox_id,
            plan,
            timeout_ms,
            output_sink,
            SandboxCancellationToken::new(),
        )
    }

    pub fn run_process_plan_with_output_sink_and_cancellation(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        validate_sandbox_process_plan(plan)?;
        let local_execution_lease = try_acquire_sandbox_local_execution_lease(store, sandbox_id)?;
        recover_sandbox_local_execution_journal_with_lease(
            store,
            &local_execution_lease,
            sandbox_id,
            OffsetDateTime::now_utc(),
        )?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let mut fs = LocalSandboxFileSystem::new(layout.clone());
        validate_sandbox_process_plan_working_directories(plan, &mut fs, &manifest.cwd)?;

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
            env.extend(step.env.clone());
            let request = SandboxCommandRequest {
                sandbox_id: sandbox_id.clone(),
                argv: step.argv.clone(),
                cwd: step.cwd.clone().unwrap_or_else(|| manifest.cwd.clone()),
                stdin,
                env,
                timeout_ms,
            };
            let outcome = self.run_container_command(
                store,
                &manifest,
                &layout,
                request,
                output_sink.clone(),
                &cancellation_token,
                &local_execution_lease,
            )?;
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

    fn run_container_command(
        &self,
        store: &LocalSandboxInstanceStore,
        manifest: &SandboxInstanceManifest,
        layout: &LocalSandboxInstanceLayout,
        request: SandboxCommandRequest,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: &SandboxCancellationToken,
        local_execution_lease: &SandboxLocalExecutionLease,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        validate_sandbox_toolchain_argv(&request.argv[0], &request.argv).map_err(|error| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { detail: error })
        })?;
        let command_id = SandboxCommandId::generate();
        let started_at = OffsetDateTime::now_utc();
        let timeout_ms =
            effective_sandbox_timeout_ms(request.timeout_ms, manifest.limits.wall_time_ms);
        if cancellation_token.is_cancelled() {
            let error = SandboxWasiError::Cancelled;
            self.persist_failed_command(
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
        if let Some(stdin) = &request.stdin
            && stdin.len() as u64 > manifest.limits.max_input_bytes
        {
            let error = SandboxWasiError::InputLimitExceeded {
                limit: manifest.limits.max_input_bytes.min(usize::MAX as u64) as usize,
            };
            self.persist_failed_command(
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

        let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        validate_sandbox_working_directory(&mut sandbox_fs, &request.cwd)?;
        let before = sandbox_fs.snapshot()?;
        let mut local_journal = begin_sandbox_local_execution_journal(
            store,
            local_execution_lease,
            manifest,
            OffsetDateTime::now_utc(),
        )?;
        prepare_sandbox_local_execution_audit_identity(
            store,
            local_execution_lease,
            &mut local_journal,
            SandboxCommandRecord {
                sandbox_id: manifest.sandbox_id.clone(),
                command_id: command_id.clone(),
                status: SandboxCommandRecordStatus::Failed,
                workspace_root: manifest.workspace_root.clone(),
                toolchain_id: manifest.toolchain_id.clone(),
                argv: request.argv.clone(),
                cwd: request.cwd.clone(),
                env: request.env.clone(),
                started_at,
                completed_at: None,
                response: None,
                error: Some(
                    "sandbox host stopped while the container guest was active".to_string(),
                ),
            },
            OffsetDateTime::now_utc(),
        )?;
        // The execution journal records ownership and audit state only. The
        // mounted workspace is authoritative and must never be rewound as an
        // implicit consequence of command failure or cancellation.
        let container_name = container_name(&command_id);
        prepare_sandbox_local_execution_external_owner(
            store,
            local_execution_lease,
            &mut local_journal,
            SandboxLocalExecutionExternalOwner::container(
                self.config.runtime.command().to_string_lossy(),
                container_name,
            ),
            OffsetDateTime::now_utc(),
        )?;
        activate_sandbox_local_execution_journal(
            store,
            local_execution_lease,
            &mut local_journal,
            OffsetDateTime::now_utc(),
        )?;
        let started = Instant::now();
        let output = self.execute_container_command(
            layout,
            &command_id,
            &request,
            &manifest.limits,
            timeout_ms,
            cancellation_token,
            store,
            local_execution_lease,
            &mut local_journal,
        );
        if let Err(cleanup_error) = cleanup_sandbox_local_execution_external_owner(
            store,
            local_execution_lease,
            &mut local_journal,
            OffsetDateTime::now_utc(),
        ) {
            let cancelled = cancellation_token.is_cancelled()
                || matches!(&output, Err(SandboxWasiError::Cancelled))
                || matches!(
                    &output,
                    Ok(ContainerCommandOutput {
                        terminal_error: Some(SandboxWasiError::Cancelled),
                        ..
                    })
                );
            if cancelled {
                return Err(SandboxWasiError::CancelledCleanupFailed {
                    detail: cleanup_error.to_string(),
                }
                .into());
            }
            return Err(cleanup_error.into());
        }
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                let completed_at = OffsetDateTime::now_utc();
                settle_sandbox_local_execution_journal(
                    store,
                    local_execution_lease,
                    &mut local_journal,
                    if matches!(error, SandboxWasiError::Cancelled) {
                        SandboxLocalExecutionOutcome::Cancelled
                    } else {
                        SandboxLocalExecutionOutcome::Failed {
                            detail: error.to_string(),
                        }
                    },
                    SandboxCommandRecord {
                        sandbox_id: manifest.sandbox_id.clone(),
                        command_id: command_id.clone(),
                        status: sandbox_command_record_status_for_wasi_error(&error),
                        workspace_root: manifest.workspace_root.clone(),
                        toolchain_id: manifest.toolchain_id.clone(),
                        argv: request.argv.clone(),
                        cwd: request.cwd.clone(),
                        env: request.env.clone(),
                        started_at,
                        completed_at: Some(completed_at),
                        response: None,
                        error: Some(error.to_string()),
                    },
                    Vec::new(),
                    Vec::new(),
                    completed_at,
                )?;
                recover_sandbox_local_execution_journal_with_lease(
                    store,
                    local_execution_lease,
                    &manifest.sandbox_id,
                    completed_at,
                )?;
                return Err(error.into());
            }
        };
        let wall_time_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let terminal_error = output.terminal_error;
        let stdout = output.stdout;
        let stderr = output.stderr;
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
        let exit_status = output.status.code().unwrap_or(128);
        let response = SandboxCommandResponse {
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

        let execution_error = if let Some(error) = terminal_error {
            Some(SandboxCommandExecutionError::Wasi(error))
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
        let status = match execution_error.as_ref() {
            Some(SandboxCommandExecutionError::Wasi(error)) => {
                sandbox_command_record_status_for_wasi_error(error)
            }
            Some(_) => SandboxCommandRecordStatus::ResourceExhausted,
            None => SandboxCommandRecordStatus::Completed,
        };
        let error = execution_error.as_ref().map(ToString::to_string);
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
        settle_sandbox_local_execution_journal(
            store,
            local_execution_lease,
            &mut local_journal,
            match execution_error.as_ref() {
                None => SandboxLocalExecutionOutcome::Succeeded,
                Some(SandboxCommandExecutionError::Wasi(SandboxWasiError::Cancelled)) => {
                    SandboxLocalExecutionOutcome::Cancelled
                }
                Some(error) => SandboxLocalExecutionOutcome::Failed {
                    detail: error.to_string(),
                },
            },
            record,
            stdout.clone(),
            stderr.clone(),
            completed_at,
        )?;
        recover_sandbox_local_execution_journal_with_lease(
            store,
            local_execution_lease,
            &manifest.sandbox_id,
            completed_at,
        )?;
        if let Some(error) = execution_error {
            return Err(error);
        }
        Ok(SandboxLocalCommandOutcome {
            response,
            stdout,
            stderr,
            output_chunks,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn persist_failed_command(
        &self,
        store: &LocalSandboxInstanceStore,
        manifest: &SandboxInstanceManifest,
        layout: &LocalSandboxInstanceLayout,
        command_id: &SandboxCommandId,
        request: &SandboxCommandRequest,
        started_at: OffsetDateTime,
        response: Option<SandboxCommandResponse>,
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

    fn execute_container_command(
        &self,
        layout: &LocalSandboxInstanceLayout,
        command_id: &SandboxCommandId,
        request: &SandboxCommandRequest,
        limits: &SandboxResourceLimits,
        timeout_ms: u64,
        cancellation_token: &SandboxCancellationToken,
        store: &LocalSandboxInstanceStore,
        local_execution_lease: &SandboxLocalExecutionLease,
        local_journal: &mut SandboxLocalExecutionJournal,
    ) -> Result<ContainerCommandOutput, SandboxWasiError> {
        ensure_container_layout_dirs(layout)?;
        let container_name = container_name(command_id);
        let args = self.container_run_args(layout, &container_name, request, limits)?;
        #[cfg(target_os = "linux")]
        let mut command = {
            let mut command = Command::new("/bin/sh");
            command
                .args([
                    "-c",
                    "IFS= read -r _ <&3; exec 3<&-; exec \"$@\"",
                    "sinomo-handshake",
                ])
                .arg(self.config.runtime.command())
                .args(&args);
            command
        };
        #[cfg(not(target_os = "linux"))]
        let mut command = {
            let mut command = Command::new(self.config.runtime.command());
            command.args(&args);
            command
        };
        command
            .stdin(if request.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(target_os = "linux")]
        let (mut launch_handshake, launch_handshake_child) =
            UnixStream::pair().map_err(|source| SandboxWasiError::Runtime {
                detail: format!("failed to create container launch handshake: {source}"),
            })?;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::CommandExt;
            let handshake_fd = launch_handshake_child.as_raw_fd();
            let expected_parent_pid = std::process::id() as libc::pid_t;
            // SAFETY: this only uses async-signal-safe libc operations before
            // exec and turns failures into child launch errors.
            unsafe {
                command.pre_exec(move || {
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::getppid() != expected_parent_pid {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "container parent exited during child launch",
                        ));
                    }
                    if libc::setpgid(0, 0) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if handshake_fd != 3 && libc::dup2(handshake_fd, 3) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        let mut child = command
            .spawn()
            .map_err(|source| SandboxWasiError::Runtime {
                detail: format!(
                    "failed to start container runtime `{}`: {source}",
                    self.config.runtime.command().to_string_lossy()
                ),
            })?;
        #[cfg(target_os = "linux")]
        {
            drop(launch_handshake_child);
            let launcher =
                SandboxLocalExecutionLinuxProcessGroup::capture(child.id() as i32, child.id())
                    .map_err(|detail| {
                        let _ = child.kill();
                        let _ = child.wait();
                        SandboxWasiError::Runtime { detail }
                    })?;
            let owner = local_journal
                .external_owner
                .clone()
                .ok_or_else(|| SandboxWasiError::Runtime {
                    detail: "container launch identity was not prepared".to_string(),
                })?
                .with_container_launcher(launcher)
                .map_err(|detail| SandboxWasiError::Runtime { detail })?;
            if let Err(error) = prepare_sandbox_local_execution_external_owner(
                store,
                local_execution_lease,
                local_journal,
                owner,
                OffsetDateTime::now_utc(),
            ) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(SandboxWasiError::Runtime {
                    detail: format!(
                        "failed to persist container launcher identity before launch: {error}"
                    ),
                });
            }
            if let Err(source) = launch_handshake.write_all(b"go\n") {
                let _ = child.kill();
                let _ = child.wait();
                return Err(SandboxWasiError::Runtime {
                    detail: format!("failed to release container launch handshake: {source}"),
                });
            }
            drop(launch_handshake);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = store;
            let _ = local_execution_lease;
            let _ = local_journal;
        }
        let stdin_writer = if let Some(stdin) = request.stdin.clone() {
            let mut child_stdin = child
                .stdin
                .take()
                .ok_or_else(|| SandboxWasiError::Runtime {
                    detail: "container stdin pipe was not available".to_string(),
                })?;
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let _ = tx.send(child_stdin.write_all(&stdin));
            });
            Some(rx)
        } else {
            None
        };
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SandboxWasiError::Runtime {
                detail: "container stdout pipe was not available".to_string(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| SandboxWasiError::Runtime {
                detail: "container stderr pipe was not available".to_string(),
            })?;
        let output_budget = Arc::new(ContainerOutputBudget::new(limits.max_output_bytes));
        let stdout_reader =
            spawn_bounded_container_reader(stdout, Arc::clone(&output_budget), "stdout");
        let stderr_reader =
            spawn_bounded_container_reader(stderr, Arc::clone(&output_budget), "stderr");
        let (status, mut terminal_error) = wait_for_container_runtime_output(
            &self.config.runtime,
            &container_name,
            &mut child,
            timeout_ms,
            cancellation_token,
            &output_budget,
        )?;
        let stdout = receive_bounded_container_reader(stdout_reader, "stdout")?;
        let stderr = receive_bounded_container_reader(stderr_reader, "stderr")?;
        if terminal_error.is_none() && output_budget.exceeded() {
            terminal_error = Some(SandboxWasiError::OutputLimitExceeded {
                limit: output_budget.limit.min(usize::MAX as u64) as usize,
            });
        }
        if let Some(stdin_writer) = stdin_writer
            && terminal_error.is_none()
        {
            stdin_writer
                .recv_timeout(Duration::from_millis(SANDBOX_CONTAINER_TERMINATION_WAIT_MS))
                .map_err(|_| SandboxWasiError::Runtime {
                    detail: "container stdin writer did not finish after runtime exit".to_string(),
                })?
                .map_err(|source| SandboxWasiError::Runtime {
                    detail: format!("failed to write container stdin: {source}"),
                })?;
        }
        Ok(ContainerCommandOutput {
            status,
            stdout,
            stderr,
            terminal_error,
        })
    }

    fn container_run_args(
        &self,
        layout: &LocalSandboxInstanceLayout,
        container_name: &str,
        request: &SandboxCommandRequest,
        limits: &SandboxResourceLimits,
    ) -> Result<Vec<String>, SandboxWasiError> {
        let mut args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            container_name.to_string(),
        ];
        if self.config.pull_never {
            args.push("--pull=never".to_string());
        }
        if !self.config.allow_network {
            args.extend(["--network".to_string(), "none".to_string()]);
        }
        if self.config.read_only_rootfs {
            args.push("--read-only".to_string());
        }
        args.extend([
            "--cap-drop".to_string(),
            "ALL".to_string(),
            "--security-opt".to_string(),
            "no-new-privileges".to_string(),
        ]);
        if request.stdin.is_some() {
            args.push("-i".to_string());
        }
        if !matches!(request.timeout_ms, 0 | SANDBOX_UNLIMITED_WALL_TIME_MS) {
            args.extend(["--stop-timeout".to_string(), "1".to_string()]);
        }
        if limits.memory_mb != SANDBOX_UNLIMITED_SIZE_LIMIT {
            args.extend(["--memory".to_string(), format!("{}m", limits.memory_mb)]);
        }
        for (host, target) in container_mounts(layout)? {
            args.extend(["--volume".to_string(), format!("{host}:{target}:rw")]);
        }
        args.extend(["--workdir".to_string(), request.cwd.to_string()]);
        for (name, value) in &request.env {
            args.extend(["--env".to_string(), format!("{name}={value}")]);
        }
        args.extend([
            "--env".to_string(),
            "HOME=/home".to_string(),
            "--env".to_string(),
            "TMPDIR=/tmp".to_string(),
        ]);
        args.push(self.config.image.clone());
        args.extend(request.argv.clone());
        Ok(args)
    }
}

fn wait_for_container_runtime_output(
    runtime: &SandboxContainerRuntime,
    container_name: &str,
    child: &mut Child,
    timeout_ms: u64,
    cancellation_token: &SandboxCancellationToken,
    output_budget: &ContainerOutputBudget,
) -> Result<(ExitStatus, Option<SandboxWasiError>), SandboxWasiError> {
    let started = Instant::now();
    loop {
        if cancellation_token.is_cancelled() {
            let status = terminate_container_runtime_and_reap(runtime, container_name, child)?;
            return Ok((status, Some(SandboxWasiError::Cancelled)));
        }
        if output_budget.exceeded() {
            let status = terminate_container_runtime_and_reap(runtime, container_name, child)?;
            return Ok((
                status,
                Some(SandboxWasiError::OutputLimitExceeded {
                    limit: output_budget.limit.min(usize::MAX as u64) as usize,
                }),
            ));
        }
        let wait = match container_wait_poll_duration(started, timeout_ms) {
            Ok(wait) => wait,
            Err(SandboxWasiError::TimedOut { timeout_ms }) => {
                let status = terminate_container_runtime_and_reap(runtime, container_name, child)?;
                return Ok((status, Some(SandboxWasiError::TimedOut { timeout_ms })));
            }
            Err(error) => return Err(error),
        };
        if let Some(status) = child
            .try_wait()
            .map_err(|source| SandboxWasiError::Runtime {
                detail: format!("container runtime wait failed: {source}"),
            })?
        {
            return Ok((status, None));
        }
        std::thread::sleep(wait);
    }
}

fn container_wait_poll_duration(
    started: Instant,
    timeout_ms: u64,
) -> Result<Duration, SandboxWasiError> {
    if timeout_ms == SANDBOX_UNLIMITED_WALL_TIME_MS {
        return Ok(Duration::from_millis(SANDBOX_CONTAINER_WAIT_POLL_MS));
    }
    let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    if elapsed_ms >= timeout_ms {
        return Err(SandboxWasiError::TimedOut { timeout_ms });
    }
    Ok(Duration::from_millis(
        timeout_ms
            .saturating_sub(elapsed_ms)
            .min(SANDBOX_CONTAINER_WAIT_POLL_MS),
    ))
}

fn terminate_container_runtime_and_reap(
    _runtime: &SandboxContainerRuntime,
    _container_name: &str,
    child: &mut Child,
) -> Result<ExitStatus, SandboxWasiError> {
    let _ = child.kill();
    child.wait().map_err(|source| SandboxWasiError::Runtime {
        detail: format!("failed to force-reap container runtime: {source}"),
    })
}

fn spawn_bounded_container_reader<R>(
    mut reader: R,
    budget: Arc<ContainerOutputBudget>,
    stream: &'static str,
) -> mpsc::Receiver<Result<Vec<u8>, String>>
where
    R: Read + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 64 * 1024];
        let result = loop {
            match reader.read(&mut buffer) {
                Ok(0) => break Ok(output),
                Ok(read) => {
                    let keep = budget.reserve(read);
                    output.extend_from_slice(&buffer[..keep]);
                }
                Err(source) => {
                    break Err(format!("failed to read container {stream}: {source}"));
                }
            }
        };
        let _ = tx.send(result);
    });
    rx
}

fn receive_bounded_container_reader(
    reader: mpsc::Receiver<Result<Vec<u8>, String>>,
    stream: &str,
) -> Result<Vec<u8>, SandboxWasiError> {
    reader
        .recv_timeout(Duration::from_millis(SANDBOX_CONTAINER_TERMINATION_WAIT_MS))
        .map_err(|_| SandboxWasiError::Runtime {
            detail: format!("container {stream} reader did not finish after runtime exit"),
        })?
        .map_err(|detail| SandboxWasiError::Runtime { detail })
}

fn ensure_container_layout_dirs(
    layout: &LocalSandboxInstanceLayout,
) -> Result<(), SandboxWasiError> {
    for dir in layout.required_dirs() {
        fs::create_dir_all(dir).map_err(|source| SandboxWasiError::Runtime {
            detail: format!(
                "failed to create sandbox directory {}: {source}",
                dir.display()
            ),
        })?;
    }
    Ok(())
}

fn container_mounts(
    layout: &LocalSandboxInstanceLayout,
) -> Result<Vec<(String, &'static str)>, SandboxWasiError> {
    [
        (&layout.root_dir, "/root"),
        (&layout.home, "/home"),
        (&layout.tmp, "/tmp"),
        (&layout.work, "/work"),
        (&layout.var, "/var"),
        (&layout.logs, "/logs"),
        (&layout.artifacts, "/artifacts"),
    ]
    .into_iter()
    .map(|(host, target)| {
        fs::canonicalize(host)
            .map(|path| (path.display().to_string(), target))
            .map_err(|source| SandboxWasiError::Runtime {
                detail: format!(
                    "failed to resolve sandbox container mount {}: {source}",
                    host.display()
                ),
            })
    })
    .collect()
}

fn container_name(command_id: &SandboxCommandId) -> String {
    format!("sinomo-sandbox-{}", command_id.as_str())
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn validate_container_image(image: &str) -> Result<(), SandboxContainerConfigError> {
    let trimmed = image.trim();
    if trimmed.is_empty()
        || trimmed.contains('\0')
        || trimmed.starts_with('-')
        || trimmed.chars().any(char::is_whitespace)
    {
        return Err(SandboxContainerConfigError::InvalidImage {
            value: image.to_string(),
        });
    }
    Ok(())
}

fn parse_container_bool(
    name: &'static str,
    value: &str,
) -> Result<bool, SandboxContainerConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(SandboxContainerConfigError::InvalidBool {
            name,
            value: value.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_layout() -> (sinomo_testdir::TestDir, LocalSandboxInstanceLayout) {
        let root = sinomo_testdir::TestDir::new("sinomo-container-runner-test");
        let sandbox_id = SandboxId::from_legacy_named("container_sandbox").expect("sandbox");
        let layout = LocalSandboxInstanceLayout::new(root.to_path_buf(), &sandbox_id);
        for dir in layout.required_dirs() {
            let _ = fs::create_dir_all(dir);
        }
        (root, layout)
    }

    #[test]
    fn container_config_reads_env_style_values() {
        let config = SandboxContainerBackendConfig::from_env_vars(|name| match name {
            SINOMO_SANDBOX_BACKEND_ENV => Some("podman".to_string()),
            SINOMO_SANDBOX_CONTAINER_IMAGE_ENV => Some("alpine:3.20".to_string()),
            SINOMO_SANDBOX_CONTAINER_ALLOW_NETWORK_ENV => Some("yes".to_string()),
            _ => None,
        })
        .expect("env config")
        .expect("enabled");
        assert_eq!(config.runtime, SandboxContainerRuntime::Podman);
        assert_eq!(config.image, "alpine:3.20");
        assert!(config.allow_network);
    }

    #[test]
    fn container_config_requires_image_when_enabled() {
        let error = SandboxContainerBackendConfig::from_env_vars(|name| match name {
            SINOMO_SANDBOX_BACKEND_ENV => Some("docker".to_string()),
            _ => None,
        })
        .expect_err("missing image should fail");
        assert!(matches!(error, SandboxContainerConfigError::MissingImage));
    }

    #[test]
    fn container_run_args_include_isolation_and_mounts() {
        let (_root, layout) = test_layout();
        let config =
            SandboxContainerBackendConfig::new(SandboxContainerRuntime::Docker, "alpine:3.20")
                .expect("config");
        let runner = SandboxContainerCommandRunner::new(config);
        let request = SandboxCommandRequest {
            sandbox_id: SandboxId::from_legacy_named("container_sandbox").expect("sandbox"),
            argv: vec!["sh".to_string(), "-c".to_string(), "pwd".to_string()],
            cwd: SandboxPath::new("/work").expect("cwd"),
            stdin: Some(b"echo hi\n".to_vec()),
            env: BTreeMap::from([("A".to_string(), "B".to_string())]),
            timeout_ms: 1000,
        };
        let command_id = SandboxCommandId::from_legacy_named("container_command").expect("command");
        let args = runner
            .container_run_args(
                &layout,
                "sinomo-sandbox-test",
                &request,
                &SandboxResourceLimits::default(),
            )
            .expect("args");
        assert_eq!(args.first().map(String::as_str), Some("run"));
        assert!(args.iter().any(|arg| arg == "--network"));
        assert!(args.iter().any(|arg| arg == "none"));
        assert!(args.iter().any(|arg| arg == "--cap-drop"));
        assert!(args.iter().any(|arg| arg == "--read-only"));
        assert!(args.iter().any(|arg| arg == "-i"));
        assert!(args.iter().any(|arg| arg == "HOME=/home"));
        assert!(args.iter().any(|arg| arg == "A=B"));
        assert!(args.iter().any(|arg| arg.ends_with(":/work:rw")));
        assert!(args.iter().any(|arg| arg == "alpine:3.20"));
        assert!(args.iter().any(|arg| arg == "sh"));
        assert!(container_name(&command_id).starts_with("sinomo-sandbox-"));
    }

    #[cfg(unix)]
    #[test]
    fn container_runner_persists_outputs_and_command_history_with_fake_runtime() {

        let root = sinomo_testdir::TestDir::new("sinomo-container-runner-e2e");
        let fake_runtime = root.join("fake-container-runtime.sh");
        fs::write(
            &fake_runtime,
            r#"#!/bin/sh
if [ "$1" = "ps" ]; then
  exit 0
fi
while [ "$#" -gt 0 ]; do
  case "$1" in
    fake-image) shift; break ;;
    --name|--network|--cap-drop|--security-opt|--memory|--stop-timeout|--volume|--workdir|--env) shift 2 ;;
    --rm|--pull=never|--read-only|-i) shift ;;
    *) shift ;;
  esac
done
exec "$@"
"#,
        )
        .expect("write fake runtime");
        sinomo_testdir::install_shell_fixture(&fake_runtime).expect("install shell fixture");

        let store = LocalSandboxInstanceStore::new(&root);
        let sandbox_id = SandboxId::from_legacy_named("container_e2e").expect("sandbox");
        let toolchain_id =
            SandboxToolchainId::from_legacy_named("container_tools").expect("toolchain");
        store
            .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
            .expect("create sandbox");
        let config = SandboxContainerBackendConfig::new(
            SandboxContainerRuntime::Command(fake_runtime),
            "fake-image",
        )
        .expect("config");
        let runner = SandboxContainerCommandRunner::new(config);
        let outcome = runner
            .run_toolchain_command_with_stdin_and_output_sink(
                &store,
                &sandbox_id,
                "printf",
                vec!["printf".to_string(), "hello".to_string()],
                None,
                1_000,
                None,
            )
            .expect("run command");

        assert_eq!(outcome.response.exit_status, 0);
        assert_eq!(outcome.stdout, b"hello");
        assert!(outcome.stderr.is_empty());
        let (manifest, layout) = store.load(&sandbox_id).expect("load manifest");
        assert_eq!(
            manifest.command_history,
            vec![outcome.response.command_id.clone()]
        );
        let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
        let record =
            load_command_record(&mut sandbox_fs, &outcome.response.command_id).expect("record");
        assert_eq!(record.status, SandboxCommandRecordStatus::Completed);
        assert_eq!(record.argv, vec!["printf", "hello"]);
        assert_eq!(
            sandbox_fs
                .read(
                    &sandbox_command_stdout_log_path(&outcome.response.command_id)
                        .expect("stdout path"),
                    None,
                )
                .expect("stdout"),
            b"hello"
        );
    }

    #[cfg(unix)]
    #[test]
    fn container_runner_cancels_active_fake_runtime() {

        let root = sinomo_testdir::TestDir::new("sinomo-container-runner-cancel");
        let fake_runtime = root.join("fake-container-runtime.sh");
        let cancel_marker = root.join("container-cancelled");
        let sandbox_id = SandboxId::from_legacy_named("container_cancel").expect("sandbox");
        let state_path = LocalSandboxInstanceLayout::new(root.to_path_buf(), &sandbox_id)
            .work
            .join("state.txt");
        fs::write(
            &fake_runtime,
            format!(
                r#"#!/bin/sh
marker='{}'
state='{}'
if [ "$1" = "rm" ]; then
  touch "$marker"
  exit 0
fi
if [ "$1" = "ps" ]; then
  exit 0
fi
while [ "$#" -gt 0 ]; do
  case "$1" in
    fake-image) shift; break ;;
    --name|--network|--cap-drop|--security-opt|--memory|--stop-timeout|--volume|--workdir|--env) shift 2 ;;
    --rm|--pull=never|--read-only|-i) shift ;;
    *) shift ;;
  esac
done
printf partial-container-effect > "$state"
while [ ! -f "$marker" ]; do
  sleep 0.05
done
exit 0
"#,
                cancel_marker.display(),
                state_path.display(),
            ),
        )
        .expect("write fake runtime");
        sinomo_testdir::install_shell_fixture(&fake_runtime).expect("install shell fixture");

        let store = LocalSandboxInstanceStore::new(&root);
        let toolchain_id =
            SandboxToolchainId::from_legacy_named("container_tools").expect("toolchain");
        store
            .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
            .expect("create sandbox");
        fs::write(&state_path, b"baseline").expect("write baseline");
        let config = SandboxContainerBackendConfig::new(
            SandboxContainerRuntime::Command(fake_runtime),
            "fake-image",
        )
        .expect("config");
        let runner = SandboxContainerCommandRunner::new(config);
        let cancellation_token = SandboxCancellationToken::new();
        let token_for_run = cancellation_token.clone();
        let runner_for_run = runner.clone();
        let store_for_run = store.clone();
        let sandbox_id_for_run = sandbox_id.clone();
        let started = Instant::now();
        let handle = std::thread::spawn(move || {
            runner_for_run.run_toolchain_command_with_stdin_and_output_sink_and_cancellation(
                &store_for_run,
                &sandbox_id_for_run,
                "sh",
                vec!["sh".to_string(), "-c".to_string(), "sleep 30".to_string()],
                Some(vec![0x5a; 16 * 1024 * 1024]),
                SANDBOX_UNLIMITED_WALL_TIME_MS,
                None,
                token_for_run,
            )
        });

        std::thread::sleep(Duration::from_millis(150));
        cancellation_token.cancel();
        let result = handle.join().expect("runner thread");
        assert!(matches!(
            result,
            Err(SandboxCommandExecutionError::Wasi(
                SandboxWasiError::Cancelled
            ))
        ));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "cancelled run should finish promptly"
        );
        assert!(cancel_marker.exists());
        assert_eq!(
            fs::read(&state_path).expect("read preserved state"),
            b"partial-container-effect"
        );
        assert_eq!(
            read_sandbox_local_execution_journal(&store, &sandbox_id)
                .expect("read execution journal"),
            None,
            "cancel audit should be finalized after external cleanup"
        );

        let (manifest, layout) = store.load(&sandbox_id).expect("load manifest");
        assert_eq!(manifest.command_history.len(), 1);
        let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
        let record =
            load_command_record(&mut sandbox_fs, &manifest.command_history[0]).expect("record");
        assert_eq!(record.status, SandboxCommandRecordStatus::Cancelled);
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|error| error.contains("cancelled"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn container_runner_stops_infinite_output_at_bounded_audit_limit() {

        let root = sinomo_testdir::TestDir::new("sinomo-container-runner-output-limit");
        let fake_runtime = root.join("fake-container-runtime.sh");
        let removal_marker = root.join("container-removed");
        fs::write(
            &fake_runtime,
            format!(
                r#"#!/bin/sh
marker='{}'
if [ "$1" = "rm" ]; then
  touch "$marker"
  exit 0
fi
if [ "$1" = "ps" ]; then
  exit 0
fi
while [ "$#" -gt 0 ]; do
  case "$1" in
    fake-image) shift; break ;;
    --name|--network|--cap-drop|--security-opt|--memory|--stop-timeout|--volume|--workdir|--env) shift 2 ;;
    --rm|--pull=never|--read-only|-i) shift ;;
    *) shift ;;
  esac
done
while [ ! -f "$marker" ]; do
  printf 0123456789abcdef
done
exit 0
"#,
                removal_marker.display()
            ),
        )
        .expect("write fake runtime");
        sinomo_testdir::install_shell_fixture(&fake_runtime).expect("install shell fixture");

        let store = LocalSandboxInstanceStore::new(&root);
        let sandbox_id = SandboxId::from_legacy_named("container_output_limit").expect("sandbox");
        let (mut manifest, layout) = store
            .create(
                sandbox_id.clone(),
                SandboxToolchainId::from_legacy_named("container_tools").expect("toolchain"),
                OffsetDateTime::now_utc(),
            )
            .expect("create sandbox");
        manifest.limits.max_output_bytes = 4 * 1024;
        store.save_manifest(&manifest).expect("save output limit");
        let runner = SandboxContainerCommandRunner::new(
            SandboxContainerBackendConfig::new(
                SandboxContainerRuntime::Command(fake_runtime),
                "fake-image",
            )
            .expect("config"),
        );
        let started = Instant::now();
        let error = runner
            .run_toolchain_command_with_stdin_and_output_sink(
                &store,
                &sandbox_id,
                "sh",
                vec!["sh".to_string()],
                None,
                SANDBOX_UNLIMITED_WALL_TIME_MS,
                None,
            )
            .expect_err("infinite output must be terminated");
        assert!(matches!(
            error,
            SandboxCommandExecutionError::Wasi(SandboxWasiError::OutputLimitExceeded {
                limit: 4096
            })
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(removal_marker.exists());

        recover_sandbox_local_execution_journal(&store, &sandbox_id, OffsetDateTime::now_utc())
            .expect("recover output-limit audit");
        let recovered = store.load(&sandbox_id).expect("load recovered sandbox").0;
        let command_id = recovered.command_history.last().expect("command audit");
        let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
        let stdout = sandbox_fs
            .read_unchecked_mut(
                &sandbox_command_stdout_log_path(command_id).expect("stdout path"),
                None,
            )
            .expect("bounded stdout audit");
        assert_eq!(stdout.len(), 4096);
        assert_eq!(
            load_command_record(&mut sandbox_fs, command_id)
                .expect("load output-limit record")
                .status,
            SandboxCommandRecordStatus::ResourceExhausted
        );
    }

    #[test]
    fn container_guest_active_crash_preserves_writes_and_audits_exactly_once() {
        let root = sinomo_testdir::TestDir::new("sinomo-container-runner-crash-recovery");
        let store = LocalSandboxInstanceStore::new(&root);
        let sandbox_id = SandboxId::from_legacy_named("container_crash").expect("sandbox");
        let (manifest, layout) = store
            .create(
                sandbox_id.clone(),
                SandboxToolchainId::from_legacy_named("container_tools").expect("toolchain"),
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("create sandbox");
        let state_path = layout.work.join("state.txt");
        fs::write(&state_path, b"baseline").expect("write baseline");
        let lease = try_acquire_sandbox_local_execution_lease(&store, &sandbox_id).expect("lease");
        let command_id =
            SandboxCommandId::from_legacy_named("container_crashed_command").expect("command");
        let mut journal = begin_sandbox_local_execution_journal(
            &store,
            &lease,
            &manifest,
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("begin journal");
        prepare_sandbox_local_execution_audit_identity(
            &store,
            &lease,
            &mut journal,
            SandboxCommandRecord {
                sandbox_id: sandbox_id.clone(),
                command_id: command_id.clone(),
                status: SandboxCommandRecordStatus::Failed,
                workspace_root: manifest.workspace_root.clone(),
                toolchain_id: manifest.toolchain_id.clone(),
                argv: vec!["container-command".to_string()],
                cwd: manifest.cwd.clone(),
                env: BTreeMap::new(),
                started_at: OffsetDateTime::UNIX_EPOCH,
                completed_at: None,
                response: None,
                error: Some("container host stopped while guest was active".to_string()),
            },
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("prepare crash audit");
        activate_sandbox_local_execution_journal(
            &store,
            &lease,
            &mut journal,
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("activate guest");
        fs::write(&state_path, b"partial-container-effect").expect("write partial effect");
        drop(lease);

        recover_sandbox_local_execution_journal(&store, &sandbox_id, OffsetDateTime::UNIX_EPOCH)
            .expect("recover simulated host crash");
        assert_eq!(
            fs::read(&state_path).expect("read state"),
            b"partial-container-effect"
        );
        let recovered = store.load(&sandbox_id).expect("load recovered sandbox").0;
        assert_eq!(
            recovered
                .command_history
                .iter()
                .filter(|candidate| **candidate == command_id)
                .count(),
            1
        );
        let mut sandbox_fs = LocalSandboxFileSystem::new(store.layout(&sandbox_id));
        assert_eq!(
            load_command_record(&mut sandbox_fs, &command_id)
                .expect("load crash record")
                .status,
            SandboxCommandRecordStatus::Failed
        );
        assert!(
            !recover_sandbox_local_execution_journal(
                &store,
                &sandbox_id,
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("second recovery")
        );
    }
}
