use crate::internal::*;

use std::ffi::OsStr;
use std::process::{Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

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
        validate_toolchain_command_name(command_name)?;
        validate_sandbox_toolchain_argv(command_name, &argv).map_err(|error| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { detail: error })
        })?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let fs = LocalSandboxFileSystem::new(layout.clone());
        validate_sandbox_working_directory(&fs, &manifest.cwd)?;
        let request = SandboxCommandRequest {
            sandbox_id: sandbox_id.clone(),
            argv,
            cwd: manifest.cwd.clone(),
            stdin,
            env: manifest.env.clone(),
            timeout_ms,
        };
        self.run_container_command(store, &manifest, &layout, request, output_sink)
    }

    pub fn run_process_plan_with_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        validate_sandbox_process_plan(plan)?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let fs = LocalSandboxFileSystem::new(layout.clone());
        validate_sandbox_process_plan_working_directories(plan, &fs, &manifest.cwd)?;

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
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        validate_sandbox_toolchain_argv(&request.argv[0], &request.argv).map_err(|error| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { detail: error })
        })?;
        let command_id = SandboxCommandId::generate();
        let started_at = OffsetDateTime::now_utc();
        let timeout_ms =
            effective_sandbox_timeout_ms(request.timeout_ms, manifest.limits.wall_time_ms);
        if let Some(stdin) = &request.stdin {
            if stdin.len() as u64 > manifest.limits.max_input_bytes {
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
        }

        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        validate_sandbox_working_directory(&sandbox_fs, &request.cwd)?;
        let before = sandbox_fs.snapshot()?;
        let started = Instant::now();
        let output = match self.execute_container_command(
            layout,
            &command_id,
            &request,
            &manifest.limits,
            timeout_ms,
        ) {
            Ok(output) => output,
            Err(error) => {
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
        };
        let wall_time_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
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
    ) -> Result<Output, SandboxWasiError> {
        ensure_container_layout_dirs(layout)?;
        let container_name = container_name(command_id);
        let args = self.container_run_args(layout, &container_name, request, limits)?;
        let mut child = Command::new(self.config.runtime.command())
            .args(&args)
            .stdin(if request.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| SandboxWasiError::Runtime {
                detail: format!(
                    "failed to start container runtime `{}`: {source}",
                    self.config.runtime.command().to_string_lossy()
                ),
            })?;
        if let Some(stdin) = &request.stdin {
            let mut child_stdin = child
                .stdin
                .take()
                .ok_or_else(|| SandboxWasiError::Runtime {
                    detail: "container stdin pipe was not available".to_string(),
                })?;
            use std::io::Write;
            child_stdin
                .write_all(stdin)
                .map_err(|source| SandboxWasiError::Runtime {
                    detail: format!("failed to write container stdin: {source}"),
                })?;
        }
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(child.wait_with_output());
        });
        if timeout_ms == SANDBOX_UNLIMITED_WALL_TIME_MS {
            return rx
                .recv()
                .map_err(|source| SandboxWasiError::Runtime {
                    detail: format!("container wait channel failed: {source}"),
                })?
                .map_err(|source| SandboxWasiError::Runtime {
                    detail: format!("container runtime wait failed: {source}"),
                });
        }
        match rx.recv_timeout(Duration::from_millis(timeout_ms)) {
            Ok(output) => output.map_err(|source| SandboxWasiError::Runtime {
                detail: format!("container runtime wait failed: {source}"),
            }),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = Command::new(self.config.runtime.command())
                    .args(["rm", "-f", &container_name])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                Err(SandboxWasiError::TimedOut { timeout_ms })
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(SandboxWasiError::Runtime {
                detail: "container wait channel disconnected".to_string(),
            }),
        }
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

    fn test_layout() -> LocalSandboxInstanceLayout {
        let root = std::env::temp_dir().join("sinomo-container-runner-test");
        let sandbox_id = SandboxId::from_legacy_named("container_sandbox").expect("sandbox");
        let layout = LocalSandboxInstanceLayout::new(root, &sandbox_id);
        for dir in layout.required_dirs() {
            let _ = fs::create_dir_all(dir);
        }
        layout
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
        let layout = test_layout();
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
        let _ = fs::remove_dir_all(layout.runtime_root);
    }

    #[cfg(unix)]
    #[test]
    fn container_runner_persists_outputs_and_command_history_with_fake_runtime() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!(
            "sinomo-container-runner-e2e-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("runtime root");
        let fake_runtime = root.join("fake-container-runtime.sh");
        fs::write(
            &fake_runtime,
            r#"#!/bin/sh
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
        let mut permissions = fs::metadata(&fake_runtime).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_runtime, permissions).expect("permissions");

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

        let _ = fs::remove_dir_all(root);
    }
}
