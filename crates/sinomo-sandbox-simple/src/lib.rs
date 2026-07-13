//! Simple native sandbox runtime.
//!
//! This runtime executes host binaries directly and exposes the host filesystem
//! layout as-is. Enabling it explicitly opts into normal host-process
//! filesystem semantics; structured sandbox filesystem permissions are not an
//! isolation boundary for this backend.

use std::env;
use std::fs;
#[cfg(not(windows))]
use std::io::{Read, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
#[cfg(not(windows))]
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sinomo_core::node::NodeId;
use sinomo_sandbox::{
    LocalSandboxFileSystem, LocalSandboxInstanceLayout, LocalSandboxInstanceStore,
    SANDBOX_OUTPUT_CHUNK_BYTES, SANDBOX_RESOURCE_USAGE_UNAVAILABLE, SANDBOX_UNLIMITED_WALL_TIME_MS,
    SandboxCancellationToken, SandboxCommandExecutionError, SandboxCommandId, SandboxCommandRecord,
    SandboxCommandRecordStatus, SandboxCommandRequest, SandboxFsError, SandboxLocalCommandOutcome,
    SandboxLocalExecutionJournal, SandboxLocalExecutionLease, SandboxLocalExecutionOutcome,
    SandboxMount, SandboxMountMode, SandboxOutputChunk, SandboxOutputSink, SandboxOutputStream,
    SandboxPath, SandboxPolicy, SandboxProcessPlan, SandboxProcessPlanOutcome,
    SandboxResourceLimits, SandboxResourceUsage, SandboxStoreError, SandboxWasiError,
    activate_sandbox_local_execution_journal, begin_sandbox_local_execution_journal,
    cleanup_sandbox_local_execution_external_owner, effective_sandbox_timeout_ms,
    persist_command_outputs, prepare_sandbox_local_execution_audit_identity,
    record_local_command_history, recover_sandbox_local_execution_journal_with_lease,
    sandbox_command_record_status_for_wasi_error, sandbox_command_stderr_path,
    sandbox_command_stdout_path, sandbox_output_chunks, settle_sandbox_local_execution_journal,
    try_acquire_sandbox_local_execution_lease, validate_sandbox_process_plan,
    validate_sandbox_toolchain_argv, validate_toolchain_command_name,
};
#[cfg(target_os = "linux")]
use sinomo_sandbox::{
    SandboxLocalExecutionExternalOwner, prepare_sandbox_local_execution_external_owner,
};
use time::OffsetDateTime;

pub const SINOMO_SANDBOX_SIMPLE_BACKEND: &str = "simple-native";
pub const SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED_ENV: &str =
    "SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED";
pub const SINOMO_SANDBOX_SIMPLE_READ_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_READ_PATHS";
pub const SINOMO_SANDBOX_SIMPLE_WRITE_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_WRITE_PATHS";
pub const SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS_ENV: &str = "SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS";
pub const SINOMO_SANDBOX_SIMPLE_HOST_SIFS_ROOT_ENV: &str = "SINOMO_SANDBOX_SIMPLE_HOST_SIFS_ROOT";

const SIMPLE_NATIVE_WAIT_POLL_MS: u64 = 50;
#[cfg(not(windows))]
const SIMPLE_NATIVE_TERMINATION_WAIT_MS: u64 = 5_000;
const SIMPLE_NATIVE_MAX_CAPTURE_BYTES: u64 = 64 * 1024 * 1024;

fn effective_simple_native_output_limit(configured: u64) -> u64 {
    configured.min(SIMPLE_NATIVE_MAX_CAPTURE_BYTES)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SandboxSimpleNativeConfig {
    pub allow_unenforced: bool,
    pub extra_read_paths: Vec<PathBuf>,
    pub extra_write_paths: Vec<PathBuf>,
    pub extra_execute_paths: Vec<PathBuf>,
    /// Host path where the live SIFS/FUSE namespace is mounted. Declared
    /// sandbox mounts are projected from this tree without copying.
    pub host_sifs_root: Option<PathBuf>,
}

impl SandboxSimpleNativeConfig {
    pub fn from_env() -> Result<Self, SandboxCommandExecutionError> {
        Ok(Self {
            allow_unenforced: env_bool(SINOMO_SANDBOX_SIMPLE_ALLOW_UNENFORCED_ENV)?,
            extra_read_paths: env_path_list(SINOMO_SANDBOX_SIMPLE_READ_PATHS_ENV),
            extra_write_paths: env_path_list(SINOMO_SANDBOX_SIMPLE_WRITE_PATHS_ENV),
            extra_execute_paths: env_path_list(SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS_ENV),
            host_sifs_root: env::var_os(SINOMO_SANDBOX_SIMPLE_HOST_SIFS_ROOT_ENV)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
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

    /// Retained for API compatibility. The simple host backend deliberately
    /// does not install Landlock or mount-namespace restrictions.
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
        self.run_toolchain_command_with_stdin_policy_output_sink_and_cancellation(
            store,
            _toolchain_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            SandboxCancellationToken::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_output_sink_and_cancellation(
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
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let local_execution_lease = try_acquire_sandbox_local_execution_lease(store, sandbox_id)?;
        recover_sandbox_local_execution_journal_with_lease(
            store,
            &local_execution_lease,
            sandbox_id,
            OffsetDateTime::now_utc(),
        )?;
        self.run_toolchain_command_with_stdin_policy_output_sink_cancellation_and_lease(
            store,
            _toolchain_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            cancellation_token,
            &local_execution_lease,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_output_sink_cancellation_and_lease(
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
        cancellation_token: SandboxCancellationToken,
        local_execution_lease: &SandboxLocalExecutionLease,
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
        self.run_native_command(
            store,
            &manifest,
            &layout,
            request,
            output_sink,
            &cancellation_token,
            &local_execution_lease,
        )
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
        self.run_process_plan_with_policy_output_sink_and_cancellation(
            store,
            _toolchain_store,
            sandbox_id,
            plan,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            SandboxCancellationToken::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_process_plan_with_policy_output_sink_and_cancellation(
        &self,
        store: &LocalSandboxInstanceStore,
        _toolchain_store: &sinomo_sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_sandbox::SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
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
            let outcome = self.run_native_command(
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

    fn run_native_command(
        &self,
        store: &LocalSandboxInstanceStore,
        manifest: &sinomo_sandbox::SandboxInstanceManifest,
        layout: &LocalSandboxInstanceLayout,
        request: SandboxCommandRequest,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: &SandboxCancellationToken,
        local_execution_lease: &SandboxLocalExecutionLease,
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

        let mount_projection = match prepare_native_mount_projection(&manifest.mounts, &self.config)
        {
            Ok(projection) => projection,
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
        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        let cwd = resolve_native_cwd(&sandbox_fs, layout, &request.cwd, mount_projection.as_ref())?;
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
                    "sandbox host stopped while the simple-native guest was active".to_string(),
                ),
            },
            OffsetDateTime::now_utc(),
        )?;
        activate_sandbox_local_execution_journal(
            store,
            local_execution_lease,
            &mut local_journal,
            OffsetDateTime::now_utc(),
        )?;
        let started = Instant::now();
        let output = execute_native_command(
            &request,
            layout,
            &cwd,
            mount_projection.as_ref(),
            timeout_ms,
            &self.config,
            &manifest.limits,
            cancellation_token,
            &command_id,
            output_sink.clone(),
            store,
            local_execution_lease,
            &mut local_journal,
        );
        let (output, _completion_guard) =
            transition_after_native_return(cancellation_token, output);
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
                    Ok(NativeCommandOutput {
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
                let command_record = SandboxCommandRecord {
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
                };
                let cancelled = matches!(error, SandboxWasiError::Cancelled);
                settle_sandbox_local_execution_journal(
                    store,
                    local_execution_lease,
                    &mut local_journal,
                    if cancelled {
                        SandboxLocalExecutionOutcome::Cancelled
                    } else {
                        SandboxLocalExecutionOutcome::Failed {
                            detail: error.to_string(),
                        }
                    },
                    command_record,
                    Vec::new(),
                    Vec::new(),
                    completed_at,
                )?;
                recover_sandbox_local_execution_journal_with_lease(
                    store,
                    local_execution_lease,
                    &manifest.sandbox_id,
                    OffsetDateTime::now_utc(),
                )?;
                return Err(error.into());
            }
        };

        let wall_time_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let exit_status = output.exit_status;
        let stdout = output.stdout;
        let stderr = output.stderr;
        let terminal_error = output.terminal_error;
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
        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        let changed_paths = sandbox_fs.changed_paths_since(&before)?;
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
                // simple-native deliberately permits arbitrary host writes;
                // its sandbox-local snapshot is only a partial view and
                // cannot honestly enforce max_written_bytes_per_command.
                written_bytes: SANDBOX_RESOURCE_USAGE_UNAVAILABLE,
            },
        };

        let execution_error = if let Some(error) = terminal_error {
            Some(SandboxCommandExecutionError::Wasi(error))
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
        let primary_outcome = match execution_error.as_ref() {
            None => SandboxLocalExecutionOutcome::Succeeded,
            Some(SandboxCommandExecutionError::Wasi(
                SandboxWasiError::Cancelled | SandboxWasiError::CancelledCleanupFailed { .. },
            )) => SandboxLocalExecutionOutcome::Cancelled,
            Some(error) => SandboxLocalExecutionOutcome::Failed {
                detail: error.to_string(),
            },
        };
        settle_sandbox_local_execution_journal(
            store,
            local_execution_lease,
            &mut local_journal,
            primary_outcome,
            record,
            stdout.clone(),
            stderr.clone(),
            completed_at,
        )?;
        if let Some(error) = execution_error {
            recover_sandbox_local_execution_journal_with_lease(
                store,
                local_execution_lease,
                &manifest.sandbox_id,
                OffsetDateTime::now_utc(),
            )?;
            return Err(error);
        }
        recover_sandbox_local_execution_journal_with_lease(
            store,
            local_execution_lease,
            &manifest.sandbox_id,
            OffsetDateTime::now_utc(),
        )?;
        Ok(SandboxLocalCommandOutcome {
            response,
            stdout,
            stderr,
            output_chunks,
        })
    }
}

#[derive(Debug)]
struct NativeCommandOutput {
    exit_status: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    terminal_error: Option<SandboxWasiError>,
}

struct NativeCompletionGuard<'a> {
    cancellation_token: &'a SandboxCancellationToken,
    claimed: bool,
}

impl Drop for NativeCompletionGuard<'_> {
    fn drop(&mut self) {
        if self.claimed {
            self.cancellation_token.complete();
        }
    }
}

fn transition_after_native_return(
    cancellation_token: &SandboxCancellationToken,
    result: Result<NativeCommandOutput, SandboxWasiError>,
) -> (
    Result<NativeCommandOutput, SandboxWasiError>,
    NativeCompletionGuard<'_>,
) {
    let cancelled = matches!(&result, Err(SandboxWasiError::Cancelled))
        || matches!(
            &result,
            Ok(NativeCommandOutput {
                terminal_error: Some(SandboxWasiError::Cancelled),
                ..
            })
        );
    if cancelled {
        return (
            result,
            NativeCompletionGuard {
                cancellation_token,
                claimed: false,
            },
        );
    }
    if cancellation_token.begin_completing() {
        (
            result,
            NativeCompletionGuard {
                cancellation_token,
                claimed: true,
            },
        )
    } else {
        (
            Err(SandboxWasiError::Cancelled),
            NativeCompletionGuard {
                cancellation_token,
                claimed: false,
            },
        )
    }
}

#[derive(Clone, Debug)]
struct NativeProjectedMount {
    source_host_path: PathBuf,
    guest_path: PathBuf,
    mode: SandboxMountMode,
}

#[derive(Clone, Debug)]
struct NativeMountProjection {
    bwrap_program: PathBuf,
    mounts: Vec<NativeProjectedMount>,
}

impl NativeMountProjection {
    fn projected_source_for_guest_path(&self, guest_path: &Path) -> Option<PathBuf> {
        self.mounts
            .iter()
            .filter_map(|mount| {
                guest_path
                    .strip_prefix(&mount.guest_path)
                    .ok()
                    .map(|suffix| {
                        (
                            mount.guest_path.components().count(),
                            mount.source_host_path.join(suffix),
                        )
                    })
            })
            .max_by_key(|(depth, _)| *depth)
            .map(|(_, source)| source)
    }
}

fn prepare_native_mount_projection(
    mounts: &[SandboxMount],
    config: &SandboxSimpleNativeConfig,
) -> Result<Option<NativeMountProjection>, SandboxWasiError> {
    if mounts.is_empty() {
        return Ok(None);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = config;
        return Err(SandboxWasiError::Runtime {
            detail: "simple-native declared mounts require Linux bubblewrap functional projection; copying mount contents is not supported"
                .to_string(),
        });
    }

    #[cfg(target_os = "linux")]
    {
        let host_sifs_root = config.host_sifs_root.as_deref().ok_or_else(|| {
            SandboxWasiError::InvalidRequest {
                detail: format!(
                    "simple-native manifest declares {} mount(s), but no live host SIFS/FUSE root is configured via {}",
                    mounts.len(),
                    SINOMO_SANDBOX_SIMPLE_HOST_SIFS_ROOT_ENV
                ),
            }
        })?;
        let host_sifs_root =
            fs::canonicalize(host_sifs_root).map_err(|source| SandboxWasiError::Runtime {
                detail: format!(
                    "failed to resolve live host SIFS/FUSE root {}: {source}",
                    host_sifs_root.display()
                ),
            })?;
        if !host_sifs_root.is_dir() {
            return Err(SandboxWasiError::Runtime {
                detail: format!(
                    "configured live host SIFS/FUSE root {} is not a directory",
                    host_sifs_root.display()
                ),
            });
        }
        let bwrap_program = find_host_program("bwrap").ok_or_else(|| SandboxWasiError::Runtime {
            detail: "simple-native manifest declares mounts, but `bwrap` is unavailable; live mounts cannot be represented without copying"
                .to_string(),
        })?;
        let mut projected = Vec::with_capacity(mounts.len());
        for mount in mounts {
            let relative = normalized_sifs_relative_path(&mount.source_sifs_path)?;
            let joined = host_sifs_root.join(relative);
            let source_host_path =
                fs::canonicalize(&joined).map_err(|source| SandboxWasiError::Runtime {
                    detail: format!(
                        "failed to resolve declared SIFS mount source {} beneath {}: {source}",
                        mount.source_sifs_path,
                        host_sifs_root.display()
                    ),
                })?;
            if !source_host_path.starts_with(&host_sifs_root) {
                return Err(SandboxWasiError::InvalidRequest {
                    detail: format!(
                        "declared SIFS mount source {} resolves outside configured host root {}",
                        mount.source_sifs_path,
                        host_sifs_root.display()
                    ),
                });
            }
            projected.push(NativeProjectedMount {
                source_host_path,
                guest_path: PathBuf::from(mount.mount_path.as_str()),
                mode: mount.mode.clone(),
            });
        }
        Ok(Some(NativeMountProjection {
            bwrap_program,
            mounts: projected,
        }))
    }
}

fn normalized_sifs_relative_path(path: &str) -> Result<PathBuf, SandboxWasiError> {
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("declared SIFS mount source must be absolute: {path:?}"),
        });
    }
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::RootDir => {}
            std::path::Component::Normal(component) => relative.push(component),
            _ => {
                return Err(SandboxWasiError::InvalidRequest {
                    detail: format!(
                        "declared SIFS mount source is not canonical: {}",
                        path.display()
                    ),
                });
            }
        }
    }
    Ok(relative)
}

fn find_host_program(name: &str) -> Option<PathBuf> {
    env::var_os("PATH")
        .into_iter()
        .flat_map(|path| env::split_paths(&path).collect::<Vec<_>>())
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
}

#[derive(Debug)]
struct BoundedOutputBudget {
    limit: u64,
    consumed: AtomicU64,
    exceeded: AtomicBool,
}

impl BoundedOutputBudget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
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

struct LiveOutputEmitter {
    sink: Option<Arc<dyn SandboxOutputSink>>,
    sandbox_id: sinomo_sandbox::SandboxId,
    command_id: SandboxCommandId,
    stream: SandboxOutputStream,
    offset: u64,
}

impl LiveOutputEmitter {
    fn new(
        sink: Option<Arc<dyn SandboxOutputSink>>,
        sandbox_id: sinomo_sandbox::SandboxId,
        command_id: SandboxCommandId,
        stream: SandboxOutputStream,
    ) -> Self {
        Self {
            sink,
            sandbox_id,
            command_id,
            stream,
            offset: 0,
        }
    }

    fn emit(&mut self, bytes: &[u8]) {
        let Some(sink) = self.sink.as_ref() else {
            self.offset = self.offset.saturating_add(bytes.len() as u64);
            return;
        };
        for bytes in bytes.chunks(SANDBOX_OUTPUT_CHUNK_BYTES) {
            sink.output_chunk(SandboxOutputChunk {
                sandbox_id: self.sandbox_id.clone(),
                command_id: self.command_id.clone(),
                stream: self.stream.clone(),
                offset: self.offset,
                bytes: bytes.to_vec(),
            });
            self.offset = self.offset.saturating_add(bytes.len() as u64);
        }
    }
}

fn execute_native_command(
    request: &SandboxCommandRequest,
    layout: &LocalSandboxInstanceLayout,
    cwd: &Path,
    mount_projection: Option<&NativeMountProjection>,
    timeout_ms: u64,
    config: &SandboxSimpleNativeConfig,
    limits: &SandboxResourceLimits,
    cancellation_token: &SandboxCancellationToken,
    command_id: &SandboxCommandId,
    output_sink: Option<Arc<dyn SandboxOutputSink>>,
    store: &LocalSandboxInstanceStore,
    local_execution_lease: &SandboxLocalExecutionLease,
    local_journal: &mut SandboxLocalExecutionJournal,
) -> Result<NativeCommandOutput, SandboxWasiError> {
    #[cfg(windows)]
    {
        let _ = mount_projection;
        let _ = store;
        let _ = local_execution_lease;
        let _ = local_journal;
        return windows::execute_native_command(
            request,
            layout,
            cwd,
            timeout_ms,
            config,
            limits,
            cancellation_token,
            command_id,
            output_sink,
        );
    }

    #[cfg(not(windows))]
    {
        let _ = config;
        execute_native_command_std(
            request,
            layout,
            cwd,
            mount_projection,
            timeout_ms,
            limits,
            cancellation_token,
            command_id,
            output_sink,
            store,
            local_execution_lease,
            local_journal,
        )
    }
}

#[cfg(not(windows))]
fn execute_native_command_std(
    request: &SandboxCommandRequest,
    layout: &LocalSandboxInstanceLayout,
    cwd: &Path,
    mount_projection: Option<&NativeMountProjection>,
    timeout_ms: u64,
    limits: &SandboxResourceLimits,
    cancellation_token: &SandboxCancellationToken,
    command_id: &SandboxCommandId,
    output_sink: Option<Arc<dyn SandboxOutputSink>>,
    store: &LocalSandboxInstanceStore,
    local_execution_lease: &SandboxLocalExecutionLease,
    local_journal: &mut SandboxLocalExecutionJournal,
) -> Result<NativeCommandOutput, SandboxWasiError> {
    let argv0 = request
        .argv
        .first()
        .ok_or_else(|| SandboxWasiError::InvalidRequest {
            detail: "simple-native command argv must include argv[0]".to_string(),
        })?;
    let env = native_environment(request, layout, cwd);
    let program = resolve_native_program(argv0, &env, cwd)?;
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut handshake = Command::new("/bin/sh");
        handshake.args([
            "-c",
            "IFS= read -r _ <&3; exec 3<&-; exec \"$@\"",
            "sinomo-handshake",
        ]);
        if let Some(projection) = mount_projection {
            handshake
                .arg(&projection.bwrap_program)
                // Functional projection only: the host root remains entirely
                // visible and writable inside the mount namespace. Preserve
                // host device-node usability too: plain `--bind / /` maps
                // devices such as /dev/null to unusable user-namespace nodes.
                .args(["--dev-bind", "/", "/"]);
            for mount in &projection.mounts {
                handshake
                    .arg(if mount.mode == SandboxMountMode::ReadOnly {
                        "--ro-bind"
                    } else {
                        "--bind"
                    })
                    .arg(&mount.source_host_path)
                    .arg(&mount.guest_path);
            }
            handshake
                .arg("--chdir")
                .arg(cwd)
                .arg("--")
                .arg(&program)
                .args(request.argv.iter().skip(1));
        } else {
            handshake.arg(&program).args(request.argv.iter().skip(1));
        }
        handshake
    };
    #[cfg(not(target_os = "linux"))]
    let mut command = Command::new(&program);
    #[cfg(not(target_os = "linux"))]
    command.args(request.argv.iter().skip(1));
    let launch_cwd = if mount_projection.is_some() {
        Path::new("/")
    } else {
        cwd
    };
    command
        .current_dir(launch_cwd)
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
    let (mut launch_handshake, launch_handshake_child) =
        UnixStream::pair().map_err(|source| SandboxWasiError::Runtime {
            detail: format!("failed to create simple-native launch handshake: {source}"),
        })?;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        let memory_mb = limits.memory_mb;
        let expected_parent_pid = std::process::id() as libc::pid_t;
        let handshake_fd = launch_handshake_child.as_raw_fd();
        // SAFETY: pre_exec only calls async-signal-safe libc/syscall operations
        // and returns an io::Error on failure before exec.
        unsafe {
            command.pre_exec(move || {
                if handshake_fd != 3 && libc::dup2(handshake_fd, 3) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                linux::prepare_child_process(memory_mb, expected_parent_pid)
            });
        }
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        use std::os::unix::process::CommandExt;
        let memory_mb = limits.memory_mb;
        // SAFETY: pre_exec only invokes async-signal-safe setpgid/setrlimit
        // operations and returns an io::Error before exec on failure.
        unsafe {
            command.pre_exec(move || linux::prepare_child_process(memory_mb, 0));
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
    #[cfg(target_os = "linux")]
    {
        drop(launch_handshake_child);
        let owner =
            SandboxLocalExecutionExternalOwner::linux_process_group(child.id() as i32, child.id())
                .map_err(|detail| {
                    linux::kill_process_group(child.id() as i32);
                    let _ = child.wait();
                    SandboxWasiError::Runtime { detail }
                })?;
        if let Err(error) = prepare_sandbox_local_execution_external_owner(
            store,
            local_execution_lease,
            local_journal,
            owner,
            OffsetDateTime::now_utc(),
        ) {
            linux::kill_process_group(child.id() as i32);
            let _ = child.wait();
            return Err(SandboxWasiError::Runtime {
                detail: format!(
                    "failed to persist simple-native process identity before launch: {error}"
                ),
            });
        }
        if let Err(source) = launch_handshake.write_all(b"go\n") {
            linux::kill_process_group(child.id() as i32);
            let _ = child.wait();
            return Err(SandboxWasiError::Runtime {
                detail: format!("failed to release simple-native launch handshake: {source}"),
            });
        }
        drop(launch_handshake);
    }
    let stdin_writer = if let Some(stdin) = request.stdin.clone() {
        let mut child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| SandboxWasiError::Runtime {
                detail: "simple-native stdin pipe was not available".to_string(),
            })?;
        Some(std::thread::spawn(move || child_stdin.write_all(&stdin)))
    } else {
        None
    };
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| SandboxWasiError::Runtime {
            detail: "simple-native stdout pipe was not available".to_string(),
        })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| SandboxWasiError::Runtime {
            detail: "simple-native stderr pipe was not available".to_string(),
        })?;
    let output_limit = effective_simple_native_output_limit(limits.max_output_bytes);
    let output_budget = Arc::new(BoundedOutputBudget::new(output_limit));
    let stdout_reader = spawn_bounded_native_reader(
        stdout,
        Arc::clone(&output_budget),
        "stdout",
        LiveOutputEmitter::new(
            output_sink.clone(),
            request.sandbox_id.clone(),
            command_id.clone(),
            SandboxOutputStream::Stdout,
        ),
    );
    let stderr_reader = spawn_bounded_native_reader(
        stderr,
        Arc::clone(&output_budget),
        "stderr",
        LiveOutputEmitter::new(
            output_sink,
            request.sandbox_id.clone(),
            command_id.clone(),
            SandboxOutputStream::Stderr,
        ),
    );
    let (status, mut terminal_error) = wait_for_native_output(
        &mut child,
        timeout_ms,
        cancellation_token,
        &output_budget,
        output_limit,
    )?;
    let stdout = join_bounded_native_reader(stdout_reader, "stdout")?;
    let stderr = join_bounded_native_reader(stderr_reader, "stderr")?;
    if terminal_error.is_none() && output_budget.exceeded() {
        terminal_error = Some(SandboxWasiError::OutputLimitExceeded {
            limit: output_limit.min(usize::MAX as u64) as usize,
        });
    }
    if let Some(writer) = stdin_writer {
        let stdin_result = writer.join().map_err(|_| SandboxWasiError::Runtime {
            detail: "simple-native stdin writer panicked".to_string(),
        })?;
        if terminal_error.is_none() {
            stdin_result.map_err(|source| SandboxWasiError::Runtime {
                detail: format!("failed to write simple-native stdin: {source}"),
            })?;
        }
    }
    Ok(NativeCommandOutput {
        exit_status: exit_status_code(status),
        stdout,
        stderr,
        terminal_error,
    })
}

#[cfg(not(windows))]
fn wait_for_native_output(
    child: &mut Child,
    timeout_ms: u64,
    cancellation_token: &SandboxCancellationToken,
    output_budget: &BoundedOutputBudget,
    output_limit: u64,
) -> Result<(ExitStatus, Option<SandboxWasiError>), SandboxWasiError> {
    let started = Instant::now();
    loop {
        if cancellation_token.is_cancelled() {
            let status = terminate_native_process_group_and_reap(child, "cancellation")?;
            return Ok((status, Some(SandboxWasiError::Cancelled)));
        }
        if output_budget.exceeded() {
            let status = terminate_native_process_group_and_reap(child, "output limit")?;
            return Ok((
                status,
                Some(SandboxWasiError::OutputLimitExceeded {
                    limit: output_limit.min(usize::MAX as u64) as usize,
                }),
            ));
        }
        let wait = match native_wait_poll_duration(started, timeout_ms) {
            Ok(wait) => wait,
            Err(SandboxWasiError::TimedOut { timeout_ms }) => {
                let status = terminate_native_process_group_and_reap(child, "timeout")?;
                return Ok((status, Some(SandboxWasiError::TimedOut { timeout_ms })));
            }
            Err(error) => return Err(error),
        };
        if let Some(status) = child
            .try_wait()
            .map_err(|source| SandboxWasiError::Runtime {
                detail: format!("simple-native command wait failed: {source}"),
            })?
        {
            // The command lifetime owns the whole process group. A shell may
            // exit while a background descendant still holds a pipe open.
            linux::kill_process_group(child.id() as i32);
            return Ok((status, None));
        }
        std::thread::sleep(wait);
    }
}

#[cfg(not(windows))]
fn terminate_native_process_group_and_reap(
    child: &mut Child,
    reason: &str,
) -> Result<ExitStatus, SandboxWasiError> {
    linux::kill_process_group(child.id() as i32);
    let deadline = Instant::now() + Duration::from_millis(SIMPLE_NATIVE_TERMINATION_WAIT_MS);
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|source| SandboxWasiError::Runtime {
                detail: format!("simple-native {reason} failed while reaping process: {source}"),
            })?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(SandboxWasiError::Runtime {
                detail: format!(
                    "simple-native {reason} could not confirm process-group termination within {SIMPLE_NATIVE_TERMINATION_WAIT_MS}ms"
                ),
            });
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(not(windows))]
fn spawn_bounded_native_reader<R>(
    mut reader: R,
    budget: Arc<BoundedOutputBudget>,
    stream: &'static str,
    mut emitter: LiveOutputEmitter,
) -> std::thread::JoinHandle<Result<Vec<u8>, String>>
where
    R: Read + Send + 'static,
{
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = reader
                .read(&mut buffer)
                .map_err(|source| format!("failed to read simple-native {stream}: {source}"))?;
            if read == 0 {
                break;
            }
            let keep = budget.reserve(read);
            output.extend_from_slice(&buffer[..keep]);
            emitter.emit(&buffer[..keep]);
        }
        Ok(output)
    })
}

#[cfg(not(windows))]
fn join_bounded_native_reader(
    reader: std::thread::JoinHandle<Result<Vec<u8>, String>>,
    stream: &str,
) -> Result<Vec<u8>, SandboxWasiError> {
    reader
        .join()
        .map_err(|_| SandboxWasiError::Runtime {
            detail: format!("simple-native {stream} reader panicked"),
        })?
        .map_err(|detail| SandboxWasiError::Runtime { detail })
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
    mount_projection: Option<&NativeMountProjection>,
) -> Result<PathBuf, SandboxCommandExecutionError> {
    let guest_cwd = PathBuf::from(cwd.as_str());
    if let Some(source_cwd) = mount_projection
        .and_then(|projection| projection.projected_source_for_guest_path(&guest_cwd))
    {
        if source_cwd.is_dir() {
            return Ok(guest_cwd);
        }
        return Err(SandboxCommandExecutionError::Fs(SandboxFsError::NotFound {
            path: cwd.clone(),
        }));
    }
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
fn exit_status_code(status: ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    status
        .code()
        .or_else(|| status.signal().map(|signal| 128 + signal))
        .unwrap_or(128)
}

#[cfg(all(not(windows), not(unix)))]
fn exit_status_code(status: ExitStatus) -> i32 {
    status.code().unwrap_or(128)
}

#[cfg(windows)]
mod windows {
    use std::collections::BTreeMap;
    use std::ffi::OsStr;
    use std::mem;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use std::ptr;
    use std::sync::Arc;
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
        BoundedOutputBudget, LiveOutputEmitter, LocalSandboxInstanceLayout, NativeCommandOutput,
        SandboxCancellationToken, SandboxCommandId, SandboxCommandRequest, SandboxOutputSink,
        SandboxOutputStream, SandboxResourceLimits, SandboxSimpleNativeConfig, SandboxWasiError,
        effective_simple_native_output_limit, native_environment, native_wait_poll_duration,
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
        cancellation_token: &SandboxCancellationToken,
        command_id: &SandboxCommandId,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<NativeCommandOutput, SandboxWasiError> {
        let _ = config;
        execute_with_windows_process(
            request,
            layout,
            cwd,
            timeout_ms,
            limits,
            None,
            cancellation_token,
            command_id,
            output_sink,
        )
    }

    fn execute_with_windows_process(
        request: &SandboxCommandRequest,
        layout: &LocalSandboxInstanceLayout,
        cwd: &Path,
        timeout_ms: u64,
        limits: &SandboxResourceLimits,
        token: Option<&OwnedHandle>,
        cancellation_token: &SandboxCancellationToken,
        command_id: &SandboxCommandId,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
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
        let output_limit = effective_simple_native_output_limit(limits.max_output_bytes);
        let output_budget = Arc::new(BoundedOutputBudget::new(output_limit));
        let stdout_reader = spawn_pipe_reader(
            stdout_read,
            "stdout",
            Arc::clone(&output_budget),
            LiveOutputEmitter::new(
                output_sink.clone(),
                request.sandbox_id.clone(),
                command_id.clone(),
                SandboxOutputStream::Stdout,
            ),
        );
        let stderr_reader = spawn_pipe_reader(
            stderr_read,
            "stderr",
            Arc::clone(&output_budget),
            LiveOutputEmitter::new(
                output_sink,
                request.sandbox_id.clone(),
                command_id.clone(),
                SandboxOutputStream::Stderr,
            ),
        );

        let stdin_writer = if let Some(stdin) = request.stdin.clone() {
            Some(thread::spawn(move || write_pipe_all(&stdin_write, &stdin)))
        } else {
            drop(stdin_write);
            None
        };
        let mut terminal_error = wait_for_process(
            &job,
            &process,
            timeout_ms,
            cancellation_token,
            &output_budget,
            output_limit,
        )?;
        let exit_status = process_exit_code(&process);
        let stdout = join_pipe_reader(stdout_reader, "stdout");
        let stderr = join_pipe_reader(stderr_reader, "stderr");
        let exit_status = exit_status?;
        let stdout = stdout?;
        let stderr = stderr?;
        if terminal_error.is_none() && output_budget.exceeded() {
            terminal_error = Some(SandboxWasiError::OutputLimitExceeded {
                limit: output_limit.min(usize::MAX as u64) as usize,
            });
        }
        if let Some(stdin_writer) = stdin_writer {
            let stdin_result = stdin_writer.join().map_err(|_| SandboxWasiError::Runtime {
                detail: "simple-native stdin writer panicked".to_string(),
            })?;
            if terminal_error.is_none() {
                stdin_result?;
            }
        }
        Ok(NativeCommandOutput {
            exit_status,
            stdout,
            stderr,
            terminal_error,
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
        cancellation_token: &SandboxCancellationToken,
        output_budget: &BoundedOutputBudget,
        output_limit: u64,
    ) -> Result<Option<SandboxWasiError>, SandboxWasiError> {
        let started = Instant::now();
        loop {
            if cancellation_token.is_cancelled() {
                terminate_job_and_wait(job, process);
                return Ok(Some(SandboxWasiError::Cancelled));
            }
            if output_budget.exceeded() {
                terminate_job_and_wait(job, process);
                return Ok(Some(SandboxWasiError::OutputLimitExceeded {
                    limit: output_limit.min(usize::MAX as u64) as usize,
                }));
            }
            let wait = match native_wait_poll_duration(started, timeout_ms) {
                Ok(wait) => wait,
                Err(SandboxWasiError::TimedOut { timeout_ms }) => {
                    terminate_job_and_wait(job, process);
                    return Ok(Some(SandboxWasiError::TimedOut { timeout_ms }));
                }
                Err(error) => return Err(error),
            };
            let wait_ms = wait.as_millis().min(u128::from(u32::MAX)) as u32;
            match unsafe { WaitForSingleObject(process.raw(), wait_ms) } {
                WAIT_OBJECT_0 => {
                    // Close the whole job before joining inherited pipe handles.
                    terminate_job_and_wait(job, process);
                    return Ok(None);
                }
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
        budget: Arc<BoundedOutputBudget>,
        mut emitter: LiveOutputEmitter,
    ) -> thread::JoinHandle<Result<Vec<u8>, String>> {
        thread::spawn(move || {
            read_pipe_to_end(handle, &budget, &mut emitter)
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

    fn read_pipe_to_end(
        handle: OwnedHandle,
        budget: &BoundedOutputBudget,
        emitter: &mut LiveOutputEmitter,
    ) -> Result<Vec<u8>, String> {
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
            let read = bytes_read as usize;
            let keep = budget.reserve(read);
            output.extend_from_slice(&buffer[..keep]);
            emitter.emit(&buffer[..keep]);
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
    use std::io;

    use sinomo_sandbox::SANDBOX_UNLIMITED_SIZE_LIMIT;

    pub(super) fn prepare_child_process(
        memory_mb: u64,
        expected_parent_pid: libc::pid_t,
    ) -> io::Result<()> {
        // Keep host-death fencing, process-tree ownership, and the memory
        // ceiling without installing a filesystem namespace or allowlist.
        if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::getppid() } != expected_parent_pid {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "simple-native parent exited during child launch",
            ));
        }
        if unsafe { libc::setpgid(0, 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        apply_memory_limit(memory_mb)
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
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
mod linux {
    use std::io;

    use sinomo_sandbox::SANDBOX_UNLIMITED_SIZE_LIMIT;

    pub(super) fn prepare_child_process(
        memory_mb: u64,
        _expected_parent_pid: i32,
    ) -> io::Result<()> {
        if unsafe { libc::setpgid(0, 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        apply_memory_limit(memory_mb)
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
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(all(not(unix), not(windows)))]
mod linux {
    pub(super) fn kill_process_group(_child_pid: i32) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_sandbox::{SandboxFileSystem, SandboxId, SandboxToolchainId};

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
            host_sifs_root: None,
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

    fn successful_native_output() -> NativeCommandOutput {
        NativeCommandOutput {
            exit_status: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
            terminal_error: None,
        }
    }

    #[test]
    fn native_completion_fences_late_cancellation_until_audit_finishes() {
        let cancellation = SandboxCancellationToken::new();
        let observer = cancellation.clone();
        let (result, completion) =
            transition_after_native_return(&cancellation, Ok(successful_native_output()));

        assert!(result.is_ok());
        assert!(
            !observer.cancel(),
            "finalization must win a late cancellation"
        );
        assert!(observer.is_completing_or_completed());
        drop(completion);
        assert!(observer.is_completing_or_completed());
    }

    #[test]
    fn native_cancellation_wins_before_completion_claim() {
        let cancellation = SandboxCancellationToken::new();
        assert!(cancellation.cancel());
        let (result, completion) =
            transition_after_native_return(&cancellation, Ok(successful_native_output()));

        assert!(matches!(result, Err(SandboxWasiError::Cancelled)));
        drop(completion);
        assert!(cancellation.is_cancelled());
    }

    #[test]
    #[cfg(unix)]
    fn simple_native_streams_first_chunk_before_delayed_exit_without_replay() {
        let (_dir, store, sandbox_id) = create_store();
        let (tx, rx) = std::sync::mpsc::channel::<SandboxOutputChunk>();
        let sink: Arc<dyn SandboxOutputSink> = Arc::new(move |chunk| {
            let _ = tx.send(chunk);
        });
        let join = std::thread::spawn(move || {
            let node_id = NodeId::from_legacy_named("sandbox_simple_live_output").expect("node");
            let toolchain_store =
                sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());
            runner().run_toolchain_command_with_stdin_policy_and_output_sink(
                &store,
                &toolchain_store,
                &sandbox_id,
                "sh",
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "printf first; sleep 1; printf second; printf error >&2".to_string(),
                ],
                None,
                5_000,
                &node_id,
                &SandboxPolicy::default(),
                Some(sink),
            )
        });

        let first = rx
            .recv_timeout(Duration::from_millis(750))
            .expect("first live output chunk before delayed exit");
        assert_eq!(first.stream, SandboxOutputStream::Stdout);
        assert_eq!(first.offset, 0);
        assert_eq!(first.bytes, b"first");
        assert!(
            !join.is_finished(),
            "the first sink callback must arrive while the command is still running"
        );

        let outcome = join
            .join()
            .expect("live output command thread")
            .expect("live output command");
        let mut streamed = vec![first];
        streamed.extend(rx.try_iter());
        let streamed_stdout = streamed
            .iter()
            .filter(|chunk| chunk.stream == SandboxOutputStream::Stdout)
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        let streamed_stderr = streamed
            .iter()
            .filter(|chunk| chunk.stream == SandboxOutputStream::Stderr)
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(streamed_stdout, outcome.stdout);
        assert_eq!(streamed_stderr, outcome.stderr);
        assert_eq!(outcome.stdout, b"firstsecond");
        assert_eq!(outcome.stderr, b"error");
        let stdout_offsets = streamed
            .iter()
            .filter(|chunk| chunk.stream == SandboxOutputStream::Stdout)
            .scan(0_u64, |expected, chunk| {
                let contiguous = chunk.offset == *expected;
                *expected = expected.saturating_add(chunk.bytes.len() as u64);
                Some(contiguous)
            })
            .collect::<Vec<_>>();
        assert!(stdout_offsets.into_iter().all(|contiguous| contiguous));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_cancellation_kills_the_active_process_group() {
        let (_dir, store, sandbox_id) = create_store();
        let (_manifest, layout) = store.load(&sandbox_id).expect("load");
        let started_path = layout.work.join("cancel-started");
        let preserved_path = layout.work.join("cancel-preserved");
        let progress_path = layout.work.join("cancel-progress");
        let cancellation = SandboxCancellationToken::new();
        let thread_cancellation = cancellation.clone();
        let streamed = Arc::new(std::sync::Mutex::new(Vec::<SandboxOutputChunk>::new()));
        let streamed_sink = Arc::clone(&streamed);
        let sink: Arc<dyn SandboxOutputSink> = Arc::new(move |chunk| {
            streamed_sink
                .lock()
                .expect("streamed cancellation output lock")
                .push(chunk);
        });
        let runner = runner();
        let join = std::thread::spawn(move || {
            let node_id = NodeId::from_legacy_named("sandbox_simple_cancel").expect("node");
            let toolchain_store =
                sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());
            runner.run_toolchain_command_with_stdin_policy_output_sink_and_cancellation(
                &store,
                &toolchain_store,
                &sandbox_id,
                "sh",
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "printf live-output; printf preserved > \"$SINOMO_WORK/cancel-preserved\"; touch \"$SINOMO_WORK/cancel-started\"; while :; do printf x >> \"$SINOMO_WORK/cancel-progress\"; sleep 0.02; done"
                        .to_string(),
                ],
                None,
                SANDBOX_UNLIMITED_WALL_TIME_MS,
                &node_id,
                &SandboxPolicy::default(),
                Some(sink),
                thread_cancellation,
            )
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !started_path.exists() {
            assert!(Instant::now() < deadline, "native command did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
        while fs::metadata(&progress_path).map_or(0, |metadata| metadata.len()) == 0 {
            assert!(Instant::now() < deadline, "native writer made no progress");
            std::thread::sleep(Duration::from_millis(10));
        }
        let cancelled_at = Instant::now();
        assert!(cancellation.cancel());
        let error = join
            .join()
            .expect("simple-native thread")
            .expect_err("active command must be cancelled");
        assert!(matches!(
            error,
            SandboxCommandExecutionError::Wasi(SandboxWasiError::Cancelled)
        ));
        assert!(cancelled_at.elapsed() < Duration::from_secs(2));
        assert_eq!(
            fs::read_to_string(&preserved_path).expect("completed pre-cancel write"),
            "preserved"
        );
        let progress_after_cancel = fs::metadata(&progress_path).expect("progress file").len();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            fs::metadata(&progress_path)
                .expect("progress file after cancellation")
                .len(),
            progress_after_cancel,
            "the terminated process group must not write after cancellation returns"
        );
        let streamed_stdout = streamed
            .lock()
            .expect("streamed cancellation output")
            .iter()
            .filter(|chunk| chunk.stream == SandboxOutputStream::Stdout)
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(streamed_stdout, b"live-output");
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_output_limit_stops_unbounded_stream_and_keeps_bounded_audit() {
        let (_dir, store, sandbox_id) = create_store();
        let (mut manifest, layout) = store.load(&sandbox_id).expect("load");
        manifest.limits.max_output_bytes = 4 * 1024;
        store.save_manifest(&manifest).expect("save output limit");
        let node_id = NodeId::from_legacy_named("sandbox_simple_output_limit").expect("node");
        let toolchain_store = sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());
        let streamed = Arc::new(std::sync::Mutex::new(Vec::<SandboxOutputChunk>::new()));
        let streamed_sink = Arc::clone(&streamed);
        let sink: Arc<dyn SandboxOutputSink> = Arc::new(move |chunk| {
            streamed_sink
                .lock()
                .expect("streamed output-limit lock")
                .push(chunk);
        });
        let started = Instant::now();
        let error = runner()
            .run_toolchain_command_with_stdin_policy_and_output_sink(
                &store,
                &toolchain_store,
                &sandbox_id,
                "sh",
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "while :; do printf 0123456789abcdef; done".to_string(),
                ],
                None,
                SANDBOX_UNLIMITED_WALL_TIME_MS,
                &node_id,
                &SandboxPolicy::default(),
                Some(sink),
            )
            .expect_err("unbounded output must be stopped at the configured limit");
        assert!(matches!(
            error,
            SandboxCommandExecutionError::Wasi(SandboxWasiError::OutputLimitExceeded {
                limit: 4096
            })
        ));
        assert!(started.elapsed() < Duration::from_secs(2));

        sinomo_sandbox::recover_sandbox_local_execution_journal(
            &store,
            &sandbox_id,
            OffsetDateTime::now_utc(),
        )
        .expect("recover output-limit audit");
        let manifest = store.load(&sandbox_id).expect("reload audit manifest").0;
        let command_id = manifest.command_history.last().expect("command audit id");
        let mut fs = LocalSandboxFileSystem::new(layout);
        let stdout = fs
            .read_unchecked_mut(
                &sinomo_sandbox::sandbox_command_stdout_log_path(command_id)
                    .expect("stdout log path"),
                None,
            )
            .expect("bounded stdout audit");
        assert_eq!(stdout.len(), 4096);
        let streamed_stdout = streamed
            .lock()
            .expect("streamed output-limit output")
            .iter()
            .filter(|chunk| chunk.stream == SandboxOutputStream::Stdout)
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(streamed_stdout, stdout);
        let record = sinomo_sandbox::load_command_record(&mut fs, command_id)
            .expect("output-limit command record");
        assert_eq!(record.status, SandboxCommandRecordStatus::ResourceExhausted);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_cancellation_interrupts_a_blocked_stdin_writer() {
        let (_dir, store, sandbox_id) = create_store();
        let (_manifest, layout) = store.load(&sandbox_id).expect("load");
        let started_path = layout.work.join("stdin-blocked-started");
        let cancellation = SandboxCancellationToken::new();
        let thread_cancellation = cancellation.clone();
        let join = std::thread::spawn(move || {
            let node_id = NodeId::from_legacy_named("sandbox_simple_stdin_cancel").expect("node");
            let toolchain_store =
                sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());
            runner().run_toolchain_command_with_stdin_policy_output_sink_and_cancellation(
                &store,
                &toolchain_store,
                &sandbox_id,
                "sh",
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "touch \"$SINOMO_WORK/stdin-blocked-started\"; sleep 30".to_string(),
                ],
                Some(vec![0x5a; 16 * 1024 * 1024]),
                SANDBOX_UNLIMITED_WALL_TIME_MS,
                &node_id,
                &SandboxPolicy::default(),
                None,
                thread_cancellation,
            )
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !started_path.exists() {
            assert!(Instant::now() < deadline, "native command did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
        let cancelled_at = Instant::now();
        assert!(cancellation.cancel());
        let error = join
            .join()
            .expect("simple-native thread")
            .expect_err("blocked stdin command must be cancelled");
        assert!(matches!(
            error,
            SandboxCommandExecutionError::Wasi(SandboxWasiError::Cancelled)
        ));
        assert!(cancelled_at.elapsed() < Duration::from_secs(2));
    }

    #[test]
    #[cfg(unix)]
    fn simple_native_leader_exit_terminates_background_pipe_holders() {
        let (dir, store, sandbox_id) = create_store();
        let delayed_write = dir.path().join("background-must-die");
        let started = Instant::now();
        let outcome = run_shell(
            &store,
            &sandbox_id,
            &format!("(sleep 0.3; touch {}) &", delayed_write.display()),
            0,
        );
        assert_eq!(outcome.response.exit_status, 0);
        assert!(started.elapsed() < Duration::from_secs(2));
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !delayed_write.exists(),
            "a background descendant must not survive command completion"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_can_read_system_files() {
        let (_dir, store, sandbox_id) = create_store();
        let outcome = run_shell(&store, &sandbox_id, "cat /etc/passwd >/dev/null", 0);
        assert_eq!(outcome.response.exit_status, 0);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_allows_arbitrary_host_reads() {
        let (dir, store, sandbox_id) = create_store();
        let secret = dir.path().join("secret.txt");
        fs::write(&secret, b"secret").expect("secret");
        let outcome = run_shell(
            &store,
            &sandbox_id,
            &format!("cat {} >/dev/null", secret.display()),
            0,
        );
        assert_eq!(outcome.response.exit_status, 0);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_allows_sandbox_work_writes() {
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
    fn simple_native_can_execute_from_arbitrary_host_workspace_cwd() {
        let (dir, store, sandbox_id) = create_store();
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let (mut manifest, _layout) = store.load(&sandbox_id).expect("load");
        manifest.cwd =
            SandboxPath::new(workspace.display().to_string()).expect("host workspace cwd");
        store.save_manifest(&manifest).expect("save manifest");
        let node_id = NodeId::from_legacy_named("sandbox_simple_test").expect("node");
        let toolchain_store = sinomo_sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir());
        let outcome = runner()
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
    fn simple_native_allows_arbitrary_host_writes() {
        let (dir, store, sandbox_id) = create_store();
        let host_path = dir.path().join("arbitrary-host-write.txt");
        let (mut manifest, _) = store.load(&sandbox_id).expect("load manifest");
        manifest.limits.max_written_bytes_per_command = 1;
        store.save_manifest(&manifest).expect("save write limit");
        let outcome = run_shell(
            &store,
            &sandbox_id,
            &format!("printf ok > {}", host_path.display()),
            0,
        );
        assert_eq!(outcome.response.exit_status, 0);
        assert_eq!(fs::read_to_string(host_path).expect("host write"), "ok");
        assert_eq!(
            outcome.response.resource_usage.written_bytes, SANDBOX_RESOURCE_USAGE_UNAVAILABLE,
            "simple-native must not claim complete host-write accounting or enforce a partial count"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_projects_live_rw_and_ro_sifs_mounts() {
        let Some(bwrap) = find_host_program("bwrap") else {
            return;
        };
        let projection_probe = Command::new(&bwrap)
            .args(["--dev-bind", "/", "/", "--", "/bin/true"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if !projection_probe.is_ok_and(|status| status.success()) {
            return;
        }

        let (dir, store, sandbox_id) = create_store();
        let host_sifs_root = dir.path().join("live-sifs");
        let source = host_sifs_root.join("services/sandbox-shared/live");
        fs::create_dir_all(&source).expect("live SIFS source");
        let (mut manifest, layout) = store.load(&sandbox_id).expect("load");
        let guest = layout.root.join("projected-live");
        fs::create_dir_all(&guest).expect("guest mount target");
        let guest_mount_path =
            SandboxPath::new(guest.display().to_string()).expect("guest mount path");
        manifest.cwd = guest_mount_path.clone();
        manifest.mounts = vec![SandboxMount {
            source_sifs_path: "/services/sandbox-shared/live".to_string(),
            mount_path: guest_mount_path,
            mode: SandboxMountMode::ReadWrite,
        }];
        store.save_manifest(&manifest).expect("save RW mount");

        let runner = SandboxSimpleNativeCommandRunner::new(SandboxSimpleNativeConfig {
            host_sifs_root: Some(host_sifs_root.clone()),
            ..SandboxSimpleNativeConfig::default()
        });
        let thread_store = store.clone();
        let thread_manifest = manifest.clone();
        let thread_layout = layout.clone();
        let guest_for_script = guest.clone();
        let join = std::thread::spawn(move || {
            let lease = try_acquire_sandbox_local_execution_lease(
                &thread_store,
                &thread_manifest.sandbox_id,
            )
            .expect("execution lease");
            runner.run_native_command(
                &thread_store,
                &thread_manifest,
                &thread_layout,
                SandboxCommandRequest {
                    sandbox_id: thread_manifest.sandbox_id.clone(),
                    argv: vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        format!(
                            "printf probe >/dev/null || exit 97; touch {}/started; while [ ! -f {}/remote-input ] || [ \"$(cat {}/remote-input)\" != remote ]; do sleep 0.01; done; cat {}/remote-input > {}/value; touch {}/published; sleep 0.3; printf done >> {}/value",
                            guest_for_script.display(),
                            guest_for_script.display(),
                            guest_for_script.display(),
                            guest_for_script.display(),
                            guest_for_script.display(),
                            guest_for_script.display(),
                            guest_for_script.display()
                        ),
                    ],
                    cwd: thread_manifest.cwd.clone(),
                    stdin: None,
                    env: thread_manifest.env.clone(),
                    timeout_ms: 5_000,
                },
                None,
                &SandboxCancellationToken::new(),
                &lease,
            )
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !source.join("started").exists() {
            assert!(Instant::now() < deadline, "projected writer did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
        fs::write(source.join("remote-input"), b"remote").expect("external live SIFS update");
        while !source.join("published").exists() {
            assert!(
                Instant::now() < deadline,
                "projected command did not observe and publish the external update"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            fs::read_to_string(source.join("value")).expect("live projected write"),
            "remote",
            "the command must observe an external live-mount update and publish its write before completion"
        );
        assert!(
            !join.is_finished(),
            "the projected write must be visible before command/checkpoint completion"
        );
        let outcome = join
            .join()
            .expect("projected writer thread")
            .expect("projected writer command");
        assert_eq!(outcome.response.exit_status, 0);
        assert_eq!(
            fs::read_to_string(source.join("value")).expect("completed projected write"),
            "remotedone"
        );

        manifest.mounts[0].mode = SandboxMountMode::ReadOnly;
        store.save_manifest(&manifest).expect("save RO mount");
        let runner = SandboxSimpleNativeCommandRunner::new(SandboxSimpleNativeConfig {
            host_sifs_root: Some(host_sifs_root),
            ..SandboxSimpleNativeConfig::default()
        });
        let lease = try_acquire_sandbox_local_execution_lease(&store, &sandbox_id)
            .expect("RO execution lease");
        let outcome = runner
            .run_native_command(
                &store,
                &manifest,
                &layout,
                SandboxCommandRequest {
                    sandbox_id: sandbox_id.clone(),
                    argv: vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        format!("printf denied > {}/ro-write", guest.display()),
                    ],
                    cwd: manifest.cwd.clone(),
                    stdin: None,
                    env: manifest.env.clone(),
                    timeout_ms: 5_000,
                },
                None,
                &SandboxCancellationToken::new(),
                &lease,
            )
            .expect("RO projection command outcome");
        assert_ne!(outcome.response.exit_status, 0);
        assert!(!source.join("ro-write").exists());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn simple_native_declared_mount_requires_a_live_host_sifs_root() {
        let error = prepare_native_mount_projection(
            &[SandboxMount {
                source_sifs_path: "/services/sandbox-shared/live".to_string(),
                mount_path: SandboxPath::new("/mnt/live").expect("mount path"),
                mode: SandboxMountMode::ReadOnly,
            }],
            &SandboxSimpleNativeConfig::default(),
        )
        .expect_err("declared mounts must never fall back to copied data");
        assert!(matches!(error, SandboxWasiError::InvalidRequest { .. }));
        assert!(error.to_string().contains("SIFS/FUSE root"));
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
    fn simple_native_windows_runs_direct_cmd() {
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
            host_sifs_root: None,
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
    fn simple_native_windows_allows_arbitrary_host_read() {
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

        assert_eq!(outcome.response.exit_status, 0);
        assert!(String::from_utf8_lossy(&outcome.stdout).contains("outside-secret"));
    }

    #[test]
    #[cfg(windows)]
    fn simple_native_windows_allows_arbitrary_host_write() {
        let (dir, store, sandbox_id) = create_store();
        let host_path = dir.path().join("arbitrary-write.txt");
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
                    format!("echo ok>{}", host_path.display()),
                ],
                None,
                5000,
                &node_id,
                &SandboxPolicy::default(),
                None,
            )
            .expect("simple-native Windows host write command");

        assert_eq!(outcome.response.exit_status, 0);
        assert!(host_path.exists());
    }
}
