use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sinomo_module::NativeModuleUnloadReport;
use time::OffsetDateTime;

use crate::*;

pub struct SandboxExecutorCommandRunner {
    module: SandboxExecutorClient,
}

impl SandboxExecutorCommandRunner {
    pub fn from_active_module(
        data_dir_root: impl AsRef<Path>,
    ) -> Result<Self, SandboxCommandExecutionError> {
        let module = SandboxExecutorClient::from_active_component(data_dir_root)
            .map_err(executor_module_error_to_wasi)?;
        Ok(Self { module })
    }

    pub fn from_module(module: SandboxExecutorClient) -> Self {
        Self { module }
    }

    /// Load a provider artifact whose bytes and provenance were already
    /// verified by the caller. The descriptor is still required to offer the
    /// generic sandbox-executor interface.
    ///
    /// # Safety
    ///
    /// Native modules share the host process address space. Callers must only
    /// pass trusted artifacts that completed policy and provenance checks.
    pub unsafe fn from_verified_module_path(
        library_path: impl AsRef<Path>,
    ) -> Result<Self, SandboxCommandExecutionError> {
        let module = unsafe { SandboxExecutorClient::load_verified(library_path) }
            .map_err(executor_module_error_to_wasi)?;
        Ok(Self { module })
    }

    pub fn unload(self) -> NativeModuleUnloadReport {
        self.module.unload()
    }

    pub fn validate_sinomo_module_v1_component_contract(
        &self,
        wasm: impl AsRef<[u8]>,
    ) -> Result<(), SandboxCommandExecutionError> {
        let request =
            SandboxExecutorRequest::ValidateComponent(SandboxExecutorValidateComponentRequest {
                wasm: wasm.as_ref().to_vec(),
            });
        match self
            .module
            .invoke(&request)
            .map_err(executor_module_error_to_wasi)?
        {
            SandboxExecutorResponse::ValidateComponent(_) => Ok(()),
            SandboxExecutorResponse::Error(error) => Err(executor_error_to_wasi(error).into()),
            other => Err(SandboxWasiError::Runtime {
                detail: format!(
                    "sandbox executor returned unexpected validation response {other:?}"
                ),
            }
            .into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_module_with_stdin_limits_preopens_output_sink_and_toolchain(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        extra_preopens: Vec<SandboxExecutorHostPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_preopens_output_sink_toolchain_and_cancellation(
            store,
            toolchain_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            extra_preopens,
            output_sink,
            SandboxCancellationToken::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_module_with_stdin_limits_preopens_output_sink_toolchain_and_cancellation(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        mut extra_preopens: Vec<SandboxExecutorHostPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let local_execution_lease = try_acquire_sandbox_local_execution_lease(store, sandbox_id)?;
        recover_pending_daemonless_execution(store, &local_execution_lease, sandbox_id)?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let (toolchain, _toolchain_layout) =
            match toolchain_store.load_manifest(&manifest.toolchain_id) {
                Ok(toolchain) => toolchain,
                Err(SandboxToolchainError::NotFound { .. }) => {
                    return self.run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
                        store,
                        &local_execution_lease,
                        sandbox_id,
                        wasm,
                        argv,
                        stdin,
                        timeout_ms,
                        limits,
                        extra_preopens,
                        output_sink,
                        default_sandbox_wasi_abi(),
                        BTreeMap::new(),
                        None,
                        BTreeMap::new(),
                        true,
                        true,
                        true,
                        false,
                        Vec::new(),
                        cancellation_token,
                        true,
                    );
                }
                Err(error) => return Err(error.into()),
            };
        let toolchain_artifacts = load_local_toolchain_artifacts(toolchain_store, &toolchain)?;
        let toolchain_commands = load_local_toolchain_commands(toolchain_store, &toolchain)?;
        let mut artifact_preopens =
            stage_toolchain_runtime_to_local(&layout, &toolchain_artifacts, &toolchain_commands)?;
        artifact_preopens.append(&mut extra_preopens);
        self.run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
            store,
            &local_execution_lease,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            artifact_preopens,
            output_sink,
            default_sandbox_wasi_abi(),
            BTreeMap::new(),
            None,
            BTreeMap::new(),
            true,
            true,
            true,
            false,
            command_overrides_for_toolchain(&toolchain),
            cancellation_token,
            true,
        )
    }

    pub fn run_toolchain_command_with_stdin_and_policy(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_and_output_sink(
            store,
            toolchain_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_and_output_sink_and_cancellation(
            store,
            toolchain_store,
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
    pub fn run_toolchain_command_with_stdin_policy_and_output_sink_and_cancellation(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env_and_cancellation(
            store,
            toolchain_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            None,
            BTreeMap::new(),
            cancellation_token,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env_and_cancellation(
            store,
            toolchain_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            cwd_override,
            env_override,
            SandboxCancellationToken::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env_and_cancellation(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_output_sink_cwd_env_cancellation_mode(
            store,
            toolchain_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            cwd_override,
            env_override,
            cancellation_token,
            true,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_toolchain_command_with_stdin_policy_output_sink_cwd_env_cancellation_mode(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        cancellation_token: SandboxCancellationToken,
        complete_outcome: bool,
        held_local_execution_lease: Option<&SandboxLocalExecutionLease>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        validate_sandbox_toolchain_argv(command_name, &argv).map_err(|detail| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { detail })
        })?;
        let owned_local_execution_lease = if held_local_execution_lease.is_none() {
            Some(try_acquire_sandbox_local_execution_lease(
                store, sandbox_id,
            )?)
        } else {
            None
        };
        let local_execution_lease = held_local_execution_lease
            .or(owned_local_execution_lease.as_ref())
            .expect("a local execution lease is always present");
        if owned_local_execution_lease.is_some() {
            recover_pending_daemonless_execution(store, local_execution_lease, sandbox_id)?;
        }
        let (manifest, layout) = store.load(sandbox_id)?;
        let (toolchain, _toolchain_layout) =
            toolchain_store.load_manifest(&manifest.toolchain_id)?;
        let wasm = toolchain_store.load_command_module(&toolchain, command_name)?;
        let toolchain_artifacts = load_local_toolchain_artifacts(toolchain_store, &toolchain)?;
        let toolchain_commands = load_local_toolchain_commands(toolchain_store, &toolchain)?;
        self.run_resolved_toolchain_command_with_policy(
            store,
            sandbox_id,
            &manifest,
            &layout,
            &toolchain,
            command_name,
            wasm,
            toolchain_artifacts,
            toolchain_commands,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            cwd_override,
            env_override,
            cancellation_token,
            complete_outcome,
            local_execution_lease,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_resolved_toolchain_command_with_policy(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        manifest: &SandboxInstanceManifest,
        layout: &LocalSandboxInstanceLayout,
        toolchain: &SandboxToolchainManifest,
        command_name: &str,
        wasm: Vec<u8>,
        toolchain_artifacts: Vec<(String, Vec<u8>)>,
        toolchain_commands: Vec<(String, Vec<u8>)>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        cancellation_token: SandboxCancellationToken,
        complete_outcome: bool,
        local_execution_lease: &SandboxLocalExecutionLease,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let command = toolchain.command(command_name)?;
        policy
            .validate_invocation(manifest, toolchain, command, execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        let limits = effective_sandbox_command_limits(&manifest.limits, &command.default_limits);
        let timeout_ms = effective_sandbox_timeout_ms(timeout_ms, limits.wall_time_ms);
        let argv = command.effective_argv(argv);
        let pinned_child_exec_only = !command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY);
        let auto_asyncify = command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY);
        let artifact_preopens =
            stage_toolchain_runtime_to_local(layout, &toolchain_artifacts, &toolchain_commands)?;
        self.run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
            store,
            local_execution_lease,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            Some(limits),
            artifact_preopens,
            output_sink,
            command.effective_target_abi(toolchain),
            command.default_env.clone(),
            cwd_override,
            env_override,
            pinned_child_exec_only,
            false,
            auto_asyncify,
            false,
            command_overrides_for_toolchain(toolchain),
            cancellation_token,
            complete_outcome,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
        &self,
        store: &LocalSandboxInstanceStore,
        local_execution_lease: &SandboxLocalExecutionLease,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        extra_preopens: Vec<SandboxExecutorHostPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        target_abi: SandboxWasiAbi,
        default_env: BTreeMap<String, String>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        pinned_child_exec_only: bool,
        instruction_metering: bool,
        auto_asyncify: bool,
        fpcast_emulation: bool,
        command_overrides: Vec<SandboxExecutorCommandOverride>,
        cancellation_token: SandboxCancellationToken,
        complete_outcome: bool,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let (manifest, layout) = store.load(sandbox_id)?;
        let limits = limits.unwrap_or_else(|| manifest.limits.clone());
        let effective_cwd = cwd_override.unwrap_or_else(|| manifest.cwd.clone());
        let started_at = OffsetDateTime::now_utc();
        let mut env = default_env;
        env.extend(manifest.env.clone());
        env.extend(env_override);
        env.insert("PWD".to_string(), effective_cwd.as_str().to_string());
        let request = SandboxCommandRequest {
            sandbox_id: manifest.sandbox_id.clone(),
            argv: argv.clone(),
            cwd: effective_cwd.clone(),
            stdin,
            env,
            timeout_ms,
        };
        let crash_audit_record = SandboxCommandRecord {
            sandbox_id: manifest.sandbox_id.clone(),
            command_id: SandboxCommandId::generate(),
            status: SandboxCommandRecordStatus::Failed,
            workspace_root: manifest.workspace_root.clone(),
            toolchain_id: manifest.toolchain_id.clone(),
            argv: argv.clone(),
            cwd: effective_cwd.clone(),
            env: request.env.clone(),
            started_at,
            completed_at: None,
            response: None,
            error: Some("sandbox host stopped while the local guest was active".to_string()),
        };
        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        validate_sandbox_working_directory(&sandbox_fs, &effective_cwd)?;
        let before = sandbox_fs
            .snapshot_with_checkpoints(|| cancellation_checkpoint(&cancellation_token))?;
        validate_workspace_usage(&before.workspace_usage(), &limits)?;
        let mut journal = begin_sandbox_local_execution_journal(
            store,
            local_execution_lease,
            &manifest,
            OffsetDateTime::now_utc(),
        )?;
        prepare_sandbox_local_execution_audit_identity(
            store,
            local_execution_lease,
            &mut journal,
            crash_audit_record,
            OffsetDateTime::now_utc(),
        )?;
        let mut local_journal = Some(journal);
        activate_sandbox_local_execution_journal(
            store,
            local_execution_lease,
            local_journal
                .as_mut()
                .expect("local execution journal was just created"),
            OffsetDateTime::now_utc(),
        )?;
        let mut options = local_staged_sandbox_wasix_options(&layout, &limits, extra_preopens);
        options.pinned_child_exec_only = pinned_child_exec_only;
        options.instruction_metering = instruction_metering;
        options.auto_asyncify = auto_asyncify;
        options.fpcast_emulation = fpcast_emulation;
        options.command_overrides = command_overrides;
        let execution = if cancellation_token.is_cancelled() {
            Err(SandboxWasiError::Cancelled)
        } else {
            self.invoke_run_module(
                target_abi,
                wasm.as_ref().to_vec(),
                request.clone(),
                options,
                output_sink,
                cancellation_token.clone(),
            )
        };
        let execution =
            transition_after_native_return(&cancellation_token, complete_outcome, execution);
        let mut outcome = match execution {
            Ok(outcome) => outcome,
            Err(error) => {
                let command_id = SandboxCommandId::generate();
                let completed_at = OffsetDateTime::now_utc();
                let command_record = SandboxCommandRecord {
                    sandbox_id: manifest.sandbox_id.clone(),
                    command_id: command_id.clone(),
                    status: sandbox_command_record_status_for_wasi_error(&error),
                    workspace_root: manifest.workspace_root.clone(),
                    toolchain_id: manifest.toolchain_id.clone(),
                    argv,
                    cwd: effective_cwd.clone(),
                    env: request.env.clone(),
                    started_at,
                    completed_at: Some(completed_at),
                    response: None,
                    error: Some(error.to_string()),
                };
                let cancelled = matches!(&error, SandboxWasiError::Cancelled);
                let journal_outcome = if cancelled {
                    SandboxLocalExecutionOutcome::Cancelled
                } else {
                    SandboxLocalExecutionOutcome::Failed {
                        detail: error.to_string(),
                    }
                };
                let settlement = settle_daemonless_outcome(
                    store,
                    local_execution_lease,
                    &mut local_journal,
                    journal_outcome,
                    command_record,
                    Vec::new(),
                    Vec::new(),
                );
                if cancelled {
                    let detail = match settlement {
                        Ok(()) => format!(
                            "audit cleanup is pending in {}",
                            sandbox_local_execution_journal_path(&store.layout(sandbox_id))
                                .display()
                        ),
                        Err(settlement_error) => format!(
                            "terminal cancellation marker update failed; GuestActive recovery remains pending: {settlement_error}"
                        ),
                    };
                    cancellation_token.record_durability_failure(detail.clone());
                    return Err(SandboxWasiError::CancelledCleanupFailed { detail }.into());
                }
                let cleanup = settlement.and_then(|()| {
                    complete_daemonless_outcome(store, local_execution_lease, &mut local_journal)
                });
                if let Err(cleanup_error) = cleanup {
                    cancellation_token.record_durability_failure(format!(
                        "finalize daemonless outcome {:?}: {cleanup_error}",
                        error
                    ));
                    return Err(cleanup_error);
                }
                if complete_outcome {
                    cancellation_token.complete();
                }
                return Err(error.into());
            }
        };

        let after = match sandbox_fs
            .snapshot_with_checkpoints(|| cancellation_checkpoint(&cancellation_token))
        {
            Ok(after) => after,
            Err(error) => {
                let completed_at = OffsetDateTime::now_utc();
                let command_record = SandboxCommandRecord {
                    sandbox_id: manifest.sandbox_id.clone(),
                    command_id: outcome.response.command_id.clone(),
                    status: if matches!(
                        &error,
                        SandboxCommandExecutionError::Wasi(SandboxWasiError::Cancelled)
                    ) {
                        SandboxCommandRecordStatus::Cancelled
                    } else {
                        SandboxCommandRecordStatus::Failed
                    },
                    workspace_root: manifest.workspace_root.clone(),
                    toolchain_id: manifest.toolchain_id.clone(),
                    argv,
                    cwd: effective_cwd.clone(),
                    env: request.env.clone(),
                    started_at,
                    completed_at: Some(completed_at),
                    response: Some(outcome.response.clone()),
                    error: Some(error.to_string()),
                };
                let cancelled = matches!(
                    &error,
                    SandboxCommandExecutionError::Wasi(SandboxWasiError::Cancelled)
                );
                let journal_outcome = if cancelled {
                    SandboxLocalExecutionOutcome::Cancelled
                } else {
                    SandboxLocalExecutionOutcome::Failed {
                        detail: error.to_string(),
                    }
                };
                let settlement = settle_daemonless_outcome(
                    store,
                    local_execution_lease,
                    &mut local_journal,
                    journal_outcome,
                    command_record,
                    outcome.stdout.clone(),
                    outcome.stderr.clone(),
                );
                if cancelled {
                    let detail = match settlement {
                        Ok(()) => format!(
                            "audit cleanup is pending in {}",
                            sandbox_local_execution_journal_path(&store.layout(sandbox_id))
                                .display()
                        ),
                        Err(settlement_error) => format!(
                            "terminal cancellation marker update failed; GuestActive recovery remains pending: {settlement_error}"
                        ),
                    };
                    cancellation_token.record_durability_failure(detail.clone());
                    return Err(SandboxWasiError::CancelledCleanupFailed { detail }.into());
                }
                let cleanup = settlement.and_then(|()| {
                    complete_daemonless_outcome(store, local_execution_lease, &mut local_journal)
                });
                if let Err(cleanup_error) = cleanup {
                    cancellation_token.record_durability_failure(format!(
                        "finalize daemonless post-execution failure: {cleanup_error}"
                    ));
                    return Err(cleanup_error);
                }
                if complete_outcome {
                    cancellation_token.complete();
                }
                return Err(error);
            }
        };
        if let Err(error) = validate_workspace_usage(&after.workspace_usage(), &limits) {
            let completed_at = OffsetDateTime::now_utc();
            let command_record = SandboxCommandRecord {
                sandbox_id: manifest.sandbox_id.clone(),
                command_id: outcome.response.command_id.clone(),
                status: SandboxCommandRecordStatus::ResourceExhausted,
                workspace_root: manifest.workspace_root.clone(),
                toolchain_id: manifest.toolchain_id.clone(),
                argv,
                cwd: effective_cwd.clone(),
                env: request.env.clone(),
                started_at,
                completed_at: Some(completed_at),
                response: Some(outcome.response.clone()),
                error: Some(error.to_string()),
            };
            settle_daemonless_outcome(
                store,
                local_execution_lease,
                &mut local_journal,
                SandboxLocalExecutionOutcome::Failed {
                    detail: error.to_string(),
                },
                command_record,
                outcome.stdout.clone(),
                outcome.stderr.clone(),
            )?;
            complete_daemonless_outcome(store, local_execution_lease, &mut local_journal)?;
            if complete_outcome {
                cancellation_token.complete();
            }
            return Err(error.into());
        }

        let written_bytes = after.written_bytes_since(&before);
        if let Err(error) = validate_written_bytes(written_bytes, &limits) {
            let mut response = outcome.response.clone();
            response.resource_usage.written_bytes = written_bytes;
            let completed_at = OffsetDateTime::now_utc();
            let command_record = SandboxCommandRecord {
                sandbox_id: manifest.sandbox_id.clone(),
                command_id: response.command_id.clone(),
                status: SandboxCommandRecordStatus::ResourceExhausted,
                workspace_root: manifest.workspace_root.clone(),
                toolchain_id: manifest.toolchain_id.clone(),
                argv,
                cwd: effective_cwd.clone(),
                env: request.env.clone(),
                started_at,
                completed_at: Some(completed_at),
                response: Some(response),
                error: Some(error.to_string()),
            };
            settle_daemonless_outcome(
                store,
                local_execution_lease,
                &mut local_journal,
                SandboxLocalExecutionOutcome::Failed {
                    detail: error.to_string(),
                },
                command_record,
                outcome.stdout.clone(),
                outcome.stderr.clone(),
            )?;
            complete_daemonless_outcome(store, local_execution_lease, &mut local_journal)?;
            if complete_outcome {
                cancellation_token.complete();
            }
            return Err(error.into());
        }

        outcome.response.changed_paths = after.changed_paths_since(&before);
        outcome.response.resource_usage.written_bytes = written_bytes;
        let completed_at = OffsetDateTime::now_utc();
        let command_record = SandboxCommandRecord {
            sandbox_id: manifest.sandbox_id.clone(),
            command_id: outcome.response.command_id.clone(),
            status: SandboxCommandRecordStatus::Completed,
            workspace_root: manifest.workspace_root.clone(),
            toolchain_id: manifest.toolchain_id.clone(),
            argv,
            cwd: effective_cwd,
            env: request.env.clone(),
            started_at,
            completed_at: Some(completed_at),
            response: Some(outcome.response.clone()),
            error: None,
        };
        settle_daemonless_outcome(
            store,
            local_execution_lease,
            &mut local_journal,
            SandboxLocalExecutionOutcome::Succeeded,
            command_record,
            outcome.stdout.clone(),
            outcome.stderr.clone(),
        )?;
        complete_daemonless_outcome(store, local_execution_lease, &mut local_journal)?;
        if complete_outcome {
            cancellation_token.complete();
        }
        Ok(outcome)
    }

    pub fn run_toolchain_process_plan_with_policy_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_process_plan_with_policy_output_sink_and_cancellation(
            store,
            toolchain_store,
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
    pub fn run_toolchain_process_plan_with_policy_output_sink_and_cancellation(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        validate_sandbox_process_plan(plan)?;
        let local_execution_lease = try_acquire_sandbox_local_execution_lease(store, sandbox_id)?;
        recover_pending_daemonless_execution(store, &local_execution_lease, sandbox_id)?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let sandbox_fs = LocalSandboxFileSystem::new(layout);
        validate_sandbox_process_plan_working_directories(plan, &sandbox_fs, &manifest.cwd)?;
        let (toolchain, _toolchain_layout) =
            toolchain_store.load_manifest(&manifest.toolchain_id)?;
        validate_sandbox_process_plan_invocations(
            plan,
            &manifest,
            &toolchain,
            execution_node_id,
            policy,
        )?;

        let mut step_outcomes = Vec::with_capacity(plan.steps.len());
        let mut piped_stdin: Option<Vec<u8>> = None;
        for step in &plan.steps {
            if cancellation_token.is_cancelled() {
                return Err(SandboxWasiError::Cancelled.into());
            }
            let stdin = step.stdin.clone().or_else(|| piped_stdin.take());
            let outcome = match self
                .run_toolchain_command_with_stdin_policy_output_sink_cwd_env_cancellation_mode(
                    store,
                    toolchain_store,
                    sandbox_id,
                    &step.command_name,
                    step.argv.clone(),
                    stdin,
                    timeout_ms,
                    execution_node_id,
                    policy,
                    output_sink.clone(),
                    step.cwd.clone(),
                    step.env.clone(),
                    cancellation_token.clone(),
                    false,
                    Some(&local_execution_lease),
                ) {
                Ok(outcome) => outcome,
                Err(error) => {
                    return complete_process_outcome(&cancellation_token, Err(error));
                }
            };
            if outcome.response.exit_status != 0 {
                let exit_status = outcome.response.exit_status;
                let stdout = outcome.stdout.clone();
                let stderr = outcome.stderr.clone();
                step_outcomes.push(outcome);
                return complete_process_outcome(
                    &cancellation_token,
                    Ok(SandboxProcessPlanOutcome {
                        step_outcomes,
                        exit_status,
                        stdout,
                        stderr,
                    }),
                );
            }
            if step.pipe_stdout_to_next {
                piped_stdin = Some(outcome.stdout.clone());
            }
            step_outcomes.push(outcome);
        }

        let last = step_outcomes
            .last()
            .expect("non-empty process plan should produce an outcome");
        complete_process_outcome(
            &cancellation_token,
            Ok(SandboxProcessPlanOutcome {
                exit_status: last.response.exit_status,
                stdout: last.stdout.clone(),
                stderr: last.stderr.clone(),
                step_outcomes,
            }),
        )
    }

    pub fn verify_busybox_toolchain_fidelity(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        execution_node_id: &sinomo_core::node::NodeId,
        policy: &SandboxPolicy,
        cases: &[SandboxBusyBoxFidelityCase],
    ) -> Result<Vec<SandboxToolFidelityResult>, SandboxToolFidelityError> {
        let mut results = Vec::new();
        for case in cases {
            let outcome = self.run_toolchain_command_with_stdin_and_policy(
                store,
                toolchain_store,
                sandbox_id,
                case.command,
                case.argv_vec(),
                case.stdin_bytes(),
                SandboxResourceLimits::default().wall_time_ms,
                execution_node_id,
                policy,
            )?;
            let stdout = String::from_utf8_lossy(&outcome.stdout).to_string();
            let stderr = String::from_utf8_lossy(&outcome.stderr).to_string();
            let mut mismatch = None;
            if outcome.response.exit_status != case.expected_exit_status {
                mismatch = Some(format!(
                    "expected exit {}, got {}",
                    case.expected_exit_status, outcome.response.exit_status
                ));
            } else if stdout != case.expected_stdout {
                mismatch = Some(format!(
                    "expected stdout {:?}, got {:?}",
                    case.expected_stdout, stdout
                ));
            }
            results.push(SandboxToolFidelityResult {
                case_name: case.name.to_string(),
                command: case.command.to_string(),
                passed: mismatch.is_none(),
                exit_status: outcome.response.exit_status,
                stdout,
                stderr,
                mismatch,
            });
        }
        let failed = results.iter().filter(|result| !result.passed).count();
        if failed > 0 {
            return Err(SandboxToolFidelityError::Failed {
                failed,
                total: results.len(),
                results,
            });
        }
        Ok(results)
    }

    fn invoke_run_module(
        &self,
        target_abi: SandboxWasiAbi,
        wasm: Vec<u8>,
        command: SandboxCommandRequest,
        options: SandboxExecutorOptions,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxWasiError> {
        let request = SandboxExecutorRequest::RunModule(SandboxExecutorRunModuleRequest {
            target_abi,
            wasm,
            command,
            options,
        });
        match self
            .module
            .invoke_with_output_sink_and_cancellation(
                &request,
                output_sink,
                Some(cancellation_token),
            )
            .map_err(executor_module_error_to_wasi)?
        {
            SandboxExecutorResponse::RunModule(response) => Ok(response.outcome),
            SandboxExecutorResponse::Error(error) => Err(executor_error_to_wasi(error)),
            other => Err(SandboxWasiError::Runtime {
                detail: format!("sandbox executor returned unexpected response {other:?}"),
            }),
        }
    }
}

fn execution_was_cancelled<T>(result: &Result<T, SandboxCommandExecutionError>) -> bool {
    matches!(
        result,
        Err(SandboxCommandExecutionError::Wasi(
            SandboxWasiError::Cancelled | SandboxWasiError::CancelledCleanupFailed { .. }
        ))
    )
}

fn transition_after_native_return<T>(
    cancellation_token: &SandboxCancellationToken,
    complete_outcome: bool,
    result: Result<T, SandboxWasiError>,
) -> Result<T, SandboxWasiError> {
    if !complete_outcome || matches!(&result, Err(SandboxWasiError::Cancelled)) {
        return result;
    }
    if cancellation_token.begin_completing() {
        result
    } else {
        Err(SandboxWasiError::Cancelled)
    }
}

fn complete_process_outcome<T>(
    cancellation_token: &SandboxCancellationToken,
    result: Result<T, SandboxCommandExecutionError>,
) -> Result<T, SandboxCommandExecutionError> {
    if execution_was_cancelled(&result) {
        return result;
    }
    if !cancellation_token.begin_completing() {
        return Err(SandboxWasiError::Cancelled.into());
    }
    cancellation_token.complete();
    result
}

fn cancellation_checkpoint(
    cancellation_token: &SandboxCancellationToken,
) -> Result<(), SandboxCommandExecutionError> {
    if cancellation_token.is_cancelled() {
        Err(SandboxWasiError::Cancelled.into())
    } else {
        Ok(())
    }
}

fn recover_pending_daemonless_execution(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    sandbox_id: &SandboxId,
) -> Result<(), SandboxCommandExecutionError> {
    recover_sandbox_local_execution_journal_with_lease(
        store,
        lease,
        sandbox_id,
        OffsetDateTime::now_utc(),
    )?;
    Ok(())
}

fn persist_daemonless_journal_audit(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &SandboxLocalExecutionJournal,
) -> Result<(), SandboxCommandExecutionError> {
    persist_sandbox_local_execution_audit(store, lease, journal)?;
    Ok(())
}

fn finish_daemonless_journal(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &mut SandboxLocalExecutionJournal,
) -> Result<(), SandboxCommandExecutionError> {
    journal.record_cleanup_failure(
        SandboxLocalExecutionCleanupStage::MarkerDelete,
        "terminal cleanup is removing the durable local execution marker",
        OffsetDateTime::now_utc(),
    );
    write_sandbox_local_execution_journal(store, lease, journal)?;
    remove_sandbox_local_execution_journal(store, lease, &journal.sandbox_id)?;
    Ok(())
}

fn settle_daemonless_outcome(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &mut Option<SandboxLocalExecutionJournal>,
    outcome: SandboxLocalExecutionOutcome,
    command_record: SandboxCommandRecord,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) -> Result<(), SandboxCommandExecutionError> {
    let Some(journal) = journal.as_mut() else {
        return Ok(());
    };
    settle_sandbox_local_execution_journal(
        store,
        lease,
        journal,
        outcome,
        command_record,
        stdout,
        stderr,
        OffsetDateTime::now_utc(),
    )?;
    Ok(())
}

fn complete_daemonless_outcome(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &mut Option<SandboxLocalExecutionJournal>,
) -> Result<(), SandboxCommandExecutionError> {
    let Some(journal) = journal.as_mut() else {
        return Ok(());
    };
    if let Err(error) = persist_daemonless_journal_audit(store, lease, journal) {
        journal.record_cleanup_failure(
            SandboxLocalExecutionCleanupStage::AuditRecord,
            error.to_string(),
            OffsetDateTime::now_utc(),
        );
        write_sandbox_local_execution_journal(store, lease, journal)?;
        return Err(error);
    }
    journal.mark_audit_completed(OffsetDateTime::now_utc());
    write_sandbox_local_execution_journal(store, lease, journal)?;
    finish_daemonless_journal(store, lease, journal)
}

pub fn run_sandbox_wasm_module_replay_fixture_with_active_module(
    data_dir_root: impl AsRef<Path>,
    fixture: &sinomo_module::ModuleReplayFixture,
) -> Result<sinomo_module::ModuleReplayReport, SandboxCommandExecutionError> {
    let runner = SandboxExecutorCommandRunner::from_active_module(data_dir_root.as_ref())?;
    let response = runner
        .module
        .invoke(&SandboxExecutorRequest::RunReplayFixture(
            SandboxExecutorRunReplayFixtureRequest {
                data_dir_root: data_dir_root.as_ref().to_path_buf(),
                fixture: fixture.clone(),
            },
        ))
        .map_err(executor_module_error_to_wasi)?;
    match response {
        SandboxExecutorResponse::RunReplayFixture(response) => Ok(response.report),
        SandboxExecutorResponse::Error(error) => Err(executor_error_to_wasi(error).into()),
        other => Err(SandboxWasiError::Runtime {
            detail: format!("sandbox executor returned unexpected replay response {other:?}"),
        }
        .into()),
    }
}

fn local_staged_sandbox_wasix_options(
    layout: &LocalSandboxInstanceLayout,
    limits: &SandboxResourceLimits,
    extra_preopens: Vec<SandboxExecutorHostPreopen>,
) -> SandboxExecutorOptions {
    let mut preopened_dirs = vec![
        local_staged_sandbox_preopen(&layout.root_dir, "/root", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.home, "/home", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.tmp, "/tmp", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.work, "/work", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.var, "/var", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.logs, "/logs", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.artifacts, "/artifacts", SandboxMountMode::ReadWrite),
    ];
    preopened_dirs.extend(extra_preopens);
    SandboxExecutorOptions {
        preopened_dirs,
        enable_virtual_devices: true,
        zero_device_bytes: SANDBOX_WASI_ZERO_DEVICE_BYTES as u64,
        memory_limit_bytes: limits.memory_mb.saturating_mul(1024 * 1024),
        input_limit_bytes: limits.max_input_bytes,
        output_limit_bytes: limits.max_output_bytes,
        workspace_limit_bytes: limits.max_workspace_bytes,
        file_count_limit: limits.max_file_count,
        ..SandboxExecutorOptions::default()
    }
}

fn local_staged_sandbox_preopen(
    host_path: &Path,
    guest_path: &str,
    mode: SandboxMountMode,
) -> SandboxExecutorHostPreopen {
    SandboxExecutorHostPreopen {
        host_path: host_path.to_path_buf(),
        guest_path: SandboxPath::new(guest_path).expect("static sandbox path"),
        mode,
    }
}

fn load_local_toolchain_artifacts(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .artifacts
        .keys()
        .map(|relative_path| {
            store
                .load_artifact(manifest, relative_path)
                .map(|bytes| (relative_path.clone(), bytes))
        })
        .collect()
}

fn load_local_toolchain_commands(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .commands
        .keys()
        .map(|command_name| {
            store
                .load_command_module(manifest, command_name)
                .map(|bytes| (command_name.clone(), bytes))
        })
        .collect()
}

fn stage_toolchain_runtime_to_local(
    layout: &LocalSandboxInstanceLayout,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<Vec<SandboxExecutorHostPreopen>, SandboxCommandExecutionError> {
    let toolchain_root = ensure_local_toolchain_runtime_cache(layout, artifacts, commands)?;
    let mut preopens = Vec::new();
    if toolchain_root.exists() {
        let mut entries = fs::read_dir(&toolchain_root)
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "list toolchain artifact stage {}: {source}",
                    toolchain_root.display()
                ),
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "read toolchain artifact stage {}: {source}",
                    toolchain_root.display()
                ),
            })?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let file_type = entry
                .file_type()
                .map_err(|source| SandboxFsError::Unavailable {
                    reason: format!(
                        "inspect toolchain artifact stage entry {}: {source}",
                        entry.path().display()
                    ),
                })?;
            if !file_type.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            validate_toolchain_stage_top_level_name(&name)?;
            preopens.push(SandboxExecutorHostPreopen {
                host_path: entry.path(),
                guest_path: SandboxPath::new(format!("/{name}")).map_err(|source| {
                    SandboxFsError::Unavailable {
                        reason: format!(
                            "invalid staged toolchain top-level path `{name}`: {source}"
                        ),
                    }
                })?,
                mode: SandboxMountMode::ReadOnly,
            });
        }
    }
    Ok(preopens)
}

fn ensure_local_toolchain_runtime_cache(
    layout: &LocalSandboxInstanceLayout,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<PathBuf, SandboxCommandExecutionError> {
    let cache_root = layout.toolchain_runtime_cache_root();
    let cache_key = local_toolchain_runtime_cache_key(artifacts, commands)?;
    let cache_manifest = local_toolchain_runtime_cache_manifest(&cache_key, artifacts, commands)?;
    let toolchain_root = cache_root.join(&cache_key);
    let complete_marker = toolchain_root.join(".complete");
    if complete_marker.is_file()
        && local_toolchain_runtime_cache_is_complete(
            &toolchain_root,
            &cache_manifest,
            artifacts,
            commands,
        )?
    {
        return Ok(toolchain_root);
    }

    fs::create_dir_all(&cache_root).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "create toolchain runtime cache {}: {source}",
            cache_root.display()
        ),
    })?;

    if toolchain_root.exists() {
        reject_symlink_chain(
            &toolchain_root,
            &SandboxPath::new("/toolchain").expect("static path"),
        )?;
        fs::remove_dir_all(&toolchain_root).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "clear incomplete toolchain runtime cache {}: {source}",
                toolchain_root.display()
            ),
        })?;
    }

    let temp_root = cache_root.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    if temp_root.exists() {
        fs::remove_dir_all(&temp_root).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "clear temporary toolchain runtime cache {}: {source}",
                temp_root.display()
            ),
        })?;
    }
    fs::create_dir_all(&temp_root).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "create temporary toolchain runtime cache {}: {source}",
            temp_root.display()
        ),
    })?;

    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        let host_path = temp_root.join("bin").join(command_name);
        if let Some(parent) = host_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "create toolchain command cache parent {}: {source}",
                    parent.display()
                ),
            })?;
        }
        fs::write(&host_path, bytes).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain command cache {}: {source}",
                host_path.display()
            ),
        })?;
        set_staged_toolchain_command_executable(&host_path)?;
    }
    for (relative_path, bytes) in artifacts {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        let host_path = temp_root.join(&relative_path);
        if let Some(parent) = host_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "create toolchain artifact cache parent {}: {source}",
                    parent.display()
                ),
            })?;
        }
        if host_path.exists() {
            fs::remove_file(&host_path).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "replace toolchain artifact cache {}: {source}",
                    host_path.display()
                ),
            })?;
        }
        fs::write(&host_path, bytes).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain artifact cache {}: {source}",
                host_path.display()
            ),
        })?;
        if relative_path.starts_with("bin/") {
            set_staged_toolchain_command_executable(&host_path)?;
        }
    }
    fs::write(temp_root.join(".complete"), &cache_manifest).map_err(|source| {
        SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain runtime cache marker {}: {source}",
                temp_root.join(".complete").display()
            ),
        }
    })?;

    match fs::rename(&temp_root, &toolchain_root) {
        Ok(()) => Ok(toolchain_root),
        Err(source) if toolchain_root.join(".complete").is_file() => {
            let _ = fs::remove_dir_all(&temp_root);
            let _ = source;
            Ok(toolchain_root)
        }
        Err(source) => Err(SandboxFsError::Unavailable {
            reason: format!(
                "publish toolchain runtime cache {}: {source}",
                toolchain_root.display()
            ),
        }
        .into()),
    }
}

fn local_toolchain_runtime_cache_is_complete(
    toolchain_root: &Path,
    expected_manifest: &str,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<bool, SandboxCommandExecutionError> {
    match fs::read_to_string(toolchain_root.join(".complete")) {
        Ok(actual_manifest) if actual_manifest == expected_manifest => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(SandboxFsError::Unavailable {
                reason: format!(
                    "read toolchain runtime cache marker {}: {source}",
                    toolchain_root.join(".complete").display()
                ),
            }
            .into());
        }
    }

    let mut expected_files = BTreeMap::new();
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        expected_files.insert(format!("bin/{command_name}"), bytes.len() as u64);
        if !local_toolchain_runtime_cache_file_matches(
            &toolchain_root.join("bin").join(command_name),
            bytes.len() as u64,
        )? {
            return Ok(false);
        }
    }
    for (relative_path, bytes) in artifacts {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        expected_files.insert(relative_path.clone(), bytes.len() as u64);
        if !local_toolchain_runtime_cache_file_matches(
            &toolchain_root.join(&relative_path),
            bytes.len() as u64,
        )? {
            return Ok(false);
        }
    }

    let mut actual_files = BTreeSet::new();
    if !collect_local_toolchain_runtime_cache_files(
        toolchain_root,
        toolchain_root,
        &mut actual_files,
    )? {
        return Ok(false);
    }
    Ok(actual_files == expected_files.keys().cloned().collect::<BTreeSet<_>>())
}

fn local_toolchain_runtime_cache_manifest(
    cache_key: &str,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<String, SandboxCommandExecutionError> {
    let mut manifest = String::new();
    manifest.push_str("sinomo-toolchain-runtime-cache-v1\n");
    manifest.push_str("key=");
    manifest.push_str(cache_key);
    manifest.push('\n');

    let mut artifact_entries = Vec::with_capacity(artifacts.len());
    for (relative_path, bytes) in artifacts {
        artifact_entries.push((
            validate_toolchain_artifact_relative_path(relative_path)?,
            bytes.len(),
            sandbox_module_hash(bytes),
        ));
    }
    artifact_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (relative_path, len, hash) in artifact_entries {
        manifest.push_str("artifact\t");
        manifest.push_str(&relative_path);
        manifest.push('\t');
        manifest.push_str(&len.to_string());
        manifest.push('\t');
        manifest.push_str(&hash);
        manifest.push('\n');
    }

    let mut command_entries = Vec::with_capacity(commands.len());
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        command_entries.push((
            command_name.clone(),
            bytes.len(),
            sandbox_module_hash(bytes),
        ));
    }
    command_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (command_name, len, hash) in command_entries {
        manifest.push_str("command\t");
        manifest.push_str(&command_name);
        manifest.push('\t');
        manifest.push_str(&len.to_string());
        manifest.push('\t');
        manifest.push_str(&hash);
        manifest.push('\n');
    }

    Ok(manifest)
}

fn collect_local_toolchain_runtime_cache_files(
    root: &Path,
    path: &Path,
    files: &mut BTreeSet<String>,
) -> Result<bool, SandboxCommandExecutionError> {
    let mut entry_count = 0usize;
    for entry in fs::read_dir(path).map_err(|source| SandboxFsError::Unavailable {
        reason: format!("list toolchain runtime cache {}: {source}", path.display()),
    })? {
        let entry = entry.map_err(|source| SandboxFsError::Unavailable {
            reason: format!("read toolchain runtime cache {}: {source}", path.display()),
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if path == root && name == ".complete" {
            continue;
        }
        entry_count += 1;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "inspect toolchain runtime cache entry {}: {source}",
                    entry.path().display()
                ),
            })?;
        if metadata.file_type().is_symlink() {
            return Err(SandboxFsError::Path(SandboxPathError::EscapesRoot {
                path: entry.path().display().to_string(),
            })
            .into());
        }
        if metadata.is_dir() {
            if !collect_local_toolchain_runtime_cache_files(root, &entry.path(), files)? {
                return Ok(false);
            }
            continue;
        }
        if !metadata.is_file() {
            return Err(SandboxFsError::Unsupported {
                operation: format!(
                    "inspect special toolchain runtime cache entry {}",
                    entry.path().display()
                ),
            }
            .into());
        }
        let relative_path = entry
            .path()
            .strip_prefix(root)
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "derive toolchain runtime cache relative path {}: {source}",
                    entry.path().display()
                ),
            })?
            .to_string_lossy()
            .replace('\\', "/");
        files.insert(relative_path);
    }
    Ok(path == root || entry_count > 0)
}

fn local_toolchain_runtime_cache_file_matches(
    path: &Path,
    expected_len: u64,
) -> Result<bool, SandboxCommandExecutionError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(SandboxFsError::Unavailable {
                reason: format!(
                    "inspect toolchain runtime cache file {}: {source}",
                    path.display()
                ),
            }
            .into());
        }
    };
    Ok(metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() == expected_len)
}

fn local_toolchain_runtime_cache_key(
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<String, SandboxCommandExecutionError> {
    let mut hasher = SandboxModuleHasher::new();
    hasher.update(b"sinomo-toolchain-runtime-cache-v1\n");
    let mut artifact_entries = Vec::with_capacity(artifacts.len());
    for (relative_path, bytes) in artifacts {
        artifact_entries.push((
            validate_toolchain_artifact_relative_path(relative_path)?,
            bytes,
        ));
    }
    artifact_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (relative_path, bytes) in artifact_entries {
        hasher.update(b"artifact\0");
        hasher.update(relative_path.as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes.len().to_string().as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes);
        hasher.update(b"\n");
    }

    let mut command_entries = Vec::with_capacity(commands.len());
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        command_entries.push((command_name, bytes));
    }
    command_entries.sort_by(|left, right| left.0.cmp(right.0));
    for (command_name, bytes) in command_entries {
        hasher.update(b"command\0");
        hasher.update(command_name.as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes.len().to_string().as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes);
        hasher.update(b"\n");
    }
    Ok(hasher
        .finalize()
        .strip_prefix("blake3:")
        .unwrap_or("invalid")
        .to_string())
}

fn validate_toolchain_stage_top_level_name(name: &str) -> Result<(), SandboxCommandExecutionError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(SandboxFsError::Unavailable {
            reason: format!("invalid staged toolchain top-level path `{name}`"),
        }
        .into());
    }
    if matches!(
        name,
        "artifacts"
            | "dev"
            | "etc"
            | "home"
            | "logs"
            | "opt"
            | "proc"
            | "root"
            | "run"
            | "sys"
            | "tmp"
            | "var"
            | "work"
    ) {
        return Err(SandboxFsError::Unavailable {
            reason: format!(
                "staged toolchain top-level path `{name}` would shadow a sandbox filesystem mount"
            ),
        }
        .into());
    }
    Ok(())
}

#[cfg(unix)]
fn set_staged_toolchain_command_executable(
    host_path: &Path,
) -> Result<(), SandboxCommandExecutionError> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(host_path)
        .map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "inspect staged toolchain command {}: {source}",
                host_path.display()
            ),
        })?
        .permissions();
    permissions.set_mode(0o555);
    fs::set_permissions(host_path, permissions).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "mark staged toolchain command executable {}: {source}",
            host_path.display()
        ),
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn set_staged_toolchain_command_executable(
    _host_path: &Path,
) -> Result<(), SandboxCommandExecutionError> {
    Ok(())
}

fn command_overrides_for_toolchain(
    toolchain: &SandboxToolchainManifest,
) -> Vec<SandboxExecutorCommandOverride> {
    toolchain
        .commands
        .values()
        .filter(|command| {
            command.effective_target_abi(toolchain) == SandboxWasiAbi::Wasix
                && (!command.argv_prefix.is_empty() || command.exec_name.is_some())
        })
        .map(|command| SandboxExecutorCommandOverride {
            name: command.name.clone(),
            argv_prefix: command.argv_prefix.clone(),
            exec_name: command.exec_name.clone(),
        })
        .collect()
}

fn executor_module_error_to_wasi(error: SandboxExecutorClientError) -> SandboxWasiError {
    SandboxWasiError::Runtime {
        detail: error.to_string(),
    }
}

fn executor_error_to_wasi(error: SandboxExecutorErrorResponse) -> SandboxWasiError {
    if matches!(
        error.code.as_str(),
        "run-module" | "run-toolchain" | "run-replay"
    ) && error.detail == SandboxWasiError::Cancelled.to_string()
    {
        return SandboxWasiError::Cancelled;
    }
    SandboxWasiError::Runtime {
        detail: format!("{}: {}", error.code, error.detail),
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[test]
    fn cancellation_remains_structured_for_local_execution() {
        let outcome: Result<(), SandboxCommandExecutionError> =
            Err(SandboxWasiError::Cancelled.into());
        assert!(execution_was_cancelled(&outcome));
    }

    #[test]
    fn process_plan_preserves_cancelled_cleanup_failure_for_caller_recovery() {
        let token = SandboxCancellationToken::new();
        token.cancel();
        let result: Result<(), SandboxCommandExecutionError> =
            Err(SandboxWasiError::CancelledCleanupFailed {
                detail: "durable local audit cleanup remains pending".to_string(),
            }
            .into());

        let error = complete_process_outcome(&token, result)
            .expect_err("process plan cancellation remains structured");
        assert!(matches!(
            error,
            SandboxCommandExecutionError::Wasi(SandboxWasiError::CancelledCleanupFailed {
                detail
            }) if detail == "durable local audit cleanup remains pending"
        ));
    }

    #[test]
    fn two_step_process_keeps_rpc_cancellation_active_until_plan_finalization() {
        let token = SandboxCancellationToken::new();

        assert_eq!(
            transition_after_native_return(&token, false, Ok::<_, SandboxWasiError>(1))
                .expect("first nested step"),
            1
        );
        assert!(!token.is_cancelled());
        assert!(!token.is_completing_or_completed());
        assert_eq!(
            transition_after_native_return(&token, false, Ok::<_, SandboxWasiError>(2))
                .expect("second nested step"),
            2
        );
        assert_eq!(
            complete_process_outcome(&token, Ok::<_, SandboxCommandExecutionError>(vec![1, 2]),)
                .expect("complete process plan"),
            vec![1, 2]
        );
        assert!(token.is_completing_or_completed());
        assert!(!token.cancel(), "late cancellation must not win");
    }

    #[test]
    fn non_cancel_native_error_enters_finalization_before_audit() {
        let token = SandboxCancellationToken::new();
        let error = transition_after_native_return::<()>(
            &token,
            true,
            Err(SandboxWasiError::Runtime {
                detail: "guest failed".to_string(),
            }),
        )
        .expect_err("native error");

        assert!(matches!(error, SandboxWasiError::Runtime { .. }));
        assert!(token.is_completing_or_completed());
        assert!(!token.cancel(), "error audit finalization owns the token");
    }
}
