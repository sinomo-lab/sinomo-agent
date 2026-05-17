use crate::internal::*;

use super::staging::*;

#[derive(Clone)]
pub struct LocalStagedSandboxCommandRunner {
    runner: LocalWasiCommandRunner,
}

impl LocalStagedSandboxCommandRunner {
    pub fn new() -> Result<Self, SandboxWasiError> {
        Ok(Self {
            runner: LocalWasiCommandRunner::new()?,
        })
    }

    pub fn run_module(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        timeout_ms: u64,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin(store, sandbox_id, wasm, argv, None, timeout_ms)
    }

    pub fn run_module_with_stdin(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin_and_limits(
            store, sandbox_id, wasm, argv, stdin, timeout_ms, None,
        )
    }

    pub fn run_module_with_stdin_and_limits(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_and_preopens(
            store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            Vec::new(),
        )
    }

    pub fn run_module_with_stdin_limits_and_preopens(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        extra_preopens: Vec<SandboxWasiPreopen>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_preopens_and_output_sink(
            store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            extra_preopens,
            None,
        )
    }

    pub fn run_module_with_stdin_limits_preopens_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        extra_preopens: Vec<SandboxWasiPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
            store,
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
            false,
            true,
            true,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_module_with_stdin_limits_preopens_output_sink_and_abi(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        extra_preopens: Vec<SandboxWasiPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        target_abi: SandboxWasiAbi,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
            store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            extra_preopens,
            output_sink,
            target_abi,
            BTreeMap::new(),
            None,
            BTreeMap::new(),
            false,
            true,
            true,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        extra_preopens: Vec<SandboxWasiPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        target_abi: SandboxWasiAbi,
        default_env: BTreeMap<String, String>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        allow_preview1_pinned_subprocesses: bool,
        wasix_pinned_child_exec_only: bool,
        wasix_instruction_metering: bool,
        wasix_command_overrides: Vec<SandboxWasixCommandOverride>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_preopens_output_sink_abi_env_and_pipes(
            store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            extra_preopens,
            output_sink,
            target_abi,
            default_env,
            cwd_override,
            env_override,
            allow_preview1_pinned_subprocesses,
            wasix_pinned_child_exec_only,
            wasix_instruction_metering,
            wasix_command_overrides,
            #[cfg(feature = "wasix-runtime")]
            SandboxWasixPipeOptions::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_module_with_stdin_limits_preopens_output_sink_abi_env_and_pipes(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        extra_preopens: Vec<SandboxWasiPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        target_abi: SandboxWasiAbi,
        default_env: BTreeMap<String, String>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        allow_preview1_pinned_subprocesses: bool,
        wasix_pinned_child_exec_only: bool,
        wasix_instruction_metering: bool,
        wasix_command_overrides: Vec<SandboxWasixCommandOverride>,
        #[cfg(feature = "wasix-runtime")] wasix_pipes: SandboxWasixPipeOptions,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let (manifest, layout) = store.load(sandbox_id)?;
        let limits = limits.unwrap_or_else(|| manifest.limits.clone());
        let effective_cwd = cwd_override.unwrap_or_else(|| manifest.cwd.clone());
        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        sandbox_fs.validate_resource_limits(&limits)?;
        validate_sandbox_working_directory(&sandbox_fs, &effective_cwd)?;
        let working_directory_host_path = sandbox_fs.host_path(&effective_cwd)?;
        let before = sandbox_fs.snapshot()?;
        let rollback_checkpoint = store.create_checkpoint(sandbox_id, OffsetDateTime::now_utc())?;
        let started_at = OffsetDateTime::now_utc();

        let mut wasi_options = local_staged_sandbox_wasi_options_with_limits_and_preopens(
            &layout,
            &limits,
            extra_preopens,
        );
        wasi_options.allow_preview1_pinned_subprocesses = allow_preview1_pinned_subprocesses;
        wasi_options.wasix_pinned_child_exec_only = wasix_pinned_child_exec_only;
        wasi_options.wasix_instruction_metering = wasix_instruction_metering;
        wasi_options.wasix_command_overrides = wasix_command_overrides;
        #[cfg(feature = "wasix-runtime")]
        {
            wasi_options.wasix_stdin_pipe = wasix_pipes.stdin_pipe;
            wasi_options.wasix_stdout_pipe = wasix_pipes.stdout_pipe;
        }
        wasi_options.working_directory_host_path = Some(working_directory_host_path);
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
        let mut outcome = match self.run_module_for_abi(
            target_abi,
            wasm.as_ref(),
            request.clone(),
            wasi_options.with_output_sink(output_sink),
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                store.resume_checkpoint(
                    sandbox_id,
                    &rollback_checkpoint.checkpoint_id,
                    OffsetDateTime::now_utc(),
                )?;
                let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
                let command_id = SandboxCommandId::generate();
                let completed_at = OffsetDateTime::now_utc();
                let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
                persist_command_record(
                    &mut sandbox_fs,
                    &SandboxCommandRecord {
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
                    },
                )?;
                record_local_command_history(store, sandbox_id, &command_id, completed_at)?;
                return Err(error.into());
            }
        };

        let after = sandbox_fs.snapshot()?;
        if let Err(error) = validate_workspace_usage(&after.workspace_usage(), &limits) {
            store.resume_checkpoint(
                sandbox_id,
                &rollback_checkpoint.checkpoint_id,
                OffsetDateTime::now_utc(),
            )?;
            let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
            let completed_at = OffsetDateTime::now_utc();
            let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
            persist_command_record(
                &mut sandbox_fs,
                &SandboxCommandRecord {
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
                },
            )?;
            record_local_command_history(
                store,
                sandbox_id,
                &outcome.response.command_id,
                completed_at,
            )?;
            return Err(error.into());
        }

        let written_bytes = after.written_bytes_since(&before);
        if let Err(error) = validate_written_bytes(written_bytes, &limits) {
            store.resume_checkpoint(
                sandbox_id,
                &rollback_checkpoint.checkpoint_id,
                OffsetDateTime::now_utc(),
            )?;
            let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
            let mut response = outcome.response.clone();
            response.resource_usage.written_bytes = written_bytes;
            let completed_at = OffsetDateTime::now_utc();
            let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
            persist_command_record(
                &mut sandbox_fs,
                &SandboxCommandRecord {
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
                },
            )?;
            record_local_command_history(
                store,
                sandbox_id,
                &outcome.response.command_id,
                completed_at,
            )?;
            return Err(error.into());
        }

        let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
        outcome.response.changed_paths = sandbox_fs.changed_paths_since(&before)?;
        outcome.response.resource_usage.written_bytes = written_bytes;
        let completed_at = OffsetDateTime::now_utc();

        let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
        persist_command_outputs(
            &mut sandbox_fs,
            &SandboxCommandRecord {
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
            },
            &outcome.stdout,
            &outcome.stderr,
        )?;
        record_local_command_history(
            store,
            sandbox_id,
            &outcome.response.command_id,
            completed_at,
        )?;
        Ok(outcome)
    }

    pub(super) fn run_module_for_abi(
        &self,
        target_abi: SandboxWasiAbi,
        wasm: &[u8],
        request: SandboxCommandRequest,
        options: SandboxWasiRunOptions,
    ) -> Result<SandboxLocalCommandOutcome, SandboxWasiError> {
        match target_abi {
            SandboxWasiAbi::Preview1 => self.runner.run_module(wasm, request, options),
            SandboxWasiAbi::Wasix => runtime::run_wasix_module(wasm, request, options),
        }
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
        execution_node_id: &NodeId,
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
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
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
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_output_sink_cwd_env_and_pipes(
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
            #[cfg(feature = "wasix-runtime")]
            SandboxWasixPipeOptions::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_toolchain_command_with_stdin_policy_output_sink_cwd_env_and_pipes(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        #[cfg(feature = "wasix-runtime")] wasix_pipes: SandboxWasixPipeOptions,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        validate_sandbox_toolchain_argv(command_name, &argv).map_err(|detail| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { detail })
        })?;
        let (manifest, _layout) = store.load(sandbox_id)?;
        let (toolchain, _toolchain_layout) =
            toolchain_store.load_manifest(&manifest.toolchain_id)?;
        let command = toolchain.command(command_name)?;
        policy
            .validate_invocation(&manifest, &toolchain, command, execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        let limits = effective_sandbox_command_limits(&manifest.limits, &command.default_limits);
        let timeout_ms = if timeout_ms == 0 {
            limits.wall_time_ms
        } else {
            timeout_ms.min(limits.wall_time_ms)
        };
        let wasm = toolchain_store.load_command_module(&toolchain, command_name)?;
        let argv = command.effective_argv(argv);
        let allow_preview1_pinned_subprocesses = command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY);
        let wasix_pinned_child_exec_only = !command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY);
        let wasix_instruction_metering = true;
        let (_manifest, layout) = store.load(sandbox_id)?;
        let toolchain_artifacts = load_local_toolchain_artifacts(toolchain_store, &toolchain)?;
        let toolchain_commands = load_local_toolchain_commands(toolchain_store, &toolchain)?;
        let artifact_preopens =
            stage_toolchain_runtime_to_local(&layout, &toolchain_artifacts, &toolchain_commands)?;
        let wasix_command_overrides = toolchain
            .commands
            .values()
            .filter(|command| {
                command.effective_target_abi(&toolchain) == SandboxWasiAbi::Wasix
                    && (!command.argv_prefix.is_empty() || command.exec_name.is_some())
            })
            .map(|command| SandboxWasixCommandOverride {
                name: command.name.clone(),
                argv_prefix: command.argv_prefix.clone(),
                exec_name: command.exec_name.clone(),
            })
            .collect::<Vec<_>>();
        self.run_module_with_stdin_limits_preopens_output_sink_abi_env_and_pipes(
            store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            Some(limits),
            artifact_preopens,
            output_sink,
            command.effective_target_abi(&toolchain),
            command.default_env.clone(),
            cwd_override,
            env_override,
            allow_preview1_pinned_subprocesses,
            wasix_pinned_child_exec_only,
            wasix_instruction_metering,
            wasix_command_overrides,
            #[cfg(feature = "wasix-runtime")]
            wasix_pipes,
        )
    }

    pub fn run_module_with_policy(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_module_with_stdin_and_policy(
            store,
            sandbox_id,
            wasm,
            argv,
            None,
            timeout_ms,
            execution_node_id,
            policy,
        )
    }

    pub fn run_module_with_stdin_and_policy(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let (manifest, _layout) = store.load(sandbox_id)?;
        policy
            .validate_manifest(&manifest, execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        self.run_module_with_stdin(store, sandbox_id, wasm, argv, stdin, timeout_ms)
    }
}
