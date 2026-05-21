use crate::internal::*;

use super::staging::*;

pub struct SifsStagedSandboxCommandOutcome<T> {
    pub dfs: NetworkedDfsService<T>,
    pub command: Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError>,
}

pub struct SifsStagedSandboxProcessPlanOutcome<T> {
    pub dfs: NetworkedDfsService<T>,
    pub process: Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError>,
}

#[derive(Clone)]
pub struct SifsStagedSandboxCommandRunner {
    pub(super) local_runner: LocalStagedSandboxCommandRunner,
    pub(super) replica_candidates: Vec<NodeId>,
}

impl SifsStagedSandboxCommandRunner {
    pub fn new() -> Result<Self, SandboxWasiError> {
        Ok(Self {
            local_runner: LocalStagedSandboxCommandRunner::new()?,
            replica_candidates: Vec::new(),
        })
    }

    pub fn with_replica_candidates(mut self, replica_candidates: Vec<NodeId>) -> Self {
        self.replica_candidates = replica_candidates;
        self.replica_candidates.sort();
        self.replica_candidates.dedup();
        self
    }

    pub(super) fn sifs_instance_store<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
    ) -> SifsSandboxInstanceStore<T> {
        let store = SifsSandboxInstanceStore::new(dfs);
        if self.replica_candidates.is_empty() {
            store
        } else {
            store.with_replica_candidates(self.replica_candidates.clone())
        }
    }

    pub(super) fn sifs_filesystem<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        manifest: &SandboxInstanceManifest,
    ) -> SifsSandboxFileSystem<T> {
        let fs = SifsSandboxFileSystem::new(dfs, manifest);
        if self.replica_candidates.is_empty() {
            fs
        } else {
            fs.with_replica_candidates(self.replica_candidates.clone())
        }
    }

    pub fn run_module<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        timeout_ms: u64,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        self.run_module_with_stdin(dfs, local_store, sandbox_id, wasm, argv, None, timeout_ms)
    }

    pub fn run_module_with_stdin<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        self.run_module_with_stdin_and_limits(
            dfs,
            local_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            None,
        )
    }

    pub fn run_module_with_stdin_and_limits<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_and_output_sink(
            dfs,
            local_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            None,
        )
    }

    pub fn run_module_with_stdin_limits_and_output_sink<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_output_sink_and_abi(
            dfs,
            local_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            output_sink,
            default_sandbox_wasi_abi(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_module_with_stdin_limits_output_sink_and_abi<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        target_abi: SandboxWasiAbi,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        self.run_module_with_stdin_limits_output_sink_abi_and_artifacts(
            dfs,
            local_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            output_sink,
            target_abi,
            Vec::new(),
            Vec::new(),
            BTreeMap::new(),
            None,
            BTreeMap::new(),
            false,
            true,
            true,
            true,
            true,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_module_with_stdin_limits_output_sink_abi_and_artifacts<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        target_abi: SandboxWasiAbi,
        toolchain_artifacts: Vec<(String, Vec<u8>)>,
        toolchain_commands: Vec<(String, Vec<u8>)>,
        default_env: BTreeMap<String, String>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        allow_preview1_pinned_subprocesses: bool,
        wasix_pinned_child_exec_only: bool,
        wasix_instruction_metering: bool,
        wasix_auto_asyncify: bool,
        wasix_fpcast_emu: bool,
        wasix_command_overrides: Vec<SandboxWasixCommandOverride>,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        let mut sifs_store = self.sifs_instance_store(dfs);
        let execution_node_id = sifs_store.dfs.node_id().clone();
        let mut manifest = sifs_store.load(sandbox_id)?;
        manifest.last_placement = Some(sandbox_execution_placement_for_node(
            &manifest,
            &execution_node_id,
        )?);
        sifs_store.save_manifest(&manifest)?;
        reset_local_staging_instance(local_store, &manifest)?;

        let sifs_fs = self.sifs_filesystem(sifs_store.into_inner(), &manifest);
        let (_local_manifest, local_layout) = local_store.load(sandbox_id)?;
        let mut local_fs = LocalSandboxFileSystem::new(local_layout.clone());
        sync_sandbox_workspace_roots(&sifs_fs, &mut local_fs)?;

        let mut dfs = sifs_fs.into_inner();
        let mut extra_preopens = stage_toolchain_runtime_to_local(
            &local_layout,
            &toolchain_artifacts,
            &toolchain_commands,
        )?;
        extra_preopens.extend(stage_sifs_mounts_to_local(
            &mut dfs,
            &manifest,
            &local_layout,
        )?);
        let command = self
            .local_runner
            .run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
                local_store,
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
                wasix_auto_asyncify,
                wasix_fpcast_emu,
                wasix_command_overrides,
            );
        sync_rw_sifs_mounts_from_local(
            &mut dfs,
            &manifest,
            &local_layout,
            &self.replica_candidates,
        )?;

        let mut sifs_fs = self.sifs_filesystem(dfs, &manifest);
        let (_local_manifest, local_layout) = local_store.load(sandbox_id)?;
        let local_fs = LocalSandboxFileSystem::new(local_layout);
        if let Ok(outcome) = &command {
            sync_sandbox_changed_paths(&local_fs, &mut sifs_fs, &outcome.response.changed_paths)?;
            let logs_path = SandboxPath::new("/logs").expect("static logs path");
            sync_sandbox_tree(&local_fs, &mut sifs_fs, &logs_path)?;
        } else {
            sync_sandbox_tree(
                &local_fs,
                &mut sifs_fs,
                &SandboxPath::new("/logs").expect("static logs path"),
            )?;
        }
        let (mut local_manifest, _) = local_store.load(sandbox_id)?;
        local_manifest.last_placement = manifest.last_placement;
        let mut sifs_store = self.sifs_instance_store(sifs_fs.into_inner());
        sifs_store.save_manifest(&local_manifest)?;

        Ok(SifsStagedSandboxCommandOutcome {
            dfs: sifs_store.into_inner(),
            command,
        })
    }

    pub fn run_module_with_policy<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        timeout_ms: u64,
        policy: &SandboxPolicy,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        self.run_module_with_stdin_and_policy(
            dfs,
            local_store,
            sandbox_id,
            wasm,
            argv,
            None,
            timeout_ms,
            policy,
        )
    }

    pub fn run_toolchain_command_with_stdin_and_policy<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        policy: &SandboxPolicy,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_and_output_sink(
            dfs,
            local_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            policy,
            None,
        )
    }

    pub fn run_toolchain_command_with_stdin_policy_and_output_sink<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
            dfs,
            local_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            policy,
            output_sink,
            None,
            BTreeMap::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env<
        T: NetworkTransport,
    >(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        validate_sandbox_toolchain_argv(command_name, &argv).map_err(|detail| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { detail })
        })?;
        let execution_node_id = dfs.node_id().clone();
        let sifs_store = self.sifs_instance_store(dfs);
        let manifest = sifs_store.load(sandbox_id)?;
        let toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
        let toolchain = toolchain_store.load_manifest(&manifest.toolchain_id)?;
        let command = toolchain.command(command_name)?;
        policy
            .validate_invocation(&manifest, &toolchain, command, &execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        let limits = effective_sandbox_command_limits(&manifest.limits, &command.default_limits);
        let timeout_ms = effective_sandbox_timeout_ms(timeout_ms, limits.wall_time_ms);
        let wasm = toolchain_store.load_command_module(&toolchain, command_name)?;
        let argv = command.effective_argv(argv);
        let toolchain_artifacts = load_sifs_toolchain_artifacts(&toolchain_store, &toolchain)?;
        let toolchain_commands = load_sifs_toolchain_commands(&toolchain_store, &toolchain)?;
        let wasix_pinned_child_exec_only = !command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY);
        let wasix_instruction_metering = false;
        let wasix_auto_asyncify = command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY);
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
        self.run_module_with_stdin_limits_output_sink_abi_and_artifacts(
            toolchain_store.into_inner(),
            local_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            Some(limits),
            output_sink,
            command.effective_target_abi(&toolchain),
            toolchain_artifacts,
            toolchain_commands,
            command.default_env.clone(),
            cwd_override,
            env_override,
            command
                .required_capabilities
                .iter()
                .any(|capability| capability == SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY),
            wasix_pinned_child_exec_only,
            wasix_instruction_metering,
            wasix_auto_asyncify,
            false,
            wasix_command_overrides,
        )
    }

    pub fn run_module_with_stdin_and_policy<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        policy: &SandboxPolicy,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        let execution_node_id = dfs.node_id().clone();
        let sifs_store = self.sifs_instance_store(dfs);
        let manifest = sifs_store.load(sandbox_id)?;
        policy
            .validate_manifest(&manifest, &execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        self.run_module_with_stdin_and_limits(
            sifs_store.into_inner(),
            local_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            None,
        )
    }
}
