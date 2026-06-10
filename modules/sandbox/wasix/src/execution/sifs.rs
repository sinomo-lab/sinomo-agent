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

    pub(super) fn sifs_instance_store<T: DfsFrameTransport>(
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

    pub(super) fn sifs_filesystem<T: DfsFrameTransport>(
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

    pub fn run_module<T: DfsFrameTransport + Send + 'static>(
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

    pub fn run_module_with_stdin<T: DfsFrameTransport + Send + 'static>(
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

    pub fn run_module_with_stdin_and_limits<T: DfsFrameTransport + Send + 'static>(
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

    pub fn run_module_with_stdin_limits_and_output_sink<T: DfsFrameTransport + Send + 'static>(
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
    pub fn run_module_with_stdin_limits_output_sink_and_abi<
        T: DfsFrameTransport + Send + 'static,
    >(
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
            true,
            true,
            true,
            true,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_module_with_stdin_limits_output_sink_abi_and_artifacts<
        T: DfsFrameTransport + Send + 'static,
    >(
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
        wasix_pinned_child_exec_only: bool,
        wasix_instruction_metering: bool,
        wasix_auto_asyncify: bool,
        wasix_fpcast_emu: bool,
        wasix_command_overrides: Vec<SandboxWasixCommandOverride>,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        if target_abi != SandboxWasiAbi::Wasix {
            return Err(SandboxWasiError::InvalidRequest {
                detail: format!(
                    "SIFS-backed sandbox execution requires WASIX; {target_abi:?} is local-only legacy execution"
                ),
            }
            .into());
        }

        self.run_module_with_direct_sifs_wasix(
            dfs,
            local_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            output_sink,
            toolchain_artifacts,
            toolchain_commands,
            default_env,
            cwd_override,
            env_override,
            wasix_pinned_child_exec_only,
            wasix_instruction_metering,
            wasix_auto_asyncify,
            wasix_fpcast_emu,
            wasix_command_overrides,
        )
    }

    #[cfg(feature = "wasix-runtime")]
    #[allow(clippy::too_many_arguments)]
    fn run_module_with_direct_sifs_wasix<T: DfsFrameTransport + Send + 'static>(
        &self,
        dfs: NetworkedDfsService<T>,
        _local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        toolchain_artifacts: Vec<(String, Vec<u8>)>,
        toolchain_commands: Vec<(String, Vec<u8>)>,
        default_env: BTreeMap<String, String>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
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
        let artifact_preopens =
            stage_toolchain_runtime_to_virtual(&toolchain_artifacts, &toolchain_commands)?;

        let limits = limits.unwrap_or_else(|| manifest.limits.clone());
        let effective_cwd = cwd_override.unwrap_or_else(|| manifest.cwd.clone());
        let mut sifs_fs = self.sifs_filesystem(sifs_store.into_inner(), &manifest);
        sifs_fs.ensure_workspace()?;
        validate_sandbox_working_directory(&sifs_fs, &effective_cwd)?;
        let snapshot_roots = sandbox_snapshot_roots_for_manifest(&manifest);
        let before_snapshot =
            snapshot_sandbox_filesystem_with_roots(&mut sifs_fs, &snapshot_roots)?;
        validate_workspace_usage(&before_snapshot.usage(), &limits)?;

        let sifs_fs = Arc::new(std::sync::Mutex::new(sifs_fs));
        let mut virtual_preopened_dirs = artifact_preopens;
        virtual_preopened_dirs.extend(runtime::wasix_sandbox_workspace_filesystem_preopens(
            Arc::clone(&sifs_fs),
            SandboxMountMode::ReadWrite,
        ));
        for mount in manifest
            .mounts
            .iter()
            .filter(|mount| sifs_manifest_mount_needs_wasix_system_preopen(&mount.mount_path))
        {
            virtual_preopened_dirs.push(runtime::wasix_sandbox_filesystem_preopen(
                mount.mount_path.clone(),
                Arc::clone(&sifs_fs),
                mount.mount_path.clone(),
                mount.mode.clone(),
            ));
        }
        let mut wasi_options = SandboxWasiRunOptions {
            preopened_dirs: Vec::new(),
            #[cfg(feature = "wasix-runtime")]
            virtual_preopened_dirs,
            enable_virtual_devices: true,
            memory_limit_bytes: limits.memory_mb.saturating_mul(1024 * 1024) as usize,
            input_limit_bytes: limits.max_input_bytes as usize,
            output_limit_bytes: limits.max_output_bytes as usize,
            workspace_limit_bytes: limits.max_workspace_bytes,
            file_count_limit: limits.max_file_count,
            ..SandboxWasiRunOptions::default()
        };
        wasi_options.wasix_pinned_child_exec_only = wasix_pinned_child_exec_only;
        wasi_options.wasix_instruction_metering = wasix_instruction_metering;
        wasi_options.wasix_auto_asyncify = wasix_auto_asyncify;
        wasi_options.wasix_fpcast_emu = wasix_fpcast_emu;
        wasi_options.wasix_command_overrides = wasix_command_overrides;

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
        let started_at = OffsetDateTime::now_utc();
        let mut command = self
            .local_runner
            .run_module_for_abi(
                SandboxWasiAbi::Wasix,
                wasm.as_ref(),
                request.clone(),
                wasi_options.with_output_sink(output_sink),
            )
            .map_err(SandboxCommandExecutionError::from);

        let completed_at = OffsetDateTime::now_utc();
        let mut sifs_fs_guard = sifs_fs.lock().map_err(|_| SandboxFsError::Unavailable {
            reason: "SIFS sandbox filesystem lock poisoned".to_string(),
        })?;

        let mut command_history_id = None;
        if let Err(error) = &command {
            restore_sandbox_filesystem_with_roots(
                &mut *sifs_fs_guard,
                &before_snapshot,
                &snapshot_roots,
            )?;
            let command_id = SandboxCommandId::generate();
            command_history_id = Some(command_id.clone());
            persist_command_record(
                &mut *sifs_fs_guard,
                &SandboxCommandRecord {
                    sandbox_id: manifest.sandbox_id.clone(),
                    command_id: command_id.clone(),
                    status: sandbox_command_record_status_for_execution_error(
                        command.as_ref().unwrap_err(),
                    ),
                    workspace_root: manifest.workspace_root.clone(),
                    toolchain_id: manifest.toolchain_id.clone(),
                    argv,
                    cwd: effective_cwd,
                    env: request.env,
                    started_at,
                    completed_at: Some(completed_at),
                    response: None,
                    error: Some(error.to_string()),
                },
            )?;
        } else if let Ok(outcome) = &mut command {
            let after_snapshot =
                snapshot_sandbox_filesystem_with_roots(&mut *sifs_fs_guard, &snapshot_roots)?;
            let changed_paths = after_snapshot.changed_paths_since(&before_snapshot);
            let written_bytes = after_snapshot.written_bytes_since(&before_snapshot);
            if let Err(error) = validate_workspace_usage(&after_snapshot.usage(), &limits)
                .and_then(|_| validate_written_bytes(written_bytes, &limits))
            {
                restore_sandbox_filesystem_with_roots(
                    &mut *sifs_fs_guard,
                    &before_snapshot,
                    &snapshot_roots,
                )?;
                let mut response = outcome.response.clone();
                response.changed_paths = changed_paths;
                response.resource_usage.written_bytes = written_bytes;
                command_history_id = Some(response.command_id.clone());
                persist_command_record(
                    &mut *sifs_fs_guard,
                    &SandboxCommandRecord {
                        sandbox_id: manifest.sandbox_id.clone(),
                        command_id: response.command_id.clone(),
                        status: SandboxCommandRecordStatus::ResourceExhausted,
                        workspace_root: manifest.workspace_root.clone(),
                        toolchain_id: manifest.toolchain_id.clone(),
                        argv,
                        cwd: effective_cwd,
                        env: request.env,
                        started_at,
                        completed_at: Some(completed_at),
                        response: Some(response),
                        error: Some(error.to_string()),
                    },
                )?;
                command = Err(error.into());
            } else {
                outcome.response.changed_paths = changed_paths;
                outcome.response.resource_usage.written_bytes = written_bytes;
                command_history_id = Some(outcome.response.command_id.clone());
                persist_command_outputs(
                    &mut *sifs_fs_guard,
                    &SandboxCommandRecord {
                        sandbox_id: manifest.sandbox_id.clone(),
                        command_id: outcome.response.command_id.clone(),
                        status: SandboxCommandRecordStatus::Completed,
                        workspace_root: manifest.workspace_root.clone(),
                        toolchain_id: manifest.toolchain_id.clone(),
                        argv,
                        cwd: effective_cwd,
                        env: request.env,
                        started_at,
                        completed_at: Some(completed_at),
                        response: Some(outcome.response.clone()),
                        error: None,
                    },
                    &outcome.stdout,
                    &outcome.stderr,
                )?;
            }
        }

        drop(sifs_fs_guard);
        let sifs_fs = Arc::try_unwrap(sifs_fs)
            .map_err(|_| SandboxFsError::Unavailable {
                reason: "SIFS sandbox filesystem still mounted after WASIX run".to_string(),
            })?
            .into_inner()
            .map_err(|_| SandboxFsError::Unavailable {
                reason: "SIFS sandbox filesystem lock poisoned".to_string(),
            })?;
        let mut local_manifest = manifest.clone();
        local_manifest.updated_at = completed_at;
        if let Some(command_id) = command_history_id
            && !local_manifest
                .command_history
                .iter()
                .any(|id| id == &command_id)
        {
            local_manifest.command_history.push(command_id);
        }
        local_manifest.last_placement = manifest.last_placement;
        let mut sifs_store = self.sifs_instance_store(sifs_fs.into_inner());
        sifs_store.save_manifest(&local_manifest)?;

        Ok(SifsStagedSandboxCommandOutcome {
            dfs: sifs_store.into_inner(),
            command,
        })
    }

    #[cfg(not(feature = "wasix-runtime"))]
    #[allow(clippy::too_many_arguments)]
    fn run_module_with_direct_sifs_wasix<T: DfsFrameTransport + Send + 'static>(
        &self,
        _dfs: NetworkedDfsService<T>,
        _local_store: &LocalSandboxInstanceStore,
        _sandbox_id: &SandboxId,
        _wasm: impl AsRef<[u8]>,
        _argv: Vec<String>,
        _stdin: Option<Vec<u8>>,
        _timeout_ms: u64,
        _limits: Option<SandboxResourceLimits>,
        _output_sink: Option<Arc<dyn SandboxOutputSink>>,
        _toolchain_artifacts: Vec<(String, Vec<u8>)>,
        _toolchain_commands: Vec<(String, Vec<u8>)>,
        _default_env: BTreeMap<String, String>,
        _cwd_override: Option<SandboxPath>,
        _env_override: BTreeMap<String, String>,
        _wasix_pinned_child_exec_only: bool,
        _wasix_instruction_metering: bool,
        _wasix_auto_asyncify: bool,
        _wasix_fpcast_emu: bool,
        _wasix_command_overrides: Vec<SandboxWasixCommandOverride>,
    ) -> Result<SifsStagedSandboxCommandOutcome<T>, SandboxCommandExecutionError> {
        Err(SandboxWasiError::Runtime {
            detail: "WASIX runtime feature is disabled".to_string(),
        }
        .into())
    }

    pub fn run_module_with_policy<T: DfsFrameTransport + Send + 'static>(
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

    pub fn run_toolchain_command_with_stdin_and_policy<T: DfsFrameTransport + Send + 'static>(
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

    pub fn run_toolchain_command_with_stdin_policy_and_output_sink<
        T: DfsFrameTransport + Send + 'static,
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
        T: DfsFrameTransport + Send + 'static,
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
        let mut sifs_store = self.sifs_instance_store(dfs);
        let manifest = sifs_store.load(sandbox_id)?;
        let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
        let toolchain = toolchain_store.load_manifest(&manifest.toolchain_id)?;
        let command = toolchain.command(command_name)?;
        policy
            .validate_invocation(&manifest, &toolchain, command, &execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        let limits = effective_sandbox_command_limits(&manifest.limits, &command.default_limits);
        let timeout_ms = effective_sandbox_timeout_ms(timeout_ms, limits.wall_time_ms);
        let wasm = toolchain_store.load_command_module(&toolchain, command_name)?;
        let argv = command.effective_argv(argv);
        let toolchain_artifacts = load_sifs_toolchain_artifacts(&mut toolchain_store, &toolchain)?;
        let toolchain_commands = load_sifs_toolchain_commands(&mut toolchain_store, &toolchain)?;
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
            wasix_pinned_child_exec_only,
            wasix_instruction_metering,
            wasix_auto_asyncify,
            false,
            wasix_command_overrides,
        )
    }

    pub fn run_module_with_stdin_and_policy<T: DfsFrameTransport + Send + 'static>(
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
        let mut sifs_store = self.sifs_instance_store(dfs);
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

#[derive(Clone, Debug, Default)]
pub(super) struct SandboxFilesystemSnapshot {
    pub(super) entries: BTreeMap<SandboxPath, SandboxFilesystemSnapshotEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum SandboxFilesystemSnapshotEntry {
    Directory {
        metadata: SandboxMetadata,
    },
    File {
        metadata: SandboxMetadata,
        bytes: Vec<u8>,
    },
    Symlink {
        metadata: SandboxMetadata,
        target: PathBuf,
    },
}

impl SandboxFilesystemSnapshot {
    pub(super) fn usage(&self) -> SandboxWorkspaceUsage {
        let mut usage = SandboxWorkspaceUsage::default();
        for entry in self.entries.values() {
            match entry {
                SandboxFilesystemSnapshotEntry::File { bytes, .. } => {
                    usage.file_count = usage.file_count.saturating_add(1);
                    usage.total_bytes = usage.total_bytes.saturating_add(bytes.len() as u64);
                }
                SandboxFilesystemSnapshotEntry::Symlink { target, .. } => {
                    usage.file_count = usage.file_count.saturating_add(1);
                    usage.total_bytes = usage
                        .total_bytes
                        .saturating_add(target.as_os_str().len() as u64);
                }
                SandboxFilesystemSnapshotEntry::Directory { .. } => {}
            }
        }
        usage
    }

    pub(super) fn changed_paths_since(&self, before: &Self) -> Vec<SandboxPath> {
        let mut changed = Vec::new();
        for (path, entry) in &self.entries {
            if before.entries.get(path) != Some(entry) {
                changed.push(path.clone());
            }
        }
        for path in before.entries.keys() {
            if !self.entries.contains_key(path) {
                changed.push(path.clone());
            }
        }
        changed.sort();
        changed.dedup();
        changed
    }

    pub(super) fn written_bytes_since(&self, before: &Self) -> u64 {
        let mut written = 0u64;
        for (path, entry) in &self.entries {
            match entry {
                SandboxFilesystemSnapshotEntry::File { bytes, .. } => {
                    if before.entries.get(path) != Some(entry) {
                        written = written.saturating_add(bytes.len() as u64);
                    }
                }
                SandboxFilesystemSnapshotEntry::Symlink { target, .. } => {
                    if before.entries.get(path) != Some(entry) {
                        written = written.saturating_add(target.as_os_str().len() as u64);
                    }
                }
                SandboxFilesystemSnapshotEntry::Directory { .. } => {}
            }
        }
        written
    }
}

pub(super) fn snapshot_sandbox_filesystem_with_roots<F: SandboxFileSystem>(
    filesystem: &mut F,
    roots: &[SandboxPath],
) -> Result<SandboxFilesystemSnapshot, SandboxFsError> {
    let mut snapshot = SandboxFilesystemSnapshot::default();
    for root in roots {
        collect_sandbox_filesystem_snapshot(filesystem, &root, &mut snapshot)?;
    }
    Ok(snapshot)
}

fn collect_sandbox_filesystem_snapshot<F: SandboxFileSystem>(
    filesystem: &mut F,
    path: &SandboxPath,
    snapshot: &mut SandboxFilesystemSnapshot,
) -> Result<(), SandboxFsError> {
    let metadata = match filesystem.stat_unchecked_mut(path) {
        Ok(metadata) => metadata,
        Err(SandboxFsError::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.is_symlink {
        snapshot.entries.insert(
            path.clone(),
            SandboxFilesystemSnapshotEntry::Symlink {
                metadata,
                target: filesystem.readlink_mut(path)?,
            },
        );
    } else if metadata.is_dir {
        snapshot.entries.insert(
            path.clone(),
            SandboxFilesystemSnapshotEntry::Directory {
                metadata: metadata.clone(),
            },
        );
        for child in filesystem.list_unchecked_mut(path)? {
            collect_sandbox_filesystem_snapshot(filesystem, &child.path, snapshot)?;
        }
    } else {
        snapshot.entries.insert(
            path.clone(),
            SandboxFilesystemSnapshotEntry::File {
                metadata,
                bytes: filesystem.read_unchecked_mut(path, None)?,
            },
        );
    }
    Ok(())
}

pub(super) fn restore_sandbox_filesystem_with_roots<F: SandboxFileSystem>(
    filesystem: &mut F,
    snapshot: &SandboxFilesystemSnapshot,
    roots: &[SandboxPath],
) -> Result<(), SandboxFsError> {
    let current = snapshot_sandbox_filesystem_with_roots(filesystem, roots)?;
    let mut removed_paths = current
        .entries
        .iter()
        .filter_map(|(path, current_entry)| {
            let snapshot_entry = snapshot.entries.get(path);
            let keep_directory = matches!(
                current_entry,
                SandboxFilesystemSnapshotEntry::Directory { .. }
            ) && matches!(
                snapshot_entry,
                Some(SandboxFilesystemSnapshotEntry::Directory { .. })
            );
            (!keep_directory && snapshot_entry != Some(current_entry)).then(|| path.clone())
        })
        .collect::<Vec<_>>();
    removed_paths.sort_by(|left, right| {
        right
            .as_str()
            .matches('/')
            .count()
            .cmp(&left.as_str().matches('/').count())
            .then_with(|| right.cmp(left))
    });
    for path in removed_paths {
        match current.entries.get(&path) {
            Some(SandboxFilesystemSnapshotEntry::Directory { .. }) => {
                match filesystem.remove_dir(&path) {
                    Ok(()) | Err(SandboxFsError::NotFound { .. }) => {}
                    Err(error) => return Err(error),
                }
            }
            _ => match filesystem.unlink(&path) {
                Ok(()) | Err(SandboxFsError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            },
        }
    }

    let mut desired_entries = snapshot.entries.iter().collect::<Vec<_>>();
    desired_entries.sort_by(|(left, _), (right, _)| {
        left.as_str()
            .matches('/')
            .count()
            .cmp(&right.as_str().matches('/').count())
            .then_with(|| left.cmp(right))
    });
    for (path, entry) in desired_entries {
        if let SandboxFilesystemSnapshotEntry::Directory { .. } = entry {
            filesystem.create_dir_all(path)?;
        }
    }

    let mut desired_entries = snapshot.entries.iter().collect::<Vec<_>>();
    desired_entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (path, entry) in desired_entries {
        match entry {
            SandboxFilesystemSnapshotEntry::Directory { .. } => {}
            SandboxFilesystemSnapshotEntry::File { bytes, .. } => {
                filesystem.write(path, bytes)?;
            }
            SandboxFilesystemSnapshotEntry::Symlink { target, .. } => {
                if let Some(parent) = snapshot_parent_path(path)? {
                    filesystem.create_dir_all(&parent)?;
                }
                filesystem.symlink(target, path)?;
            }
        }
    }
    let mut hardlink_groups = BTreeMap::<String, Vec<SandboxPath>>::new();
    for (path, entry) in &snapshot.entries {
        let metadata = match entry {
            SandboxFilesystemSnapshotEntry::Directory { .. } => continue,
            SandboxFilesystemSnapshotEntry::File { metadata, .. }
            | SandboxFilesystemSnapshotEntry::Symlink { metadata, .. } => metadata,
        };
        if let Some(key) = snapshot_hardlink_key(metadata) {
            hardlink_groups.entry(key).or_default().push(path.clone());
        }
    }
    for paths in hardlink_groups.values_mut() {
        paths.sort();
        let Some(representative) = paths.last().cloned() else {
            continue;
        };
        for path in paths.iter().filter(|path| **path != representative) {
            match filesystem.unlink(path) {
                Ok(()) | Err(SandboxFsError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
            filesystem.hard_link(&representative, path)?;
        }
    }
    for (path, entry) in &snapshot.entries {
        match entry {
            SandboxFilesystemSnapshotEntry::Directory { metadata }
            | SandboxFilesystemSnapshotEntry::File { metadata, .. } => {
                restore_sandbox_metadata(filesystem, path, metadata, false)?;
            }
            SandboxFilesystemSnapshotEntry::Symlink { metadata, .. } => {
                restore_sandbox_metadata(filesystem, path, metadata, true)?;
            }
        }
    }
    Ok(())
}

fn snapshot_hardlink_key(metadata: &SandboxMetadata) -> Option<String> {
    if metadata.link_count <= 1 {
        return None;
    }
    metadata
        .object_identity
        .clone()
        .or_else(|| (metadata.inode != 0).then(|| format!("inode:{}", metadata.inode)))
}

fn restore_sandbox_metadata<F: SandboxFileSystem>(
    filesystem: &mut F,
    path: &SandboxPath,
    metadata: &SandboxMetadata,
    symlink: bool,
) -> Result<(), SandboxFsError> {
    if !symlink {
        filesystem.set_mode(path, metadata.mode)?;
        filesystem.set_owner(path, metadata.uid, metadata.gid)?;
        filesystem.set_times(
            path,
            Some(metadata.accessed_at_unix_nanos),
            Some(metadata.modified_at_unix_nanos),
        )?;
    } else {
        filesystem.set_symlink_times(
            path,
            Some(metadata.accessed_at_unix_nanos),
            Some(metadata.modified_at_unix_nanos),
        )?;
    }
    Ok(())
}

fn sandbox_snapshot_roots() -> Vec<SandboxPath> {
    [
        "/root",
        "/home",
        "/tmp",
        "/work",
        "/var",
        "/logs",
        "/artifacts",
    ]
    .into_iter()
    .map(|path| SandboxPath::new(path).expect("static sandbox path"))
    .collect()
}

pub(super) fn sandbox_snapshot_roots_for_manifest(
    manifest: &SandboxInstanceManifest,
) -> Vec<SandboxPath> {
    let mut roots = sandbox_snapshot_roots();
    roots.extend(
        manifest
            .mounts
            .iter()
            .filter(|mount| mount.mode == SandboxMountMode::ReadWrite)
            .map(|mount| mount.mount_path.clone()),
    );
    roots.sort();
    roots.dedup();
    roots
}

pub(super) fn sifs_manifest_mount_needs_wasix_system_preopen(path: &SandboxPath) -> bool {
    ["/etc", "/sys", "/run", "/usr", "/opt"]
        .into_iter()
        .any(|parent| path.as_str().starts_with(&format!("{parent}/")))
}

fn snapshot_parent_path(path: &SandboxPath) -> Result<Option<SandboxPath>, SandboxFsError> {
    let value = path.as_str();
    if value == "/" {
        return Ok(None);
    }
    let Some((parent, _name)) = value.rsplit_once('/') else {
        return Ok(None);
    };
    if parent.is_empty() {
        return Ok(Some(SandboxPath::new("/")?));
    }
    Ok(Some(SandboxPath::new(parent)?))
}

pub(super) fn sandbox_command_record_status_for_execution_error(
    error: &SandboxCommandExecutionError,
) -> SandboxCommandRecordStatus {
    match error {
        SandboxCommandExecutionError::Wasi(error) => {
            sandbox_command_record_status_for_wasi_error(error)
        }
        SandboxCommandExecutionError::Fs(SandboxFsError::QuotaExceeded { .. }) => {
            SandboxCommandRecordStatus::ResourceExhausted
        }
        _ => SandboxCommandRecordStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_dfs::{DfsMetadataShardId, LocalContentStore, LocalSobjMetadataBackend};
    use sinomo_network::InProcessNetwork;

    fn unique_test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{name}-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
    }

    #[test]
    fn sifs_snapshot_fetches_remote_file_and_symlink_content() {
        let runtime_root = unique_test_dir("sinomo-sandbox-sifs-snapshot-remote-content");
        let network = InProcessNetwork::new();
        let owner = NodeId::from_legacy_named("node_sifs_snapshot_owner").expect("owner");
        let reader = NodeId::from_legacy_named("node_sifs_snapshot_reader").expect("reader");
        let metadata_root = runtime_root.join("metadata");
        let owner_backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
                .expect("owner metadata");
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(runtime_root.join("owner-content")).expect("owner content"),
        )
        .with_metadata_backend(owner_backend)
        .expect("owner metadata backend");
        owner_dfs
            .bootstrap_distributed_sifs()
            .expect("owner bootstrap");
        let sandbox_id =
            SandboxId::from_legacy_named("sandbox_sifs_snapshot_remote").expect("sandbox");
        let mut owner_store =
            SifsSandboxInstanceStore::new(owner_dfs).with_replica_candidates(vec![owner.clone()]);
        let manifest = owner_store
            .create(
                sandbox_id,
                SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
                OffsetDateTime::now_utc(),
            )
            .expect("create");
        let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
            .with_replica_candidates(vec![owner.clone()]);
        owner_fs
            .write(
                &SandboxPath::new("/work/target.txt").expect("target"),
                b"target",
            )
            .expect("write target");
        owner_fs
            .symlink(
                Path::new("target.txt"),
                &SandboxPath::new("/work/link.txt").expect("link"),
            )
            .expect("symlink");
        let owner_dfs = Arc::new(Mutex::new(owner_fs.into_inner()));

        let reader_backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
                .expect("reader metadata");
        let reader_dfs = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(runtime_root.join("reader-content")).expect("reader content"),
        )
        .with_metadata_backend(reader_backend)
        .expect("reader metadata backend");
        let mut reader_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);

        let owner_worker = {
            let owner_dfs = Arc::clone(&owner_dfs);
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut served = 0usize;
                loop {
                    let events = owner_dfs
                        .lock()
                        .expect("owner dfs lock")
                        .process_incoming()
                        .expect("owner serves snapshot reads");
                    served += events
                        .iter()
                        .filter(|event| {
                            matches!(
                                event,
                                sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed { .. }
                            )
                        })
                        .count();
                    if served >= 2 {
                        return served;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "owner did not receive remote snapshot read requests"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            })
        };
        let snapshot = snapshot_sandbox_filesystem_with_roots(
            &mut reader_fs,
            &[SandboxPath::new("/work").expect("work")],
        )
        .expect("snapshot");
        assert_eq!(owner_worker.join().expect("owner worker"), 2);

        let target = SandboxPath::new("/work/target.txt").expect("target");
        let link = SandboxPath::new("/work/link.txt").expect("link");
        assert!(matches!(
            snapshot.entries.get(&target),
            Some(SandboxFilesystemSnapshotEntry::File { bytes, .. }) if bytes == b"target"
        ));
        assert!(matches!(
            snapshot.entries.get(&link),
            Some(SandboxFilesystemSnapshotEntry::Symlink { target, .. })
                if target == &PathBuf::from("target.txt")
        ));

        fs::remove_dir_all(runtime_root).ok();
    }
}
