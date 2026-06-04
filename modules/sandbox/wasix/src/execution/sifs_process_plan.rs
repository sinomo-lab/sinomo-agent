use crate::internal::*;

#[cfg(feature = "wasix-runtime")]
use super::sifs::{restore_sandbox_filesystem_with_roots, snapshot_sandbox_filesystem_with_roots};
#[cfg(feature = "wasix-runtime")]
use super::staging::*;

impl SifsStagedSandboxCommandRunner {
    pub fn run_toolchain_process_plan_with_policy<T: NetworkTransport + Send + 'static>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        policy: &SandboxPolicy,
    ) -> Result<SifsStagedSandboxProcessPlanOutcome<T>, SandboxCommandExecutionError> {
        self.run_toolchain_process_plan_with_policy_and_output_sink(
            dfs,
            local_store,
            sandbox_id,
            plan,
            timeout_ms,
            policy,
            None,
        )
    }

    pub fn run_toolchain_process_plan_with_policy_and_output_sink<
        T: NetworkTransport + Send + 'static,
    >(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SifsStagedSandboxProcessPlanOutcome<T>, SandboxCommandExecutionError> {
        validate_sandbox_process_plan(plan)?;

        let mut dfs = dfs;
        let mut store = self.sifs_instance_store(dfs);
        let manifest = store.load(sandbox_id)?;
        let sifs_fs = self.sifs_filesystem(store.into_inner(), &manifest);
        validate_sandbox_process_plan_working_directories(plan, &sifs_fs, &manifest.cwd)?;
        dfs = sifs_fs.into_inner();
        let execution_node_id = dfs.node_id().clone();
        let mut toolchain_store = SifsSandboxToolchainStore::new(dfs);
        let toolchain = toolchain_store.load_manifest(&manifest.toolchain_id)?;
        validate_sandbox_process_plan_invocations(
            plan,
            &manifest,
            &toolchain,
            &execution_node_id,
            policy,
        )?;
        dfs = toolchain_store.into_inner();

        #[cfg(feature = "wasix-runtime")]
        {
            let (next_dfs, concurrent_process) = self.try_run_wasix_concurrent_process_pipeline(
                dfs,
                local_store,
                sandbox_id,
                plan,
                timeout_ms,
                policy,
                output_sink.clone(),
                &manifest,
                &toolchain,
            )?;
            dfs = next_dfs;
            if let Some(process) = concurrent_process {
                return Ok(SifsStagedSandboxProcessPlanOutcome { dfs, process });
            }
        }

        let mut step_outcomes = Vec::with_capacity(plan.steps.len());
        let mut piped_stdin: Option<Vec<u8>> = None;
        for step in &plan.steps {
            let stdin = step.stdin.clone().or_else(|| piped_stdin.take());
            let command_outcome = self
                .run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
                    dfs,
                    local_store,
                    sandbox_id,
                    &step.command_name,
                    step.argv.clone(),
                    stdin,
                    timeout_ms,
                    policy,
                    output_sink.clone(),
                    step.cwd.clone(),
                    step.env.clone(),
                )?;
            dfs = command_outcome.dfs;
            let outcome = match command_outcome.command {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Ok(SifsStagedSandboxProcessPlanOutcome {
                        dfs,
                        process: Err(error),
                    });
                }
            };
            if outcome.response.exit_status != 0 {
                let exit_status = outcome.response.exit_status;
                let stdout = outcome.stdout.clone();
                let stderr = outcome.stderr.clone();
                step_outcomes.push(outcome);
                return Ok(SifsStagedSandboxProcessPlanOutcome {
                    dfs,
                    process: Ok(SandboxProcessPlanOutcome {
                        step_outcomes,
                        exit_status,
                        stdout,
                        stderr,
                    }),
                });
            }
            if step.pipe_stdout_to_next {
                piped_stdin = Some(outcome.stdout.clone());
            }
            step_outcomes.push(outcome);
        }

        let last = step_outcomes
            .last()
            .expect("non-empty process plan should produce an outcome");
        Ok(SifsStagedSandboxProcessPlanOutcome {
            dfs,
            process: Ok(SandboxProcessPlanOutcome {
                exit_status: last.response.exit_status,
                stdout: last.stdout.clone(),
                stderr: last.stderr.clone(),
                step_outcomes,
            }),
        })
    }

    #[cfg(feature = "wasix-runtime")]
    #[allow(clippy::too_many_arguments)]
    fn try_run_wasix_concurrent_process_pipeline<T: NetworkTransport + Send + 'static>(
        &self,
        dfs: NetworkedDfsService<T>,
        _local_store: &LocalSandboxInstanceStore,
        _sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        _policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        manifest: &SandboxInstanceManifest,
        toolchain: &SandboxToolchainManifest,
    ) -> Result<
        (
            NetworkedDfsService<T>,
            Option<Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError>>,
        ),
        SandboxCommandExecutionError,
    > {
        if !sifs_process_plan_is_wasix_concurrent_candidate(plan, toolchain)? {
            return Ok((dfs, None));
        }

        let mut sifs_store = self.sifs_instance_store(dfs);
        let execution_node_id = sifs_store.dfs.node_id().clone();
        let mut manifest = manifest.clone();
        manifest.last_placement = Some(sandbox_execution_placement_for_node(
            &manifest,
            &execution_node_id,
        )?);
        sifs_store.save_manifest(&manifest)?;
        let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
        let toolchain_artifacts = load_sifs_toolchain_artifacts(&mut toolchain_store, toolchain)?;
        let toolchain_commands = load_sifs_toolchain_commands(&mut toolchain_store, toolchain)?;
        let artifact_preopens =
            stage_toolchain_runtime_to_virtual(&toolchain_artifacts, &toolchain_commands)?;

        let mut sifs_fs = self.sifs_filesystem(toolchain_store.into_inner(), &manifest);
        sifs_fs.ensure_workspace()?;
        let snapshot_roots = super::sifs::sandbox_snapshot_roots_for_manifest(&manifest);
        let before_snapshot =
            snapshot_sandbox_filesystem_with_roots(&mut sifs_fs, &snapshot_roots)?;

        let toolchain_command_modules = toolchain_commands
            .iter()
            .cloned()
            .collect::<BTreeMap<String, Vec<u8>>>();
        let wasix_command_overrides = toolchain
            .commands
            .values()
            .filter(|command| {
                command.effective_target_abi(toolchain) == SandboxWasiAbi::Wasix
                    && (!command.argv_prefix.is_empty() || command.exec_name.is_some())
            })
            .map(|command| SandboxWasixCommandOverride {
                name: command.name.clone(),
                argv_prefix: command.argv_prefix.clone(),
                exec_name: command.exec_name.clone(),
            })
            .collect::<Vec<_>>();

        let mut prepared_steps = Vec::with_capacity(plan.steps.len());
        for step in &plan.steps {
            let command = toolchain.command(&step.command_name)?;
            let limits =
                effective_sandbox_command_limits(&manifest.limits, &command.default_limits);
            validate_workspace_usage(&before_snapshot.usage(), &limits)?;
            let effective_cwd = step.cwd.clone().unwrap_or_else(|| manifest.cwd.clone());
            validate_sandbox_working_directory(&sifs_fs, &effective_cwd)?;
            let timeout_ms = effective_sandbox_timeout_ms(timeout_ms, limits.wall_time_ms);
            let module = toolchain_command_modules
                .get(&step.command_name)
                .cloned()
                .ok_or_else(|| SandboxToolchainError::CommandNotFound {
                    toolchain_id: toolchain.toolchain_id.clone(),
                    name: step.command_name.clone(),
                })?;
            let mut env = command.default_env.clone();
            env.extend(manifest.env.clone());
            env.extend(step.env.clone());
            env.insert("PWD".to_string(), effective_cwd.as_str().to_string());
            prepared_steps.push(SifsWasixProcessPlanPreparedStep {
                argv: command.effective_argv(step.argv.clone()),
                stdin: step.stdin.clone(),
                cwd: effective_cwd,
                env,
                timeout_ms,
                limits,
                module,
                wasix_pinned_child_exec_only: !command
                    .required_capabilities
                    .iter()
                    .any(|capability| capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY),
            });
        }

        let sifs_fs = Arc::new(std::sync::Mutex::new(sifs_fs));
        let pipe_pairs = (0..plan.steps.len() - 1)
            .map(|_| virtual_fs::Pipe::channel())
            .collect::<Vec<_>>();
        let mut stdout_pipes = pipe_pairs
            .iter()
            .map(|(writer, _)| Some(writer.clone()))
            .collect::<Vec<_>>();
        let mut stdin_pipes = pipe_pairs
            .into_iter()
            .map(|(_, reader)| Some(virtual_fs::ArcFile::new(Box::new(reader))))
            .collect::<Vec<_>>();
        let mut handles = Vec::with_capacity(plan.steps.len());
        for (index, prepared) in prepared_steps.into_iter().enumerate() {
            let runner = self.local_runner.clone();
            let manifest = manifest.clone();
            let artifact_preopens = artifact_preopens.clone();
            let wasix_command_overrides = wasix_command_overrides.clone();
            let output_sink = output_sink.clone();
            let sifs_fs = Arc::clone(&sifs_fs);
            let stdin_pipe = if index == 0 {
                None
            } else {
                stdin_pipes[index - 1].take()
            };
            let stdout_pipe = if index + 1 < plan.steps.len() {
                stdout_pipes[index].take()
            } else {
                None
            };
            handles.push(std::thread::spawn(move || {
                let started_at = OffsetDateTime::now_utc();
                let mut virtual_preopened_dirs = artifact_preopens.clone();
                virtual_preopened_dirs.extend(
                    runtime::wasix_sandbox_workspace_filesystem_preopens(
                        Arc::clone(&sifs_fs),
                        SandboxMountMode::ReadWrite,
                    ),
                );
                for mount in manifest.mounts.iter().filter(|mount| {
                    super::sifs::sifs_manifest_mount_needs_wasix_system_preopen(&mount.mount_path)
                }) {
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
                    memory_limit_bytes: prepared.limits.memory_mb.saturating_mul(1024 * 1024)
                        as usize,
                    input_limit_bytes: prepared.limits.max_input_bytes as usize,
                    output_limit_bytes: prepared.limits.max_output_bytes as usize,
                    workspace_limit_bytes: prepared.limits.max_workspace_bytes,
                    file_count_limit: prepared.limits.max_file_count,
                    ..SandboxWasiRunOptions::default()
                };
                wasi_options.wasix_pinned_child_exec_only = prepared.wasix_pinned_child_exec_only;
                wasi_options.wasix_instruction_metering = false;
                wasi_options.wasix_command_overrides = wasix_command_overrides;
                wasi_options.wasix_stdin_pipe = stdin_pipe;
                wasi_options.wasix_stdout_pipe = stdout_pipe;
                let request = SandboxCommandRequest {
                    sandbox_id: manifest.sandbox_id.clone(),
                    argv: prepared.argv.clone(),
                    cwd: prepared.cwd.clone(),
                    stdin: prepared.stdin.clone(),
                    env: prepared.env.clone(),
                    timeout_ms: prepared.timeout_ms,
                };
                let result = runner.run_module_for_abi(
                    SandboxWasiAbi::Wasix,
                    &prepared.module,
                    request.clone(),
                    wasi_options.with_output_sink(output_sink),
                );
                SifsWasixProcessPlanStepRun {
                    argv: prepared.argv,
                    cwd: prepared.cwd,
                    env: request.env,
                    limits: prepared.limits,
                    started_at,
                    result,
                }
            }));
        }

        let mut step_runs = Vec::with_capacity(handles.len());
        let mut panic_error = None;
        for handle in handles {
            match handle.join() {
                Ok(step_run) => step_runs.push(step_run),
                Err(_) => {
                    panic_error = Some(SandboxWasiError::Runtime {
                        detail: "SIFS WASIX concurrent process-plan step panicked".to_string(),
                    });
                }
            }
        }

        let completed_at = OffsetDateTime::now_utc();
        let mut sifs_fs_guard = sifs_fs.lock().map_err(|_| SandboxFsError::Unavailable {
            reason: "SIFS sandbox filesystem lock poisoned".to_string(),
        })?;
        let process: Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError>;

        if let Some(error) = panic_error {
            restore_sandbox_filesystem_with_roots(
                &mut *sifs_fs_guard,
                &before_snapshot,
                &snapshot_roots,
            )?;
            process = Err(SandboxCommandExecutionError::from(error));
        } else if let Some(failed_index) = step_runs.iter().position(|step| step.result.is_err()) {
            let failed_step = step_runs.remove(failed_index);
            restore_sandbox_filesystem_with_roots(
                &mut *sifs_fs_guard,
                &before_snapshot,
                &snapshot_roots,
            )?;
            let Err(error) = failed_step.result else {
                unreachable!("failed step selected by is_err");
            };
            let command_id = SandboxCommandId::generate();
            persist_command_record(
                &mut *sifs_fs_guard,
                &SandboxCommandRecord {
                    sandbox_id: manifest.sandbox_id.clone(),
                    command_id,
                    status: sandbox_command_record_status_for_wasi_error(&error),
                    workspace_root: manifest.workspace_root.clone(),
                    toolchain_id: manifest.toolchain_id.clone(),
                    argv: failed_step.argv,
                    cwd: failed_step.cwd,
                    env: failed_step.env,
                    started_at: failed_step.started_at,
                    completed_at: Some(completed_at),
                    response: None,
                    error: Some(error.to_string()),
                },
            )?;
            process = Err(error.into());
        } else {
            let after_snapshot =
                snapshot_sandbox_filesystem_with_roots(&mut *sifs_fs_guard, &snapshot_roots)?;
            let changed_paths = after_snapshot.changed_paths_since(&before_snapshot);
            let workspace_usage = after_snapshot.usage();
            let written_bytes = after_snapshot.written_bytes_since(&before_snapshot);
            if let Some((step, error)) = step_runs.iter().find_map(|step| {
                validate_workspace_usage(&workspace_usage, &step.limits)
                    .and_then(|_| validate_written_bytes(written_bytes, &step.limits))
                    .err()
                    .map(|error| (step, error))
            }) {
                restore_sandbox_filesystem_with_roots(
                    &mut *sifs_fs_guard,
                    &before_snapshot,
                    &snapshot_roots,
                )?;
                let outcome = step
                    .result
                    .as_ref()
                    .expect("successful step after preflight");
                let mut response = outcome.response.clone();
                response.changed_paths = changed_paths;
                response.resource_usage.written_bytes = written_bytes;
                persist_command_record(
                    &mut *sifs_fs_guard,
                    &SandboxCommandRecord {
                        sandbox_id: manifest.sandbox_id.clone(),
                        command_id: response.command_id.clone(),
                        status: SandboxCommandRecordStatus::ResourceExhausted,
                        workspace_root: manifest.workspace_root.clone(),
                        toolchain_id: manifest.toolchain_id.clone(),
                        argv: step.argv.clone(),
                        cwd: step.cwd.clone(),
                        env: step.env.clone(),
                        started_at: step.started_at,
                        completed_at: Some(completed_at),
                        response: Some(response),
                        error: Some(error.to_string()),
                    },
                )?;
                process = Err(error.into());
            } else {
                let mut step_outcomes = Vec::with_capacity(step_runs.len());
                for step in step_runs {
                    let mut outcome = step.result.expect("errors handled before persistence");
                    outcome.response.changed_paths = changed_paths.clone();
                    outcome.response.resource_usage.written_bytes = written_bytes;
                    persist_command_outputs(
                        &mut *sifs_fs_guard,
                        &SandboxCommandRecord {
                            sandbox_id: manifest.sandbox_id.clone(),
                            command_id: outcome.response.command_id.clone(),
                            status: SandboxCommandRecordStatus::Completed,
                            workspace_root: manifest.workspace_root.clone(),
                            toolchain_id: manifest.toolchain_id.clone(),
                            argv: step.argv,
                            cwd: step.cwd,
                            env: step.env,
                            started_at: step.started_at,
                            completed_at: Some(completed_at),
                            response: Some(outcome.response.clone()),
                            error: None,
                        },
                        &outcome.stdout,
                        &outcome.stderr,
                    )?;
                    step_outcomes.push(outcome);
                }
                let process_outcome = if let Some(index) = step_outcomes
                    .iter()
                    .position(|outcome| outcome.response.exit_status != 0)
                {
                    SandboxProcessPlanOutcome {
                        exit_status: step_outcomes[index].response.exit_status,
                        stdout: step_outcomes[index].stdout.clone(),
                        stderr: step_outcomes[index].stderr.clone(),
                        step_outcomes,
                    }
                } else {
                    let last = step_outcomes
                        .last()
                        .expect("non-empty concurrent process plan should produce an outcome");
                    SandboxProcessPlanOutcome {
                        exit_status: last.response.exit_status,
                        stdout: last.stdout.clone(),
                        stderr: last.stderr.clone(),
                        step_outcomes,
                    }
                };
                process = Ok(process_outcome);
            }
        }

        drop(sifs_fs_guard);
        let sifs_fs = Arc::try_unwrap(sifs_fs)
            .map_err(|_| SandboxFsError::Unavailable {
                reason: "SIFS sandbox filesystem still mounted after WASIX process plan"
                    .to_string(),
            })?
            .into_inner()
            .map_err(|_| SandboxFsError::Unavailable {
                reason: "SIFS sandbox filesystem lock poisoned".to_string(),
            })?;
        let mut updated_manifest = manifest.clone();
        updated_manifest.updated_at = completed_at;
        if let Ok(outcome) = &process {
            for step in &outcome.step_outcomes {
                if !updated_manifest
                    .command_history
                    .iter()
                    .any(|id| id == &step.response.command_id)
                {
                    updated_manifest
                        .command_history
                        .push(step.response.command_id.clone());
                }
            }
        }
        updated_manifest.last_placement = manifest.last_placement;
        let mut sifs_store = self.sifs_instance_store(sifs_fs.into_inner());
        sifs_store.save_manifest(&updated_manifest)?;
        Ok((sifs_store.into_inner(), Some(process)))
    }
}

#[cfg(feature = "wasix-runtime")]
struct SifsWasixProcessPlanPreparedStep {
    argv: Vec<String>,
    stdin: Option<Vec<u8>>,
    cwd: SandboxPath,
    env: BTreeMap<String, String>,
    timeout_ms: u64,
    limits: SandboxResourceLimits,
    module: Vec<u8>,
    wasix_pinned_child_exec_only: bool,
}

#[cfg(feature = "wasix-runtime")]
struct SifsWasixProcessPlanStepRun {
    argv: Vec<String>,
    cwd: SandboxPath,
    env: BTreeMap<String, String>,
    limits: SandboxResourceLimits,
    started_at: OffsetDateTime,
    result: Result<SandboxLocalCommandOutcome, SandboxWasiError>,
}

#[cfg(feature = "wasix-runtime")]
fn sifs_process_plan_is_wasix_concurrent_candidate(
    plan: &SandboxProcessPlan,
    toolchain: &SandboxToolchainManifest,
) -> Result<bool, SandboxCommandExecutionError> {
    if plan.steps.len() < 2
        || !plan
            .steps
            .iter()
            .take(plan.steps.len() - 1)
            .all(|step| step.pipe_stdout_to_next)
        || plan
            .steps
            .last()
            .map(|step| step.pipe_stdout_to_next)
            .unwrap_or(false)
    {
        return Ok(false);
    }
    for step in &plan.steps {
        let command = toolchain.command(&step.command_name)?;
        if command.effective_target_abi(toolchain) != SandboxWasiAbi::Wasix {
            return Ok(false);
        }
    }
    Ok(true)
}
