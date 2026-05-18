use crate::internal::*;

#[cfg(feature = "wasix-runtime")]
use super::staging::*;

impl LocalStagedSandboxCommandRunner {
    pub fn run_toolchain_process_plan_with_policy(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_process_plan_with_policy_and_output_sink(
            store,
            toolchain_store,
            sandbox_id,
            plan,
            timeout_ms,
            execution_node_id,
            policy,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_process_plan_with_policy_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        validate_sandbox_process_plan(plan)?;
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

        #[cfg(feature = "wasix-runtime")]
        if let Some(outcome) = self.try_run_wasix_concurrent_process_pipeline(
            store,
            toolchain_store,
            sandbox_id,
            plan,
            timeout_ms,
            output_sink.clone(),
            &toolchain,
            Vec::new(),
        )? {
            return Ok(outcome);
        }

        let mut step_outcomes = Vec::with_capacity(plan.steps.len());
        let mut piped_stdin: Option<Vec<u8>> = None;
        for step in &plan.steps {
            let stdin = step.stdin.clone().or_else(|| piped_stdin.take());
            let outcome = self.run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
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
            )?;
            if outcome.response.exit_status != 0 {
                let exit_status = outcome.response.exit_status;
                let stdout = outcome.stdout.clone();
                let stderr = outcome.stderr.clone();
                step_outcomes.push(outcome);
                return Ok(SandboxProcessPlanOutcome {
                    step_outcomes,
                    exit_status,
                    stdout,
                    stderr,
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
        Ok(SandboxProcessPlanOutcome {
            exit_status: last.response.exit_status,
            stdout: last.stdout.clone(),
            stderr: last.stderr.clone(),
            step_outcomes,
        })
    }

    #[cfg(feature = "wasix-runtime")]
    #[allow(clippy::too_many_arguments)]
    fn try_run_wasix_concurrent_process_pipeline(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        toolchain: &SandboxToolchainManifest,
        extra_preopens: Vec<SandboxWasiPreopen>,
    ) -> Result<Option<SandboxProcessPlanOutcome>, SandboxCommandExecutionError> {
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
            return Ok(None);
        }
        for step in &plan.steps {
            let command = toolchain.command(&step.command_name)?;
            if command.effective_target_abi(toolchain) != SandboxWasiAbi::Wasix {
                return Ok(None);
            }
        }

        let (manifest, layout) = store.load(sandbox_id)?;
        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        let before = sandbox_fs.snapshot()?;
        let toolchain_artifacts = load_local_toolchain_artifacts(toolchain_store, toolchain)?;
        let toolchain_commands = load_local_toolchain_commands(toolchain_store, toolchain)?;
        let mut artifact_preopens =
            stage_toolchain_runtime_to_local(&layout, &toolchain_artifacts, &toolchain_commands)?;
        artifact_preopens.extend(extra_preopens.clone());
        let toolchain_command_modules = toolchain_commands
            .into_iter()
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
        for (step_index, step) in plan.steps.iter().enumerate() {
            let command = toolchain.command(&step.command_name)?;
            let limits =
                effective_sandbox_command_limits(&manifest.limits, &command.default_limits);
            sandbox_fs.validate_resource_limits(&limits)?;
            let timeout_ms = effective_sandbox_timeout_ms(timeout_ms, limits.wall_time_ms);
            let effective_cwd = step.cwd.clone().unwrap_or_else(|| manifest.cwd.clone());
            let working_directory_host_path = host_path_for_process_plan_cwd(
                &sandbox_fs,
                &artifact_preopens,
                step_index,
                &effective_cwd,
            )?;
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
            let argv = command.effective_argv(step.argv.clone());
            prepared_steps.push(PreparedWasixProcessPlanStep {
                command_name: step.command_name.clone(),
                argv,
                stdin: step.stdin.clone(),
                cwd: effective_cwd,
                env,
                timeout_ms,
                limits,
                module,
                working_directory_host_path,
                allow_preview1_pinned_subprocesses: command
                    .required_capabilities
                    .iter()
                    .any(|capability| capability == SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY),
                wasix_pinned_child_exec_only: !command
                    .required_capabilities
                    .iter()
                    .any(|capability| capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY),
            });
        }

        let rollback_checkpoint = store.create_checkpoint(sandbox_id, OffsetDateTime::now_utc())?;
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
            let runner = self.clone();
            let manifest = manifest.clone();
            let layout = layout.clone();
            let artifact_preopens = artifact_preopens.clone();
            let wasix_command_overrides = wasix_command_overrides.clone();
            let output_sink = output_sink.clone();
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
                let mut wasi_options = local_staged_sandbox_wasi_options_with_limits_and_preopens(
                    &layout,
                    &prepared.limits,
                    artifact_preopens,
                );
                wasi_options.allow_preview1_pinned_subprocesses =
                    prepared.allow_preview1_pinned_subprocesses;
                wasi_options.wasix_pinned_child_exec_only = prepared.wasix_pinned_child_exec_only;
                wasi_options.wasix_instruction_metering = false;
                wasi_options.wasix_command_overrides = wasix_command_overrides;
                wasi_options.wasix_stdin_pipe = stdin_pipe;
                wasi_options.wasix_stdout_pipe = stdout_pipe;
                wasi_options.working_directory_host_path =
                    Some(prepared.working_directory_host_path);
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
                WasixProcessPlanStepRun {
                    command_name: prepared.command_name,
                    argv: prepared.argv,
                    cwd: prepared.cwd,
                    env: request.env,
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
                        detail: "WASIX concurrent process-plan step panicked".to_string(),
                    });
                }
            }
        }

        if let Some(error) = panic_error {
            store.resume_checkpoint(
                sandbox_id,
                &rollback_checkpoint.checkpoint_id,
                OffsetDateTime::now_utc(),
            )?;
            let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
            return Err(error.into());
        }

        if let Some(failed_index) = step_runs.iter().position(|step| step.result.is_err()) {
            let failed_step = step_runs.remove(failed_index);
            store.resume_checkpoint(
                sandbox_id,
                &rollback_checkpoint.checkpoint_id,
                OffsetDateTime::now_utc(),
            )?;
            let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
            let Err(error) = failed_step.result else {
                unreachable!("failed step selected by is_err");
            };
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
                    argv: failed_step.argv.clone(),
                    cwd: failed_step.cwd.clone(),
                    env: failed_step.env.clone(),
                    started_at: failed_step.started_at,
                    completed_at: Some(completed_at),
                    response: None,
                    error: Some(error.to_string()),
                },
            )?;
            record_local_command_history(store, sandbox_id, &command_id, completed_at)?;
            return Err(error.into());
        }

        let after = sandbox_fs.snapshot()?;
        let changed_paths = sandbox_fs.changed_paths_since(&before)?;
        let workspace_usage = after.workspace_usage();
        let written_bytes = after.written_bytes_since(&before);
        for step in &step_runs {
            let limits = prepared_limit_for_step(toolchain, &manifest, &step.command_name)?;
            if let Err(error) = validate_workspace_usage(&workspace_usage, &limits) {
                return rollback_wasix_process_plan_for_resource_error(
                    store,
                    sandbox_id,
                    &rollback_checkpoint,
                    &layout,
                    &manifest,
                    step,
                    step.result
                        .as_ref()
                        .expect("successful step after error preflight"),
                    error,
                );
            }
            if let Err(error) = validate_written_bytes(written_bytes, &limits) {
                return rollback_wasix_process_plan_for_resource_error(
                    store,
                    sandbox_id,
                    &rollback_checkpoint,
                    &layout,
                    &manifest,
                    step,
                    step.result
                        .as_ref()
                        .expect("successful step after error preflight"),
                    error,
                );
            }
        }

        let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
        let mut step_outcomes = Vec::with_capacity(step_runs.len());
        let completed_at = OffsetDateTime::now_utc();
        let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
        for step in step_runs {
            let mut outcome = step.result.expect("errors handled before persistence");
            outcome.response.changed_paths = changed_paths.clone();
            outcome.response.resource_usage.written_bytes = written_bytes;
            persist_command_outputs(
                &mut sandbox_fs,
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
        let now = OffsetDateTime::now_utc();
        for outcome in &step_outcomes {
            record_local_command_history(store, sandbox_id, &outcome.response.command_id, now)?;
        }
        for index in 0..step_outcomes.len() {
            if step_outcomes[index].response.exit_status != 0 {
                return Ok(Some(SandboxProcessPlanOutcome {
                    exit_status: step_outcomes[index].response.exit_status,
                    stdout: step_outcomes[index].stdout.clone(),
                    stderr: step_outcomes[index].stderr.clone(),
                    step_outcomes,
                }));
            }
        }
        let last = step_outcomes
            .last()
            .expect("non-empty concurrent process plan should produce an outcome");
        Ok(Some(SandboxProcessPlanOutcome {
            exit_status: last.response.exit_status,
            stdout: last.stdout.clone(),
            stderr: last.stderr.clone(),
            step_outcomes,
        }))
    }
}

#[cfg(feature = "wasix-runtime")]
impl LocalStagedSandboxCommandRunner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_toolchain_process_plan_with_policy_output_sink_and_preopens(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        extra_preopens: Vec<SandboxWasiPreopen>,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        validate_sandbox_process_plan(plan)?;
        let (manifest, _layout) = store.load(sandbox_id)?;
        let (toolchain, _toolchain_layout) =
            toolchain_store.load_manifest(&manifest.toolchain_id)?;
        validate_sandbox_process_plan_invocations(
            plan,
            &manifest,
            &toolchain,
            execution_node_id,
            policy,
        )?;
        if let Some(outcome) = self.try_run_wasix_concurrent_process_pipeline(
            store,
            toolchain_store,
            sandbox_id,
            plan,
            timeout_ms,
            output_sink.clone(),
            &toolchain,
            extra_preopens,
        )? {
            return Ok(outcome);
        }
        self.run_toolchain_process_plan_with_policy_and_output_sink(
            store,
            toolchain_store,
            sandbox_id,
            plan,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
        )
    }
}

#[cfg(feature = "wasix-runtime")]
struct PreparedWasixProcessPlanStep {
    command_name: String,
    argv: Vec<String>,
    stdin: Option<Vec<u8>>,
    cwd: SandboxPath,
    env: BTreeMap<String, String>,
    timeout_ms: u64,
    limits: SandboxResourceLimits,
    module: Vec<u8>,
    working_directory_host_path: PathBuf,
    allow_preview1_pinned_subprocesses: bool,
    wasix_pinned_child_exec_only: bool,
}

#[cfg(feature = "wasix-runtime")]
struct WasixProcessPlanStepRun {
    command_name: String,
    argv: Vec<String>,
    cwd: SandboxPath,
    env: BTreeMap<String, String>,
    started_at: OffsetDateTime,
    result: Result<SandboxLocalCommandOutcome, SandboxWasiError>,
}

#[cfg(feature = "wasix-runtime")]
fn prepared_limit_for_step(
    toolchain: &SandboxToolchainManifest,
    manifest: &SandboxInstanceManifest,
    command_name: &str,
) -> Result<SandboxResourceLimits, SandboxCommandExecutionError> {
    let command = toolchain.command(command_name)?;
    Ok(effective_sandbox_command_limits(
        &manifest.limits,
        &command.default_limits,
    ))
}

#[cfg(feature = "wasix-runtime")]
fn host_path_for_process_plan_cwd(
    sandbox_fs: &LocalSandboxFileSystem,
    extra_preopens: &[SandboxWasiPreopen],
    step_index: usize,
    cwd: &SandboxPath,
) -> Result<PathBuf, SandboxCommandExecutionError> {
    match validate_sandbox_working_directory(sandbox_fs, cwd) {
        Ok(()) => return sandbox_fs.host_path(cwd).map_err(Into::into),
        Err(local_error) => {
            if let Some(host_path) =
                mounted_process_plan_cwd_host_path(extra_preopens, step_index, cwd)?
            {
                return Ok(host_path);
            }
            Err(SandboxCommandExecutionError::Wasi(
                SandboxWasiError::InvalidRequest {
                    detail: format!(
                        "process plan step {step_index} cwd `{cwd}` is invalid: {local_error}"
                    ),
                },
            ))
        }
    }
}

#[cfg(feature = "wasix-runtime")]
fn mounted_process_plan_cwd_host_path(
    extra_preopens: &[SandboxWasiPreopen],
    step_index: usize,
    cwd: &SandboxPath,
) -> Result<Option<PathBuf>, SandboxCommandExecutionError> {
    let Some((preopen, tail)) = extra_preopens
        .iter()
        .filter_map(|preopen| {
            sandbox_path_tail_under_mount(cwd, &preopen.guest_path).map(|tail| (preopen, tail))
        })
        .max_by_key(|(preopen, _)| preopen.guest_path.as_str().len())
    else {
        return Ok(None);
    };
    let host_path = tail
        .split('/')
        .filter(|component| !component.is_empty())
        .fold(preopen.host_path.clone(), |path, component| {
            path.join(component)
        });
    reject_symlink_chain(&host_path, cwd)?;
    let metadata = fs::metadata(&host_path).map_err(|source| {
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest {
            detail: format!("process plan step {step_index} cwd `{cwd}` is unavailable: {source}"),
        })
    })?;
    if !metadata.is_dir() {
        return Err(SandboxCommandExecutionError::Wasi(
            SandboxWasiError::InvalidRequest {
                detail: format!("process plan step {step_index} cwd `{cwd}` is not a directory"),
            },
        ));
    }
    Ok(Some(host_path))
}

#[cfg(feature = "wasix-runtime")]
fn sandbox_path_tail_under_mount(path: &SandboxPath, mount_path: &SandboxPath) -> Option<String> {
    let value = path.as_str();
    let mount_value = mount_path.as_str();
    if value == mount_value {
        return Some(String::new());
    }
    let prefix = format!("{mount_value}/");
    value
        .strip_prefix(&prefix)
        .map(|tail| tail.trim_start_matches('/').to_string())
}

#[cfg(feature = "wasix-runtime")]
#[allow(clippy::too_many_arguments)]
fn rollback_wasix_process_plan_for_resource_error(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    rollback_checkpoint: &SandboxCheckpoint,
    layout: &LocalSandboxInstanceLayout,
    manifest: &SandboxInstanceManifest,
    step: &WasixProcessPlanStepRun,
    outcome: &SandboxLocalCommandOutcome,
    error: SandboxFsError,
) -> Result<Option<SandboxProcessPlanOutcome>, SandboxCommandExecutionError> {
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
            argv: step.argv.clone(),
            cwd: step.cwd.clone(),
            env: step.env.clone(),
            started_at: step.started_at,
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
    Err(error.into())
}
