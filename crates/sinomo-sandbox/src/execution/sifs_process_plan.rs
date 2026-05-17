use crate::internal::*;

#[cfg(feature = "wasix-runtime")]
use super::staging::*;

impl SifsStagedSandboxCommandRunner {
    pub fn run_toolchain_process_plan_with_policy<T: NetworkTransport>(
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

    pub fn run_toolchain_process_plan_with_policy_and_output_sink<T: NetworkTransport>(
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
        let store = self.sifs_instance_store(dfs);
        let manifest = store.load(sandbox_id)?;
        let sifs_fs = self.sifs_filesystem(store.into_inner(), &manifest);
        validate_sandbox_process_plan_working_directories(plan, &sifs_fs, &manifest.cwd)?;
        dfs = sifs_fs.into_inner();
        let execution_node_id = dfs.node_id().clone();
        let toolchain_store = SifsSandboxToolchainStore::new(dfs);
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
    fn try_run_wasix_concurrent_process_pipeline<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        policy: &SandboxPolicy,
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
        reset_local_staging_instance(local_store, &manifest)?;

        let sifs_fs = self.sifs_filesystem(sifs_store.into_inner(), &manifest);
        let (_local_manifest, local_layout) = local_store.load(sandbox_id)?;
        let mut local_fs = LocalSandboxFileSystem::new(local_layout.clone());
        sync_sandbox_workspace_roots(&sifs_fs, &mut local_fs)?;

        let mut dfs = sifs_fs.into_inner();
        let extra_preopens = stage_sifs_mounts_to_local(&mut dfs, &manifest, &local_layout)?;
        let toolchain_store = SifsSandboxToolchainStore::new(dfs);
        let toolchain_artifacts = load_sifs_toolchain_artifacts(&toolchain_store, toolchain)?;
        let toolchain_commands = load_sifs_toolchain_commands(&toolchain_store, toolchain)?;
        let dfs = toolchain_store.into_inner();
        let local_toolchain_store = stage_sifs_toolchain_for_local_process_plan(
            &local_layout,
            toolchain,
            &toolchain_artifacts,
            &toolchain_commands,
        )?;

        let process = self
            .local_runner
            .run_toolchain_process_plan_with_policy_output_sink_and_preopens(
                local_store,
                &local_toolchain_store,
                sandbox_id,
                plan,
                timeout_ms,
                &execution_node_id,
                policy,
                output_sink,
                extra_preopens,
            );

        let (_local_manifest, local_layout) = local_store.load(sandbox_id)?;
        let mut dfs = dfs;
        sync_rw_sifs_mounts_from_local(
            &mut dfs,
            &manifest,
            &local_layout,
            &self.replica_candidates,
        )?;
        let mut sifs_fs = self.sifs_filesystem(dfs, &manifest);
        let local_fs = LocalSandboxFileSystem::new(local_layout);
        match &process {
            Ok(outcome) => {
                let mut changed_paths = outcome
                    .step_outcomes
                    .iter()
                    .flat_map(|step| step.response.changed_paths.iter().cloned())
                    .collect::<Vec<_>>();
                changed_paths.sort();
                changed_paths.dedup();
                sync_sandbox_changed_paths(&local_fs, &mut sifs_fs, &changed_paths)?;
                sync_sandbox_tree(
                    &local_fs,
                    &mut sifs_fs,
                    &SandboxPath::new("/logs").expect("static logs path"),
                )?;
            }
            Err(_) => {
                sync_sandbox_tree(
                    &local_fs,
                    &mut sifs_fs,
                    &SandboxPath::new("/logs").expect("static logs path"),
                )?;
            }
        }

        let (mut local_manifest, _) = local_store.load(sandbox_id)?;
        local_manifest.last_placement = manifest.last_placement;
        let mut sifs_store = self.sifs_instance_store(sifs_fs.into_inner());
        sifs_store.save_manifest(&local_manifest)?;
        Ok((sifs_store.into_inner(), Some(process)))
    }
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

#[cfg(feature = "wasix-runtime")]
fn stage_sifs_toolchain_for_local_process_plan(
    local_layout: &LocalSandboxInstanceLayout,
    toolchain: &SandboxToolchainManifest,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<LocalSandboxToolchainStore, SandboxCommandExecutionError> {
    let local_toolchain_store =
        LocalSandboxToolchainStore::new(local_layout.root.join("sifs-process-plan-toolchain"));
    let local_toolchain_layout = local_toolchain_store.layout(&toolchain.toolchain_id);
    if local_toolchain_layout.root.exists() {
        reject_symlink_chain(
            &local_toolchain_layout.root,
            &SandboxPath::new("/toolchain").expect("static path"),
        )?;
        fs::remove_dir_all(&local_toolchain_layout.root).map_err(|source| {
            SandboxToolchainError::Io {
                path: local_toolchain_layout.root.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    }
    local_toolchain_store.save_manifest(toolchain)?;
    for (command_name, bytes) in commands {
        let command_path = local_toolchain_layout.command_module(command_name);
        if let Some(parent) = command_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxToolchainError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&command_path, bytes).map_err(|source| SandboxToolchainError::Io {
            path: command_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    for (relative_path, bytes) in artifacts {
        let artifact_path = local_toolchain_layout.artifact(relative_path)?;
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxToolchainError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&artifact_path, bytes).map_err(|source| SandboxToolchainError::Io {
            path: artifact_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(local_toolchain_store)
}
