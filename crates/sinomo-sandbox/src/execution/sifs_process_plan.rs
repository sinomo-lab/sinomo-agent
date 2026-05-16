use crate::internal::*;

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
}
