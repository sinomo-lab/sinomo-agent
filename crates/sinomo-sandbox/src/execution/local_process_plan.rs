use crate::internal::*;

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
}
