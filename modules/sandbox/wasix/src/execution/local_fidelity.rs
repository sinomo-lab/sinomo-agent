use crate::internal::*;

impl LocalStagedSandboxCommandRunner {
    pub fn verify_busybox_toolchain_fidelity(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        execution_node_id: &NodeId,
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
}
