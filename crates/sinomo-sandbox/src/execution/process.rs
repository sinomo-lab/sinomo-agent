use crate::internal::*;

use super::SandboxCommandExecutionError;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxLocalCommandOutcome {
    pub response: SandboxCommandResponse,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_chunks: Vec<SandboxOutputChunk>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxProcessPlanStep {
    pub command_name: String,
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<SandboxPath>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub pipe_stdout_to_next: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxProcessPlan {
    pub steps: Vec<SandboxProcessPlanStep>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxProcessPlanOutcome {
    pub step_outcomes: Vec<SandboxLocalCommandOutcome>,
    pub exit_status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub fn validate_sandbox_process_plan(
    plan: &SandboxProcessPlan,
) -> Result<(), SandboxCommandExecutionError> {
    if plan.steps.is_empty() {
        return Err(SandboxCommandExecutionError::Wasi(
            SandboxWasiError::InvalidRequest {
                detail: "process plan must contain at least one step".to_string(),
            },
        ));
    }
    for (index, step) in plan.steps.iter().enumerate() {
        if step.command_name.is_empty() {
            return Err(SandboxCommandExecutionError::Wasi(
                SandboxWasiError::InvalidRequest {
                    detail: format!("process plan step {index} has an empty command name"),
                },
            ));
        }
        validate_sandbox_toolchain_argv(&step.command_name, &step.argv).map_err(|error| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest {
                detail: format!("process plan step {index}: {error}"),
            })
        })?;
        for (name, value) in &step.env {
            if !sandbox_env_name_is_valid(name) {
                return Err(SandboxCommandExecutionError::Wasi(
                    SandboxWasiError::InvalidRequest {
                        detail: format!(
                            "process plan step {index} has an invalid environment variable name `{name}`"
                        ),
                    },
                ));
            }
            if value.contains('\0') {
                return Err(SandboxCommandExecutionError::Wasi(
                    SandboxWasiError::InvalidRequest {
                        detail: format!(
                            "process plan step {index} environment variable `{name}` contains an embedded NUL byte"
                        ),
                    },
                ));
            }
        }
        if step.pipe_stdout_to_next && index + 1 >= plan.steps.len() {
            return Err(SandboxCommandExecutionError::Wasi(
                SandboxWasiError::InvalidRequest {
                    detail: format!("process plan step {index} pipes stdout but has no next step"),
                },
            ));
        }
        if index > 0 && plan.steps[index - 1].pipe_stdout_to_next && step.stdin.is_some() {
            return Err(SandboxCommandExecutionError::Wasi(
                SandboxWasiError::InvalidRequest {
                    detail: format!(
                        "process plan step {index} declares stdin while the previous step pipes stdout into it"
                    ),
                },
            ));
        }
    }
    Ok(())
}

pub fn validate_sandbox_process_plan_working_directories(
    plan: &SandboxProcessPlan,
    fs: &impl SandboxFileSystem,
    default_cwd: &SandboxPath,
) -> Result<(), SandboxCommandExecutionError> {
    for (index, step) in plan.steps.iter().enumerate() {
        let cwd = step.cwd.as_ref().unwrap_or(default_cwd);
        validate_sandbox_working_directory(fs, cwd).map_err(|error| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest {
                detail: format!("process plan step {index} cwd `{cwd}` is invalid: {error}"),
            })
        })?;
    }
    Ok(())
}

pub fn validate_sandbox_process_plan_invocations(
    plan: &SandboxProcessPlan,
    manifest: &SandboxInstanceManifest,
    toolchain: &SandboxToolchainManifest,
    execution_node_id: &NodeId,
    policy: &SandboxPolicy,
) -> Result<(), SandboxCommandExecutionError> {
    for step in &plan.steps {
        let command = toolchain.command(&step.command_name)?;
        policy
            .validate_invocation(manifest, toolchain, command, execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        if let Some(stdin) = &step.stdin {
            let limits =
                effective_sandbox_command_limits(&manifest.limits, &command.default_limits);
            let input_limit = limits.max_input_bytes;
            if stdin.len() as u64 > input_limit {
                return Err(SandboxCommandExecutionError::Wasi(
                    SandboxWasiError::InputLimitExceeded {
                        limit: input_limit.min(usize::MAX as u64) as usize,
                    },
                ));
            }
        }
    }
    Ok(())
}

pub fn validate_sandbox_toolchain_argv(
    command_name: &str,
    argv: &[String],
) -> Result<(), String> {
    let Some(argv0) = argv.first() else {
        return Err(format!(
            "toolchain command `{command_name}` must include argv[0]"
        ));
    };
    if argv0 != command_name {
        return Err(format!(
            "toolchain command `{command_name}` must use matching argv[0], got `{argv0}`"
        ));
    }
    Ok(())
}
