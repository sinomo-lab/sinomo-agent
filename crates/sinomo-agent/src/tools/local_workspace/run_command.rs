use super::*;

#[derive(Clone)]
pub(super) struct RunCommandTool {
    config: LocalWorkspaceAgentConfig,
}

impl RunCommandTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for RunCommandTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        local_run_command_from_call(
            &self.config,
            request.session,
            request.call,
            request.cancellation_token,
            request.progress_sink,
        )
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::RunShellCommand))
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {
                    "type": "string",
                    "description": "Shell command to run. Avoid pipelines for verification unless the shell status still reflects the command that matters."
                },
                "cwd": {
                    "type": "string",
                    "description": "Optional workspace-relative directory. Omit this to run at the workspace root; do not use evidence artifact paths."
                },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional timeout in milliseconds."
                }
            }
        }))
}
