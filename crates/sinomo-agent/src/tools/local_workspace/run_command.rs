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
        local_run_command_from_call(&self.config, request.call)
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::RunShellCommand))
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": { "type": "string" },
                "timeout_ms": { "type": "integer", "minimum": 1 }
            }
        }))
}
