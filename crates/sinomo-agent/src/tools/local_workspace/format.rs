use super::*;

#[derive(Clone)]
pub(super) struct FormatTool {
    config: LocalWorkspaceAgentConfig,
}

impl FormatTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for FormatTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        local_named_command(&self.config, request.call, "format", "cargo fmt --all")
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::Format))
        .with_capabilities([
            AgentToolCapability::WriteWorkspace,
            AgentToolCapability::ExecuteProcess,
        ])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "command": { "type": "string" },
                "timeout_ms": { "type": "integer", "minimum": 1 }
            }
        }))
}
