use super::*;

#[derive(Clone)]
pub(super) struct TestTool {
    config: LocalWorkspaceAgentConfig,
}

impl TestTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for TestTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        local_named_command(&self.config, request.call, "test", "cargo test")
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::Test))
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "command": { "type": "string" },
                "timeout_ms": { "type": "integer", "minimum": 1 }
            }
        }))
}
