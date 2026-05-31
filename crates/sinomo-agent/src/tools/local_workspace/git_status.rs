use super::*;

#[derive(Clone)]
pub(super) struct GitStatusTool {
    config: LocalWorkspaceAgentConfig,
}

impl GitStatusTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for GitStatusTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, _request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        local_git(&self.config, &["status", "--short"])
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::GitStatus))
        .with_capabilities([AgentToolCapability::InspectVersionControl])
}
