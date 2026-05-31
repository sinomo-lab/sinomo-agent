use super::*;

#[derive(Clone)]
pub(super) struct GitDiffTool {
    config: LocalWorkspaceAgentConfig,
}

impl GitDiffTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for GitDiffTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        if input_bool(request.call, "stat").unwrap_or(false) {
            local_git(&self.config, &["diff", "--stat"])
        } else {
            local_git(&self.config, &["diff", "--"])
        }
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::GitDiff))
        .with_capabilities([AgentToolCapability::InspectVersionControl])
}
