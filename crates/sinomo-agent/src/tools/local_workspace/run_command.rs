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
        descriptor_for_config(&self.config)
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
    permission_gated_descriptor()
}

pub(super) fn descriptor_for_config(_config: &LocalWorkspaceAgentConfig) -> AgentToolDescriptor {
    permission_gated_descriptor()
}

fn permission_gated_descriptor() -> AgentToolDescriptor {
    base_descriptor().requiring_local_permission(
        sinomo_runtime::node::LocalNodePermission::RemoteManagement.as_str(),
    )
}

fn base_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::RunShellCommand))
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {
                    "type": "string",
                    "description": "Shell command to run inside simple-sandbox with generic host command semantics. It may read or write any location permitted to the Sinomo process; workspace file-tool permissions do not restrict shell commands. Avoid pipelines for verification unless the shell status still reflects the command that matters."
                },
                "cwd": {
                    "type": "string",
                    "description": "Optional command directory. Relative paths use the primary workspace; an absolute path may select any granted workspace. Omit this to run at the primary workspace root; do not use evidence artifact paths."
                },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional timeout in milliseconds."
                }
            }
        }))
}
