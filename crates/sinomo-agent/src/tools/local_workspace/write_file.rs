use super::*;

#[derive(Clone)]
pub(super) struct WriteFileTool {
    config: LocalWorkspaceAgentConfig,
}

impl WriteFileTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for WriteFileTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        if !self.config.allow_file_writes {
            return AgentToolResult::blocked(
                "write_file is disabled for this local workspace agent",
                serde_json::json!({
                    "error": "file_writes_disabled",
                    "tool": "write_file",
                }),
            )
            .with_evidence(AgentToolEvidence::availability_only(
                false,
                "file writes are not enabled for this local workspace",
            ));
        }
        let Some(path) = input_string(request.call, "path") else {
            return AgentToolResult::failed(
                "write_file requires a string path",
                serde_json::json!({ "error": "missing_path" }),
            );
        };
        let Some(content) = input_content(request.call) else {
            return AgentToolResult::failed(
                "write_file requires content",
                serde_json::json!({ "error": "missing_content" }),
            );
        };
        let overwrite = input_bool(request.call, "overwrite").unwrap_or(false);
        let create_parents = input_bool(request.call, "create_parents")
            .or_else(|| input_bool(request.call, "createParents"))
            .unwrap_or(false);
        apply_patch::apply_write_file_change(
            &self.config,
            "write_file",
            path,
            content,
            overwrite,
            create_parents,
            &request.cancellation_token,
        )
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::WriteFile))
        .with_capabilities([AgentToolCapability::WriteWorkspace])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["path", "content"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Workspace-relative path to create or replace."
                },
                "content": {
                    "type": "string",
                    "description": "Complete file content. This replaces the whole file when overwrite=true."
                },
                "overwrite": {
                    "type": "boolean",
                    "default": false,
                    "description": "Allow replacing an existing file. Defaults to false."
                },
                "create_parents": {
                    "type": "boolean",
                    "default": false,
                    "description": "Create missing parent directories. Defaults to false."
                }
            }
        }))
}

fn input_content(call: &AgentToolCall) -> Option<String> {
    call.input
        .get("content")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}
