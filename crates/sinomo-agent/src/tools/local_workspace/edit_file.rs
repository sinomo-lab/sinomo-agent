use super::*;

#[derive(Clone)]
pub(super) struct EditFileTool {
    config: LocalWorkspaceAgentConfig,
}

impl EditFileTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for EditFileTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        if !self.config.allow_file_writes {
            return AgentToolResult::blocked(
                "edit_file is disabled for this local workspace agent",
                serde_json::json!({
                    "error": "file_writes_disabled",
                    "tool": "edit_file",
                }),
            )
            .with_evidence(AgentToolEvidence::availability_only(
                false,
                "file writes are not enabled for this local workspace",
            ));
        }
        let Some(path) = input_string(request.call, "path") else {
            return AgentToolResult::failed(
                "edit_file requires a string path",
                serde_json::json!({ "error": "missing_path" }),
            );
        };
        let Some(old_text) =
            input_raw_string_any(request.call, &["old_text", "old_string", "oldString"])
        else {
            return AgentToolResult::failed(
                "edit_file requires old_text",
                serde_json::json!({ "error": "missing_old_text" }),
            );
        };
        let Some(new_text) = input_raw_string_any_allow_empty(
            request.call,
            &["new_text", "new_string", "newString", "replacement"],
        ) else {
            return AgentToolResult::failed(
                "edit_file requires new_text",
                serde_json::json!({ "error": "missing_new_text" }),
            );
        };
        let replace_all = input_bool(request.call, "replace_all")
            .or_else(|| input_bool(request.call, "replaceAll"))
            .unwrap_or(false);
        let expected_occurrences = input_usize(request.call, "expected_occurrences")
            .or_else(|| input_usize(request.call, "expectedOccurrences"));
        apply_patch::apply_edit_file_change(
            &self.config,
            "edit_file",
            path,
            old_text,
            new_text,
            replace_all,
            expected_occurrences,
            &request.cancellation_token,
        )
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::EditFile))
        .with_capabilities([AgentToolCapability::WriteWorkspace])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["path", "old_text", "new_text"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Workspace-relative path to edit."
                },
                "old_text": {
                    "type": "string",
                    "description": "Exact text to replace. It must be unique unless replace_all is true."
                },
                "new_text": {
                    "type": "string",
                    "description": "Replacement text. Use an empty string to delete old_text."
                },
                "replace_all": {
                    "type": "boolean",
                    "default": false,
                    "description": "Replace every occurrence of old_text instead of requiring exactly one match."
                },
                "expected_occurrences": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional guard for the number of old_text matches expected before editing."
                }
            }
        }))
}

fn input_raw_string_any(call: &AgentToolCall, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| input_raw_string(call, key))
}

fn input_raw_string_any_allow_empty(call: &AgentToolCall, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        call.input
            .get(*key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    })
}
