use super::*;

#[derive(Clone)]
pub(super) struct ListFilesTool {
    config: LocalWorkspaceAgentConfig,
}

impl ListFilesTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ListFilesTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        let path = input_string(request.call, "path").unwrap_or_else(|| ".".to_string());
        let recursive = input_bool(request.call, "recursive").unwrap_or(false);
        let max_entries = input_usize(request.call, "max_entries")
            .unwrap_or(self.config.max_list_entries)
            .min(self.config.max_list_entries);
        let path = match resolve_existing_workspace_path(&self.config, &path) {
            Ok(path) => path,
            Err(detail) => {
                return AgentToolResult::blocked(
                    detail.clone(),
                    serde_json::json!({ "error": "path_not_allowed", "detail": detail }),
                );
            }
        };
        let mut entries = Vec::new();
        let truncated = match collect_workspace_entries(
            &self.config,
            &path,
            recursive,
            max_entries,
            &mut entries,
        ) {
            Ok(truncated) => truncated,
            Err(detail) => {
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "list_failed", "detail": detail }),
                );
            }
        };
        let payload = serde_json::json!({
            "path": display_workspace_path(&self.config, &path),
            "recursive": recursive,
            "truncated": truncated,
            "entries": entries,
        });
        let evidence = AgentToolEvidence::policy_only(true, "path resolved inside workspace root")
            .with_output_value(&payload, truncated);
        AgentToolResult::succeeded(format!("listed {} entries", entries.len()), payload)
            .with_evidence(evidence)
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::ListFiles))
        .with_capabilities([AgentToolCapability::ReadWorkspace])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "recursive": { "type": "boolean" },
                "max_entries": { "type": "integer", "minimum": 1 }
            }
        }))
}
