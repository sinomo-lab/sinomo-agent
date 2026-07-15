use super::*;

#[derive(Clone)]
pub(super) struct SearchFilesTool {
    config: LocalWorkspaceAgentConfig,
}

impl SearchFilesTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for SearchFilesTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        let Some(query) = input_string(request.call, "query") else {
            return AgentToolResult::failed(
                "search requires a string query",
                serde_json::json!({ "error": "missing_query" }),
            );
        };
        if query.is_empty() {
            return AgentToolResult::failed(
                "search query must not be empty",
                serde_json::json!({ "error": "empty_query" }),
            );
        }
        let path = input_string(request.call, "path").unwrap_or_else(|| ".".to_string());
        let case_sensitive = input_bool(request.call, "case_sensitive").unwrap_or(false);
        let max_matches = input_usize(request.call, "max_matches")
            .unwrap_or(self.config.max_search_matches)
            .min(self.config.max_search_matches);
        let path = match resolve_existing_workspace_path(&self.config, &path) {
            Ok(path) => path,
            Err(detail) => {
                return AgentToolResult::blocked(
                    detail.clone(),
                    serde_json::json!({ "error": "path_not_allowed", "detail": detail }),
                );
            }
        };
        let mut matches = Vec::new();
        let truncated = match collect_workspace_search_matches(
            &self.config,
            &path,
            &query,
            case_sensitive,
            max_matches,
            &mut matches,
            &request.cancellation_token,
        ) {
            Ok(truncated) => truncated,
            Err(detail) => {
                if request.cancellation_token.is_cancelled() {
                    return local_tool_cancelled_result("search", detail);
                }
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "search_failed", "detail": detail }),
                );
            }
        };
        let payload = serde_json::json!({
            "path": display_workspace_path(&self.config, &path),
            "query": query,
            "case_sensitive": case_sensitive,
            "truncated": truncated,
            "matches": matches,
        });
        let evidence =
            AgentToolEvidence::availability_only(true, "path resolved inside workspace grants")
                .with_output_value(&payload, truncated);
        AgentToolResult::succeeded(format!("found {} match(es)", matches.len()), payload)
            .with_evidence(evidence)
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::SearchFiles))
        .with_capabilities([AgentToolCapability::ReadWorkspace])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": { "type": "string" },
                "path": {
                    "type": "string",
                    "description": "Search root. Relative paths use the primary workspace; absolute paths may address any granted workspace."
                },
                "case_sensitive": { "type": "boolean" },
                "max_matches": { "type": "integer", "minimum": 1 }
            }
        }))
}
