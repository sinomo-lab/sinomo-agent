use super::*;

#[derive(Clone)]
pub(super) struct ReadFileTool {
    config: LocalWorkspaceAgentConfig,
}

impl ReadFileTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ReadFileTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        let Some(path) = input_string(request.call, "path") else {
            return AgentToolResult::failed(
                "read_file requires a string path",
                serde_json::json!({ "error": "missing_path" }),
            );
        };
        let max_bytes = input_usize(request.call, "max_bytes")
            .unwrap_or(self.config.max_read_bytes)
            .min(self.config.max_read_bytes);
        let path = match resolve_existing_workspace_path(&self.config, &path) {
            Ok(path) => path,
            Err(detail) => {
                return AgentToolResult::blocked(
                    detail.clone(),
                    serde_json::json!({ "error": "path_not_allowed", "detail": detail }),
                );
            }
        };
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) => {
                return AgentToolResult::failed(
                    format!("failed to read {}: {source}", path.display()),
                    serde_json::json!({ "error": "read_failed", "detail": source.to_string() }),
                );
            }
        };
        let truncated = bytes.len() > max_bytes;
        let content = String::from_utf8_lossy(&bytes[..bytes.len().min(max_bytes)]).to_string();
        let full_output_ref = if truncated {
            write_local_evidence(&self.config, "read_file", "txt", &bytes)
        } else {
            None
        };
        let payload = serde_json::json!({
            "path": display_workspace_path(&self.config, &path),
            "bytes": bytes.len(),
            "truncated": truncated,
            "content": content,
        });
        let mut evidence =
            AgentToolEvidence::policy_only(true, "path resolved inside workspace root")
                .with_output_value(&payload, truncated);
        if let Some(full_output_ref) = full_output_ref {
            evidence = evidence.with_artifact(
                AgentToolArtifactRef::new("full_output", full_output_ref)
                    .with_media_type("text/plain"),
            );
        }
        AgentToolResult::succeeded(
            format!("read {}", display_workspace_path(&self.config, &path)),
            payload,
        )
        .with_evidence(evidence)
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::ReadFile))
        .with_capabilities([AgentToolCapability::ReadWorkspace])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": { "type": "string" },
                "max_bytes": { "type": "integer", "minimum": 1 }
            }
        }))
}
