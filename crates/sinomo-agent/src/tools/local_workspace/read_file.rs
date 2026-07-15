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
        let start_line = input_usize(request.call, "start_line");
        let end_line = input_usize(request.call, "end_line");
        if let (Some(start_line), Some(end_line)) = (start_line, end_line)
            && end_line < start_line
        {
            return AgentToolResult::failed(
                "read_file end_line must be greater than or equal to start_line",
                serde_json::json!({
                    "error": "invalid_line_range",
                    "start_line": start_line,
                    "end_line": end_line,
                }),
            );
        }
        let path = match resolve_existing_workspace_path(&self.config, &path) {
            Ok(path) => path,
            Err(detail) => {
                return AgentToolResult::blocked(
                    detail.clone(),
                    serde_json::json!({ "error": "path_not_allowed", "detail": detail }),
                );
            }
        };
        if let Err(detail) =
            ensure_local_tool_not_cancelled(&request.cancellation_token, "read_file")
        {
            return local_tool_cancelled_result("read_file", detail);
        }
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) => {
                return AgentToolResult::failed(
                    format!("failed to read {}: {source}", path.display()),
                    serde_json::json!({ "error": "read_failed", "detail": source.to_string() }),
                );
            }
        };
        if let Err(detail) =
            ensure_local_tool_not_cancelled(&request.cancellation_token, "read_file")
        {
            return local_tool_cancelled_result("read_file", detail);
        }
        let decoded = String::from_utf8_lossy(&bytes);
        let total_lines = count_text_lines(&decoded);
        let selected = select_line_range(&decoded, start_line, end_line);
        let selected_bytes = selected.content.as_bytes();
        let truncated = selected_bytes.len() > max_bytes;
        let visible = truncate_utf8_lossy(selected_bytes, max_bytes);
        let content = visible.text;
        let full_output_ref = if truncated {
            write_local_evidence(&self.config, "read_file", "txt", selected_bytes)
        } else {
            None
        };
        let payload = serde_json::json!({
            "path": display_workspace_path(&self.config, &path),
            "bytes": bytes.len(),
            "content_bytes": selected_bytes.len(),
            "truncated": truncated,
            "content": content,
            "total_lines": total_lines,
            "line_range": selected.line_range_payload(),
        });
        let mut evidence =
            AgentToolEvidence::availability_only(true, "path resolved inside workspace grants")
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
                "path": {
                    "type": "string",
                    "description": "Path to read. Relative paths use the primary workspace; absolute paths may address any granted workspace."
                },
                "max_bytes": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum model-visible bytes to return. Larger selected content is stored as evidence outside model context."
                },
                "start_line": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional one-based inclusive start line for focused reads."
                },
                "end_line": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional one-based inclusive end line for focused reads."
                }
            }
        }))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectedReadContent {
    content: String,
    requested_start_line: Option<usize>,
    requested_end_line: Option<usize>,
    returned_start_line: Option<usize>,
    returned_end_line: Option<usize>,
}

impl SelectedReadContent {
    fn whole_file(content: &str) -> Self {
        Self {
            content: content.to_string(),
            requested_start_line: None,
            requested_end_line: None,
            returned_start_line: None,
            returned_end_line: None,
        }
    }

    fn line_range_payload(&self) -> serde_json::Value {
        match (self.requested_start_line, self.requested_end_line) {
            (None, None) => serde_json::Value::Null,
            _ => serde_json::json!({
                "requested_start_line": self.requested_start_line,
                "requested_end_line": self.requested_end_line,
                "returned_start_line": self.returned_start_line,
                "returned_end_line": self.returned_end_line,
            }),
        }
    }
}

fn select_line_range(
    content: &str,
    start_line: Option<usize>,
    end_line: Option<usize>,
) -> SelectedReadContent {
    if start_line.is_none() && end_line.is_none() {
        return SelectedReadContent::whole_file(content);
    }
    let lines = split_inclusive_text_lines(content);
    let total_lines = lines.len();
    let start = start_line.unwrap_or(1);
    let requested_end = end_line.unwrap_or(total_lines.max(start));
    if start > total_lines || total_lines == 0 {
        return SelectedReadContent {
            content: String::new(),
            requested_start_line: Some(start),
            requested_end_line: Some(requested_end),
            returned_start_line: None,
            returned_end_line: None,
        };
    }
    let returned_end = requested_end.min(total_lines);
    let content = lines[start - 1..returned_end].concat();
    SelectedReadContent {
        content,
        requested_start_line: Some(start),
        requested_end_line: Some(requested_end),
        returned_start_line: Some(start),
        returned_end_line: Some(returned_end),
    }
}

fn count_text_lines(content: &str) -> usize {
    split_inclusive_text_lines(content).len()
}

fn split_inclusive_text_lines(content: &str) -> Vec<&str> {
    if content.is_empty() {
        Vec::new()
    } else {
        content.split_inclusive('\n').collect()
    }
}
