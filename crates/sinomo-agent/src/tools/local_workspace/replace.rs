use super::*;

#[derive(Clone)]
pub(super) struct ReplaceExactTool {
    config: LocalWorkspaceAgentConfig,
}

impl ReplaceExactTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ReplaceExactTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        exact_descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        if let Some(blocked) = writes_disabled(&self.config, "replace_exact") {
            return blocked;
        }
        let Some(path) = input_string(request.call, "path") else {
            return missing_argument("replace_exact", "path");
        };
        let Some(old) = input_raw_string(request.call, "old") else {
            return missing_argument("replace_exact", "old");
        };
        let Some(new) = input_raw_string(request.call, "new") else {
            return missing_argument("replace_exact", "new");
        };
        let replace_all = input_bool(request.call, "replace_all").unwrap_or(false);
        let occurrence = input_usize(request.call, "occurrence");
        let expected_replacements = input_usize(request.call, "expected_replacements");
        let resolved = match resolve_existing_workspace_path(&self.config, &path) {
            Ok(path) => path,
            Err(detail) => return path_blocked(detail),
        };
        let old_content = match read_text_file(&resolved) {
            Ok(content) => content,
            Err(result) => return result,
        };
        let (new_content, replacements) = match replace_exact_content(
            &old_content,
            &old,
            &new,
            replace_all,
            occurrence,
            expected_replacements,
        ) {
            Ok(result) => result,
            Err(detail) => {
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "replace_exact_failed", "detail": detail }),
                );
            }
        };
        replacement_result(
            &self.config,
            "replace_exact",
            &resolved,
            &old_content,
            &new_content,
            replacements,
        )
    }
}

#[derive(Clone)]
pub(super) struct ReplaceRangeTool {
    config: LocalWorkspaceAgentConfig,
}

impl ReplaceRangeTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ReplaceRangeTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        range_descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        if let Some(blocked) = writes_disabled(&self.config, "replace_range") {
            return blocked;
        }
        let Some(path) = input_string(request.call, "path") else {
            return missing_argument("replace_range", "path");
        };
        let Some(start_line) = input_usize(request.call, "start_line") else {
            return missing_argument("replace_range", "start_line");
        };
        let Some(end_line) = input_usize(request.call, "end_line") else {
            return missing_argument("replace_range", "end_line");
        };
        let Some(replacement) = input_raw_string(request.call, "replacement") else {
            return missing_argument("replace_range", "replacement");
        };
        let resolved = match resolve_existing_workspace_path(&self.config, &path) {
            Ok(path) => path,
            Err(detail) => return path_blocked(detail),
        };
        let old_content = match read_text_file(&resolved) {
            Ok(content) => content,
            Err(result) => return result,
        };
        let new_content = match replace_line_range(&old_content, start_line, end_line, &replacement)
        {
            Ok(content) => content,
            Err(detail) => {
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "replace_range_failed", "detail": detail }),
                );
            }
        };
        replacement_result(
            &self.config,
            "replace_range",
            &resolved,
            &old_content,
            &new_content,
            1,
        )
    }
}

pub(super) fn exact_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::ReplaceExact))
        .with_capabilities([AgentToolCapability::ReadWorkspace, AgentToolCapability::WriteWorkspace])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["path", "old", "new"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Workspace-relative text file path."
                },
                "old": {
                    "type": "string",
                    "description": "Exact text snippet to replace."
                },
                "new": {
                    "type": "string",
                    "description": "Replacement text."
                },
                "replace_all": {
                    "type": "boolean",
                    "description": "Replace every occurrence. Defaults to false."
                },
                "occurrence": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "One-based occurrence to replace when the old snippet appears multiple times."
                },
                "expected_replacements": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional exact number of replacements expected."
                }
            }
        }))
}

pub(super) fn range_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::ReplaceRange))
        .with_capabilities([
            AgentToolCapability::ReadWorkspace,
            AgentToolCapability::WriteWorkspace,
        ])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["path", "start_line", "end_line", "replacement"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Workspace-relative text file path."
                },
                "start_line": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "One-based inclusive start line."
                },
                "end_line": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "One-based inclusive end line."
                },
                "replacement": {
                    "type": "string",
                    "description": "Replacement text for the selected line range."
                }
            }
        }))
}

fn writes_disabled(config: &LocalWorkspaceAgentConfig, tool_name: &str) -> Option<AgentToolResult> {
    (!config.allow_file_writes).then(|| {
        AgentToolResult::blocked(
            format!("{tool_name} is disabled for this local workspace agent"),
            serde_json::json!({
                "error": "file_writes_disabled",
                "tool": tool_name,
            }),
        )
        .with_evidence(AgentToolEvidence::policy_only(
            false,
            "file writes disabled by local workspace policy",
        ))
    })
}

fn missing_argument(tool_name: &str, argument: &str) -> AgentToolResult {
    AgentToolResult::failed(
        format!("{tool_name} requires `{argument}`"),
        serde_json::json!({
            "error": "missing_argument",
            "tool": tool_name,
            "argument": argument,
        }),
    )
}

fn path_blocked(detail: String) -> AgentToolResult {
    AgentToolResult::blocked(
        detail.clone(),
        serde_json::json!({ "error": "path_not_allowed", "detail": detail }),
    )
}

fn read_text_file(path: &Path) -> Result<String, AgentToolResult> {
    let bytes = fs::read(path).map_err(|source| {
        AgentToolResult::failed(
            format!("failed to read {}: {source}", path.display()),
            serde_json::json!({
                "error": "read_failed",
                "detail": source.to_string(),
            }),
        )
    })?;
    String::from_utf8(bytes).map_err(|source| {
        AgentToolResult::failed(
            format!("{} is not valid UTF-8 text", path.display()),
            serde_json::json!({
                "error": "non_utf8_file",
                "detail": source.to_string(),
            }),
        )
    })
}

fn replace_exact_content(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
    occurrence: Option<usize>,
    expected_replacements: Option<usize>,
) -> Result<(String, usize), String> {
    if old.is_empty() {
        return Err("replace_exact old snippet must not be empty".to_string());
    }
    if replace_all && occurrence.is_some() {
        return Err("replace_exact cannot use both replace_all and occurrence".to_string());
    }
    let matches = content
        .match_indices(old)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return Err("replace_exact old snippet was not found".to_string());
    }
    let (updated, replacements) = if replace_all {
        (content.replace(old, new), matches.len())
    } else if let Some(occurrence) = occurrence {
        let Some(start) = matches.get(occurrence - 1).copied() else {
            return Err(format!(
                "replace_exact occurrence {occurrence} was requested but only {} match(es) exist",
                matches.len()
            ));
        };
        (
            replace_range_bytes(content, start, start + old.len(), new),
            1,
        )
    } else {
        if matches.len() != 1 {
            return Err(format!(
                "replace_exact old snippet matched {} times; set occurrence or replace_all",
                matches.len()
            ));
        }
        let start = matches[0];
        (
            replace_range_bytes(content, start, start + old.len(), new),
            1,
        )
    };
    if let Some(expected) = expected_replacements
        && replacements != expected
    {
        return Err(format!(
            "replace_exact made {replacements} replacement(s), expected {expected}"
        ));
    }
    Ok((updated, replacements))
}

fn replace_range_bytes(content: &str, start: usize, end: usize, replacement: &str) -> String {
    let mut updated = String::with_capacity(content.len() - (end - start) + replacement.len());
    updated.push_str(&content[..start]);
    updated.push_str(replacement);
    updated.push_str(&content[end..]);
    updated
}

fn replace_line_range(
    content: &str,
    start_line: usize,
    end_line: usize,
    replacement: &str,
) -> Result<String, String> {
    if end_line < start_line {
        return Err(
            "replace_range end_line must be greater than or equal to start_line".to_string(),
        );
    }
    let spans = line_spans(content);
    if spans.is_empty() {
        return Err("replace_range cannot replace lines in an empty file".to_string());
    }
    if start_line > spans.len() || end_line > spans.len() {
        return Err(format!(
            "replace_range requested lines {start_line}-{end_line}, but file has {} line(s)",
            spans.len()
        ));
    }
    let start = spans[start_line - 1].0;
    let end = spans[end_line - 1].1;
    let replacement = normalize_replacement_line_endings(content, replacement);
    Ok(replace_range_bytes(content, start, end, &replacement))
}

fn line_spans(content: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    for (index, ch) in content.char_indices() {
        if ch == '\n' {
            spans.push((start, index + ch.len_utf8()));
            start = index + ch.len_utf8();
        }
    }
    if start < content.len() {
        spans.push((start, content.len()));
    }
    spans
}

fn normalize_replacement_line_endings(original: &str, replacement: &str) -> String {
    let normalized = replacement.replace("\r\n", "\n");
    if original.contains("\r\n") {
        normalized.replace('\n', "\r\n")
    } else {
        normalized
    }
}

fn replacement_result(
    config: &LocalWorkspaceAgentConfig,
    tool_name: &str,
    path: &Path,
    old_content: &str,
    new_content: &str,
    replacements: usize,
) -> AgentToolResult {
    if old_content == new_content {
        return AgentToolResult::failed(
            format!("{tool_name} produced no file change"),
            serde_json::json!({ "error": "no_change" }),
        );
    }
    if let Err(source) = fs::write(path, new_content) {
        return AgentToolResult::failed(
            format!("failed to write {}: {source}", path.display()),
            serde_json::json!({
                "error": "write_failed",
                "detail": source.to_string(),
            }),
        );
    }
    let display_path = display_workspace_path(config, path);
    let diff = render_replace_diff(&display_path, old_content, new_content);
    let diff_ref = write_local_evidence(config, tool_name, "diff", diff.as_bytes());
    let diff_summary = format!("M {display_path}");
    let output = serde_json::json!({
        "path": &display_path,
        "replacements": replacements,
        "changed_files": [&display_path],
        "diff_ref": &diff_ref,
        "diff": {
            "files": [{
                "status": "M",
                "path": &display_path,
                "added_lines": count_replace_lines(new_content),
                "removed_lines": count_replace_lines(old_content),
                "diff": &diff,
            }],
            "unified": &diff,
        },
    });
    let mut evidence = AgentToolEvidence::policy_only(true, "edited workspace text file")
        .with_output_value(&output, false)
        .with_record(
            AGENT_CODING_EVIDENCE_KIND,
            &AgentCodingToolEvidence {
                diff_ref: diff_ref.clone(),
                diff_summary: Some(diff_summary.clone()),
                changed_files: vec![display_path.clone()],
                ..AgentCodingToolEvidence::default()
            },
        );
    if let Some(diff_ref) = diff_ref {
        evidence = evidence.with_artifact(
            AgentToolArtifactRef::new("diff", diff_ref).with_media_type("text/x-diff"),
        );
    }
    AgentToolResult::succeeded(diff_summary, output).with_evidence(evidence)
}

fn render_replace_diff(path: &str, old_content: &str, new_content: &str) -> String {
    const MAX_DIFF_BYTES: usize = 128 * 1024;
    let mut diff = format!("--- a/{path}\n+++ b/{path}\n@@\n");
    push_prefixed_lines(&mut diff, '-', old_content, MAX_DIFF_BYTES);
    push_prefixed_lines(&mut diff, '+', new_content, MAX_DIFF_BYTES);
    if diff.len() > MAX_DIFF_BYTES {
        diff.truncate(MAX_DIFF_BYTES);
        diff.push_str("\n... diff truncated ...\n");
    }
    diff
}

fn push_prefixed_lines(diff: &mut String, prefix: char, content: &str, max_bytes: usize) {
    for line in content.lines() {
        if diff.len() > max_bytes {
            return;
        }
        diff.push(prefix);
        diff.push_str(line);
        diff.push('\n');
    }
    if !content.ends_with('\n') && !content.is_empty() && diff.len() <= max_bytes {
        diff.push_str("\\ No newline at end of file\n");
    }
}

fn count_replace_lines(content: &str) -> usize {
    if content.is_empty() {
        0
    } else {
        content.lines().count()
    }
}
