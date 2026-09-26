//! Harness-compatible editor syntax over Sinomo's workspace transaction paths.
use super::super::AgentToolRequest;
use super::*;
use serde_json::{Value, json};

pub(super) struct StrReplaceEditorTool {
    config: LocalWorkspaceAgentConfig,
}
impl StrReplaceEditorTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

fn invalid(detail: &str) -> AgentToolResult {
    AgentToolResult::failed(
        detail,
        json!({"error":"invalid_editor_input", "detail":detail}),
    )
}

impl AgentTool for StrReplaceEditorTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        let Some(path) = input_string(request.call, "path") else {
            return invalid("path is required");
        };
        let command = request.call.input["command"].as_str().unwrap_or_default();
        if !matches!(command, "view" | "create" | "str_replace" | "insert") {
            return invalid("unknown editor command");
        }
        if command != "view" && !self.config.allow_file_writes {
            return AgentToolResult::blocked(
                "workspace file writes are disabled",
                json!({"error":"file_writes_disabled"}),
            );
        }
        if request.cancellation_token.is_cancelled() {
            return local_tool_cancelled_result("str_replace_editor", "cancelled");
        }
        let path = if command != "view" && Path::new(&path).is_absolute() {
            let root = match canonical_workspace_root(&self.config) {
                Ok(root) => root,
                Err(detail) => {
                    return AgentToolResult::blocked(
                        detail.clone(),
                        json!({"error":"path_not_allowed","detail":detail}),
                    );
                }
            };
            match Path::new(&path)
                .strip_prefix(&root)
                .or_else(|_| Path::new(&path).strip_prefix(&self.config.root))
            {
                Ok(relative) => relative.to_string_lossy().into_owned(),
                Err(_) => {
                    return AgentToolResult::blocked(
                        "write path is outside the primary workspace",
                        json!({"error":"path_not_allowed"}),
                    );
                }
            }
        } else {
            path
        };
        let mut call = request.call.clone();
        call.input = json!({"path":path});
        let tool: Box<dyn AgentTool> = match command {
            "view" => {
                let resolved = match resolve_existing_workspace_path(&self.config, &path) {
                    Ok(path) => path,
                    Err(detail) => {
                        return AgentToolResult::blocked(
                            detail.clone(),
                            json!({"error":"path_not_allowed","detail":detail}),
                        );
                    }
                };
                if resolved.is_dir() {
                    return directory_view(&self.config, &resolved, &request.cancellation_token);
                }
                if let Some(range) = request
                    .call
                    .input
                    .get("view_range")
                    .filter(|v| !v.is_null())
                {
                    let Some(range) = range.as_array().filter(|r| r.len() == 2) else {
                        return invalid("view_range must contain two integers");
                    };
                    let Some(start) = range[0].as_u64().filter(|v| *v >= 1) else {
                        return invalid("view_range starts at line 1 or later");
                    };
                    let Some(end) = range[1]
                        .as_i64()
                        .filter(|v| *v == -1 || (*v >= 1 && *v as u64 >= start))
                    else {
                        return invalid("invalid view_range end");
                    };
                    call.input["start_line"] = json!(start);
                    if end != -1 {
                        call.input["end_line"] = json!(end);
                    }
                }
                Box::new(read_file::ReadFileTool::new(self.config.clone()))
            }
            "create" => {
                let Some(text) = request.call.input["file_text"].as_str() else {
                    return invalid("create requires file_text");
                };
                call.input["content"] = json!(text);
                call.input["create_parents"] = json!(true);
                Box::new(write_file::WriteFileTool::new(self.config.clone()))
            }
            "str_replace" => {
                let Some(old) = request.call.input["old_str"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                else {
                    return invalid("str_replace requires non-empty old_str");
                };
                let new = match request.call.input.get("new_str") {
                    None => "",
                    Some(Value::String(text)) => text,
                    _ => return invalid("new_str must be a string; omit it to delete"),
                };
                call.input["old_text"] = json!(old);
                call.input["new_text"] = json!(new);
                Box::new(edit_file::EditFileTool::new(self.config.clone()))
            }
            "insert" => {
                let Some(line) = request.call.input["insert_line"]
                    .as_u64()
                    .and_then(|line| usize::try_from(line).ok())
                else {
                    return invalid("insert requires a non-negative insert_line");
                };
                let Some(text) = request.call.input["new_str"].as_str() else {
                    return invalid("insert requires new_str");
                };
                return apply_patch::apply_insert_file_change(
                    &self.config,
                    path,
                    line,
                    text.into(),
                    &request.cancellation_token,
                );
            }
            _ => unreachable!(),
        };
        let start = call.input["start_line"].as_u64().unwrap_or(1);
        let mut result = tool.execute(AgentToolRequest {
            call: &call,
            ..request
        });
        if command == "view" && result.status == AgentToolStatus::Succeeded {
            if let Some(content) = result.output["content"].as_str() {
                let numbered = content
                    .lines()
                    .enumerate()
                    .map(|(i, line)| format!("{:>6}\t{line}", start + i as u64))
                    .collect::<Vec<_>>()
                    .join("\n");
                result.output["content"] = json!(numbered);
            }
            if result.output["truncated"] == true {
                let content = result.output["content"].as_str().unwrap_or_default();
                result.output["content"] = json!(format!("{content}\n<response clipped>"));
            }
            result.evidence = result.evidence.map(|evidence| {
                evidence.with_output_value(&result.output, result.output["truncated"] == true)
            });
        }
        result
    }
}

fn directory_view(
    config: &LocalWorkspaceAgentConfig,
    path: &Path,
    cancellation: &AgentCancellationToken,
) -> AgentToolResult {
    let mut pending = vec![(path.to_path_buf(), 0)];
    let mut entries = Vec::new();
    let mut truncated = false;
    while let Some((directory, depth)) = pending.pop() {
        if cancellation.is_cancelled() {
            return local_tool_cancelled_result("str_replace_editor", "cancelled");
        }
        let directory = match resolve_existing_workspace_path(config, &directory.to_string_lossy())
        {
            Ok(directory) => directory,
            Err(detail) => {
                return AgentToolResult::blocked(
                    detail.clone(),
                    json!({"error":"path_not_allowed","detail":detail}),
                );
            }
        };
        let children = match fs::read_dir(&directory) {
            Ok(children) => children,
            Err(error) => return invalid(&format!("cannot list directory: {error}")),
        };
        for child in children {
            let child = match child {
                Ok(child) => child,
                Err(error) => return invalid(&error.to_string()),
            };
            if child.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            if entries.len() >= config.max_list_entries {
                truncated = true;
                break;
            }
            let child_path = child.path();
            entries.push(display_workspace_path(config, &child_path));
            if depth == 0 && child.file_type().is_ok_and(|t| t.is_dir()) {
                pending.push((child_path, 1));
            }
        }
        if truncated {
            break;
        }
    }
    entries.sort();
    let output = json!({"entries":entries,"truncated":truncated,"content":format!("{}{}", entries.join("\n"), if truncated {"\n<response clipped>"} else {""})});
    AgentToolResult::succeeded("listed directory", output.clone()).with_evidence(
        AgentToolEvidence::availability_only(true, "directory resolved inside workspace grants")
            .with_output_value(&output, truncated),
    )
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(AgentToolKind::EditFile, "str_replace_editor",
        "View, create, uniquely replace text, or insert lines in workspace files. view numbers file lines and lists non-hidden directory entries two levels deep. create refuses existing files. str_replace requires a unique old_str; omit new_str to delete it. insert adds new_str after insert_line (0 prepends). Unused null parameters are ignored. Workspace grants and write policy apply.", true))
        .with_toolsets(["deepseek"])
        .with_invocation_modes(AgentToolInvocationMode::all())
        .with_capabilities([AgentToolCapability::ReadWorkspace, AgentToolCapability::WriteWorkspace])
        .with_input_schema(json!({"type":"object","required":["command","path"],"properties":{
            "command":{"type":"string","enum":["view","create","str_replace","insert"]},
            "path":{"type":"string"}, "file_text":{"type":["string","null"]},
            "old_str":{"type":["string","null"]}, "new_str":{"type":["string","null"]},
            "insert_line":{"type":["integer","null"],"minimum":0},
            "view_range":{"type":["array","null"],"items":{"type":"integer"},"minItems":2,"maxItems":2}
        }}))
}
