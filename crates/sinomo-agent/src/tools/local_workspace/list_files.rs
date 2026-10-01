use super::*;
use serde::Serialize;

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
        let include_hidden = input_bool(request.call, "include_hidden").unwrap_or(false);
        let include_files = input_bool(request.call, "include_files").unwrap_or(true);
        let include_dirs = input_bool(request.call, "include_dirs").unwrap_or(true);
        let max_entries = input_usize(request.call, "max_entries")
            .unwrap_or(self.config.max_list_entries)
            .min(self.config.max_list_entries);
        let resolved = self.config.workspace_files().and_then(|files| {
            let file = resolve_existing_workspace_file(&files, &path)?;
            Ok((files, file))
        });
        let (files, file) = match resolved {
            Ok(resolved) => resolved,
            Err(detail) => {
                return AgentToolResult::blocked(
                    detail.clone(),
                    serde_json::json!({ "error": "path_not_allowed", "detail": detail }),
                );
            }
        };
        let mut entries = Vec::new();
        let truncated = match collect_workspace_file_entries(
            &files,
            &file,
            recursive,
            include_hidden,
            include_files,
            include_dirs,
            max_entries,
            &mut entries,
            &request.cancellation_token,
        ) {
            Ok(truncated) => truncated,
            Err(detail) => {
                if request.cancellation_token.is_cancelled() {
                    return local_tool_cancelled_result("list_files", detail);
                }
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "list_files_failed", "detail": detail }),
                );
            }
        };
        let payload = serde_json::json!({
            "path": files.display(&file),
            "recursive": recursive,
            "include_hidden": include_hidden,
            "include_files": include_files,
            "include_dirs": include_dirs,
            "max_entries": max_entries,
            "truncated": truncated,
            "count": entries.len(),
            "entries": entries,
        });
        let evidence =
            AgentToolEvidence::availability_only(true, "path resolved inside workspace grants")
                .with_output_value(&payload, truncated);
        AgentToolResult::succeeded(format!("listed {} entrie(s)", entries.len()), payload)
            .with_evidence(evidence)
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    descriptor_for_kind(AgentToolKind::ListFiles)
        .with_capabilities([AgentToolCapability::ReadWorkspace])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Directory or file to list. Relative paths use the primary workspace; absolute paths may address any granted workspace. Defaults to the primary workspace root."
                },
                "recursive": {
                    "type": "boolean",
                    "description": "Whether to recurse into subdirectories. Defaults to false."
                },
                "include_hidden": {
                    "type": "boolean",
                    "description": "Whether to include dotfiles and dot-directories. Defaults to false."
                },
                "include_files": {
                    "type": "boolean",
                    "description": "Whether to include files. Defaults to true."
                },
                "include_dirs": {
                    "type": "boolean",
                    "description": "Whether to include directories. Defaults to true."
                },
                "max_entries": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum number of entries to return. Capped by the runtime."
                }
            }
        }))
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
struct WorkspaceFileEntry {
    path: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
}

fn collect_workspace_file_entries(
    files: &WorkspaceFiles,
    file: &WorkspaceFile,
    recursive: bool,
    include_hidden: bool,
    include_files: bool,
    include_dirs: bool,
    max_entries: usize,
    entries: &mut Vec<WorkspaceFileEntry>,
    cancellation_token: &AgentCancellationToken,
) -> Result<bool, String> {
    ensure_local_tool_not_cancelled(cancellation_token, "list_files")?;
    if entries.len() >= max_entries {
        return Ok(true);
    }
    let metadata = files
        .stat(file)
        .map_err(|source| format!("failed to stat {}: {source}", files.display(file)))?
        .ok_or_else(|| format!("failed to stat {}: not found", files.display(file)))?;
    if metadata.kind != VfsObjectKind::Directory {
        maybe_push_workspace_file_entry(
            files,
            file,
            &metadata.kind,
            metadata.size_bytes,
            include_hidden,
            include_files,
            include_dirs,
            entries,
        );
        return Ok(entries.len() >= max_entries);
    }

    let dir_entries = files
        .list(file)
        .map_err(|source| format!("failed to list {}: {source}", files.display(file)))?;
    for (entry_file, entry) in dir_entries {
        ensure_local_tool_not_cancelled(cancellation_token, "list_files")?;
        if entries.len() >= max_entries {
            return Ok(true);
        }
        if !include_hidden && file_name_is_hidden(&entry.name) {
            continue;
        }
        maybe_push_workspace_file_entry(
            files,
            &entry_file,
            &entry.kind,
            entry.size_bytes,
            include_hidden,
            include_files,
            include_dirs,
            entries,
        );
        if recursive && entry.kind == VfsObjectKind::Directory {
            let truncated = collect_workspace_file_entries(
                files,
                &entry_file,
                recursive,
                include_hidden,
                include_files,
                include_dirs,
                max_entries,
                entries,
                cancellation_token,
            )?;
            if truncated {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
fn maybe_push_workspace_file_entry(
    files: &WorkspaceFiles,
    file: &WorkspaceFile,
    kind: &VfsObjectKind,
    size_bytes: Option<u64>,
    include_hidden: bool,
    include_files: bool,
    include_dirs: bool,
    entries: &mut Vec<WorkspaceFileEntry>,
) {
    if !include_hidden && file.file_name().is_some_and(file_name_is_hidden) {
        return;
    }
    let kind = match kind {
        VfsObjectKind::Symlink => "symlink",
        VfsObjectKind::Directory => "directory",
        VfsObjectKind::File => "file",
        _ => "other",
    };
    let include = match kind {
        "directory" => include_dirs,
        "file" => include_files,
        _ => include_files || include_dirs,
    };
    if !include {
        return;
    }
    entries.push(WorkspaceFileEntry {
        path: files.display(file),
        kind: kind.to_string(),
        size_bytes: if kind == "file" { size_bytes } else { None },
    });
}

fn file_name_is_hidden(name: &str) -> bool {
    name.starts_with('.') && name != "." && name != ".."
}
