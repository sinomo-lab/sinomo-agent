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
        let truncated = match collect_workspace_file_entries(
            &self.config,
            &path,
            recursive,
            include_hidden,
            include_files,
            include_dirs,
            max_entries,
            &mut entries,
        ) {
            Ok(truncated) => truncated,
            Err(detail) => {
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "list_files_failed", "detail": detail }),
                );
            }
        };
        let payload = serde_json::json!({
            "path": display_workspace_path(&self.config, &path),
            "recursive": recursive,
            "include_hidden": include_hidden,
            "include_files": include_files,
            "include_dirs": include_dirs,
            "max_entries": max_entries,
            "truncated": truncated,
            "count": entries.len(),
            "entries": entries,
        });
        let evidence = AgentToolEvidence::policy_only(true, "path resolved inside workspace root")
            .with_output_value(&payload, truncated);
        AgentToolResult::succeeded(format!("listed {} entrie(s)", entries.len()), payload)
            .with_evidence(evidence)
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::ListFiles))
        .with_capabilities([AgentToolCapability::ReadWorkspace])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Workspace-relative directory or file to list. Defaults to the workspace root."
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
    config: &LocalWorkspaceAgentConfig,
    path: &Path,
    recursive: bool,
    include_hidden: bool,
    include_files: bool,
    include_dirs: bool,
    max_entries: usize,
    entries: &mut Vec<WorkspaceFileEntry>,
) -> Result<bool, String> {
    if entries.len() >= max_entries {
        return Ok(true);
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| format!("failed to stat {}: {source}", path.display()))?;
    if !metadata.is_dir() {
        maybe_push_workspace_file_entry(
            config,
            path,
            &metadata,
            include_hidden,
            include_files,
            include_dirs,
            entries,
        );
        return Ok(entries.len() >= max_entries);
    }

    let mut dir_entries = fs::read_dir(path)
        .map_err(|source| format!("failed to list {}: {source}", path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| format!("failed to read entry in {}: {source}", path.display()))?;
    dir_entries.sort_by_key(|entry| entry.file_name());
    for entry in dir_entries {
        if entries.len() >= max_entries {
            return Ok(true);
        }
        let entry_path = entry.path();
        if !include_hidden && file_name_is_hidden(&entry_path) {
            continue;
        }
        let metadata = fs::symlink_metadata(&entry_path)
            .map_err(|source| format!("failed to stat {}: {source}", entry_path.display()))?;
        maybe_push_workspace_file_entry(
            config,
            &entry_path,
            &metadata,
            include_hidden,
            include_files,
            include_dirs,
            entries,
        );
        if recursive && metadata.is_dir() {
            let truncated = collect_workspace_file_entries(
                config,
                &entry_path,
                recursive,
                include_hidden,
                include_files,
                include_dirs,
                max_entries,
                entries,
            )?;
            if truncated {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn maybe_push_workspace_file_entry(
    config: &LocalWorkspaceAgentConfig,
    path: &Path,
    metadata: &fs::Metadata,
    include_hidden: bool,
    include_files: bool,
    include_dirs: bool,
    entries: &mut Vec<WorkspaceFileEntry>,
) {
    if !include_hidden && file_name_is_hidden(path) {
        return;
    }
    let file_type = metadata.file_type();
    let kind = if file_type.is_symlink() {
        "symlink"
    } else if metadata.is_dir() {
        "directory"
    } else if metadata.is_file() {
        "file"
    } else {
        "other"
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
        path: display_workspace_path(config, path),
        kind: kind.to_string(),
        size_bytes: (kind == "file").then_some(metadata.len()),
    });
}

fn file_name_is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with('.') && name != "." && name != "..")
}
