use super::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Component;

const APPLY_PATCH_LARK_GRAMMAR: &str = r#"start: begin_patch hunk+ end_patch

begin_patch: "*** Begin Patch" LF
end_patch: "*** End Patch" LF?

hunk: add_hunk | delete_hunk | update_hunk

add_hunk: "*** Add File: " filename LF add_line+
delete_hunk: "*** Delete File: " filename LF
update_hunk: "*** Update File: " filename LF change_move? change?

filename: /(.+)/
add_line: "+" /(.*)/ LF -> line

change_move: "*** Move to: " filename LF
change: (change_context | change_line)+ eof_line?
change_context: ("@@" | "@@ " /(.+)/) LF
change_line: ("+" | "-" | " ") /(.*)/ LF
eof_line: "*** End of File" LF

%import common.LF
"#;

#[derive(Clone)]
pub(super) struct ApplyPatchTool {
    config: LocalWorkspaceAgentConfig,
}

impl ApplyPatchTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ApplyPatchTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        if !self.config.allow_file_writes {
            return AgentToolResult::blocked(
                "apply_patch is disabled for this local workspace agent",
                serde_json::json!({
                    "error": "file_writes_disabled",
                    "tool": "apply_patch",
                }),
            )
            .with_evidence(AgentToolEvidence::availability_only(
                false,
                "file writes are not enabled for this local workspace",
            ));
        }
        if let Err(detail) =
            ensure_local_tool_not_cancelled(&request.cancellation_token, "apply_patch")
        {
            return local_tool_cancelled_result("apply_patch", detail);
        }

        if let Some(changes) = request
            .call
            .input
            .get("changes")
            .or_else(|| request.call.input.get("edits"))
            .and_then(serde_json::Value::as_array)
        {
            let patch_bytes = match serde_json::to_vec_pretty(changes) {
                Ok(bytes) => bytes,
                Err(source) => {
                    return AgentToolResult::failed(
                        format!("failed to serialize structured patch changes: {source}"),
                        serde_json::json!({
                            "error": "structured_patch_serialization_failed",
                            "detail": source.to_string(),
                        }),
                    );
                }
            };
            if patch_bytes.len() > self.config.max_patch_bytes {
                return patch_too_large(patch_bytes.len(), self.config.max_patch_bytes);
            }
            let patch_ref = write_local_evidence(&self.config, "apply_patch", "json", &patch_bytes);
            return apply_structured_json_changes(
                &self.config,
                changes,
                patch_ref,
                &request.cancellation_token,
            );
        }

        let Some(patch) = input_patch_string(request.call) else {
            return AgentToolResult::failed(
                "apply_patch requires a raw string patch, a patch string field, or a structured changes array",
                serde_json::json!({ "error": "missing_patch" }),
            );
        };
        if patch.len() > self.config.max_patch_bytes {
            return patch_too_large(patch.len(), self.config.max_patch_bytes);
        }
        let patch_bytes = patch.as_bytes();
        let patch_ref = write_local_evidence(&self.config, "apply_patch", "patch", patch_bytes);
        apply_freeform_patch(&self.config, &patch, patch_ref, &request.cancellation_token)
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::ApplyPatch))
        .with_capabilities([AgentToolCapability::WriteWorkspace])
        .with_toolsets(["provider_specific", "coding_advanced"])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "anyOf": [
                { "required": ["patch"] },
                { "required": ["changes"] },
                { "required": ["edits"] }
            ],
            "properties": {
                "patch": {
                    "type": "string",
                    "description": "Patch text beginning with *** Begin Patch and ending with *** End Patch. Use Add File, Update File, Delete File, and Move to hunks; update hunks use @@ context plus space/-/+ lines, not git unified range headers."
                },
                "changes": {
                    "type": "array",
                    "description": "Structured edit operations. Supported actions: add_file, delete_file, update_file/replace_file for full-file replacement, move_file, replace_exact for one exact text occurrence, and replace_range for one-based inclusive line ranges.",
                    "items": { "type": "object" }
                },
                "edits": {
                    "type": "array",
                    "description": "Alias for changes.",
                    "items": { "type": "object" }
                }
            }
        }))
        .with_provider_specs([AgentToolProviderSpec::custom_grammar(
            "openai",
            "lark",
            APPLY_PATCH_LARK_GRAMMAR,
        )
        .with_raw_input_argument("patch")])
}

fn input_patch_string(call: &AgentToolCall) -> Option<String> {
    call.input
        .as_str()
        .map(str::to_string)
        .filter(|value| !value.is_empty())
        .or_else(|| input_raw_string(call, "patch"))
}

fn patch_too_large(bytes: usize, max_bytes: usize) -> AgentToolResult {
    AgentToolResult::blocked(
        format!("apply_patch input is {bytes} bytes, above the configured {max_bytes} byte limit"),
        serde_json::json!({
            "error": "patch_too_large",
            "bytes": bytes,
            "max_bytes": max_bytes,
        }),
    )
}

pub(super) fn apply_edit_file_change(
    config: &LocalWorkspaceAgentConfig,
    tool_name: &str,
    path: String,
    old_text: String,
    new_text: String,
    replace_all: bool,
    expected_occurrences: Option<usize>,
    cancellation_token: &AgentCancellationToken,
) -> AgentToolResult {
    let expected_occurrences = if replace_all {
        expected_occurrences
    } else {
        Some(expected_occurrences.unwrap_or(1))
    };
    let changes = vec![StructuredPatchChange::ReplaceText {
        path,
        old: old_text,
        new: new_text,
        replace_all,
        expected_occurrences,
    }];
    apply_structured_changes(
        config,
        changes,
        None,
        tool_name,
        tool_name,
        cancellation_token,
    )
}

pub(super) fn apply_write_file_change(
    config: &LocalWorkspaceAgentConfig,
    tool_name: &str,
    path: String,
    content: String,
    overwrite: bool,
    create_parents: bool,
    cancellation_token: &AgentCancellationToken,
) -> AgentToolResult {
    if let Err(detail) = ensure_local_tool_not_cancelled(cancellation_token, tool_name) {
        return local_tool_cancelled_result(tool_name, detail);
    }
    let root = match canonical_workspace_root(config) {
        Ok(root) => root,
        Err(detail) => {
            return workspace_edit_preflight_failed(
                tool_name,
                "workspace_root_not_allowed",
                detail,
            );
        }
    };
    let target = match resolve_virtual_write_path(&root, &path) {
        Ok(target) => target,
        Err(detail) => {
            return workspace_edit_preflight_failed(tool_name, "path_not_allowed", detail);
        }
    };
    let exists = target.symlink_metadata().is_ok();
    if exists && !overwrite {
        return workspace_edit_preflight_failed(
            tool_name,
            "target_exists",
            format!("write_file target `{path}` already exists; pass overwrite=true to replace it"),
        );
    }
    if !create_parents
        && let Some(parent) = target.parent()
        && !parent.exists()
    {
        return workspace_edit_preflight_failed(
            tool_name,
            "parent_missing",
            format!(
                "parent directory for `{path}` does not exist; pass create_parents=true to create it"
            ),
        );
    }

    let change = if exists {
        StructuredPatchChange::Replace { path, content }
    } else {
        StructuredPatchChange::Add { path, content }
    };
    apply_structured_changes(
        config,
        vec![change],
        None,
        tool_name,
        tool_name,
        cancellation_token,
    )
}

fn workspace_edit_preflight_failed(
    tool_name: &str,
    error: &str,
    detail: impl Into<String>,
) -> AgentToolResult {
    let detail = detail.into();
    let summary = format!("{tool_name} verification failed: {detail}");
    let payload = serde_json::json!({
        "error": error,
        "detail": detail,
        "message": summary,
        "changed_files": [],
        "operations": [],
    });
    let evidence = AgentToolEvidence::availability_only(true, "workspace edit preflight failed")
        .with_output_value(&payload, false)
        .with_record(
            AGENT_CODING_EVIDENCE_KIND,
            &AgentCodingToolEvidence::default(),
        );
    AgentToolResult::failed(summary, payload).with_evidence(evidence)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StructuredPatchChange {
    Add {
        path: String,
        content: String,
    },
    Delete {
        path: String,
    },
    Update {
        path: String,
        move_to: Option<String>,
        hunks: Vec<StructuredPatchHunk>,
    },
    Replace {
        path: String,
        content: String,
    },
    Move {
        path: String,
        move_to: String,
    },
    ReplaceExact {
        path: String,
        old: String,
        new: String,
    },
    ReplaceText {
        path: String,
        old: String,
        new: String,
        replace_all: bool,
        expected_occurrences: Option<usize>,
    },
    ReplaceRange {
        path: String,
        start_line: usize,
        end_line: usize,
        content: String,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct StructuredPatchHunk {
    change_context: Option<String>,
    old_lines: Vec<String>,
    new_lines: Vec<String>,
    is_end_of_file: bool,
}

fn apply_freeform_patch(
    config: &LocalWorkspaceAgentConfig,
    patch: &str,
    patch_ref: Option<String>,
    cancellation_token: &AgentCancellationToken,
) -> AgentToolResult {
    if let Err(detail) = ensure_local_tool_not_cancelled(cancellation_token, "apply_patch") {
        return local_tool_cancelled_result("apply_patch", detail);
    }
    let changes = match parse_freeform_patch(patch) {
        Ok(changes) => changes,
        Err(detail) => {
            let summary = format_patch_failure_summary("apply_patch", &detail);
            return with_patch_evidence(
                AgentToolResult::failed(
                    summary.clone(),
                    serde_json::json!({
                        "error": "freeform_patch_parse_failed",
                        "detail": detail,
                        "message": summary,
                    }),
                ),
                patch_ref,
                None,
                summary,
            );
        }
    };
    apply_structured_changes(
        config,
        changes,
        patch_ref,
        "freeform",
        "apply_patch",
        cancellation_token,
    )
}

fn apply_structured_json_changes(
    config: &LocalWorkspaceAgentConfig,
    values: &[serde_json::Value],
    patch_ref: Option<String>,
    cancellation_token: &AgentCancellationToken,
) -> AgentToolResult {
    let mut changes = Vec::new();
    for value in values {
        if let Err(detail) = ensure_local_tool_not_cancelled(cancellation_token, "apply_patch") {
            return local_tool_cancelled_result("apply_patch", detail);
        }
        let action = value
            .get("action")
            .or_else(|| value.get("kind"))
            .or_else(|| value.get("type"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let path = value
            .get("path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        let change = match action {
            "add_file" | "add" => StructuredPatchChange::Add {
                path,
                content: value
                    .get("content")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            },
            "delete_file" | "delete" => StructuredPatchChange::Delete { path },
            "update_file" | "replace_file" | "replace" => StructuredPatchChange::Replace {
                path,
                content: value
                    .get("content")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            },
            "move_file" | "move" => StructuredPatchChange::Move {
                path,
                move_to: value
                    .get("new_path")
                    .or_else(|| value.get("move_to"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            },
            "replace_exact" | "replace_text" => {
                let old = match required_structured_string(
                    value,
                    &["old", "old_text", "expected"],
                    action,
                ) {
                    Ok(value) => value,
                    Err(detail) => {
                        return structured_patch_change_error(detail, patch_ref);
                    }
                };
                let new = match required_structured_string(
                    value,
                    &["new", "new_text", "replacement", "content"],
                    action,
                ) {
                    Ok(value) => value,
                    Err(detail) => {
                        return structured_patch_change_error(detail, patch_ref);
                    }
                };
                StructuredPatchChange::ReplaceExact { path, old, new }
            }
            "replace_range" | "replace_lines" => {
                let start_line =
                    match required_structured_usize(value, &["start_line", "start"], action) {
                        Ok(value) => value,
                        Err(detail) => {
                            return structured_patch_change_error(detail, patch_ref);
                        }
                    };
                let end_line = match required_structured_usize(value, &["end_line", "end"], action)
                {
                    Ok(value) => value,
                    Err(detail) => {
                        return structured_patch_change_error(detail, patch_ref);
                    }
                };
                StructuredPatchChange::ReplaceRange {
                    path,
                    start_line,
                    end_line,
                    content: value
                        .get("content")
                        .or_else(|| value.get("replacement"))
                        .or_else(|| value.get("new"))
                        .or_else(|| value.get("new_text"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                }
            }
            other => {
                let detail = format!("unsupported structured patch action `{other}`");
                let summary = format_patch_failure_summary("apply_patch", &detail);
                return with_patch_evidence(
                    AgentToolResult::failed(
                        summary.clone(),
                        serde_json::json!({
                            "error": "unsupported_structured_patch_action",
                            "action": other,
                            "message": summary,
                        }),
                    ),
                    patch_ref,
                    None,
                    summary,
                );
            }
        };
        changes.push(change);
    }
    apply_structured_changes(
        config,
        changes,
        patch_ref,
        "structured",
        "apply_patch",
        cancellation_token,
    )
}

fn structured_patch_change_error(detail: String, patch_ref: Option<String>) -> AgentToolResult {
    let summary = format_patch_failure_summary("apply_patch", &detail);
    with_patch_evidence(
        AgentToolResult::failed(
            summary.clone(),
            serde_json::json!({
                "error": "invalid_structured_patch_change",
                "detail": detail,
                "message": summary,
            }),
        ),
        patch_ref,
        None,
        summary,
    )
}

fn required_structured_string(
    value: &serde_json::Value,
    fields: &[&str],
    action: &str,
) -> Result<String, String> {
    fields
        .iter()
        .find_map(|field| {
            value
                .get(*field)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| {
            format!(
                "structured patch action `{action}` requires one of: {}",
                fields.join(", ")
            )
        })
}

fn required_structured_usize(
    value: &serde_json::Value,
    fields: &[&str],
    action: &str,
) -> Result<usize, String> {
    let Some(raw) = fields.iter().find_map(|field| value.get(*field)) else {
        return Err(format!(
            "structured patch action `{action}` requires one of: {}",
            fields.join(", ")
        ));
    };
    match raw {
        serde_json::Value::Number(number) => number
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| format!("structured patch action `{action}` line value is invalid")),
        serde_json::Value::String(text) => text.parse::<usize>().map_err(|source| {
            format!("structured patch action `{action}` line value `{text}` is invalid: {source}")
        }),
        _ => Err(format!(
            "structured patch action `{action}` line value must be a positive integer"
        )),
    }
}

fn apply_structured_changes(
    config: &LocalWorkspaceAgentConfig,
    changes: Vec<StructuredPatchChange>,
    patch_ref: Option<String>,
    patch_format: &str,
    tool_name: &str,
    cancellation_token: &AgentCancellationToken,
) -> AgentToolResult {
    if let Err(detail) = ensure_local_tool_not_cancelled(cancellation_token, tool_name) {
        return local_tool_cancelled_result(tool_name, detail);
    }
    let root = match canonical_workspace_root(config) {
        Ok(root) => root,
        Err(detail) => {
            let summary = format_patch_failure_summary(tool_name, &detail);
            return with_patch_evidence(
                AgentToolResult::blocked(
                    summary.clone(),
                    serde_json::json!({
                        "error": "workspace_root_not_allowed",
                        "detail": detail,
                        "message": summary,
                    }),
                ),
                patch_ref,
                None,
                summary,
            );
        }
    };
    let plan = match plan_structured_changes(
        &root,
        config,
        changes,
        patch_format,
        tool_name,
        cancellation_token,
    ) {
        Ok(plan) => plan,
        Err(detail) => {
            if cancellation_token.is_cancelled() {
                return local_tool_cancelled_result(tool_name, detail);
            }
            return with_patch_evidence(
                patch_failure_result(
                    tool_name,
                    patch_format,
                    &root,
                    &format!("{tool_name} preflight failed"),
                    &detail,
                ),
                patch_ref,
                None,
                format_patch_failure_summary(tool_name, &detail),
            );
        }
    };
    if let Err(detail) = ensure_local_tool_not_cancelled(cancellation_token, tool_name) {
        return local_tool_cancelled_result(tool_name, detail);
    }
    let diff_ref = write_local_evidence(config, tool_name, "diff", plan.unified_diff.as_bytes());
    if let Err(detail) = apply_planned_patch(config, &plan, tool_name, cancellation_token) {
        if cancellation_token.is_cancelled() {
            return local_tool_cancelled_result(tool_name, detail);
        }
        return with_patch_evidence(
            patch_result(
                AgentToolStatus::Failed,
                format_patch_failure_summary(tool_name, &detail),
                config,
                &plan,
                &root,
                &format!("{tool_name} failed during mutation"),
                diff_ref.clone(),
            ),
            patch_ref,
            diff_ref,
            format_patch_failure_summary(tool_name, &detail),
        );
    }
    let summary = format_patch_success_summary(&plan.file_diffs);
    with_patch_evidence(
        patch_result(
            AgentToolStatus::Succeeded,
            summary.clone(),
            config,
            &plan,
            &root,
            "structured patch applied inside workspace root",
            diff_ref.clone(),
        ),
        patch_ref,
        diff_ref,
        summary,
    )
}

#[derive(Clone, Debug)]
struct PlannedPatch {
    root: PathBuf,
    patch_format: String,
    operations: Vec<serde_json::Value>,
    file_changes: Vec<PlannedFileChange>,
    file_diffs: Vec<PatchFileDiff>,
    changed_files: Vec<String>,
    unified_diff: String,
}

#[derive(Clone, Debug)]
struct PlannedFileChange {
    path: PathBuf,
    display_path: String,
    status: PatchFileStatus,
    old_content: Option<String>,
    new_content: Option<String>,
    mode_source: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PatchFileStatus {
    Added,
    Modified,
    Deleted,
}

impl PatchFileStatus {
    fn code(self) -> &'static str {
        match self {
            PatchFileStatus::Added => "A",
            PatchFileStatus::Modified => "M",
            PatchFileStatus::Deleted => "D",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct PatchFileDiff {
    status: String,
    path: String,
    added_lines: usize,
    removed_lines: usize,
    diff: String,
}

#[derive(Clone, Debug)]
struct VirtualFileState {
    content: String,
    display_path: String,
    mode_source: Option<PathBuf>,
}

struct PatchPreflight<'a> {
    root: &'a Path,
    config: &'a LocalWorkspaceAgentConfig,
    files: HashMap<PathBuf, Option<VirtualFileState>>,
    original_content: HashMap<PathBuf, Option<String>>,
    original_display_paths: HashMap<PathBuf, String>,
    operations: Vec<serde_json::Value>,
}

impl<'a> PatchPreflight<'a> {
    fn new(root: &'a Path, config: &'a LocalWorkspaceAgentConfig) -> Self {
        Self {
            root,
            config,
            files: HashMap::new(),
            original_content: HashMap::new(),
            original_display_paths: HashMap::new(),
            operations: Vec::new(),
        }
    }

    fn apply_change(&mut self, change: StructuredPatchChange) -> Result<(), String> {
        match change {
            StructuredPatchChange::Add { path, content } => self.add_file(path, content),
            StructuredPatchChange::Delete { path } => self.delete_file(path),
            StructuredPatchChange::Replace { path, content } => self.replace_file(path, content),
            StructuredPatchChange::Move { path, move_to } => self.move_file(path, move_to),
            StructuredPatchChange::ReplaceExact { path, old, new } => {
                self.replace_exact(path, old, new)
            }
            StructuredPatchChange::ReplaceText {
                path,
                old,
                new,
                replace_all,
                expected_occurrences,
            } => self.replace_text(path, old, new, replace_all, expected_occurrences),
            StructuredPatchChange::ReplaceRange {
                path,
                start_line,
                end_line,
                content,
            } => self.replace_range(path, start_line, end_line, content),
            StructuredPatchChange::Update {
                path,
                move_to,
                hunks,
            } => self.update_file(path, move_to, hunks),
        }
    }

    fn finish(self, patch_format: &str) -> PlannedPatch {
        let mut final_changes = BTreeMap::new();
        for (path, original) in &self.original_content {
            let final_state = self.files.get(path).cloned().unwrap_or_else(|| {
                original.as_ref().map(|content| VirtualFileState {
                    content: content.clone(),
                    display_path: self
                        .original_display_paths
                        .get(path)
                        .cloned()
                        .unwrap_or_else(|| display_workspace_path(self.config, path)),
                    mode_source: Some(path.clone()),
                })
            });
            let final_content = final_state.as_ref().map(|state| state.content.clone());
            if original == &final_content {
                continue;
            }
            let display_path = final_state
                .as_ref()
                .map(|state| state.display_path.clone())
                .or_else(|| self.original_display_paths.get(path).cloned())
                .unwrap_or_else(|| display_workspace_path(self.config, path));
            let status = match (original, &final_content) {
                (None, Some(_)) => PatchFileStatus::Added,
                (Some(_), None) => PatchFileStatus::Deleted,
                (Some(_), Some(_)) => PatchFileStatus::Modified,
                (None, None) => continue,
            };
            let mode_source = final_state.and_then(|state| state.mode_source);
            final_changes.insert(
                display_path.clone(),
                PlannedFileChange {
                    path: path.clone(),
                    display_path,
                    status,
                    old_content: original.clone(),
                    new_content: final_content,
                    mode_source,
                },
            );
        }
        let file_changes = final_changes.into_values().collect::<Vec<_>>();
        let file_diffs = file_changes
            .iter()
            .map(PatchFileDiff::from_change)
            .collect::<Vec<_>>();
        let changed_files = file_changes
            .iter()
            .map(|change| change.display_path.clone())
            .collect::<Vec<_>>();
        let unified_diff = file_diffs
            .iter()
            .map(|file| file.diff.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        PlannedPatch {
            root: self.root.to_path_buf(),
            patch_format: patch_format.to_string(),
            operations: self.operations,
            file_changes,
            file_diffs,
            changed_files,
            unified_diff,
        }
    }

    fn add_file(&mut self, path: String, content: String) -> Result<(), String> {
        let target = resolve_virtual_write_path(self.root, &path)?;
        if self.virtual_exists(&target)? {
            return Err(format!("add file target `{path}` already exists"));
        }
        self.remember_original_absent(&target);
        self.files.insert(
            target,
            Some(VirtualFileState {
                content,
                display_path: path.clone(),
                mode_source: None,
            }),
        );
        self.operations
            .push(serde_json::json!({ "action": "add_file", "path": path }));
        Ok(())
    }

    fn delete_file(&mut self, path: String) -> Result<(), String> {
        let (target, _) = self.virtual_existing_file(&path)?;
        self.files.insert(target, None);
        self.operations
            .push(serde_json::json!({ "action": "delete_file", "path": path }));
        Ok(())
    }

    fn replace_file(&mut self, path: String, content: String) -> Result<(), String> {
        let (target, old_state) = self.virtual_existing_file(&path)?;
        let content = preserve_replacement_file_style(&old_state.content, &content);
        self.files.insert(
            target,
            Some(VirtualFileState {
                content,
                display_path: path.clone(),
                mode_source: old_state.mode_source,
            }),
        );
        self.operations
            .push(serde_json::json!({ "action": "update_file", "path": path }));
        Ok(())
    }

    fn move_file(&mut self, path: String, move_to: String) -> Result<(), String> {
        let (source, old_state) = self.virtual_existing_file(&path)?;
        let target = resolve_virtual_write_path(self.root, &move_to)?;
        if target != source && self.virtual_exists(&target)? {
            return Err(format!("move target `{move_to}` already exists"));
        }
        self.remember_original_absent(&target);
        self.files.insert(source, None);
        self.files.insert(
            target,
            Some(VirtualFileState {
                content: old_state.content,
                display_path: move_to.clone(),
                mode_source: old_state.mode_source,
            }),
        );
        self.operations.push(serde_json::json!({
            "action": "move_file",
            "path": path,
            "new_path": move_to,
        }));
        Ok(())
    }

    fn update_file(
        &mut self,
        path: String,
        move_to: Option<String>,
        hunks: Vec<StructuredPatchHunk>,
    ) -> Result<(), String> {
        let (source, old_state) = self.virtual_existing_file(&path)?;
        let new_content = apply_update_hunks(&old_state.content, &hunks, &path)?;
        let target = if let Some(move_to) = &move_to {
            let target = resolve_virtual_write_path(self.root, move_to)?;
            if target != source && self.virtual_exists(&target)? {
                return Err(format!("move target `{move_to}` already exists"));
            }
            self.remember_original_absent(&target);
            target
        } else {
            source.clone()
        };
        if target != source {
            self.files.insert(source, None);
        }
        self.files.insert(
            target,
            Some(VirtualFileState {
                content: new_content,
                display_path: move_to.clone().unwrap_or_else(|| path.clone()),
                mode_source: old_state.mode_source,
            }),
        );
        self.operations.push(serde_json::json!({
            "action": "update_file",
            "path": path,
            "move_to": move_to,
        }));
        Ok(())
    }

    fn replace_exact(&mut self, path: String, old: String, new: String) -> Result<(), String> {
        let (target, old_state) = self.virtual_existing_file(&path)?;
        let content = replace_exact_content(&old_state.content, &old, &new, &path)?;
        self.files.insert(
            target,
            Some(VirtualFileState {
                content,
                display_path: path.clone(),
                mode_source: old_state.mode_source,
            }),
        );
        self.operations.push(serde_json::json!({
            "action": "replace_exact",
            "path": path,
            "old_bytes": old.len(),
            "new_bytes": new.len(),
        }));
        Ok(())
    }

    fn replace_text(
        &mut self,
        path: String,
        old: String,
        new: String,
        replace_all: bool,
        expected_occurrences: Option<usize>,
    ) -> Result<(), String> {
        let (target, old_state) = self.virtual_existing_file(&path)?;
        let (content, matches) = replace_text_content(
            &old_state.content,
            &old,
            &new,
            replace_all,
            expected_occurrences,
            &path,
        )?;
        self.files.insert(
            target,
            Some(VirtualFileState {
                content,
                display_path: path.clone(),
                mode_source: old_state.mode_source,
            }),
        );
        self.operations.push(serde_json::json!({
            "action": "replace_text",
            "path": path,
            "old_bytes": old.len(),
            "new_bytes": new.len(),
            "matches": matches,
            "replace_all": replace_all,
        }));
        Ok(())
    }

    fn replace_range(
        &mut self,
        path: String,
        start_line: usize,
        end_line: usize,
        content: String,
    ) -> Result<(), String> {
        let (target, old_state) = self.virtual_existing_file(&path)?;
        let new_content =
            replace_line_range_content(&old_state.content, start_line, end_line, &content, &path)?;
        self.files.insert(
            target,
            Some(VirtualFileState {
                content: new_content,
                display_path: path.clone(),
                mode_source: old_state.mode_source,
            }),
        );
        self.operations.push(serde_json::json!({
            "action": "replace_range",
            "path": path,
            "start_line": start_line,
            "end_line": end_line,
            "replacement_bytes": content.len(),
        }));
        Ok(())
    }

    fn virtual_exists(&mut self, path: &Path) -> Result<bool, String> {
        if let Some(state) = self.files.get(path) {
            return Ok(state.is_some());
        }
        match read_existing_patch_file(self.root, self.config, path) {
            Ok(state) => {
                self.remember_original(path.to_path_buf(), Some(state.content.clone()), &state);
                self.files.insert(path.to_path_buf(), Some(state));
                Ok(true)
            }
            Err(PatchFileReadError::NotFound) => {
                self.remember_original_absent(path);
                Ok(false)
            }
            Err(PatchFileReadError::Invalid(detail)) => Err(detail),
        }
    }

    fn virtual_existing_file(
        &mut self,
        requested: &str,
    ) -> Result<(PathBuf, VirtualFileState), String> {
        let logical_path = resolve_virtual_write_path(self.root, requested)?;
        if let Some(state) = self.files.get(&logical_path) {
            return state
                .clone()
                .map(|state| (logical_path, state))
                .ok_or_else(|| format!("path `{requested}` was deleted earlier in this patch"));
        }
        let existing_path = resolve_existing_patch_path(self.root, requested)?;
        if let Some(state) = self.files.get(&existing_path) {
            return state
                .clone()
                .map(|state| (existing_path, state))
                .ok_or_else(|| format!("path `{requested}` was deleted earlier in this patch"));
        }
        let state =
            read_existing_patch_file(self.root, self.config, &existing_path).map_err(|error| {
                match error {
                    PatchFileReadError::NotFound => format!("path `{requested}` does not exist"),
                    PatchFileReadError::Invalid(detail) => detail,
                }
            })?;
        self.remember_original(existing_path.clone(), Some(state.content.clone()), &state);
        self.files
            .insert(existing_path.clone(), Some(state.clone()));
        Ok((existing_path, state))
    }

    fn remember_original(
        &mut self,
        path: PathBuf,
        content: Option<String>,
        state: &VirtualFileState,
    ) {
        self.original_content.entry(path.clone()).or_insert(content);
        self.original_display_paths
            .entry(path)
            .or_insert_with(|| state.display_path.clone());
    }

    fn remember_original_absent(&mut self, path: &Path) {
        self.original_content
            .entry(path.to_path_buf())
            .or_insert(None);
        self.original_display_paths
            .entry(path.to_path_buf())
            .or_insert_with(|| display_workspace_path(self.config, path));
    }
}

fn plan_structured_changes(
    root: &Path,
    config: &LocalWorkspaceAgentConfig,
    changes: Vec<StructuredPatchChange>,
    patch_format: &str,
    tool_name: &str,
    cancellation_token: &AgentCancellationToken,
) -> Result<PlannedPatch, String> {
    let mut preflight = PatchPreflight::new(root, config);
    for change in changes {
        ensure_local_tool_not_cancelled(cancellation_token, tool_name)?;
        preflight.apply_change(change)?;
    }
    Ok(preflight.finish(patch_format))
}

fn apply_planned_patch(
    config: &LocalWorkspaceAgentConfig,
    plan: &PlannedPatch,
    tool_name: &str,
    cancellation_token: &AgentCancellationToken,
) -> Result<(), String> {
    ensure_local_tool_not_cancelled(cancellation_token, tool_name)?;
    let root = canonical_workspace_root(config)?;
    if root != plan.root {
        return Err(format!(
            "workspace root changed between patch preflight and mutation: {} -> {}",
            plan.root.display(),
            root.display()
        ));
    }
    validate_planned_patch_sandbox_access(config, &root, plan)?;
    for change in plan
        .file_changes
        .iter()
        .filter(|change| change.new_content.is_some())
    {
        ensure_local_tool_not_cancelled(cancellation_token, tool_name)?;
        prepare_write_target(&root, change)?;
    }
    for change in plan
        .file_changes
        .iter()
        .filter(|change| change.new_content.is_none())
    {
        ensure_local_tool_not_cancelled(cancellation_token, tool_name)?;
        prepare_delete_target(&root, change)?;
    }
    for change in plan
        .file_changes
        .iter()
        .filter(|change| change.new_content.is_some())
    {
        ensure_local_tool_not_cancelled(cancellation_token, tool_name)?;
        let content = change.new_content.as_ref().expect("filtered");
        atomic_write_text_file(&change.path, content, change.mode_source.as_deref())
            .map_err(|source| format!("write `{}`: {source}", change.display_path))?;
    }
    for change in plan
        .file_changes
        .iter()
        .filter(|change| change.new_content.is_none())
    {
        ensure_local_tool_not_cancelled(cancellation_token, tool_name)?;
        fs::remove_file(&change.path)
            .map_err(|source| format!("delete `{}`: {source}", change.display_path))?;
    }
    Ok(())
}

fn validate_planned_patch_sandbox_access(
    config: &LocalWorkspaceAgentConfig,
    root: &Path,
    plan: &PlannedPatch,
) -> Result<(), String> {
    for change in &plan.file_changes {
        if change.old_content.is_some() {
            validate_local_workspace_file_access(
                config,
                root,
                &change.path,
                LocalWorkspaceFileAccess::Read,
                &change.display_path,
            )?;
        }
        if change.new_content.is_some() || change.status == PatchFileStatus::Deleted {
            validate_local_workspace_file_access(
                config,
                root,
                &change.path,
                LocalWorkspaceFileAccess::Write,
                &change.display_path,
            )?;
        }
    }
    Ok(())
}

fn prepare_write_target(root: &Path, change: &PlannedFileChange) -> Result<(), String> {
    ensure_path_stays_under_root(root, &change.path, &change.display_path)?;
    ensure_no_symlink_components(root, &change.path, false, &change.display_path)?;
    if let Some(parent) = change.path.parent() {
        ensure_write_parent_stays_under_root(root, &change.path, &change.display_path)?;
        fs::create_dir_all(parent)
            .map_err(|source| format!("create parent for `{}`: {source}", change.display_path))?;
        ensure_no_symlink_components(root, parent, true, &change.display_path)?;
    }
    if change.status == PatchFileStatus::Added && change.path.exists() {
        return Err(format!(
            "add file target `{}` appeared after preflight",
            change.display_path
        ));
    }
    if change.path.exists() {
        let metadata = fs::symlink_metadata(&change.path)
            .map_err(|source| format!("inspect `{}`: {source}", change.display_path))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "path `{}` became a symlink after preflight",
                change.display_path
            ));
        }
        if metadata.is_dir() {
            return Err(format!("path `{}` is a directory", change.display_path));
        }
    }
    Ok(())
}

fn prepare_delete_target(root: &Path, change: &PlannedFileChange) -> Result<(), String> {
    ensure_path_stays_under_root(root, &change.path, &change.display_path)?;
    ensure_no_symlink_components(root, &change.path, true, &change.display_path)?;
    let metadata = fs::symlink_metadata(&change.path)
        .map_err(|source| format!("inspect `{}` before delete: {source}", change.display_path))?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "path `{}` became a symlink after preflight",
            change.display_path
        ));
    }
    if metadata.is_dir() {
        return Err(format!("path `{}` is a directory", change.display_path));
    }
    Ok(())
}

fn ensure_path_stays_under_root(root: &Path, path: &Path, requested: &str) -> Result<(), String> {
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(format!(
            "path `{requested}` escapes workspace root {}",
            root.display()
        ))
    }
}

fn atomic_write_text_file(
    target: &Path,
    content: &str,
    mode_source: Option<&Path>,
) -> Result<(), String> {
    let parent = target
        .parent()
        .ok_or_else(|| format!("path `{}` has no parent directory", target.display()))?;
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let temp = parent.join(format!(
        ".{file_name}.sinomo-write-{}",
        TaskId::generate().as_str()
    ));
    let write_result = (|| -> Result<(), String> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|source| format!("create temp file `{}`: {source}", temp.display()))?;
        file.write_all(content.as_bytes())
            .map_err(|source| format!("write temp file `{}`: {source}", temp.display()))?;
        file.sync_all()
            .map_err(|source| format!("sync temp file `{}`: {source}", temp.display()))?;
        apply_preserved_permissions(&temp, mode_source)?;
        fs::rename(&temp, target).map_err(|source| {
            format!(
                "replace `{}` with temp file `{}`: {source}",
                target.display(),
                temp.display()
            )
        })?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    write_result
}

#[cfg(unix)]
fn apply_preserved_permissions(temp: &Path, mode_source: Option<&Path>) -> Result<(), String> {
    let Some(mode_source) = mode_source else {
        return Ok(());
    };
    let mode = fs::metadata(mode_source)
        .map_err(|source| format!("read mode from `{}`: {source}", mode_source.display()))?
        .permissions()
        .mode();
    fs::set_permissions(temp, fs::Permissions::from_mode(mode))
        .map_err(|source| format!("set mode on `{}`: {source}", temp.display()))
}

#[cfg(not(unix))]
fn apply_preserved_permissions(_temp: &Path, _mode_source: Option<&Path>) -> Result<(), String> {
    Ok(())
}

enum PatchFileReadError {
    NotFound,
    Invalid(String),
}

fn read_existing_patch_file(
    root: &Path,
    config: &LocalWorkspaceAgentConfig,
    path: &Path,
) -> Result<VirtualFileState, PatchFileReadError> {
    let display_path = display_workspace_path(config, path);
    ensure_path_stays_under_root(root, path, &display_path).map_err(PatchFileReadError::Invalid)?;
    validate_local_workspace_file_access(
        config,
        root,
        path,
        LocalWorkspaceFileAccess::Read,
        &display_path,
    )
    .map_err(PatchFileReadError::Invalid)?;
    if !path.exists() {
        return Err(PatchFileReadError::NotFound);
    }
    ensure_no_symlink_components(root, path, true, &display_path)
        .map_err(PatchFileReadError::Invalid)?;
    let metadata = fs::metadata(path).map_err(|source| {
        PatchFileReadError::Invalid(format!("inspect `{display_path}`: {source}"))
    })?;
    if metadata.is_dir() {
        return Err(PatchFileReadError::Invalid(format!(
            "path `{display_path}` is a directory"
        )));
    }
    let content = fs::read_to_string(path).map_err(|source| {
        PatchFileReadError::Invalid(format!("read `{display_path}`: {source}"))
    })?;
    Ok(VirtualFileState {
        content,
        display_path,
        mode_source: Some(path.to_path_buf()),
    })
}

fn resolve_existing_patch_path(root: &Path, requested: &str) -> Result<PathBuf, String> {
    let relative = validate_relative_patch_path(requested)?;
    let joined = root.join(relative);
    let resolved = fs::canonicalize(&joined)
        .map_err(|source| format!("path `{requested}` is not accessible: {source}"))?;
    ensure_path_stays_under_root(root, &resolved, requested)?;
    ensure_no_symlink_components(root, &joined, true, requested)?;
    Ok(resolved)
}

fn resolve_virtual_write_path(root: &Path, requested: &str) -> Result<PathBuf, String> {
    let relative = validate_relative_patch_path(requested)?;
    let target = root.join(relative);
    ensure_write_parent_stays_under_root(root, &target, requested)?;
    Ok(target)
}

fn validate_relative_patch_path(requested: &str) -> Result<PathBuf, String> {
    let requested = requested.trim();
    if requested.is_empty() {
        return Err("path must not be empty".to_string());
    }
    let requested_path = Path::new(requested);
    if requested_path.is_absolute() {
        return Err(format!(
            "path `{requested}` must be relative to the workspace root"
        ));
    }
    for component in requested_path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            _ => {
                return Err(format!(
                    "path `{requested}` contains unsupported or escaping components"
                ));
            }
        }
    }
    Ok(requested_path.to_path_buf())
}

fn ensure_no_symlink_components(
    root: &Path,
    path: &Path,
    include_target: bool,
    requested: &str,
) -> Result<(), String> {
    ensure_path_stays_under_root(root, path, requested)?;
    let relative = path.strip_prefix(root).map_err(|_| {
        format!(
            "path `{requested}` escapes workspace root {}",
            root.display()
        )
    })?;
    let mut current = root.to_path_buf();
    let component_count = relative.components().count();
    for (index, component) in relative.components().enumerate() {
        current.push(component);
        if !include_target && index + 1 == component_count {
            break;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "path `{requested}` crosses symlink component `{}`",
                    current.display()
                ));
            }
            Ok(_) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => break,
            Err(source) => {
                return Err(format!(
                    "inspect path component `{}` for `{requested}`: {source}",
                    current.display()
                ));
            }
        }
    }
    Ok(())
}

fn preserve_replacement_file_style(old_content: &str, content: &str) -> String {
    let (had_bom, _) = strip_utf8_bom(old_content);
    let mut content = if preferred_line_ending(old_content) == "\r\n" {
        normalize_line_endings(content).replace('\n', "\r\n")
    } else {
        normalize_line_endings(content)
    };
    if had_bom && !content.starts_with('\u{feff}') {
        content.insert(0, '\u{feff}');
    }
    content
}

fn replace_exact_content(
    old_content: &str,
    old: &str,
    new: &str,
    path: &str,
) -> Result<String, String> {
    let normalized_original = normalize_line_endings(old_content);
    let (had_bom, normalized_content) = strip_utf8_bom(&normalized_original);
    let old = normalize_line_endings(old);
    if old.is_empty() {
        return Err(format!(
            "replace_exact old text for `{path}` must not be empty"
        ));
    }
    let new = normalize_line_endings(new);
    let matches = normalized_content.match_indices(&old).count();
    if matches != 1 {
        return Err(format!(
            "replace_exact expected one match for `{path}`, found {matches}"
        ));
    }
    let mut content = normalized_content.replacen(&old, &new, 1);
    if preferred_line_ending(old_content) == "\r\n" {
        content = content.replace('\n', "\r\n");
    }
    if had_bom {
        content.insert(0, '\u{feff}');
    }
    Ok(content)
}

fn replace_text_content(
    old_content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
    expected_occurrences: Option<usize>,
    path: &str,
) -> Result<(String, usize), String> {
    let normalized_original = normalize_line_endings(old_content);
    let (had_bom, normalized_content) = strip_utf8_bom(&normalized_original);
    let old = normalize_line_endings(old);
    if old.is_empty() {
        return Err(format!("edit_file old_text for `{path}` must not be empty"));
    }
    let new = normalize_line_endings(new);
    let matches = normalized_content.match_indices(&old).count();
    if matches == 0 {
        return Err(format!(
            "edit_file expected at least one match for `{path}`, found 0"
        ));
    }
    if let Some(expected) = expected_occurrences
        && matches != expected
    {
        return Err(format!(
            "edit_file expected {expected} match(es) for `{path}`, found {matches}"
        ));
    }
    if !replace_all && matches != 1 {
        return Err(format!(
            "edit_file expected one match for `{path}`, found {matches}; provide more context or set replace_all=true"
        ));
    }
    let mut content = if replace_all {
        normalized_content.replace(&old, &new)
    } else {
        normalized_content.replacen(&old, &new, 1)
    };
    if preferred_line_ending(old_content) == "\r\n" {
        content = content.replace('\n', "\r\n");
    }
    if had_bom {
        content.insert(0, '\u{feff}');
    }
    Ok((content, matches))
}

fn replace_line_range_content(
    old_content: &str,
    start_line: usize,
    end_line: usize,
    replacement: &str,
    path: &str,
) -> Result<String, String> {
    if start_line == 0 {
        return Err(format!(
            "replace_range start_line for `{path}` must be >= 1"
        ));
    }
    if end_line < start_line {
        return Err(format!(
            "replace_range end_line for `{path}` must be >= start_line"
        ));
    }
    let normalized_original = normalize_line_endings(old_content);
    let had_trailing_newline = normalized_original.ends_with('\n');
    let (had_bom, normalized_content) = strip_utf8_bom(&normalized_original);
    let preferred_line_ending = preferred_line_ending(old_content);
    let mut lines = split_patch_lines(&normalized_content);
    if start_line > lines.len() || end_line > lines.len() {
        return Err(format!(
            "replace_range {start_line}..{end_line} for `{path}` is outside the file's {} lines",
            lines.len()
        ));
    }
    let replacement = normalize_line_endings(replacement);
    let replacement_lines = split_patch_lines(&replacement);
    lines.splice(start_line - 1..end_line, replacement_lines);
    let mut new_content = join_patch_lines(lines, had_trailing_newline);
    if preferred_line_ending == "\r\n" {
        new_content = new_content.replace('\n', "\r\n");
    }
    if had_bom {
        new_content.insert(0, '\u{feff}');
    }
    Ok(new_content)
}

impl PatchFileDiff {
    fn from_change(change: &PlannedFileChange) -> Self {
        let old_content = change.old_content.as_deref().unwrap_or_default();
        let new_content = change.new_content.as_deref().unwrap_or_default();
        let removed_lines = count_content_lines(old_content);
        let added_lines = count_content_lines(new_content);
        Self {
            status: change.status.code().to_string(),
            path: change.display_path.clone(),
            added_lines,
            removed_lines,
            diff: render_file_diff(change, old_content, new_content),
        }
    }
}

fn count_content_lines(content: &str) -> usize {
    if content.is_empty() {
        0
    } else {
        content.lines().count()
    }
}

fn render_file_diff(change: &PlannedFileChange, old_content: &str, new_content: &str) -> String {
    const MAX_DIFF_BYTES: usize = 128 * 1024;
    let mut diff = String::new();
    match change.status {
        PatchFileStatus::Added => {
            diff.push_str("--- /dev/null\n");
            diff.push_str(&format!("+++ b/{}\n", change.display_path));
        }
        PatchFileStatus::Deleted => {
            diff.push_str(&format!("--- a/{}\n", change.display_path));
            diff.push_str("+++ /dev/null\n");
        }
        PatchFileStatus::Modified => {
            diff.push_str(&format!("--- a/{}\n", change.display_path));
            diff.push_str(&format!("+++ b/{}\n", change.display_path));
        }
    }
    diff.push_str("@@\n");
    match change.status {
        PatchFileStatus::Added => push_diff_content(&mut diff, '+', new_content, MAX_DIFF_BYTES),
        PatchFileStatus::Deleted => push_diff_content(&mut diff, '-', old_content, MAX_DIFF_BYTES),
        PatchFileStatus::Modified => {
            push_diff_content(&mut diff, '-', old_content, MAX_DIFF_BYTES);
            push_diff_content(&mut diff, '+', new_content, MAX_DIFF_BYTES);
        }
    }
    if diff.len() > MAX_DIFF_BYTES {
        diff.truncate(MAX_DIFF_BYTES);
        diff.push_str("\n... diff truncated ...\n");
    }
    diff
}

fn push_diff_content(diff: &mut String, prefix: char, content: &str, max_bytes: usize) {
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

fn format_patch_success_summary(files: &[PatchFileDiff]) -> String {
    let mut summary = String::from("Success. Updated the following files:");
    if files.is_empty() {
        summary.push_str("\nNo files changed");
        return summary;
    }
    for file in files {
        summary.push('\n');
        summary.push_str(&file.status);
        summary.push(' ');
        summary.push_str(&file.path);
    }
    summary
}

fn format_patch_failure_summary(tool_name: &str, detail: &str) -> String {
    format!("{tool_name} verification failed: {detail}")
}

fn patch_failure_result(
    tool_name: &str,
    patch_format: &str,
    root: &Path,
    evidence_reason: &str,
    detail: &str,
) -> AgentToolResult {
    let summary = format_patch_failure_summary(tool_name, detail);
    let payload = serde_json::json!({
        "format": patch_format,
        "error": format!("{tool_name}_verification_failed"),
        "detail": detail,
        "message": summary,
        "changed_files": [],
        "operations": [],
    });
    let evidence = AgentToolEvidence::availability_only(true, evidence_reason)
        .with_command(root.display().to_string(), Some(1))
        .with_output_value(&payload, false)
        .with_record(
            AGENT_CODING_EVIDENCE_KIND,
            &AgentCodingToolEvidence::default(),
        );
    AgentToolResult::failed(summary, payload).with_evidence(evidence)
}

fn patch_result(
    status: AgentToolStatus,
    summary: String,
    config: &LocalWorkspaceAgentConfig,
    plan: &PlannedPatch,
    root: &Path,
    evidence_reason: &str,
    diff_ref: Option<String>,
) -> AgentToolResult {
    let payload = serde_json::json!({
        "format": plan.patch_format.clone(),
        "message": summary,
        "changed_files": plan.changed_files.clone(),
        "operations": plan.operations.clone(),
        "diff": {
            "files": plan.file_diffs.clone(),
            "unified": plan.unified_diff.clone(),
        },
    });
    let mut evidence =
        AgentToolEvidence::availability_only(status != AgentToolStatus::Blocked, evidence_reason)
            .with_command(
                root.display().to_string(),
                Some(if status == AgentToolStatus::Succeeded {
                    0
                } else {
                    1
                }),
            )
            .with_output_value(&payload, false)
            .with_metadata(serde_json::json!({
                "workspace_root": root.display().to_string(),
                "sandbox": local_workspace_sandbox_metadata(config),
            }))
            .with_record(
                AGENT_CODING_EVIDENCE_KIND,
                &AgentCodingToolEvidence {
                    diff_ref: diff_ref.clone(),
                    diff_summary: Some(summary.clone()),
                    changed_files: plan.changed_files.clone(),
                    ..AgentCodingToolEvidence::default()
                },
            )
            .with_record_value(
                "patch_diff",
                serde_json::json!({ "files": plan.file_diffs.clone() }),
            );
    if let Some(diff_ref) = diff_ref {
        evidence = evidence.with_artifact(
            AgentToolArtifactRef::new("diff", diff_ref).with_media_type("text/x-diff"),
        );
    }
    match status {
        AgentToolStatus::Succeeded => {
            AgentToolResult::succeeded(summary, payload).with_evidence(evidence)
        }
        AgentToolStatus::Failed => {
            AgentToolResult::failed(summary, payload).with_evidence(evidence)
        }
        AgentToolStatus::Blocked => {
            AgentToolResult::blocked(summary, payload).with_evidence(evidence)
        }
        AgentToolStatus::Cancelled => {
            AgentToolResult::cancelled(summary, payload).with_evidence(evidence)
        }
    }
}

fn parse_freeform_patch(patch: &str) -> Result<Vec<StructuredPatchChange>, String> {
    let normalized = patch.replace("\r\n", "\n").replace('\r', "\n");
    let trimmed = normalized.trim();
    let mut parser = FreeformPatchParser::new(trimmed);
    parser.parse()
}

struct FreeformPatchParser<'a> {
    lines: Vec<&'a str>,
    index: usize,
}

impl<'a> FreeformPatchParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            lines: input.lines().collect(),
            index: 0,
        }
    }

    fn parse(&mut self) -> Result<Vec<StructuredPatchChange>, String> {
        self.expect_boundary(0, "*** Begin Patch")?;
        let last_index = self
            .lines
            .len()
            .checked_sub(1)
            .ok_or_else(|| "patch is empty".to_string())?;
        self.expect_boundary(last_index, "*** End Patch")?;

        self.index = 1;
        let mut changes = Vec::new();
        while self.index < last_index {
            if self.current().is_some_and(|line| line.trim().is_empty()) {
                self.index += 1;
                continue;
            }
            changes.push(self.parse_hunk(last_index)?);
        }
        if changes.is_empty() {
            return Err("patch must contain at least one file hunk".to_string());
        }
        Ok(changes)
    }

    fn expect_boundary(&self, index: usize, marker: &str) -> Result<(), String> {
        match self.lines.get(index).map(|line| line.trim()) {
            Some(line) if line == marker => Ok(()),
            _ if marker == "*** Begin Patch" => {
                Err("patch must start with `*** Begin Patch`".to_string())
            }
            _ => Err("patch must end with `*** End Patch`".to_string()),
        }
    }

    fn parse_hunk(&mut self, end_index: usize) -> Result<StructuredPatchChange, String> {
        let line = self
            .current()
            .ok_or_else(|| self.error("expected a patch hunk"))?;
        if let Some(path) = line.strip_prefix("*** Add File: ") {
            return self.parse_add_file(path, end_index);
        }
        if let Some(path) = line.strip_prefix("*** Delete File: ") {
            self.index += 1;
            return Ok(StructuredPatchChange::Delete {
                path: required_path(path, self.line_number(self.index - 1))?,
            });
        }
        if let Some(path) = line.strip_prefix("*** Update File: ") {
            return self.parse_update_file(path, end_index);
        }
        Err(self.error(format!(
            "expected `*** Add File:`, `*** Delete File:`, or `*** Update File:`, got `{line}`"
        )))
    }

    fn parse_add_file(
        &mut self,
        path: &str,
        end_index: usize,
    ) -> Result<StructuredPatchChange, String> {
        let path = required_path(path, self.line_number(self.index))?;
        self.index += 1;
        let mut content = String::new();
        let mut saw_line = false;
        while self.index < end_index {
            let line = self.current().expect("index guarded");
            if is_file_hunk_header(line) {
                break;
            }
            let Some(content_line) = line.strip_prefix('+') else {
                return Err(self.error(format!(
                    "add file `{path}` expects every content line to start with `+`"
                )));
            };
            content.push_str(content_line);
            content.push('\n');
            saw_line = true;
            self.index += 1;
        }
        if !saw_line {
            return Err(self.error(format!(
                "add file `{path}` must contain at least one `+` line"
            )));
        }
        Ok(StructuredPatchChange::Add { path, content })
    }

    fn parse_update_file(
        &mut self,
        path: &str,
        end_index: usize,
    ) -> Result<StructuredPatchChange, String> {
        let path = required_path(path, self.line_number(self.index))?;
        self.index += 1;
        let mut move_to = None;
        if self.index < end_index
            && let Some(target) = self
                .current()
                .and_then(|line| line.strip_prefix("*** Move to: "))
        {
            move_to = Some(required_path(target, self.line_number(self.index))?);
            self.index += 1;
        }

        let mut hunks = Vec::new();
        let mut current = StructuredPatchHunk::default();
        let mut current_has_marker = false;
        while self.index < end_index {
            let line = self.current().expect("index guarded");
            if is_file_hunk_header(line) {
                break;
            }
            if line == "*** End of File" {
                if !hunk_has_lines(&current) {
                    return Err(self.error(format!(
                        "update file `{path}` has `*** End of File` before any change lines"
                    )));
                }
                current.is_end_of_file = true;
                hunks.push(current);
                current = StructuredPatchHunk::default();
                current_has_marker = false;
                self.index += 1;
                if self.index < end_index
                    && !self.current().is_some_and(is_file_hunk_header)
                    && !self.current().is_some_and(|line| line.trim().is_empty())
                {
                    return Err(self.error(format!(
                        "update file `{path}` must not contain more hunk lines after `*** End of File`"
                    )));
                }
                break;
            }
            if line.starts_with("@@") {
                if hunk_has_lines(&current) {
                    hunks.push(current);
                    current = StructuredPatchHunk::default();
                } else if current_has_marker {
                    return Err(self.error(format!(
                        "update file `{path}` has a context marker without change lines"
                    )));
                }
                current.change_context = parse_change_context(line)
                    .map_err(|detail| self.error(format!("update file `{path}`: {detail}")))?;
                current_has_marker = true;
                self.index += 1;
                continue;
            }
            let Some(marker) = line.chars().next() else {
                return Err(self.error(format!(
                    "update file `{path}` contains an empty raw line; prefix blank context lines with a space"
                )));
            };
            let content = &line[marker.len_utf8()..];
            match marker {
                ' ' => {
                    current.old_lines.push(content.to_string());
                    current.new_lines.push(content.to_string());
                }
                '-' => current.old_lines.push(content.to_string()),
                '+' => current.new_lines.push(content.to_string()),
                '\\' => {}
                _ => {
                    return Err(self.error(format!(
                        "update file `{path}` contains invalid hunk line `{line}`"
                    )));
                }
            }
            self.index += 1;
        }
        if hunk_has_lines(&current) {
            hunks.push(current);
        } else if current_has_marker {
            return Err(self.error(format!(
                "update file `{path}` has a context marker without change lines"
            )));
        }
        if hunks.is_empty() && move_to.is_none() {
            return Err(self.error(format!(
                "update file `{path}` must contain a move target or at least one hunk"
            )));
        }
        Ok(StructuredPatchChange::Update {
            path,
            move_to,
            hunks,
        })
    }

    fn current(&self) -> Option<&'a str> {
        self.lines.get(self.index).copied()
    }

    fn line_number(&self, index: usize) -> usize {
        index + 1
    }

    fn error(&self, detail: impl Into<String>) -> String {
        format!("line {}: {}", self.line_number(self.index), detail.into())
    }
}

fn required_path(path: &str, line_number: usize) -> Result<String, String> {
    let path = path.trim();
    if path.is_empty() {
        Err(format!(
            "line {line_number}: patch file path must not be empty"
        ))
    } else {
        Ok(path.to_string())
    }
}

fn is_file_hunk_header(line: &str) -> bool {
    line.starts_with("*** Add File: ")
        || line.starts_with("*** Delete File: ")
        || line.starts_with("*** Update File: ")
        || line.trim() == "*** End Patch"
}

fn parse_change_context(line: &str) -> Result<Option<String>, String> {
    if line == "@@" {
        return Ok(None);
    }
    let Some(context) = line.strip_prefix("@@ ") else {
        return Err("context marker must be `@@` or `@@ <context>`".to_string());
    };
    if context.is_empty() {
        Err("context marker must not end with an empty context".to_string())
    } else {
        Ok(Some(context.to_string()))
    }
}

fn hunk_has_lines(hunk: &StructuredPatchHunk) -> bool {
    !hunk.old_lines.is_empty() || !hunk.new_lines.is_empty()
}

fn apply_update_hunks(
    old_content: &str,
    hunks: &[StructuredPatchHunk],
    path: &str,
) -> Result<String, String> {
    let normalized_original = normalize_line_endings(old_content);
    let had_trailing_newline = normalized_original.ends_with('\n');
    let (had_bom, normalized_content) = strip_utf8_bom(&normalized_original);
    let preferred_line_ending = preferred_line_ending(old_content);
    let mut lines = split_patch_lines(&normalized_content);
    let mut replacements: Vec<(usize, usize, Vec<String>)> = Vec::new();
    let mut line_index = 0usize;
    for hunk in hunks {
        if let Some(change_context) = &hunk.change_context {
            let Some(context_index) = seek_line_sequence(
                &lines,
                std::slice::from_ref(change_context),
                line_index,
                false,
            ) else {
                return Err(format!(
                    "failed to find context `{change_context}` in `{path}`{}",
                    best_match_hint(&lines, std::slice::from_ref(change_context))
                ));
            };
            line_index = context_index + 1;
        }
        if hunk.old_lines.is_empty() {
            let insertion_index = if hunk.change_context.is_some() && !hunk.is_end_of_file {
                line_index
            } else {
                lines.len()
            };
            replacements.push((insertion_index, 0, hunk.new_lines.clone()));
            line_index = insertion_index;
            continue;
        }
        let mut old_lines = hunk.old_lines.clone();
        let mut new_lines = hunk.new_lines.clone();
        let mut matched = seek_line_sequence(&lines, &old_lines, line_index, hunk.is_end_of_file);
        if matched.is_none() && old_lines.last().is_some_and(String::is_empty) {
            old_lines.pop();
            if new_lines.last().is_some_and(String::is_empty) {
                new_lines.pop();
            }
            matched = seek_line_sequence(&lines, &old_lines, line_index, hunk.is_end_of_file);
        }
        let Some(start) = matched else {
            return Err(format!(
                "failed to find expected lines in `{path}`:\n{}{}",
                hunk.old_lines.join("\n"),
                best_match_hint(&lines, &hunk.old_lines)
            ));
        };
        replacements.push((start, old_lines.len(), new_lines));
        line_index = start + old_lines.len();
    }
    replacements.sort_by_key(|(start, _, _)| *start);
    for (start, old_len, replacement) in replacements.into_iter().rev() {
        lines.splice(start..start + old_len, replacement);
    }
    let mut new_content = join_patch_lines(lines, had_trailing_newline);
    if preferred_line_ending == "\r\n" {
        new_content = new_content.replace('\n', "\r\n");
    }
    if had_bom {
        new_content.insert(0, '\u{feff}');
    }
    Ok(new_content)
}

fn preferred_line_ending(content: &str) -> &'static str {
    if content.contains("\r\n") {
        "\r\n"
    } else {
        "\n"
    }
}

fn normalize_line_endings(content: &str) -> String {
    content.replace("\r\n", "\n").replace('\r', "\n")
}

fn strip_utf8_bom(content: &str) -> (bool, String) {
    if let Some(stripped) = content.strip_prefix('\u{feff}') {
        (true, stripped.to_string())
    } else {
        (false, content.to_string())
    }
}

fn split_patch_lines(content: &str) -> Vec<String> {
    let mut lines: Vec<String> = content.split('\n').map(str::to_string).collect();
    if lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

fn join_patch_lines(mut lines: Vec<String>, trailing_newline: bool) -> String {
    if trailing_newline && lines.last().is_none_or(|line| !line.is_empty()) {
        lines.push(String::new());
    }
    lines.join("\n")
}

fn seek_line_sequence(
    lines: &[String],
    pattern: &[String],
    start: usize,
    eof: bool,
) -> Option<usize> {
    if pattern.is_empty() || pattern.len() > lines.len() {
        return None;
    }
    let max_start = lines.len() - pattern.len();
    let search_start = if eof { max_start } else { start };
    if search_start > max_start {
        return None;
    }
    for normalize in [
        normalize_identity as fn(&str) -> String,
        normalize_trim_end,
        normalize_trim,
        normalize_collapse_whitespace,
        normalize_punctuation_trimmed,
        normalize_lowercase_punctuation_trimmed,
    ] {
        for index in search_start..=max_start {
            if line_sequence_matches(lines, pattern, index, normalize) {
                return Some(index);
            }
        }
    }
    None
}

fn line_sequence_matches(
    lines: &[String],
    pattern: &[String],
    start: usize,
    normalize: fn(&str) -> String,
) -> bool {
    pattern
        .iter()
        .enumerate()
        .all(|(offset, expected)| normalize(&lines[start + offset]) == normalize(expected))
}

fn best_match_hint(lines: &[String], pattern: &[String]) -> String {
    if lines.is_empty() || pattern.is_empty() {
        return String::new();
    }
    let pattern_len = pattern.len().min(lines.len());
    let normalized_pattern = pattern
        .iter()
        .map(|line| normalize_lowercase_punctuation_trimmed(line))
        .collect::<Vec<_>>();
    let mut best: Option<(usize, usize)> = None;
    for start in 0..=lines.len().saturating_sub(pattern_len) {
        let score = (0..pattern_len)
            .filter(|offset| {
                normalize_lowercase_punctuation_trimmed(&lines[start + offset])
                    == normalized_pattern[*offset]
            })
            .count();
        if best.is_none_or(|(_, best_score)| score > best_score) {
            best = Some((start, score));
        }
    }
    let Some((start, score)) = best else {
        return String::new();
    };
    let preview = lines
        .iter()
        .skip(start)
        .take(pattern_len.min(3))
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "\nClosest candidate starts at line {} with {score}/{pattern_len} normalized line match(es):\n{preview}",
        start + 1
    )
}

fn normalize_identity(value: &str) -> String {
    value.to_string()
}

fn normalize_trim_end(value: &str) -> String {
    value.trim_end().to_string()
}

fn normalize_trim(value: &str) -> String {
    value.trim().to_string()
}

fn normalize_collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_punctuation_trimmed(value: &str) -> String {
    normalize_unicode_punctuation(value.trim())
}

fn normalize_lowercase_punctuation_trimmed(value: &str) -> String {
    normalize_unicode_punctuation(value.trim()).to_ascii_lowercase()
}

fn normalize_unicode_punctuation(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2014}' | '\u{2015}'
            | '\u{2212}' => '-',
            '\u{2018}' | '\u{2019}' | '\u{201A}' | '\u{201B}' => '\'',
            '\u{201C}' | '\u{201D}' | '\u{201E}' | '\u{201F}' => '"',
            '\u{00A0}' | '\u{2002}' | '\u{2003}' | '\u{2004}' | '\u{2005}' | '\u{2006}'
            | '\u{2007}' | '\u{2008}' | '\u{2009}' | '\u{200A}' | '\u{202F}' | '\u{205F}'
            | '\u{3000}' => ' ',
            other => other,
        })
        .collect()
}

fn ensure_write_parent_stays_under_root(
    root: &Path,
    target: &Path,
    requested: &str,
) -> Result<(), String> {
    let mut ancestor = target
        .parent()
        .ok_or_else(|| format!("path `{requested}` has no parent directory"))?;
    while !ancestor.exists() {
        ancestor = ancestor
            .parent()
            .ok_or_else(|| format!("path `{requested}` has no existing parent directory"))?;
    }
    let resolved_parent = fs::canonicalize(ancestor)
        .map_err(|source| format!("parent for `{requested}` is not accessible: {source}"))?;
    if !resolved_parent.starts_with(root) {
        return Err(format!(
            "path `{requested}` escapes workspace root {}",
            root.display()
        ));
    }
    Ok(())
}

fn with_patch_evidence(
    mut result: AgentToolResult,
    patch_ref: Option<String>,
    diff_ref: Option<String>,
    diff_summary: impl Into<String>,
) -> AgentToolResult {
    let summary = diff_summary.into();
    let base_evidence = result.evidence.take().unwrap_or_else(|| {
        AgentToolEvidence::availability_only(
            result.status != AgentToolStatus::Blocked,
            "apply_patch produced patch evidence",
        )
    });
    let mut evidence = with_coding_evidence(base_evidence, |coding| {
        coding.patch_ref = patch_ref.clone();
        if coding.diff_ref.is_none() {
            coding.diff_ref = diff_ref.clone();
        }
        coding.diff_summary = Some(summary);
    });
    if let Some(patch_ref) = patch_ref {
        evidence = evidence.with_artifact(
            AgentToolArtifactRef::new("patch", patch_ref).with_media_type("text/x-patch"),
        );
    }
    if let Some(diff_ref) = diff_ref
        && !evidence
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "diff")
    {
        evidence = evidence.with_artifact(
            AgentToolArtifactRef::new("diff", diff_ref).with_media_type("text/x-diff"),
        );
    }
    result.evidence = Some(evidence);
    result
}
