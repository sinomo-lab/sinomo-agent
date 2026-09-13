//! Context assembly, source selection, budgets, and model request construction.

use super::{
    Agent, AgentConfig, AgentContextAttachment, AgentDefinition, AgentError, AgentFollowUpMessage,
    AgentLoopInput, AgentOutputFormat, AgentRole, AgentRunContext, AgentSessionProjection,
    AgentSkillDescriptor, AgentSkillPlan, AgentSteeringMessage, AgentToolCustomFormat,
    AgentToolCustomSpec, AgentToolDescriptor, AgentToolInvocationMode, AgentToolPlan,
    AgentToolPlanner, AgentToolProviderSpec, AgentTranscriptItem, AgentTranscriptRole,
    AsyncTaskUpdate, BTreeSet, CONTEXT_ESTIMATE_BYTES_PER_TOKEN, DEFAULT_RETRIEVED_MEMORY_BYTES,
    DEFAULT_RETRIEVED_MEMORY_RECORDS, Deserialize, EvidenceCoverage, InteractionModel,
    LanguageModelCustomToolFormat, LanguageModelCustomToolSpec, LanguageModelMessage,
    LanguageModelProviderToolSpec, LanguageModelRequest, LanguageModelRequestItem,
    LanguageModelResponseFormat, LanguageModelRole, LanguageModelToolChoice, LanguageModelToolSpec,
    MemoryAccessContext, MemoryEvidence, MemoryEvidenceBundle, MemoryQuery, MemoryScope, Path,
    Serialize, SessionId, TaskId, agent_output_contract, fs, language_model_system_instructions,
    model_visible_text_excerpt, render_agent_package_system_prompt,
    render_agent_system_prompt_with_descriptors,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TurnContext {
    pub session_id: SessionId,
    pub role: AgentRole,
    pub provider: String,
    pub model: String,
    pub output_format: AgentOutputFormat,
    pub response_format: LanguageModelResponseFormat,
    pub system_instructions: String,
    #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
    pub run_context: AgentRunContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_projection: Option<AgentSessionProjection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript_view: Vec<AgentTranscriptItem>,
    #[serde(default)]
    pub workspace_facts: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub loaded_instruction_files: Vec<InstructionFileContext>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prompt_references: Vec<PromptReferenceContext>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_attachments: Vec<AgentContextAttachment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_evidence_bundle: Option<MemoryEvidenceBundle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_update: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available_tool_specs: Vec<AgentToolDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available_skills: Vec<AvailableAgentSkill>,
    pub budget_estimate: ContextBudgetEstimate,
    pub report: ContextAssemblyReport,
    pub context_frame: ContextFrame,
    pub context_delta: ContextDelta,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AvailableAgentSkill {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExplicitAgentSkillContext {
    pub name: String,
    pub description: String,
    pub instructions: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ContextAssemblyReport {
    pub report_id: TaskId,
    pub step_index: usize,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub included: Vec<ContextSource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted: Vec<ContextOmission>,
    pub budget_estimate: ContextBudgetEstimate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_frame_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_context_frame_id: Option<String>,
    #[serde(default)]
    pub context_block_count: usize,
    #[serde(default)]
    pub stable_prefix_bytes: usize,
    #[serde(default)]
    pub cache_reuse_prefix_bytes: usize,
    #[serde(default)]
    pub cache_reuse_prefix_block_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_changed_block_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContextSource {
    pub kind: String,
    pub label: String,
    pub bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContextOmission {
    pub kind: String,
    pub label: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContextBudgetEstimate {
    pub system_prompt_bytes: usize,
    #[serde(default)]
    pub current_work_bytes: usize,
    #[serde(default)]
    pub old_summary_bytes: usize,
    #[serde(default)]
    pub retrieved_memory_bytes: usize,
    pub transcript_bytes: usize,
    pub tool_schema_bytes: usize,
    pub workspace_facts_bytes: usize,
    #[serde(default)]
    pub instruction_file_bytes: usize,
    #[serde(default)]
    pub prompt_reference_bytes: usize,
    #[serde(default)]
    pub context_attachment_bytes: usize,
    #[serde(default)]
    pub async_update_bytes: usize,
    pub total_bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_threshold_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lanes: Vec<ContextBudgetLane>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContextBudgetLane {
    pub kind: String,
    pub label: String,
    pub bytes: usize,
    pub priority: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum ContextBlockKind {
    SystemInstructions,
    ToolSchemas,
    ToolSurface,
    AvailableSkills,
    WorkspaceFacts,
    InstructionFile,
    PromptReference,
    SessionProjection,
    TranscriptItem,
    ContextAttachment,
    AsyncUpdate,
    RetrievedMemory,
    BudgetEstimate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ContextCachePolicy {
    StablePrefix,
    AppendOnly,
    VolatileTail,
    TraceOnly,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ContextBlock {
    pub block_id: String,
    pub stable_key: String,
    pub kind: ContextBlockKind,
    pub priority: u8,
    pub cache_policy: ContextCachePolicy,
    pub source: ContextSource,
    pub content_bytes: usize,
    pub token_estimate: usize,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContextFrameBlockRef {
    pub block_id: String,
    pub stable_key: String,
    pub kind: ContextBlockKind,
    pub cache_policy: ContextCachePolicy,
    pub bytes: usize,
    pub priority: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ContextFrame {
    pub frame_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_frame_id: Option<String>,
    pub session_id: SessionId,
    pub step_index: usize,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocks: Vec<ContextBlock>,
    pub total_bytes: usize,
    pub stable_prefix_bytes: usize,
    #[serde(default)]
    pub cache_reuse_prefix_bytes: usize,
    #[serde(default)]
    pub cache_reuse_prefix_block_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_changed_block_id: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ContextDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_frame_id: Option<String>,
    pub to_frame_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kept: Vec<ContextFrameBlockRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<ContextFrameBlockRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub updated: Vec<ContextBlockUpdate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compressed: Vec<ContextRetentionDecision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<ContextFrameBlockRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub externalized: Vec<ContextRetentionDecision>,
    #[serde(default)]
    pub cache_reuse_prefix_bytes: usize,
    #[serde(default)]
    pub cache_reuse_prefix_block_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_changed_block_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContextBlockUpdate {
    pub stable_key: String,
    pub previous_block_id: String,
    pub new_block_id: String,
    pub reason: ContextRetentionReason,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContextRetentionDecision {
    pub stable_key: String,
    pub block_id: String,
    pub action: ContextRetentionAction,
    pub reason: ContextRetentionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_block_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ContextRetentionAction {
    Keep,
    CompressInto,
    Drop,
    Externalize,
    Defer,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ContextRetentionReason {
    Unchanged,
    Added,
    SourceChanged,
    Removed,
    BudgetExceeded,
    Superseded,
    ToolResultTooLarge,
    PromptReferenceMissing,
    UserRequested,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InstructionFileContext {
    pub path: String,
    pub bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_bytes: Option<usize>,
    #[serde(default)]
    pub truncated: bool,
    pub content: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PromptReferenceContext {
    pub label: String,
    pub path: String,
    pub kind: PromptReferenceKind,
    pub bytes: usize,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<PromptReferenceEntry>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PromptReferenceKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PromptReferenceEntry {
    pub path: String,
    pub kind: String,
}

pub(super) fn resolve_prompt_references(
    input: &AgentLoopInput,
    config: &AgentConfig,
) -> (
    Vec<PromptReferenceContext>,
    Vec<ContextSource>,
    Vec<ContextOmission>,
) {
    if !config.expand_prompt_references {
        return (Vec::new(), Vec::new(), Vec::new());
    }
    let labels = extract_prompt_reference_labels(&latest_event_prompt_text(input));
    if labels.is_empty() {
        return (Vec::new(), Vec::new(), Vec::new());
    }
    let Some(root) = config.workspace_root.as_ref() else {
        let omitted = labels
            .into_iter()
            .map(|label| ContextOmission {
                kind: "prompt_reference".to_string(),
                label,
                reason: "workspace root is not configured for prompt reference expansion"
                    .to_string(),
            })
            .collect();
        return (Vec::new(), Vec::new(), omitted);
    };

    let mut references = Vec::new();
    let mut included = Vec::new();
    let mut omitted = Vec::new();
    for label in labels {
        match resolve_prompt_reference(root, &label, config) {
            Ok(reference) => {
                included.push(ContextSource {
                    kind: match reference.kind {
                        PromptReferenceKind::File => "prompt_reference_file".to_string(),
                        PromptReferenceKind::Directory => "prompt_reference_directory".to_string(),
                    },
                    label: reference.path.clone(),
                    bytes: reference.bytes,
                });
                references.push(reference);
            }
            Err(reason) => omitted.push(ContextOmission {
                kind: "prompt_reference".to_string(),
                label,
                reason,
            }),
        }
    }
    (references, included, omitted)
}

pub(super) fn resolve_instruction_files(
    config: &AgentConfig,
) -> (
    Vec<InstructionFileContext>,
    Vec<ContextSource>,
    Vec<ContextOmission>,
) {
    let Some(root) = config.workspace_root.as_ref() else {
        return (Vec::new(), Vec::new(), Vec::new());
    };
    let mut files = Vec::new();
    let mut included = Vec::new();
    let mut omitted = Vec::new();
    for instruction_file in &config.workspace_instruction_files {
        match resolve_instruction_file(root, instruction_file, config.instruction_file_max_bytes) {
            Ok(file) => {
                included.push(ContextSource {
                    kind: "instruction_file".to_string(),
                    label: file.path.clone(),
                    bytes: file.bytes,
                });
                files.push(file);
            }
            Err(reason) => omitted.push(ContextOmission {
                kind: "instruction_file".to_string(),
                label: instruction_file.display().to_string(),
                reason,
            }),
        }
    }
    (files, included, omitted)
}

pub(super) fn resolve_instruction_file(
    root: &Path,
    instruction_file: &Path,
    max_bytes: Option<usize>,
) -> Result<InstructionFileContext, String> {
    let root = fs::canonicalize(root).map_err(|source| {
        format!(
            "workspace root {} is not accessible: {source}",
            root.display()
        )
    })?;
    let joined = if instruction_file.is_absolute() {
        instruction_file.to_path_buf()
    } else {
        root.join(instruction_file)
    };
    let resolved = fs::canonicalize(&joined).map_err(|source| {
        format!(
            "instruction file {} is not accessible: {source}",
            joined.display()
        )
    })?;
    if !resolved.starts_with(&root) {
        return Err(format!(
            "instruction file {} escapes workspace root {}",
            resolved.display(),
            root.display()
        ));
    }
    let metadata = fs::metadata(&resolved)
        .map_err(|source| format!("failed to stat {}: {source}", resolved.display()))?;
    if !metadata.is_file() {
        return Err(format!(
            "instruction path {} is not a regular file",
            resolved.display()
        ));
    }
    let bytes = fs::read(&resolved)
        .map_err(|source| format!("failed to read {}: {source}", resolved.display()))?;
    let visible_bytes = max_bytes
        .map(|max_bytes| bytes.len().min(max_bytes))
        .unwrap_or(bytes.len());
    let truncated = visible_bytes < bytes.len();
    Ok(InstructionFileContext {
        path: resolved
            .strip_prefix(&root)
            .unwrap_or(&resolved)
            .display()
            .to_string(),
        bytes: visible_bytes,
        original_bytes: truncated.then_some(bytes.len()),
        truncated,
        content: String::from_utf8_lossy(&bytes[..visible_bytes]).to_string(),
    })
}

pub(super) fn latest_event_prompt_text(input: &AgentLoopInput) -> String {
    let mut text = String::new();
    collect_prompt_text(&input.latest_event.payload, &mut text);
    text
}

pub(super) fn collect_prompt_text(value: &serde_json::Value, text: &mut String) {
    match value {
        serde_json::Value::String(value) => {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(value);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_prompt_text(value, text);
            }
        }
        serde_json::Value::Object(map) => {
            for key in ["text", "prompt", "message", "content"] {
                if let Some(value) = map.get(key) {
                    collect_prompt_text(value, text);
                }
            }
        }
        _ => {}
    }
}

pub(super) fn extract_prompt_reference_labels(text: &str) -> Vec<String> {
    let mut labels = Vec::new();
    for token in text.split_whitespace() {
        let Some(raw) = token.strip_prefix('@') else {
            continue;
        };
        let label = raw
            .trim_matches(|ch: char| {
                matches!(
                    ch,
                    ',' | ';' | ':' | '!' | '?' | ')' | '(' | '[' | ']' | '"' | '\''
                )
            })
            .trim();
        if label.is_empty()
            || label.contains("://")
            || label.contains('@')
            || labels.iter().any(|existing| existing == label)
        {
            continue;
        }
        labels.push(label.to_string());
    }
    labels
}

pub(super) fn resolve_prompt_reference(
    root: &Path,
    label: &str,
    config: &AgentConfig,
) -> Result<PromptReferenceContext, String> {
    let root = fs::canonicalize(root).map_err(|source| {
        format!(
            "workspace root {} is not accessible: {source}",
            root.display()
        )
    })?;
    let requested = Path::new(label);
    let joined = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        root.join(requested)
    };
    let resolved = fs::canonicalize(&joined).map_err(|source| {
        format!(
            "prompt reference {} is not accessible: {source}",
            joined.display()
        )
    })?;
    if !resolved.starts_with(&root) {
        return Err(format!(
            "prompt reference {} escapes workspace root {}",
            resolved.display(),
            root.display()
        ));
    }
    let metadata = fs::metadata(&resolved)
        .map_err(|source| format!("failed to stat {}: {source}", resolved.display()))?;
    let display_path = resolved
        .strip_prefix(&root)
        .unwrap_or(&resolved)
        .display()
        .to_string();
    if metadata.is_dir() {
        return resolve_prompt_reference_directory(&resolved, &root, label, &display_path, config);
    }
    if metadata.is_file() {
        return resolve_prompt_reference_file(&resolved, label, &display_path, config);
    }
    Err(format!(
        "prompt reference {} is neither a regular file nor a directory",
        resolved.display()
    ))
}

pub(super) fn resolve_prompt_reference_file(
    resolved: &Path,
    label: &str,
    display_path: &str,
    config: &AgentConfig,
) -> Result<PromptReferenceContext, String> {
    let bytes = fs::read(resolved)
        .map_err(|source| format!("failed to read {}: {source}", resolved.display()))?;
    let visible_bytes = config
        .prompt_reference_max_bytes
        .map(|max_bytes| bytes.len().min(max_bytes))
        .unwrap_or(bytes.len());
    let truncated = visible_bytes < bytes.len();
    Ok(PromptReferenceContext {
        label: label.to_string(),
        path: display_path.to_string(),
        kind: PromptReferenceKind::File,
        bytes: visible_bytes,
        truncated,
        content: Some(String::from_utf8_lossy(&bytes[..visible_bytes]).to_string()),
        entries: Vec::new(),
    })
}

pub(super) fn resolve_prompt_reference_directory(
    resolved: &Path,
    root: &Path,
    label: &str,
    display_path: &str,
    config: &AgentConfig,
) -> Result<PromptReferenceContext, String> {
    let mut dir_entries = fs::read_dir(resolved)
        .map_err(|source| format!("failed to list {}: {source}", resolved.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| format!("failed to read entry in {}: {source}", resolved.display()))?;
    dir_entries.sort_by_key(|entry| entry.file_name());
    let max_entries = config
        .prompt_reference_max_entries
        .unwrap_or(dir_entries.len());
    let truncated = dir_entries.len() > max_entries;
    let mut entries = Vec::new();
    for entry in dir_entries.into_iter().take(max_entries) {
        let file_type = entry
            .file_type()
            .map_err(|source| format!("failed to read type for {:?}: {source}", entry.path()))?;
        let kind = if file_type.is_dir() {
            "directory"
        } else if file_type.is_file() {
            "file"
        } else if file_type.is_symlink() {
            "symlink"
        } else {
            "other"
        };
        let path = entry.path();
        entries.push(PromptReferenceEntry {
            path: path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string(),
            kind: kind.to_string(),
        });
    }
    let bytes = serde_json::to_vec(&entries)
        .map(|bytes| bytes.len())
        .unwrap_or_default();
    Ok(PromptReferenceContext {
        label: label.to_string(),
        path: display_path.to_string(),
        kind: PromptReferenceKind::Directory,
        bytes,
        truncated,
        content: None,
        entries,
    })
}

pub(super) fn model_visible_context_attachments(
    attachments: &[AgentContextAttachment],
    max_payload_bytes: Option<usize>,
) -> Vec<AgentContextAttachment> {
    attachments
        .iter()
        .map(|attachment| model_visible_context_attachment(attachment, max_payload_bytes))
        .collect()
}

pub(super) fn model_visible_context_attachment(
    attachment: &AgentContextAttachment,
    max_payload_bytes: Option<usize>,
) -> AgentContextAttachment {
    let Some(max_payload_bytes) = max_payload_bytes else {
        return attachment.clone();
    };
    let payload_bytes = serde_json::to_vec(&attachment.payload_json).unwrap_or_default();
    if payload_bytes.len() <= max_payload_bytes {
        return attachment.clone();
    }
    let payload_text = serde_json::to_string_pretty(&attachment.payload_json)
        .or_else(|_| serde_json::to_string(&attachment.payload_json))
        .unwrap_or_default();
    let payload_json = bounded_single_string_json_projection(
        &payload_text,
        max_payload_bytes,
        |payload_json_excerpt| {
            serde_json::json!({
                "model_visible_truncated": true,
                "reason": "context attachment payload exceeded model-visible byte budget",
                "payload_original_bytes": payload_bytes.len(),
                "payload_budget_bytes": max_payload_bytes,
                "payload_json_excerpt": payload_json_excerpt,
            })
        },
    );
    AgentContextAttachment {
        kind: attachment.kind.clone(),
        summary: attachment.summary.clone(),
        payload_json,
        payload_original_bytes: Some(payload_bytes.len()),
        payload_model_visible_truncated: true,
    }
}

pub(super) fn model_visible_async_update(
    update: Option<&AsyncTaskUpdate>,
    max_update_bytes: Option<usize>,
) -> Option<serde_json::Value> {
    let update = update?;
    let value = serde_json::to_value(update).unwrap_or_else(|_| {
        serde_json::json!({
            "kind": &update.kind,
            "task_id": &update.task_id,
            "status": &update.status,
            "summary": &update.summary,
            "needs_user_attention": update.needs_user_attention,
            "result_ref": &update.result_ref,
            "next_action_hint": &update.next_action_hint,
        })
    });
    let Some(max_update_bytes) = max_update_bytes else {
        return Some(value);
    };
    let update_bytes = serde_json::to_vec(&value).unwrap_or_default();
    if update_bytes.len() <= max_update_bytes {
        return Some(value);
    }
    Some(bounded_async_update_projection(
        update,
        update_bytes.len(),
        max_update_bytes,
    ))
}

pub(super) fn bounded_single_string_json_projection<F>(
    source: &str,
    max_bytes: usize,
    build: F,
) -> serde_json::Value
where
    F: Fn(String) -> serde_json::Value,
{
    let mut low = 0usize;
    let mut high = source.len().min(max_bytes);
    let mut best = build(String::new());
    while low <= high {
        let mid = low + (high - low) / 2;
        let candidate = build(model_visible_text_excerpt(source, mid));
        let candidate_bytes = serde_json::to_vec(&candidate)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if candidate_bytes <= max_bytes {
            best = candidate;
            low = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
    best
}

pub(super) fn bounded_async_update_projection(
    update: &AsyncTaskUpdate,
    original_bytes: usize,
    max_bytes: usize,
) -> serde_json::Value {
    let build = |excerpt_bytes: usize| {
        serde_json::json!({
            "kind": &update.kind,
            "task_id": &update.task_id,
            "status": &update.status,
            "needs_user_attention": update.needs_user_attention,
            "next_action_hint": &update.next_action_hint,
            "model_visible_truncated": true,
            "reason": "async task update exceeded model-visible byte budget",
            "async_update_original_bytes": original_bytes,
            "async_update_budget_bytes": max_bytes,
            "summary_excerpt": model_visible_text_excerpt(&update.summary, excerpt_bytes),
            "summary_original_bytes": update.summary.len(),
            "summary_model_visible_truncated": update.summary.len() > excerpt_bytes,
            "result_ref_excerpt": update
                .result_ref
                .as_deref()
                .map(|result_ref| model_visible_text_excerpt(result_ref, excerpt_bytes)),
            "result_ref_original_bytes": update
                .result_ref
                .as_ref()
                .map(|result_ref| result_ref.len()),
            "result_ref_model_visible_truncated": update
                .result_ref
                .as_ref()
                .is_some_and(|result_ref| result_ref.len() > excerpt_bytes),
        })
    };
    let mut low = 0usize;
    let mut high = update
        .summary
        .len()
        .max(
            update
                .result_ref
                .as_ref()
                .map(|value| value.len())
                .unwrap_or(0),
        )
        .min(max_bytes);
    let mut best = build(0);
    while low <= high {
        let mid = low + (high - low) / 2;
        let candidate = build(mid);
        let candidate_bytes = serde_json::to_vec(&candidate)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if candidate_bytes <= max_bytes {
            best = candidate;
            low = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
    best
}

pub(super) fn serialized_nonempty_slice_bytes<T>(values: &[T]) -> usize
where
    T: Serialize,
{
    if values.is_empty() {
        0
    } else {
        serde_json::to_vec(values)
            .map(|bytes| bytes.len())
            .unwrap_or_default()
    }
}

pub(super) fn serialized_option_value_bytes(value: &Option<serde_json::Value>) -> usize {
    value
        .as_ref()
        .and_then(|value| serde_json::to_vec(value).ok())
        .map(|bytes| bytes.len())
        .unwrap_or_default()
}

pub(super) fn context_block(
    kind: ContextBlockKind,
    stable_key: impl Into<String>,
    label: impl Into<String>,
    priority: u8,
    cache_policy: ContextCachePolicy,
    payload: serde_json::Value,
) -> ContextBlock {
    let stable_key = stable_key.into();
    let label = label.into();
    let content_bytes = serde_json::to_vec(&payload)
        .map(|bytes| bytes.len())
        .unwrap_or_default();
    let block_id = context_block_id(&kind, &stable_key, &payload);
    ContextBlock {
        block_id,
        stable_key,
        kind: kind.clone(),
        priority,
        cache_policy,
        source: ContextSource {
            kind: context_block_source_kind(&kind).to_string(),
            label,
            bytes: content_bytes,
        },
        content_bytes,
        token_estimate: content_bytes.div_ceil(CONTEXT_ESTIMATE_BYTES_PER_TOKEN),
        payload,
    }
}

pub(super) fn context_block_id(
    kind: &ContextBlockKind,
    stable_key: &str,
    payload: &serde_json::Value,
) -> String {
    let fingerprint = serde_json::json!({
        "schema": "sinomo.context_block.v1",
        "kind": kind,
        "stable_key": stable_key,
        "payload": payload,
    });
    format!("ctxb_{}", blake3_hash_json(&fingerprint))
}

pub(super) fn context_frame_id(provider: &str, model: &str, blocks: &[ContextBlock]) -> String {
    let block_ids = blocks
        .iter()
        .map(|block| block.block_id.as_str())
        .collect::<Vec<_>>();
    let fingerprint = serde_json::json!({
        "schema": "sinomo.context_frame.v1",
        "provider": provider,
        "model": model,
        "blocks": block_ids,
    });
    format!("ctxf_{}", blake3_hash_json(&fingerprint))
}

pub(super) fn blake3_hash_json(value: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    blake3::hash(&bytes).to_hex().to_string()
}

pub(super) fn context_block_source_kind(kind: &ContextBlockKind) -> &'static str {
    match kind {
        ContextBlockKind::SystemInstructions => "system_instructions",
        ContextBlockKind::ToolSchemas => "tool_schemas",
        ContextBlockKind::ToolSurface => "agent_tool_surface",
        ContextBlockKind::AvailableSkills => "available_skills",
        ContextBlockKind::WorkspaceFacts => "workspace_facts",
        ContextBlockKind::InstructionFile => "instruction_file",
        ContextBlockKind::PromptReference => "prompt_reference",
        ContextBlockKind::SessionProjection => "session_projection",
        ContextBlockKind::TranscriptItem => "transcript_item",
        ContextBlockKind::ContextAttachment => "context_attachment",
        ContextBlockKind::AsyncUpdate => "async_update",
        ContextBlockKind::RetrievedMemory => "retrieved_memory",
        ContextBlockKind::BudgetEstimate => "context_budget",
    }
}

pub(super) fn context_frame(
    session_id: SessionId,
    step_index: usize,
    provider: impl Into<String>,
    model: impl Into<String>,
    blocks: Vec<ContextBlock>,
) -> ContextFrame {
    let provider = provider.into();
    let model = model.into();
    let total_bytes = blocks
        .iter()
        .map(|block| block.content_bytes)
        .sum::<usize>();
    let stable_prefix_bytes = stable_prefix_bytes(&blocks);
    let frame_id = context_frame_id(&provider, &model, &blocks);
    ContextFrame {
        frame_id,
        parent_frame_id: None,
        session_id,
        step_index,
        provider,
        model,
        blocks,
        total_bytes,
        stable_prefix_bytes,
        cache_reuse_prefix_bytes: 0,
        cache_reuse_prefix_block_count: 0,
        first_changed_block_id: None,
    }
}

pub(super) fn memory_evidence_context_block(
    evidence: &MemoryEvidence,
    bundle: &MemoryEvidenceBundle,
) -> ContextBlock {
    let cache_policy = bundle
        .files
        .iter()
        .find(|file| file.source.version == evidence.anchor.source_version)
        .map(|file| memory_cache_policy(&file.source.policy.scope))
        .unwrap_or(ContextCachePolicy::AppendOnly);
    context_block(
        ContextBlockKind::RetrievedMemory,
        format!("memory/evidence/{}", evidence.anchor.anchor_id),
        format!(
            "memory evidence {} ({})",
            evidence.anchor.anchor_id, evidence.anchor.source_version.file_ref.path_hint
        ),
        1,
        cache_policy,
        // Ranking, freshness, coverage and the query trace are intentionally
        // absent. The exact source version, selector and content are stable
        // model context; volatile retrieval details remain in the bundle.
        serde_json::json!({
            "anchor": &evidence.anchor,
            "content": &evidence.content,
        }),
    )
}

pub(super) fn memory_cache_policy(scope: &MemoryScope) -> ContextCachePolicy {
    match scope {
        MemoryScope::Cluster | MemoryScope::User { .. } | MemoryScope::Workspace { .. } => {
            ContextCachePolicy::StablePrefix
        }
        MemoryScope::Agent { .. }
        | MemoryScope::Chat { .. }
        | MemoryScope::Channel { .. }
        | MemoryScope::Task { .. }
        | MemoryScope::Team { .. } => ContextCachePolicy::AppendOnly,
    }
}

pub(super) fn refresh_context_budget_block(context: &mut TurnContext) {
    if let Some(index) = context
        .context_frame
        .blocks
        .iter()
        .position(|block| block.kind == ContextBlockKind::BudgetEstimate)
    {
        context.context_frame.blocks[index] = context_block(
            ContextBlockKind::BudgetEstimate,
            "context/budget/estimate",
            "context budget estimate",
            3,
            ContextCachePolicy::VolatileTail,
            serde_json::to_value(&context.budget_estimate).unwrap_or(serde_json::Value::Null),
        );
    }
}

pub(super) fn apply_memory_evidence_to_context(
    context: &mut TurnContext,
    bundle: MemoryEvidenceBundle,
    byte_budget: usize,
) {
    let mut memory_blocks = Vec::new();
    let mut accepted_keys = BTreeSet::new();
    let mut omitted_count = 0usize;
    let mut retrieved_memory_bytes = 0usize;
    let mut model_visible_payloads = Vec::new();

    for evidence in &bundle.evidence {
        if memory_blocks.len() >= DEFAULT_RETRIEVED_MEMORY_RECORDS
            || !accepted_keys.insert(evidence.anchor.anchor_id.to_string())
        {
            omitted_count = omitted_count.saturating_add(1);
            continue;
        }
        let block = memory_evidence_context_block(evidence, &bundle);
        let mut candidate_payloads = model_visible_payloads.clone();
        candidate_payloads.push(block.payload.clone());
        let rendered_bytes = serde_json::to_vec(&candidate_payloads)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if rendered_bytes > byte_budget {
            omitted_count = omitted_count.saturating_add(1);
            continue;
        }
        model_visible_payloads = candidate_payloads;
        retrieved_memory_bytes = retrieved_memory_bytes.saturating_add(block.content_bytes);
        memory_blocks.push(block);
    }
    if let Some(reason) = memory_coverage_omission_reason(&bundle.coverage) {
        context.report.omitted.push(ContextOmission {
            kind: "memory_coverage".to_string(),
            label: "memory evidence retrieval".to_string(),
            reason,
        });
    }
    // Preserve the provider's complete bounded bundle and trace for
    // inspection. Only the defensively packed ContextBlocks are rendered to
    // the model.
    context.memory_evidence_bundle = Some(bundle);

    if omitted_count > 0 {
        context.report.omitted.push(ContextOmission {
            kind: "retrieved_memory".to_string(),
            label: format!("{omitted_count} memory evidence item(s)"),
            reason: format!(
                "agent context packing omitted evidence beyond the {byte_budget} byte and {} item limits",
                DEFAULT_RETRIEVED_MEMORY_RECORDS
            ),
        });
    }
    if memory_blocks.is_empty() {
        return;
    }

    context.budget_estimate.retrieved_memory_bytes += retrieved_memory_bytes;
    context.budget_estimate.total_bytes += retrieved_memory_bytes;
    if let Some(lane) = context
        .budget_estimate
        .lanes
        .iter_mut()
        .find(|lane| lane.kind == "retrieved_memories")
    {
        lane.bytes += retrieved_memory_bytes;
        lane.label = format!("{} retrieved memory evidence item(s)", memory_blocks.len());
    } else {
        context.budget_estimate.lanes.push(ContextBudgetLane {
            kind: "retrieved_memories".to_string(),
            label: format!("{} retrieved memory evidence item(s)", memory_blocks.len()),
            bytes: retrieved_memory_bytes,
            priority: 3,
        });
    }

    context.report.included.push(ContextSource {
        kind: "retrieved_memory".to_string(),
        label: format!("{} retrieved memory evidence item(s)", memory_blocks.len()),
        bytes: retrieved_memory_bytes,
    });
    context.report.budget_estimate = context.budget_estimate.clone();

    let insertion_index = context
        .context_frame
        .blocks
        .iter()
        .position(|block| block.kind == ContextBlockKind::TranscriptItem)
        .unwrap_or(context.context_frame.blocks.len());
    context
        .context_frame
        .blocks
        .splice(insertion_index..insertion_index, memory_blocks);
    refresh_context_budget_block(context);
}

pub(super) fn memory_coverage_omission_reason(coverage: &EvidenceCoverage) -> Option<String> {
    match coverage {
        EvidenceCoverage::Complete => None,
        EvidenceCoverage::Approximate { reason } => {
            Some(format!("memory evidence coverage is approximate: {reason}"))
        }
        EvidenceCoverage::BudgetTruncated { reason } => Some(format!(
            "memory evidence coverage is budget truncated: {reason}"
        )),
    }
}

pub(super) fn memory_context_byte_budget(context: &TurnContext) -> usize {
    context
        .budget_estimate
        .max_context_bytes
        .map(|max_context_bytes| {
            DEFAULT_RETRIEVED_MEMORY_BYTES
                .min(max_context_bytes.saturating_sub(context.budget_estimate.total_bytes))
        })
        .unwrap_or(DEFAULT_RETRIEVED_MEMORY_BYTES)
}

pub(super) fn memory_query_and_access(
    input: &AgentLoopInput,
    config: &AgentConfig,
    max_bytes: usize,
) -> (MemoryQuery, MemoryAccessContext) {
    let agent_id = config.definition.key().to_string();
    let task_id = config
        .run_context
        .task
        .as_ref()
        .map(|task| task.task_id.to_string());
    let workspace_id = config
        .run_context
        .workspace
        .as_ref()
        .map(|workspace| workspace.id.clone());
    let query = MemoryQuery {
        session_id: Some(input.session.session_id.clone()),
        agent_id: Some(agent_id.clone()),
        task_id: task_id.clone(),
        workspace_id: workspace_id.clone(),
        task_hint: memory_task_hint(input),
        max_records: DEFAULT_RETRIEVED_MEMORY_RECORDS,
        max_bytes,
        ..MemoryQuery::default()
    };

    let mut access = MemoryAccessContext::cluster_local();
    access.subject_agent_id = Some(agent_id.clone());
    access.allowed_scopes.push(MemoryScope::Agent { agent_id });
    if let Some(task_id) = task_id {
        access.allowed_scopes.push(MemoryScope::Task { task_id });
    }
    if let Some(workspace_id) = workspace_id {
        access
            .allowed_scopes
            .push(MemoryScope::Workspace { workspace_id });
    }
    (query, access)
}

pub(super) fn memory_task_hint(input: &AgentLoopInput) -> Option<String> {
    let mut hint = input.latest_event.kind.to_string();
    if !input.latest_event.payload.is_null() {
        let payload = serde_json::to_string(&input.latest_event.payload).unwrap_or_default();
        if !payload.is_empty() {
            hint.push(' ');
            hint.extend(payload.chars().take(512));
        }
    }
    Some(hint)
}

pub(super) fn refresh_context_frame(frame: &mut ContextFrame) {
    frame.total_bytes = frame
        .blocks
        .iter()
        .map(|block| block.content_bytes)
        .sum::<usize>();
    frame.stable_prefix_bytes = stable_prefix_bytes(&frame.blocks);
    frame.frame_id = context_frame_id(&frame.provider, &frame.model, &frame.blocks);
}

pub(super) fn stable_prefix_bytes(blocks: &[ContextBlock]) -> usize {
    blocks
        .iter()
        .take_while(|block| block.cache_policy == ContextCachePolicy::StablePrefix)
        .map(|block| block.content_bytes)
        .sum()
}

pub(super) fn context_block_ref(block: &ContextBlock) -> ContextFrameBlockRef {
    ContextFrameBlockRef {
        block_id: block.block_id.clone(),
        stable_key: block.stable_key.clone(),
        kind: block.kind.clone(),
        cache_policy: block.cache_policy.clone(),
        bytes: block.content_bytes,
        priority: block.priority,
    }
}

pub(super) fn context_delta(parent: Option<&ContextFrame>, frame: &ContextFrame) -> ContextDelta {
    let Some(parent) = parent else {
        return ContextDelta {
            from_frame_id: None,
            to_frame_id: frame.frame_id.clone(),
            added: frame.blocks.iter().map(context_block_ref).collect(),
            first_changed_block_id: frame.blocks.first().map(|block| block.block_id.clone()),
            ..ContextDelta::default()
        };
    };

    let mut kept = Vec::new();
    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();
    let (cache_reuse_prefix_bytes, cache_reuse_prefix_block_count, first_changed_block_id) =
        context_cache_reuse(parent, frame);

    for block in &frame.blocks {
        match parent
            .blocks
            .iter()
            .find(|previous| previous.stable_key == block.stable_key)
        {
            Some(previous) if previous.block_id == block.block_id => {
                kept.push(context_block_ref(block))
            }
            Some(previous) => updated.push(ContextBlockUpdate {
                stable_key: block.stable_key.clone(),
                previous_block_id: previous.block_id.clone(),
                new_block_id: block.block_id.clone(),
                reason: ContextRetentionReason::SourceChanged,
            }),
            None => added.push(context_block_ref(block)),
        }
    }

    for previous in &parent.blocks {
        if !frame
            .blocks
            .iter()
            .any(|block| block.stable_key == previous.stable_key)
        {
            removed.push(context_block_ref(previous));
        }
    }

    ContextDelta {
        from_frame_id: Some(parent.frame_id.clone()),
        to_frame_id: frame.frame_id.clone(),
        kept,
        added,
        updated,
        removed,
        cache_reuse_prefix_bytes,
        cache_reuse_prefix_block_count,
        first_changed_block_id,
        compressed: Vec::new(),
        externalized: Vec::new(),
    }
}

pub(super) fn context_cache_reuse(
    parent: &ContextFrame,
    frame: &ContextFrame,
) -> (usize, usize, Option<String>) {
    let mut bytes = 0usize;
    let mut block_count = 0usize;
    for (previous, current) in parent.blocks.iter().zip(frame.blocks.iter()) {
        if previous.block_id != current.block_id {
            return (bytes, block_count, Some(current.block_id.clone()));
        }
        bytes = bytes.saturating_add(current.content_bytes);
        block_count = block_count.saturating_add(1);
    }
    if frame.blocks.len() > parent.blocks.len() {
        return (
            bytes,
            block_count,
            frame
                .blocks
                .get(block_count)
                .map(|block| block.block_id.clone()),
        );
    }
    if parent.blocks.len() > frame.blocks.len() {
        return (bytes, block_count, None);
    }
    (bytes, block_count, None)
}

pub(super) fn apply_context_parent(context: &mut TurnContext, parent: Option<&ContextFrame>) {
    context.context_frame.parent_frame_id = parent.map(|frame| frame.frame_id.clone());
    refresh_context_frame(&mut context.context_frame);
    context.context_delta = context_delta(parent, &context.context_frame);
    context.context_frame.cache_reuse_prefix_bytes = context.context_delta.cache_reuse_prefix_bytes;
    context.context_frame.cache_reuse_prefix_block_count =
        context.context_delta.cache_reuse_prefix_block_count;
    context.context_frame.first_changed_block_id =
        context.context_delta.first_changed_block_id.clone();
    apply_context_frame_summary_to_report(
        &mut context.report,
        &context.context_frame,
        &context.context_delta,
    );
}

pub(super) fn apply_context_frame_summary_to_report(
    report: &mut ContextAssemblyReport,
    frame: &ContextFrame,
    delta: &ContextDelta,
) {
    report.context_frame_id = Some(frame.frame_id.clone());
    report.parent_context_frame_id = frame.parent_frame_id.clone();
    report.context_block_count = frame.blocks.len();
    report.stable_prefix_bytes = frame.stable_prefix_bytes;
    report.cache_reuse_prefix_bytes = delta.cache_reuse_prefix_bytes;
    report.cache_reuse_prefix_block_count = delta.cache_reuse_prefix_block_count;
    report.first_changed_block_id = delta.first_changed_block_id.clone();
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ContextAssembler;

impl ContextAssembler {
    pub fn assemble(
        &self,
        input: &AgentLoopInput,
        role: AgentRole,
        config: &AgentConfig,
        transcript: &[AgentTranscriptItem],
        session_projection: Option<AgentSessionProjection>,
        tool_descriptors: Vec<AgentToolDescriptor>,
        step_index: usize,
    ) -> Result<TurnContext, AgentError> {
        let mut system_instructions = render_agent_package_system_prompt(&config.definition);
        let transient_system_context = transient_system_context(input);
        if let Some(context) = transient_system_context {
            if !system_instructions.is_empty() {
                system_instructions.push_str("\n\n");
            }
            system_instructions.push_str(context);
        }
        let rendered_system_instruction_bytes = language_model_system_instructions(
            &config.provider,
            &config.model,
            &system_instructions,
            Some(agent_output_contract(&config.definition.output_format)),
        )
        .len();
        let mut workspace_facts = serde_json::json!({
            "session_id": input.session.session_id,
            "surface": input.session.surface,
            "latest_event_kind": input.latest_event.kind,
            "latest_event_id": input.latest_event.event_id,
        });
        if !config.run_context.is_empty()
            && let Some(object) = workspace_facts.as_object_mut()
        {
            object.insert(
                "run_context".to_string(),
                serde_json::json!(&config.run_context),
            );
        }
        let tool_schema_bytes = serde_json::to_vec(&tool_descriptors)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let workspace_facts_bytes = serde_json::to_vec(&workspace_facts)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let (loaded_instruction_files, instruction_sources, instruction_omissions) =
            resolve_instruction_files(config);
        let instruction_file_bytes = serialized_nonempty_slice_bytes(&loaded_instruction_files);
        let (prompt_references, prompt_reference_sources, prompt_reference_omissions) =
            resolve_prompt_references(input, config);
        let prompt_reference_bytes = serialized_nonempty_slice_bytes(&prompt_references);
        let context_attachments = model_visible_context_attachments(
            &input.context_attachments,
            config.context_attachment_max_bytes,
        );
        let context_attachment_bytes = serialized_nonempty_slice_bytes(&context_attachments);
        let async_update =
            model_visible_async_update(input.async_update.as_ref(), config.async_update_max_bytes);
        let async_update_bytes = serialized_option_value_bytes(&async_update);
        let session_projection_bytes = session_projection
            .as_ref()
            .and_then(|projection| serde_json::to_vec(projection).ok())
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let current_work_bytes = session_projection
            .as_ref()
            .and_then(|projection| serde_json::to_vec(&projection.working_state).ok())
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let old_summary_bytes = session_projection
            .as_ref()
            .map(|projection| projection.summary.len())
            .unwrap_or_default();
        let retrieved_memory_bytes = 0usize;
        let transcript_bytes: usize = transcript.iter().map(|item| item.content.len()).sum();
        let mut lanes = vec![
            ContextBudgetLane {
                kind: "fixed_prompt".to_string(),
                label:
                    "system prompt, request tool schemas, workspace facts, and instruction files"
                        .to_string(),
                bytes: rendered_system_instruction_bytes
                    + tool_schema_bytes
                    + workspace_facts_bytes
                    + instruction_file_bytes,
                priority: 0,
            },
            ContextBudgetLane {
                kind: "current_work".to_string(),
                label: "typed active work state from session projection".to_string(),
                bytes: current_work_bytes,
                priority: 1,
            },
            ContextBudgetLane {
                kind: "recent_tail".to_string(),
                label: format!("{} transcript item(s)", transcript.len()),
                bytes: transcript_bytes,
                priority: 2,
            },
            ContextBudgetLane {
                kind: "retrieved_memories".to_string(),
                label: "retrieved memory/context entries".to_string(),
                bytes: retrieved_memory_bytes,
                priority: 3,
            },
            ContextBudgetLane {
                kind: "old_summaries".to_string(),
                label: "prior compacted session summaries".to_string(),
                bytes: old_summary_bytes,
                priority: 4,
            },
        ];
        if prompt_reference_bytes > 0 {
            lanes.push(ContextBudgetLane {
                kind: "prompt_references".to_string(),
                label: "user-requested prompt references".to_string(),
                bytes: prompt_reference_bytes,
                priority: 1,
            });
        }
        if context_attachment_bytes > 0 {
            lanes.push(ContextBudgetLane {
                kind: "context_attachments".to_string(),
                label: "typed context attachments".to_string(),
                bytes: context_attachment_bytes,
                priority: 1,
            });
        }
        if async_update_bytes > 0 {
            lanes.push(ContextBudgetLane {
                kind: "async_update".to_string(),
                label: "latest async task update".to_string(),
                bytes: async_update_bytes,
                priority: 1,
            });
        }
        let budget_estimate = ContextBudgetEstimate {
            system_prompt_bytes: rendered_system_instruction_bytes,
            current_work_bytes,
            old_summary_bytes,
            retrieved_memory_bytes,
            transcript_bytes,
            tool_schema_bytes,
            workspace_facts_bytes,
            instruction_file_bytes,
            prompt_reference_bytes,
            context_attachment_bytes,
            async_update_bytes,
            total_bytes: rendered_system_instruction_bytes
                + session_projection_bytes
                + transcript_bytes
                + tool_schema_bytes
                + workspace_facts_bytes
                + instruction_file_bytes
                + prompt_reference_bytes
                + context_attachment_bytes
                + async_update_bytes,
            max_context_bytes: config.context_budget.max_context_bytes,
            compaction_threshold_bytes: config.context_budget.compaction_threshold_bytes(),
            lanes,
        };
        let mut included = vec![
            ContextSource {
                kind: "system_instructions".to_string(),
                label: config.definition.name.clone(),
                bytes: budget_estimate.system_prompt_bytes,
            },
            ContextSource {
                kind: "transcript_tail".to_string(),
                label: format!("{} transcript item(s)", transcript.len()),
                bytes: budget_estimate.transcript_bytes,
            },
            ContextSource {
                kind: "tool_schemas".to_string(),
                label: format!("{} visible tool(s)", tool_descriptors.len()),
                bytes: budget_estimate.tool_schema_bytes,
            },
            ContextSource {
                kind: "workspace_facts".to_string(),
                label: "session surface and latest event".to_string(),
                bytes: budget_estimate.workspace_facts_bytes,
            },
        ];
        if let Some(context) = transient_system_context {
            included.push(ContextSource {
                kind: "transient_system_context".to_string(),
                label: "surface-owned turn instructions".to_string(),
                bytes: context.len(),
            });
        }
        if session_projection_bytes > 0 {
            included.push(ContextSource {
                kind: "session_projection".to_string(),
                label: "current work projection".to_string(),
                bytes: session_projection_bytes,
            });
        }
        included.extend(instruction_sources);
        included.extend(prompt_reference_sources);
        if !context_attachments.is_empty() {
            included.push(ContextSource {
                kind: "context_attachments".to_string(),
                label: "typed context attachments".to_string(),
                bytes: budget_estimate.context_attachment_bytes,
            });
        }
        if input.async_update.is_some() {
            included.push(ContextSource {
                kind: "async_update".to_string(),
                label: "latest async task update".to_string(),
                bytes: budget_estimate.async_update_bytes,
            });
        }
        let mut omitted = instruction_omissions;
        omitted.extend(prompt_reference_omissions);
        if config
            .context_budget
            .max_context_bytes
            .is_some_and(|max_context_bytes| budget_estimate.total_bytes > max_context_bytes)
        {
            omitted.push(ContextOmission {
                kind: "context_budget".to_string(),
                label: "assembled_context".to_string(),
                reason: "assembled context exceeds inferred model budget; compaction should run before provider rendering".to_string(),
            });
        }
        let mut context_blocks = vec![
            context_block(
                ContextBlockKind::SystemInstructions,
                format!("agent/system/{}", config.definition.key()),
                config.definition.name.clone(),
                0,
                ContextCachePolicy::StablePrefix,
                serde_json::json!({
                    "content": language_model_system_instructions(
                        &config.provider,
                        &config.model,
                        &system_instructions,
                        Some(agent_output_contract(&config.definition.output_format)),
                    ),
                }),
            ),
            context_block(
                ContextBlockKind::ToolSchemas,
                format!("agent/tools/{}", config.definition.key()),
                format!("{} visible tool(s)", tool_descriptors.len()),
                0,
                ContextCachePolicy::StablePrefix,
                serde_json::to_value(&tool_descriptors).unwrap_or(serde_json::Value::Null),
            ),
        ];
        for instruction_file in &loaded_instruction_files {
            context_blocks.push(context_block(
                ContextBlockKind::InstructionFile,
                format!("workspace/instruction/{}", instruction_file.path),
                instruction_file.path.clone(),
                0,
                ContextCachePolicy::StablePrefix,
                serde_json::to_value(instruction_file).unwrap_or(serde_json::Value::Null),
            ));
        }
        for (index, item) in transcript.iter().enumerate() {
            let stable_key = item
                .tool_call_id
                .as_ref()
                .map(|call_id| format!("session/transcript/tool/{call_id}"))
                .unwrap_or_else(|| {
                    format!(
                        "session/transcript/{index}/{:?}/{}",
                        item.role, item.step_index
                    )
                });
            context_blocks.push(context_block(
                ContextBlockKind::TranscriptItem,
                stable_key,
                format!("{:?} transcript item {index}", item.role),
                2,
                ContextCachePolicy::AppendOnly,
                serde_json::to_value(item).unwrap_or(serde_json::Value::Null),
            ));
        }
        if let Some(projection) = &session_projection {
            context_blocks.push(context_block(
                ContextBlockKind::SessionProjection,
                "session/projection/current",
                "current work projection",
                1,
                ContextCachePolicy::VolatileTail,
                serde_json::to_value(projection).unwrap_or(serde_json::Value::Null),
            ));
        }
        for prompt_reference in &prompt_references {
            context_blocks.push(context_block(
                ContextBlockKind::PromptReference,
                format!("workspace/prompt-reference/{}", prompt_reference.path),
                prompt_reference.path.clone(),
                1,
                ContextCachePolicy::VolatileTail,
                serde_json::to_value(prompt_reference).unwrap_or(serde_json::Value::Null),
            ));
        }
        for (index, attachment) in context_attachments.iter().enumerate() {
            context_blocks.push(context_block(
                ContextBlockKind::ContextAttachment,
                format!("context/attachment/{index}/{}", attachment.kind),
                attachment.summary.clone(),
                1,
                ContextCachePolicy::VolatileTail,
                serde_json::to_value(attachment).unwrap_or(serde_json::Value::Null),
            ));
        }
        if let Some(update) = &async_update {
            context_blocks.push(context_block(
                ContextBlockKind::AsyncUpdate,
                "async/latest-update",
                "latest async task update",
                1,
                ContextCachePolicy::VolatileTail,
                update.clone(),
            ));
        }
        context_blocks.push(context_block(
            ContextBlockKind::WorkspaceFacts,
            "workspace/facts/latest-event",
            "session surface and latest event",
            2,
            ContextCachePolicy::VolatileTail,
            workspace_facts.clone(),
        ));
        context_blocks.push(context_block(
            ContextBlockKind::BudgetEstimate,
            "context/budget/estimate",
            "context budget estimate",
            3,
            ContextCachePolicy::VolatileTail,
            serde_json::to_value(&budget_estimate).unwrap_or(serde_json::Value::Null),
        ));
        let context_frame = context_frame(
            input.session.session_id.clone(),
            step_index,
            config.provider.clone(),
            config.model.clone(),
            context_blocks,
        );
        let context_delta = context_delta(None, &context_frame);
        let mut report = ContextAssemblyReport {
            report_id: TaskId::generate(),
            step_index,
            provider: config.provider.clone(),
            model: config.model.clone(),
            included,
            omitted,
            budget_estimate: budget_estimate.clone(),
            context_frame_id: None,
            parent_context_frame_id: None,
            context_block_count: 0,
            stable_prefix_bytes: 0,
            cache_reuse_prefix_bytes: 0,
            cache_reuse_prefix_block_count: 0,
            first_changed_block_id: None,
        };
        apply_context_frame_summary_to_report(&mut report, &context_frame, &context_delta);
        Ok(TurnContext {
            session_id: input.session.session_id.clone(),
            role,
            provider: config.provider.clone(),
            model: config.model.clone(),
            output_format: config.definition.output_format.clone(),
            response_format: config
                .definition
                .output_format
                .language_model_response_format(),
            system_instructions,
            run_context: config.run_context.clone(),
            session_projection,
            transcript_view: transcript.to_vec(),
            workspace_facts,
            loaded_instruction_files,
            prompt_references,
            context_attachments,
            memory_evidence_bundle: None,
            async_update,
            available_tool_specs: tool_descriptors,
            available_skills: Vec::new(),
            budget_estimate,
            report,
            context_frame,
            context_delta,
        })
    }
}

impl<M: InteractionModel> Agent<M> {
    pub fn request(
        &self,
        input: &AgentLoopInput,
        transcript: &[AgentTranscriptItem],
        step_index: usize,
    ) -> Result<LanguageModelRequest, AgentError> {
        let turn_context = self.assemble_context(input, transcript, step_index)?;
        self.request_from_context(input, &turn_context, step_index)
    }

    pub fn assemble_context(
        &self,
        input: &AgentLoopInput,
        transcript: &[AgentTranscriptItem],
        step_index: usize,
    ) -> Result<TurnContext, AgentError> {
        self.assemble_context_with_projection(input, transcript, None, step_index)
    }

    pub(super) fn assemble_context_with_projection(
        &self,
        input: &AgentLoopInput,
        transcript: &[AgentTranscriptItem],
        session_projection: Option<&AgentSessionProjection>,
        step_index: usize,
    ) -> Result<TurnContext, AgentError> {
        let tool_plan = self.tool_plan(input);
        let skill_plan = self.skill_plan();
        let available_skills = standard_available_skills(&skill_plan);
        let has_available_skills = !available_skills.is_empty();
        let readiness_issues = tool_plan.readiness_issues();
        let unready_visible_tools = readiness_issues
            .iter()
            .map(|issue| issue.tool.as_str())
            .collect::<BTreeSet<_>>();
        let all_tool_descriptors = match self.config.definition.output_format {
            AgentOutputFormat::JsonDecision => tool_plan
                .model_visible_descriptors()
                .into_iter()
                .filter(|descriptor| !unready_visible_tools.contains(descriptor.spec.name.as_str()))
                .filter(|descriptor| {
                    has_available_skills
                        || !matches!(
                            descriptor.spec.name.as_str(),
                            "skill_search" | "skill_describe" | "skill_activate"
                        )
                })
                .collect(),
            AgentOutputFormat::Text => Vec::new(),
        };
        let (tool_descriptors, policy_omissions) = self
            .tool_surface
            .filter_tool_descriptors(all_tool_descriptors);
        let mut context = self.context_assembler.assemble(
            input,
            self.config.definition.role.clone(),
            &self.config,
            transcript,
            session_projection.cloned(),
            tool_descriptors,
            step_index,
        )?;
        context.available_skills = available_skills;
        if !context.available_skills.is_empty() {
            let skill_catalog_bytes = serde_json::to_vec(&context.available_skills)
                .map(|bytes| bytes.len())
                .unwrap_or_default();
            context.report.included.push(ContextSource {
                kind: "available_skills".to_string(),
                label: format!("{} Agent Skills", context.available_skills.len()),
                bytes: skill_catalog_bytes,
            });
            context.budget_estimate.lanes.push(ContextBudgetLane {
                kind: "fixed_prompt".to_string(),
                label: "Agent Skills discovery catalog".to_string(),
                bytes: skill_catalog_bytes,
                priority: 0,
            });
            context.budget_estimate.total_bytes += skill_catalog_bytes;
            let skill_catalog_block = context_block(
                ContextBlockKind::AvailableSkills,
                format!("agent/skills/{}", self.config.definition.key()),
                format!("{} Agent Skills", context.available_skills.len()),
                0,
                ContextCachePolicy::StablePrefix,
                serde_json::to_value(&context.available_skills).unwrap_or(serde_json::Value::Null),
            );
            let insertion_index = context
                .context_frame
                .blocks
                .iter()
                .position(|block| block.kind == ContextBlockKind::ToolSchemas)
                .map(|index| index.saturating_add(1))
                .unwrap_or(1)
                .min(context.context_frame.blocks.len());
            context
                .context_frame
                .blocks
                .insert(insertion_index, skill_catalog_block);
        }
        let policy_bytes = serde_json::to_vec(&self.tool_surface)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        context.report.included.push(ContextSource {
            kind: "agent_tool_surface".to_string(),
            label: self.tool_surface.surface_id.clone(),
            bytes: policy_bytes,
        });
        context.budget_estimate.lanes.push(ContextBudgetLane {
            kind: "fixed_prompt".to_string(),
            label: "agent tool surface".to_string(),
            bytes: policy_bytes,
            priority: 0,
        });
        let tool_surface_block = context_block(
            ContextBlockKind::ToolSurface,
            format!("agent/tool-surface/{}", self.tool_surface.surface_id),
            self.tool_surface.surface_id.clone(),
            0,
            ContextCachePolicy::StablePrefix,
            serde_json::to_value(&self.tool_surface).unwrap_or(serde_json::Value::Null),
        );
        let insertion_index = context
            .context_frame
            .blocks
            .iter()
            .position(|block| block.kind == ContextBlockKind::ToolSchemas)
            .map(|index| index.saturating_add(1))
            .unwrap_or(1)
            .min(context.context_frame.blocks.len());
        context
            .context_frame
            .blocks
            .insert(insertion_index, tool_surface_block);
        context
            .report
            .omitted
            .extend(readiness_issues.into_iter().map(|issue| ContextOmission {
                kind: "tool_readiness".to_string(),
                label: issue.tool,
                reason: issue.reason,
            }));
        context.report.omitted.extend(policy_omissions);
        context.budget_estimate.total_bytes += policy_bytes;
        context.report.budget_estimate = context.budget_estimate.clone();
        refresh_context_budget_block(&mut context);
        if let Some(retriever) = &self.memory_retriever {
            let byte_budget = memory_context_byte_budget(&context);
            if byte_budget == 0 {
                context.report.omitted.push(ContextOmission {
                    kind: "retrieved_memory".to_string(),
                    label: "memory retriever".to_string(),
                    reason: "no model context byte budget remains for memory evidence".to_string(),
                });
            } else {
                let (query, access) = memory_query_and_access(input, &self.config, byte_budget);
                match retriever.retrieve_evidence(&access, &query) {
                    Ok(bundle) => {
                        apply_memory_evidence_to_context(&mut context, bundle, byte_budget)
                    }
                    Err(source) => context.report.omitted.push(ContextOmission {
                        kind: "retrieved_memory".to_string(),
                        label: "memory retriever".to_string(),
                        reason: format!("memory retrieval failed: {source}"),
                    }),
                }
            }
        }
        apply_context_parent(&mut context, None);
        Ok(context)
    }

    pub(super) fn request_from_context(
        &self,
        input: &AgentLoopInput,
        turn_context: &TurnContext,
        step_index: usize,
    ) -> Result<LanguageModelRequest, AgentError> {
        let turn_context_message = render_turn_context_message(input, turn_context)?;
        let mut messages = vec![LanguageModelMessage {
            role: LanguageModelRole::System,
            content: language_model_system_instructions(
                &turn_context.provider,
                &turn_context.model,
                &turn_context.system_instructions,
                Some(agent_output_contract(&self.config.definition.output_format)),
            ),
        }];
        messages.push(LanguageModelMessage {
            role: LanguageModelRole::User,
            content: turn_context_message.clone(),
        });
        messages.extend(
            turn_context
                .transcript_view
                .iter()
                .filter_map(language_model_message_from_transcript_item),
        );

        let mut items = vec![LanguageModelRequestItem::message(
            LanguageModelRole::User,
            turn_context_message,
        )];
        for item in &turn_context.transcript_view {
            items.extend(language_model_request_items_from_transcript_item(item));
        }
        let tools: Vec<LanguageModelToolSpec> = turn_context
            .available_tool_specs
            .iter()
            .map(language_model_tool_spec_from_agent_tool)
            .collect();
        let tool_choice = if tools.is_empty() {
            None
        } else {
            Some(LanguageModelToolChoice::Auto)
        };

        Ok(LanguageModelRequest {
            provider: self.config.provider.clone(),
            model: self.config.model.clone(),
            messages,
            items,
            tools,
            tool_choice,
            parallel_tool_calls: Some(false),
            temperature: self
                .config
                .temperature
                .or(self.config.definition.temperature),
            max_output_tokens: self
                .config
                .max_output_tokens
                .or(self.config.definition.max_output_tokens),
            response_format: turn_context.response_format.clone(),
            metadata: serde_json::json!({
                "loop": "agent",
                "agent_name": self.config.definition.name,
                "agent_role": self.config.definition.role,
                "agent_output_format": self.config.definition.output_format,
                "agent_output_contract": agent_output_contract(&self.config.definition.output_format),
                "step_index": step_index,
                "session_id": input.session.session_id,
                "event_id": input.latest_event.event_id,
                "context_report_id": turn_context.report.report_id,
                "context_frame_id": turn_context.context_frame.frame_id,
                "parent_context_frame_id": turn_context.context_frame.parent_frame_id,
                "context_block_count": turn_context.context_frame.blocks.len(),
                "context_cache_reuse_prefix_bytes": turn_context.context_frame.cache_reuse_prefix_bytes,
                "context_cache_reuse_prefix_block_count": turn_context.context_frame.cache_reuse_prefix_block_count,
                "context_first_changed_block_id": turn_context.context_frame.first_changed_block_id,
                "context_budget_estimate": turn_context.budget_estimate,
                "agent_tool_surface_id": self.tool_surface.surface_id.clone(),
                "run_context": &turn_context.run_context,
            }),
        })
    }

    pub(super) fn tool_plan(&self, input: &AgentLoopInput) -> AgentToolPlan {
        AgentToolPlanner::plan(
            self.tools.descriptors(),
            &self.config.module_tool_projection,
            input.tool_specs.clone(),
            self.config.definition.tools.clone(),
            input.tool_specs.is_empty(),
            self.config.visible_toolsets.clone(),
            self.tools.generation(),
        )
        .for_invocation_mode(AgentToolInvocationMode::TurnBased)
    }

    pub(super) fn skill_plan(&self) -> AgentSkillPlan {
        let agent_keys = [
            self.config.definition.key().to_string(),
            self.config.definition.name.clone(),
        ];
        let catalog_descriptors = self
            .config
            .skill_catalog
            .active_entries_for_scopes(agent_keys.iter().map(String::as_str))
            .into_iter()
            .map(|entry| {
                let key = crate::skill_catalog_entry_text_key(entry);
                let text = self.config.skill_catalog_texts.get(&key).cloned();
                let mut descriptor = entry.to_agent_descriptor(text);
                if let Some(resources) = self.config.skill_catalog_resources.get(&key) {
                    descriptor.resource_contents = resources
                        .iter()
                        .map(|resource| (resource.relative_path.clone(), resource.bytes.clone()))
                        .collect();
                }
                descriptor
            });
        AgentSkillPlan::from_package_modules_and_catalog(
            self.config.definition.skills.clone(),
            &self.config.module_tool_projection,
            agent_keys,
            catalog_descriptors,
        )
    }
}

pub(super) fn standard_available_skills(skill_plan: &AgentSkillPlan) -> Vec<AvailableAgentSkill> {
    skill_plan
        .skills
        .iter()
        .filter_map(|skill| {
            standard_skill_document(skill).map(|_| AvailableAgentSkill {
                name: skill.id.clone(),
                description: skill.description.clone(),
            })
        })
        .collect()
}

pub(super) fn standard_skill_document(
    skill: &AgentSkillDescriptor,
) -> Option<crate::SkillMarkdownDocument> {
    let text = skill.embedded_text.as_deref()?;
    let document = crate::parse_skill_markdown(text).ok()?;
    (document.name() == skill.id && document.description() == skill.description).then_some(document)
}

pub(super) fn language_model_message_from_transcript_item(
    item: &AgentTranscriptItem,
) -> Option<LanguageModelMessage> {
    let role = match item.role {
        AgentTranscriptRole::User | AgentTranscriptRole::Tool => LanguageModelRole::User,
        AgentTranscriptRole::Assistant => LanguageModelRole::Assistant,
        AgentTranscriptRole::ToolCall => return None,
    };
    Some(LanguageModelMessage {
        role,
        content: item.content.clone(),
    })
}

pub(super) fn language_model_request_items_from_transcript_item(
    item: &AgentTranscriptItem,
) -> Vec<LanguageModelRequestItem> {
    let mut items = Vec::new();
    match item.role {
        AgentTranscriptRole::ToolCall => {}
        AgentTranscriptRole::Tool if !item.native_items.is_empty() => {}
        AgentTranscriptRole::User | AgentTranscriptRole::Assistant | AgentTranscriptRole::Tool => {
            if !item.content.trim().is_empty() {
                let role = match item.role {
                    AgentTranscriptRole::Assistant => LanguageModelRole::Assistant,
                    AgentTranscriptRole::User | AgentTranscriptRole::Tool => {
                        LanguageModelRole::User
                    }
                    AgentTranscriptRole::ToolCall => unreachable!(),
                };
                items.push(LanguageModelRequestItem::message(
                    role,
                    item.content.clone(),
                ));
            }
        }
    }
    items.extend(item.native_items.clone());
    items
}

pub(super) fn language_model_tool_spec_from_agent_tool(
    descriptor: &AgentToolDescriptor,
) -> LanguageModelToolSpec {
    LanguageModelToolSpec {
        name: descriptor.spec.name.clone(),
        description: descriptor.spec.description.clone(),
        input_schema: descriptor.input_schema.clone(),
        output_schema: descriptor.output_schema.clone(),
        supports_parallel: descriptor.supports_parallel,
        provider_specs: descriptor
            .provider_specs
            .iter()
            .map(language_model_provider_tool_spec_from_agent_tool)
            .collect(),
    }
}

pub(super) fn language_model_provider_tool_spec_from_agent_tool(
    provider_spec: &AgentToolProviderSpec,
) -> LanguageModelProviderToolSpec {
    LanguageModelProviderToolSpec {
        provider: provider_spec.provider.clone(),
        custom: provider_spec
            .custom
            .as_ref()
            .map(language_model_custom_tool_spec_from_agent_tool),
    }
}

pub(super) fn language_model_custom_tool_spec_from_agent_tool(
    custom: &AgentToolCustomSpec,
) -> LanguageModelCustomToolSpec {
    LanguageModelCustomToolSpec {
        format: match &custom.format {
            AgentToolCustomFormat::Grammar { syntax, definition } => {
                LanguageModelCustomToolFormat::Grammar {
                    syntax: syntax.clone(),
                    definition: definition.clone(),
                }
            }
        },
        raw_input_argument: custom.raw_input_argument.clone(),
    }
}

pub fn agent_system_prompt(tools: &[AgentToolDescriptor]) -> String {
    render_agent_system_prompt_with_descriptors(&AgentDefinition::coding(), tools)
}

pub(super) fn render_initial_agent_input(input: &AgentLoopInput) -> Result<String, AgentError> {
    serde_json::to_string_pretty(&serde_json::json!({
        "kind": "agent_loop_input",
        "input": input,
        "agent_contract": {
            "tool_results_return_as": "user messages with kind=tool_results",
            "continue_until": "empty tool_calls or configured agent step limit",
            "event_model": "model turns and tool completions are emitted as agent.loop.* events"
        }
    }))
    .map_err(|source| AgentError::ModelRequest {
        detail: source.to_string(),
    })
}

pub(super) fn transient_system_context(input: &AgentLoopInput) -> Option<&str> {
    input
        .transient_system_context
        .as_deref()
        .map(str::trim)
        .filter(|context| !context.is_empty())
}

pub(super) fn render_queued_turn_context(
    steering: &[AgentSteeringMessage],
    follow_ups: &[AgentFollowUpMessage],
) -> Option<String> {
    if steering.is_empty() && follow_ups.is_empty() {
        return None;
    }
    serde_json::to_string_pretty(&serde_json::json!({
        "kind": "agent_queued_turn_context",
        "steering": steering,
        "follow_ups": follow_ups,
    }))
    .ok()
}

pub(super) fn render_turn_context_message(
    _input: &AgentLoopInput,
    turn_context: &TurnContext,
) -> Result<String, AgentError> {
    let mut session_context = serde_json::Map::new();
    session_context.insert(
        "session_projection".to_string(),
        serde_json::to_value(&turn_context.session_projection).unwrap_or(serde_json::Value::Null),
    );
    session_context.insert(
        "current_work".to_string(),
        serde_json::to_value(
            turn_context
                .session_projection
                .as_ref()
                .map(|projection| &projection.working_state),
        )
        .unwrap_or(serde_json::Value::Null),
    );
    session_context.insert(
        "prompt_references".to_string(),
        serde_json::to_value(&turn_context.prompt_references).unwrap_or(serde_json::Value::Null),
    );
    session_context.insert(
        "context_attachments".to_string(),
        serde_json::to_value(&turn_context.context_attachments).unwrap_or(serde_json::Value::Null),
    );
    let evidence = turn_context
        .context_frame
        .blocks
        .iter()
        .filter(|block| block.kind == ContextBlockKind::RetrievedMemory)
        .map(|block| block.payload.clone())
        .collect::<Vec<_>>();
    session_context.insert("memory_evidence".to_string(), serde_json::json!(evidence));

    let mut stable_prefix = serde_json::json!({
        "runtime": {
            "provider": turn_context.provider,
            "model": turn_context.model,
            "agent_output_format": turn_context.output_format,
            "response_format": turn_context.response_format,
            "available_tool_count": turn_context.available_tool_specs.len(),
        },
        "run_context": &turn_context.run_context,
        "loaded_instruction_files": turn_context.loaded_instruction_files,
    });
    if !turn_context.available_skills.is_empty()
        && let Some(object) = stable_prefix.as_object_mut()
    {
        object.insert(
            "agent_skills".to_string(),
            serde_json::json!({
                "instructions": "When a task matches an available skill description, call skill_activate with its name before proceeding. If its instructions require a listed resource, call skill_activate again with that relative resource path and load only the resources needed.",
                "available": turn_context.available_skills,
            }),
        );
    }

    serde_json::to_string_pretty(&serde_json::json!({
        "kind": "turn_context",
        "stable_prefix": stable_prefix,
        "session_context": session_context,
        "volatile_tail": {
            "step_index": turn_context.report.step_index,
            "workspace_facts": turn_context.workspace_facts,
            "async_update": turn_context.async_update,
            "context_budget_estimate": turn_context.budget_estimate,
            "context_frame": {
                "frame_id": turn_context.context_frame.frame_id,
                "parent_frame_id": turn_context.context_frame.parent_frame_id,
                "block_count": turn_context.context_frame.blocks.len(),
                "stable_prefix_bytes": turn_context.context_frame.stable_prefix_bytes,
                "cache_reuse_prefix_bytes": turn_context.context_frame.cache_reuse_prefix_bytes,
                "cache_reuse_prefix_block_count": turn_context.context_frame.cache_reuse_prefix_block_count,
                "first_changed_block_id": turn_context.context_frame.first_changed_block_id,
            },
        },
    }))
    .map_err(|source| AgentError::ModelRequest {
        detail: source.to_string(),
    })
}
