use crate::session_store::{
    AgentSessionStore, AgentSessionStoreRecord, AgentSessionStoreRecordKind, NoopAgentSessionStore,
};
use crate::tools::{
    AGENT_CODING_EVIDENCE_KIND, AgentCodingToolEvidence, AgentTool, AgentToolCapability,
    AgentToolCustomFormat, AgentToolCustomSpec, AgentToolDescriptor, AgentToolEvidence,
    AgentToolOutcome, AgentToolPlan, AgentToolPlanner, AgentToolPolicyDecision,
    AgentToolProviderSpec, AgentToolRegistry, AgentToolStatus,
};
use crate::{
    AgentError, AgentLoopDecision, AgentLoopInput, AgentToolCall, AsyncTaskNextAction,
    InteractionModel, InteractionSession, TaskId, default_main_agent_tools,
};
use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelCustomToolFormat, LanguageModelCustomToolSpec, LanguageModelMessage,
    LanguageModelProviderToolSpec, LanguageModelRequest, LanguageModelRequestItem,
    LanguageModelResponse, LanguageModelResponseFormat, LanguageModelResponseItem,
    LanguageModelRole, LanguageModelStreamEvent, LanguageModelToolCall, LanguageModelToolChoice,
    LanguageModelToolSpec, inferred_model_context_window_tokens,
};
use sinomo_runtime::core::{
    CandidateBundleManifest, Confidence, EventEnvelope, EventKind, EvolutionJob, LatencyClass,
    MutationPacket, PrivacyClass, RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_runtime::events::{EventId, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO: f32 = 0.80;
const CONTEXT_ESTIMATE_BYTES_PER_TOKEN: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRole {
    Main,
    Maid,
    Deliberation,
    Evolution,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentContextBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_threshold_ratio: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_tail_items: Option<usize>,
}

impl Default for AgentContextBudget {
    fn default() -> Self {
        Self {
            max_context_bytes: None,
            compaction_threshold_ratio: None,
            retained_tail_items: None,
        }
    }
}

impl AgentContextBudget {
    pub fn for_loaded_model(provider: &str, model: &str, max_output_tokens: Option<u32>) -> Self {
        Self {
            max_context_bytes: inferred_model_context_window_tokens(provider, model).map(
                |window_tokens| context_max_bytes_for_window(window_tokens, max_output_tokens),
            ),
            compaction_threshold_ratio: Some(DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO),
            retained_tail_items: None,
        }
    }

    pub fn compaction_threshold_bytes(&self) -> Option<usize> {
        let max_context_bytes = self.max_context_bytes?;
        let ratio = self.compaction_threshold_ratio?;
        let ratio = if ratio.is_finite() {
            ratio.clamp(0.0, 1.0)
        } else {
            return None;
        };
        Some((max_context_bytes as f32 * ratio).round() as usize)
    }
}

fn context_max_bytes_for_window(window_tokens: usize, max_output_tokens: Option<u32>) -> usize {
    let output_reserve = max_output_tokens.unwrap_or_default() as usize;
    let input_tokens = window_tokens
        .saturating_sub(output_reserve)
        .max(window_tokens / 2);
    input_tokens.saturating_mul(CONTEXT_ESTIMATE_BYTES_PER_TOKEN)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSession {
    pub session_id: SessionId,
    pub role: AgentRole,
    pub transcript: AgentTranscript,
    #[serde(default, skip_serializing_if = "AgentTranscript::is_empty")]
    pub raw_transcript: AgentTranscript,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<AgentSessionProjection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steering_queue: Vec<AgentSteeringMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub follow_up_queue: Vec<AgentFollowUpMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort: Option<AgentAbortRecord>,
}

impl AgentSession {
    pub fn new(session_id: SessionId, role: AgentRole) -> Self {
        Self {
            session_id,
            role,
            transcript: AgentTranscript::new(),
            raw_transcript: AgentTranscript::new(),
            projection: None,
            steering_queue: Vec::new(),
            follow_up_queue: Vec::new(),
            abort: None,
        }
    }

    pub fn append_run(&mut self, run: &AgentRun) {
        if self.raw_transcript.items.is_empty() && !self.transcript.items.is_empty() {
            self.raw_transcript = self.transcript.clone();
        }
        self.transcript.items.extend(run.items.iter().cloned());
        self.raw_transcript.items.extend(run.items.iter().cloned());
        if let Some(projection) = run
            .compactions
            .last()
            .map(|compaction| compaction.projection.clone())
        {
            self.projection = Some(projection);
        }
    }

    pub fn is_aborted(&self) -> bool {
        self.abort.is_some()
    }

    fn compaction_source_transcript(&mut self) -> AgentTranscript {
        if self.raw_transcript.items.is_empty() && !self.transcript.items.is_empty() {
            self.raw_transcript = self.transcript.clone();
        }
        if self.raw_transcript.items.is_empty() {
            self.transcript.clone()
        } else {
            self.raw_transcript.clone()
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentTranscript {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<AgentItem>,
}

impl AgentTranscript {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn push(&mut self, item: AgentItem) {
        self.items.push(item);
    }

    pub fn retained_tail(&self, retained_tail_count: usize) -> Vec<AgentItem> {
        if self.items.len() <= retained_tail_count {
            return self.items.clone();
        }
        self.items[self.items.len() - retained_tail_count..].to_vec()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSteeringMessage {
    pub steering_id: TaskId,
    pub message: String,
}

impl AgentSteeringMessage {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            steering_id: TaskId::generate(),
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentFollowUpMessage {
    pub follow_up_id: TaskId,
    pub message: String,
}

impl AgentFollowUpMessage {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            follow_up_id: TaskId::generate(),
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentAbortRecord {
    pub abort_id: TaskId,
    pub reason: String,
}

impl AgentAbortRecord {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            abort_id: TaskId::generate(),
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTraceRecord {
    pub record_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: time::OffsetDateTime,
    pub record: AgentTraceRecordKind,
}

impl AgentTraceRecord {
    pub fn new(session_id: SessionId, record: AgentTraceRecordKind) -> Self {
        Self {
            record_id: TaskId::generate(),
            session_id,
            recorded_at: time::OffsetDateTime::now_utc(),
            record,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentTraceRecordKind {
    RunStarted {
        provider: String,
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_steps: Option<usize>,
    },
    ContextReport {
        report: ContextAssemblyReport,
    },
    ModelEvent {
        event: AgentModelEvent,
    },
    Item {
        item: AgentItem,
    },
    ToolOutcome {
        outcome: AgentToolOutcome,
    },
    Compaction {
        summary: AgentCompactionSummary,
    },
    Event {
        event: EventEnvelope,
    },
    RunCompleted {
        stop_reason: AgentStopReason,
        final_decision: AgentLoopDecision,
    },
}

pub trait AgentTraceStore: Send + Sync {
    fn append(&self, record: &AgentTraceRecord) -> Result<(), AgentTraceStoreError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentStoragePaths {
    pub root: PathBuf,
    pub sessions_dir: PathBuf,
    pub traces_dir: PathBuf,
    pub evidence_dir: PathBuf,
}

impl AgentStoragePaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            sessions_dir: root.join("sessions"),
            traces_dir: root.join("traces"),
            evidence_dir: root.join("evidence"),
            root,
        }
    }

    pub fn from_data_dir(data_dir: &SinomoDataDir) -> Self {
        Self::new(data_dir.root().join("events").join("agent"))
    }

    pub fn trace_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.traces_dir
            .join(format!("{}.jsonl", session_id.as_str()))
    }

    pub fn session_dir_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.sessions_dir.join(session_id.as_str())
    }

    pub fn session_snapshot_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id)
            .join("snapshot.json")
    }

    pub fn session_events_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id)
            .join("events.jsonl")
    }

    pub fn session_items_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id).join("items.jsonl")
    }

    pub fn session_projections_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id)
            .join("projections.jsonl")
    }

    pub fn evidence_dir_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.evidence_dir.join(session_id.as_str())
    }
}

#[derive(Default)]
pub struct NoopAgentTraceStore;

impl AgentTraceStore for NoopAgentTraceStore {
    fn append(&self, _record: &AgentTraceRecord) -> Result<(), AgentTraceStoreError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileAgentTraceStore {
    path: PathBuf,
}

impl FileAgentTraceStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn for_session(paths: &AgentStoragePaths, session_id: &SessionId) -> Self {
        Self::new(paths.trace_path_for_session(session_id))
    }

    pub fn for_data_dir_session(data_dir: &SinomoDataDir, session_id: &SessionId) -> Self {
        Self::for_session(&AgentStoragePaths::from_data_dir(data_dir), session_id)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl AgentTraceStore for FileAgentTraceStore {
    fn append(&self, record: &AgentTraceRecord) -> Result<(), AgentTraceStoreError> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| AgentTraceStoreError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        let line = serde_json::to_string(record).map_err(|source| {
            AgentTraceStoreError::Serialization {
                detail: source.to_string(),
            }
        })?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| AgentTraceStoreError::Io {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })?;
        writeln!(file, "{line}").map_err(|source| AgentTraceStoreError::Io {
            path: self.path.display().to_string(),
            detail: source.to_string(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AgentTraceStoreError {
    #[error("I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("failed to serialize trace record: {detail}")]
    Serialization { detail: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentItem {
    UserMessage {
        item_id: TaskId,
        step_index: usize,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
    },
    AssistantText {
        item_id: TaskId,
        step_index: usize,
        content: String,
    },
    AssistantReasoningSummary {
        item_id: TaskId,
        step_index: usize,
        summary: String,
    },
    ToolCall {
        item_id: TaskId,
        step_index: usize,
        call_index: usize,
        call: AgentToolCall,
    },
    ToolResult {
        item_id: TaskId,
        step_index: usize,
        call_index: usize,
        call_id: TaskId,
        status: AgentToolStatus,
        summary: String,
        #[serde(default)]
        output: serde_json::Value,
        evidence: AgentToolEvidence,
    },
    AsyncTaskUpdate {
        item_id: TaskId,
        update: crate::AsyncTaskUpdate,
    },
    ContextInjection {
        item_id: TaskId,
        report: ContextAssemblyReport,
    },
    CompactionSummary {
        item_id: TaskId,
        summary: AgentCompactionSummary,
    },
    ModelUsage {
        item_id: TaskId,
        step_index: usize,
        provider: String,
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<sinomo_lm::LanguageModelUsage>,
    },
    Error {
        item_id: TaskId,
        step_index: usize,
        detail: String,
    },
    PatchSnapshot {
        item_id: TaskId,
        step_index: usize,
        call_id: TaskId,
        status: AgentToolStatus,
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        patch_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changed_files: Vec<String>,
    },
    VerificationResult {
        item_id: TaskId,
        step_index: usize,
        call_id: TaskId,
        command: String,
        status: AgentVerificationState,
        summary: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentVerificationState {
    NotRun,
    Passed,
    Failed,
    Blocked,
}

impl Default for AgentVerificationState {
    fn default() -> Self {
        Self::NotRun
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentEvolutionContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<EvolutionJob>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_packet: Option<MutationPacket>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_bundle: Option<CandidateBundleManifest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_results: Vec<AgentEvolutionVerificationResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_evidence: Vec<AgentCandidateEvidence>,
}

impl AgentEvolutionContext {
    pub fn for_job(job: EvolutionJob) -> Self {
        Self {
            job: Some(job),
            mutation_packet: None,
            candidate_bundle: None,
            verification_results: Vec::new(),
            candidate_evidence: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentEvolutionVerificationResult {
    pub command: String,
    pub status: AgentVerificationState,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentCandidateEvidence {
    Patch {
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        patch_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changed_files: Vec<String>,
    },
    Verification {
        command: String,
        status: AgentVerificationState,
        summary: String,
    },
    ToolResult {
        tool: String,
        status: AgentToolStatus,
        summary: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentCandidateEvidenceBundle {
    pub bundle_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_packet_job_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_bundle_path: Option<String>,
    pub final_response: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<AgentCandidateEvidence>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TurnContext {
    pub session_id: SessionId,
    pub role: AgentRole,
    pub provider: String,
    pub model: String,
    pub system_instructions: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evolution_job_facts: Option<AgentEvolutionContext>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available_tool_specs: Vec<AgentToolDescriptor>,
    pub budget_estimate: ContextBudgetEstimate,
    pub report: ContextAssemblyReport,
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
    pub evolution_context_bytes: usize,
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InstructionFileContext {
    pub path: String,
    pub bytes: usize,
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

fn resolve_prompt_references(
    input: &AgentLoopInput,
    config: &AgentConfig,
) -> (
    Vec<PromptReferenceContext>,
    Vec<ContextSource>,
    Vec<ContextOmission>,
) {
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

fn resolve_instruction_files(
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
        match resolve_instruction_file(root, instruction_file) {
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

fn resolve_instruction_file(
    root: &Path,
    instruction_file: &Path,
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
    Ok(InstructionFileContext {
        path: resolved
            .strip_prefix(&root)
            .unwrap_or(&resolved)
            .display()
            .to_string(),
        bytes: bytes.len(),
        truncated: false,
        content: String::from_utf8_lossy(&bytes).to_string(),
    })
}

fn latest_event_prompt_text(input: &AgentLoopInput) -> String {
    let mut text = String::new();
    collect_prompt_text(&input.latest_event.payload, &mut text);
    text
}

fn collect_prompt_text(value: &serde_json::Value, text: &mut String) {
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

fn extract_prompt_reference_labels(text: &str) -> Vec<String> {
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

fn resolve_prompt_reference(
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

fn resolve_prompt_reference_file(
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

fn resolve_prompt_reference_directory(
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
        let system_instructions = agent_system_prompt(&tool_descriptors);
        let workspace_facts = serde_json::json!({
            "session_id": input.session.session_id,
            "surface": input.session.surface,
            "latest_event_kind": input.latest_event.kind,
            "latest_event_id": input.latest_event.event_id,
        });
        let tool_schema_bytes = serde_json::to_vec(&tool_descriptors)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let workspace_facts_bytes = serde_json::to_vec(&workspace_facts)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let (loaded_instruction_files, instruction_sources, instruction_omissions) =
            resolve_instruction_files(config);
        let instruction_file_bytes = serde_json::to_vec(&loaded_instruction_files)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let (prompt_references, prompt_reference_sources, prompt_reference_omissions) =
            resolve_prompt_references(input, config);
        let prompt_reference_bytes = serde_json::to_vec(&prompt_references)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let evolution_context_bytes = input
            .evolution_context
            .as_ref()
            .and_then(|context| serde_json::to_vec(context).ok())
            .map(|bytes| bytes.len())
            .unwrap_or_default();
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
                label: "system prompt, tool schemas, workspace facts, and instruction files"
                    .to_string(),
                bytes: system_instructions.len()
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
        if evolution_context_bytes > 0 {
            lanes.push(ContextBudgetLane {
                kind: "evolution_context".to_string(),
                label: "evolution job and candidate evidence".to_string(),
                bytes: evolution_context_bytes,
                priority: 1,
            });
        }
        let budget_estimate = ContextBudgetEstimate {
            system_prompt_bytes: system_instructions.len(),
            current_work_bytes,
            old_summary_bytes,
            retrieved_memory_bytes,
            transcript_bytes,
            tool_schema_bytes,
            workspace_facts_bytes,
            instruction_file_bytes,
            prompt_reference_bytes,
            evolution_context_bytes,
            total_bytes: system_instructions.len()
                + session_projection_bytes
                + transcript_bytes
                + tool_schema_bytes
                + workspace_facts_bytes
                + instruction_file_bytes
                + prompt_reference_bytes
                + evolution_context_bytes,
            max_context_bytes: config.context_budget.max_context_bytes,
            compaction_threshold_bytes: config.context_budget.compaction_threshold_bytes(),
            lanes,
        };
        let mut included = vec![
            ContextSource {
                kind: "system_instructions".to_string(),
                label: "agent_system_prompt".to_string(),
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
        if session_projection_bytes > 0 {
            included.push(ContextSource {
                kind: "session_projection".to_string(),
                label: "current work projection".to_string(),
                bytes: session_projection_bytes,
            });
        }
        included.extend(instruction_sources);
        included.extend(prompt_reference_sources);
        if input.evolution_context.is_some() {
            included.push(ContextSource {
                kind: "evolution_context".to_string(),
                label: "evolution job, mutation, verification, and candidate evidence".to_string(),
                bytes: budget_estimate.evolution_context_bytes,
            });
        }
        if input.async_update.is_some() {
            included.push(ContextSource {
                kind: "async_update".to_string(),
                label: "latest async task update".to_string(),
                bytes: serde_json::to_vec(&input.async_update)
                    .map(|bytes| bytes.len())
                    .unwrap_or_default(),
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
        let report = ContextAssemblyReport {
            report_id: TaskId::generate(),
            step_index,
            provider: config.provider.clone(),
            model: config.model.clone(),
            included,
            omitted,
            budget_estimate: budget_estimate.clone(),
        };
        Ok(TurnContext {
            session_id: input.session.session_id.clone(),
            role,
            provider: config.provider.clone(),
            model: config.model.clone(),
            system_instructions,
            session_projection,
            transcript_view: transcript.to_vec(),
            workspace_facts,
            loaded_instruction_files,
            prompt_references,
            evolution_job_facts: input.evolution_context.clone(),
            available_tool_specs: tool_descriptors,
            budget_estimate,
            report,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentModelEvent {
    Started {
        step_index: usize,
        provider: String,
        model: String,
    },
    AssistantTextDelta {
        step_index: usize,
        delta: String,
    },
    AssistantReasoningSummary {
        step_index: usize,
        summary: String,
    },
    ToolCallStarted {
        step_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_index: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        tool: String,
    },
    ToolCallDelta {
        step_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool: Option<String>,
        arguments_delta: String,
    },
    ToolCallCompleted {
        step_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_index: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        call: AgentToolCall,
    },
    Usage {
        step_index: usize,
        provider: String,
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<sinomo_lm::LanguageModelUsage>,
    },
    Finished {
        step_index: usize,
        response_mode: crate::AgentLoopResponseMode,
        tool_call_count: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finish_reason: Option<String>,
    },
    Error {
        step_index: usize,
        detail: String,
    },
    Aborted {
        reason: String,
    },
}

impl AgentModelEvent {
    fn from_model_response(
        step_index: usize,
        response: &LanguageModelResponse,
        decision: &AgentLoopDecision,
        stream_events: Vec<LanguageModelStreamEvent>,
    ) -> Vec<Self> {
        let mut events = Vec::new();
        let mut saw_started = false;
        let mut saw_text_delta = false;
        let mut saw_usage = false;
        let mut saw_finished = false;
        let mut saw_tool_completed = false;
        for stream_event in stream_events {
            match stream_event {
                LanguageModelStreamEvent::Started { provider, model } => {
                    saw_started = true;
                    events.push(AgentModelEvent::Started {
                        step_index,
                        provider,
                        model,
                    });
                }
                LanguageModelStreamEvent::TextDelta { delta } => {
                    if !delta.is_empty() {
                        saw_text_delta = true;
                        events.push(AgentModelEvent::AssistantTextDelta { step_index, delta });
                    }
                }
                LanguageModelStreamEvent::ReasoningSummary { summary } => {
                    events.push(AgentModelEvent::AssistantReasoningSummary {
                        step_index,
                        summary,
                    });
                }
                LanguageModelStreamEvent::ToolCallStarted { call_id, name } => {
                    events.push(AgentModelEvent::ToolCallStarted {
                        step_index,
                        call_index: decision
                            .tool_calls
                            .iter()
                            .position(|call| call.tool == name),
                        call_id,
                        tool: name,
                    });
                }
                LanguageModelStreamEvent::ToolCallDelta {
                    call_id,
                    name,
                    arguments_delta,
                } => {
                    events.push(AgentModelEvent::ToolCallDelta {
                        step_index,
                        call_id,
                        tool: name,
                        arguments_delta,
                    });
                }
                LanguageModelStreamEvent::ToolCallCompleted {
                    call_id,
                    name,
                    input,
                } => {
                    saw_tool_completed = true;
                    let call = AgentToolCall {
                        tool: name.clone(),
                        input,
                        r#async: false,
                        summary: String::new(),
                    };
                    events.push(AgentModelEvent::ToolCallCompleted {
                        step_index,
                        call_index: decision
                            .tool_calls
                            .iter()
                            .position(|candidate| candidate.tool == name),
                        call_id,
                        call,
                    });
                }
                LanguageModelStreamEvent::Usage { usage } => {
                    saw_usage = true;
                    events.push(AgentModelEvent::Usage {
                        step_index,
                        provider: response.provider.clone(),
                        model: response.model.clone(),
                        usage: Some(usage),
                    });
                }
                LanguageModelStreamEvent::Finished { finish_reason } => {
                    saw_finished = true;
                    events.push(AgentModelEvent::Finished {
                        step_index,
                        response_mode: decision.response_mode.clone(),
                        tool_call_count: decision.tool_calls.len(),
                        finish_reason,
                    });
                }
                LanguageModelStreamEvent::Error { detail } => {
                    events.push(AgentModelEvent::Error { step_index, detail });
                }
                LanguageModelStreamEvent::Aborted { reason } => {
                    events.push(AgentModelEvent::Aborted { reason });
                }
            }
        }
        if !saw_started {
            events.insert(
                0,
                AgentModelEvent::Started {
                    step_index,
                    provider: response.provider.clone(),
                    model: response.model.clone(),
                },
            );
        }
        if !saw_text_delta && !response.content.is_empty() {
            events.push(AgentModelEvent::AssistantTextDelta {
                step_index,
                delta: response.content.clone(),
            });
        }
        if !saw_tool_completed {
            for (call_index, call) in decision.tool_calls.iter().enumerate() {
                events.push(AgentModelEvent::ToolCallStarted {
                    step_index,
                    call_index: Some(call_index),
                    call_id: None,
                    tool: call.tool.clone(),
                });
                events.push(AgentModelEvent::ToolCallCompleted {
                    step_index,
                    call_index: Some(call_index),
                    call_id: None,
                    call: call.clone(),
                });
            }
        }
        if !saw_usage && response.usage.is_some() {
            events.push(AgentModelEvent::Usage {
                step_index,
                provider: response.provider.clone(),
                model: response.model.clone(),
                usage: response.usage.clone(),
            });
        }
        if !saw_finished {
            events.push(AgentModelEvent::Finished {
                step_index,
                response_mode: decision.response_mode.clone(),
                tool_call_count: decision.tool_calls.len(),
                finish_reason: None,
            });
        }
        events
    }

    fn from_unparsed_model_response(
        step_index: usize,
        response: &LanguageModelResponse,
        stream_events: Vec<LanguageModelStreamEvent>,
        detail: String,
    ) -> Vec<Self> {
        let mut events = Vec::new();
        let mut saw_started = false;
        let mut saw_text_delta = false;
        let mut saw_usage = false;
        for stream_event in stream_events {
            match stream_event {
                LanguageModelStreamEvent::Started { provider, model } => {
                    saw_started = true;
                    events.push(AgentModelEvent::Started {
                        step_index,
                        provider,
                        model,
                    });
                }
                LanguageModelStreamEvent::TextDelta { delta } => {
                    if !delta.is_empty() {
                        saw_text_delta = true;
                        events.push(AgentModelEvent::AssistantTextDelta { step_index, delta });
                    }
                }
                LanguageModelStreamEvent::ReasoningSummary { summary } => {
                    events.push(AgentModelEvent::AssistantReasoningSummary {
                        step_index,
                        summary,
                    });
                }
                LanguageModelStreamEvent::ToolCallStarted { call_id, name } => {
                    events.push(AgentModelEvent::ToolCallStarted {
                        step_index,
                        call_index: None,
                        call_id,
                        tool: name,
                    });
                }
                LanguageModelStreamEvent::ToolCallDelta {
                    call_id,
                    name,
                    arguments_delta,
                } => {
                    events.push(AgentModelEvent::ToolCallDelta {
                        step_index,
                        call_id,
                        tool: name,
                        arguments_delta,
                    });
                }
                LanguageModelStreamEvent::ToolCallCompleted {
                    call_id,
                    name,
                    input,
                } => {
                    events.push(AgentModelEvent::ToolCallCompleted {
                        step_index,
                        call_index: None,
                        call_id,
                        call: AgentToolCall {
                            tool: name,
                            input,
                            r#async: false,
                            summary: String::new(),
                        },
                    });
                }
                LanguageModelStreamEvent::Usage { usage } => {
                    saw_usage = true;
                    events.push(AgentModelEvent::Usage {
                        step_index,
                        provider: response.provider.clone(),
                        model: response.model.clone(),
                        usage: Some(usage),
                    });
                }
                LanguageModelStreamEvent::Finished { .. } => {}
                LanguageModelStreamEvent::Error { detail } => {
                    events.push(AgentModelEvent::Error { step_index, detail });
                }
                LanguageModelStreamEvent::Aborted { reason } => {
                    events.push(AgentModelEvent::Aborted { reason });
                }
            }
        }
        if !saw_started {
            events.insert(
                0,
                AgentModelEvent::Started {
                    step_index,
                    provider: response.provider.clone(),
                    model: response.model.clone(),
                },
            );
        }
        if !saw_text_delta && !response.content.is_empty() {
            events.push(AgentModelEvent::AssistantTextDelta {
                step_index,
                delta: response.content.clone(),
            });
        }
        if !saw_usage && response.usage.is_some() {
            events.push(AgentModelEvent::Usage {
                step_index,
                provider: response.provider.clone(),
                model: response.model.clone(),
                usage: response.usage.clone(),
            });
        }
        events.push(AgentModelEvent::Error { step_index, detail });
        events
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionProjection {
    pub projection_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_projection_id: Option<TaskId>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub reason: String,
    pub summary: String,
    pub source_item_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_item_ids: Vec<TaskId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_item_ids: Vec<TaskId>,
    pub working_state: AgentWorkingState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentWorkingState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_objective: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_user_request: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_action: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_questions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_tool_calls: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_modified: Vec<String>,
    #[serde(default)]
    pub verification_state: AgentVerificationState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assumptions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decisions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_risks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_steering: Vec<AgentSteeringMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_follow_ups: Vec<AgentFollowUpMessage>,
}

impl Default for AgentWorkingState {
    fn default() -> Self {
        Self {
            active_objective: None,
            current_user_request: None,
            next_action: None,
            pending_questions: Vec::new(),
            open_tool_calls: Vec::new(),
            files_read: Vec::new(),
            files_modified: Vec::new(),
            verification_state: AgentVerificationState::NotRun,
            blockers: Vec::new(),
            assumptions: Vec::new(),
            decisions: Vec::new(),
            open_risks: Vec::new(),
            queued_steering: Vec::new(),
            queued_follow_ups: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentCompactionSummary {
    pub compaction_id: TaskId,
    pub reason: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_retained_item: Option<TaskId>,
    pub token_estimate_before: usize,
    pub retained_tail_count: usize,
    pub projection: AgentSessionProjection,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_modified: Vec<String>,
    pub verification_state: AgentVerificationState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_risks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_constraints: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentCompactor {
    pub retained_tail_count: Option<usize>,
}

impl Default for AgentCompactor {
    fn default() -> Self {
        Self {
            retained_tail_count: None,
        }
    }
}

impl AgentCompactor {
    pub fn compact(
        &self,
        transcript: &AgentTranscript,
        reason: impl Into<String>,
        token_estimate_before: usize,
    ) -> AgentCompactionSummary {
        let retained_tail_count = self.retained_tail_count.unwrap_or(transcript.items.len());
        self.compact_retaining_tail_count(
            transcript,
            reason,
            token_estimate_before,
            retained_tail_count,
        )
    }

    pub fn compact_retaining_tail_count(
        &self,
        transcript: &AgentTranscript,
        reason: impl Into<String>,
        token_estimate_before: usize,
        retained_tail_count: usize,
    ) -> AgentCompactionSummary {
        self.compact_retaining_tail_count_with_session_state(
            transcript,
            reason,
            token_estimate_before,
            retained_tail_count,
            None,
            &[],
            &[],
        )
    }

    pub fn compact_retaining_tail_count_with_session_state(
        &self,
        transcript: &AgentTranscript,
        reason: impl Into<String>,
        token_estimate_before: usize,
        retained_tail_count: usize,
        previous_projection: Option<&AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
    ) -> AgentCompactionSummary {
        let reason = reason.into();
        let retained = transcript.retained_tail(retained_tail_count);
        let working_state = working_state_from_items(
            &transcript.items,
            steering,
            follow_ups,
            previous_projection.map(|projection| &projection.working_state),
        );
        let projection_summary = format!(
            "Compacted {} item(s), retaining {} most recent item(s). Current work: {}",
            transcript.items.len(),
            retained.len(),
            working_state
                .current_user_request
                .as_deref()
                .or(working_state.active_objective.as_deref())
                .unwrap_or("no active user request recorded")
        );
        let projection = AgentSessionProjection {
            projection_id: TaskId::generate(),
            previous_projection_id: previous_projection
                .map(|projection| projection.projection_id.clone()),
            created_at: time::OffsetDateTime::now_utc(),
            reason: reason.clone(),
            summary: projection_summary,
            source_item_count: transcript.items.len(),
            source_item_ids: transcript.items.iter().map(agent_item_id).collect(),
            retained_item_ids: retained.iter().map(agent_item_id).collect(),
            working_state: working_state.clone(),
        };
        AgentCompactionSummary {
            compaction_id: TaskId::generate(),
            reason,
            summary: projection.summary.clone(),
            first_retained_item: retained.first().map(agent_item_id),
            token_estimate_before,
            retained_tail_count: retained.len(),
            files_read: working_state.files_read.clone(),
            files_modified: working_state.files_modified.clone(),
            verification_state: working_state.verification_state.clone(),
            open_risks: working_state.open_risks.clone(),
            projection,
            policy_constraints: vec![
                "runtime policy remains authoritative after compaction".to_string(),
                "verification evidence is structured state, not summary text".to_string(),
            ],
        }
    }
}

fn working_state_from_items(
    items: &[AgentItem],
    steering: &[AgentSteeringMessage],
    follow_ups: &[AgentFollowUpMessage],
    previous: Option<&AgentWorkingState>,
) -> AgentWorkingState {
    let mut state = previous.cloned().unwrap_or_default();
    state.queued_steering = steering.to_vec();
    state.queued_follow_ups = follow_ups.to_vec();
    let mut open_tool_calls: Vec<String> = Vec::new();
    for item in items {
        match item {
            AgentItem::UserMessage { content, .. } => {
                if !content.contains("\"kind\": \"agent_queued_turn_context\"") {
                    state.current_user_request = Some(compact_multiline(content, 2000));
                    if state.active_objective.is_none() {
                        state.active_objective = state.current_user_request.clone();
                    }
                }
            }
            AgentItem::AssistantText { content, .. } => {
                let text = compact_multiline(content, 1000);
                if !text.is_empty() {
                    state.decisions.push(text);
                    if state.decisions.len() > 5 {
                        state.decisions.remove(0);
                    }
                }
            }
            AgentItem::ToolCall { call, .. } => {
                let description = format!("{}: {}", call.tool, call.summary);
                open_tool_calls.push(description);
                if call.tool == "read_file"
                    && let Some(path) = call.input.get("path").and_then(|value| value.as_str())
                {
                    push_unique(&mut state.files_read, path.to_string());
                }
            }
            AgentItem::ToolResult {
                status,
                summary,
                evidence,
                ..
            } => {
                if !open_tool_calls.is_empty() {
                    open_tool_calls.remove(0);
                }
                if matches!(status, AgentToolStatus::Failed | AgentToolStatus::Blocked) {
                    push_unique(&mut state.blockers, compact_multiline(summary, 500));
                    state.verification_state = merge_verification_state(
                        &state.verification_state,
                        &AgentVerificationState::Blocked,
                    );
                }
                let coding = evidence
                    .record::<AgentCodingToolEvidence>(AGENT_CODING_EVIDENCE_KIND)
                    .unwrap_or_default();
                for changed_file in coding.changed_files {
                    push_unique(&mut state.files_modified, changed_file);
                }
                state.verification_state =
                    merge_verification_state(&state.verification_state, &coding.verification_state);
            }
            AgentItem::PatchSnapshot {
                status,
                summary,
                changed_files,
                ..
            } => {
                for changed_file in changed_files {
                    push_unique(&mut state.files_modified, changed_file.clone());
                }
                if matches!(status, AgentToolStatus::Failed | AgentToolStatus::Blocked) {
                    push_unique(&mut state.open_risks, compact_multiline(summary, 500));
                }
            }
            AgentItem::VerificationResult {
                command,
                status,
                summary,
                ..
            } => {
                state.verification_state =
                    merge_verification_state(&state.verification_state, status);
                if matches!(
                    status,
                    AgentVerificationState::Failed | AgentVerificationState::Blocked
                ) {
                    push_unique(
                        &mut state.open_risks,
                        format!("{}: {}", command, compact_multiline(summary, 500)),
                    );
                }
            }
            AgentItem::Error { detail, .. } => {
                push_unique(&mut state.blockers, compact_multiline(detail, 500));
            }
            AgentItem::CompactionSummary { summary, .. } => {
                if state.active_objective.is_none() {
                    state.active_objective =
                        summary.projection.working_state.active_objective.clone();
                }
                if state.current_user_request.is_none() {
                    state.current_user_request = summary
                        .projection
                        .working_state
                        .current_user_request
                        .clone();
                }
            }
            AgentItem::AssistantReasoningSummary { .. }
            | AgentItem::AsyncTaskUpdate { .. }
            | AgentItem::ContextInjection { .. }
            | AgentItem::ModelUsage { .. } => {}
        }
    }
    state.open_tool_calls = open_tool_calls;
    state.next_action = if !state.open_tool_calls.is_empty() {
        Some("wait for pending tool results".to_string())
    } else if state.verification_state == AgentVerificationState::Failed {
        Some("address failed verification before finalizing".to_string())
    } else if state.verification_state == AgentVerificationState::NotRun
        && !state.files_modified.is_empty()
    {
        Some("run verification for modified files".to_string())
    } else {
        None
    };
    state
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn compact_multiline(value: &str, max_chars: usize) -> String {
    let compacted = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compacted.chars().count() <= max_chars {
        return compacted;
    }
    compacted.chars().take(max_chars).collect::<String>()
}

fn merge_verification_state(
    current: &AgentVerificationState,
    next: &AgentVerificationState,
) -> AgentVerificationState {
    match (current, next) {
        (AgentVerificationState::Failed, _) | (_, AgentVerificationState::Failed) => {
            AgentVerificationState::Failed
        }
        (AgentVerificationState::Blocked, _) | (_, AgentVerificationState::Blocked) => {
            AgentVerificationState::Blocked
        }
        (AgentVerificationState::Passed, _) | (_, AgentVerificationState::Passed) => {
            AgentVerificationState::Passed
        }
        _ => AgentVerificationState::NotRun,
    }
}

pub(crate) fn agent_item_id(item: &AgentItem) -> TaskId {
    match item {
        AgentItem::UserMessage { item_id, .. }
        | AgentItem::AssistantText { item_id, .. }
        | AgentItem::AssistantReasoningSummary { item_id, .. }
        | AgentItem::ToolCall { item_id, .. }
        | AgentItem::ToolResult { item_id, .. }
        | AgentItem::AsyncTaskUpdate { item_id, .. }
        | AgentItem::ContextInjection { item_id, .. }
        | AgentItem::CompactionSummary { item_id, .. }
        | AgentItem::ModelUsage { item_id, .. }
        | AgentItem::Error { item_id, .. }
        | AgentItem::PatchSnapshot { item_id, .. }
        | AgentItem::VerificationResult { item_id, .. } => item_id.clone(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentConfig {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls_per_step: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_model_repair_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<PathBuf>,
    #[serde(default = "default_workspace_instruction_files")]
    pub workspace_instruction_files: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_reference_max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_reference_max_entries: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub context_budget: AgentContextBudget,
}

impl AgentConfig {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        let provider = provider.into();
        let model = model.into();
        Self {
            provider,
            model,
            max_steps: None,
            max_tool_calls_per_step: None,
            max_model_repair_attempts: None,
            workspace_root: None,
            workspace_instruction_files: default_workspace_instruction_files(),
            prompt_reference_max_bytes: None,
            prompt_reference_max_entries: None,
            temperature: Some(0.2),
            max_output_tokens: None,
            context_budget: AgentContextBudget::default(),
        }
    }

    pub fn with_max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = Some(max_steps.max(1));
        self
    }

    pub fn with_max_tool_calls_per_step(mut self, max_tool_calls_per_step: usize) -> Self {
        self.max_tool_calls_per_step = Some(max_tool_calls_per_step.max(1));
        self
    }

    pub fn with_max_model_repair_attempts(mut self, max_model_repair_attempts: usize) -> Self {
        self.max_model_repair_attempts = Some(max_model_repair_attempts);
        self
    }

    pub fn with_workspace_root(mut self, workspace_root: impl Into<PathBuf>) -> Self {
        self.workspace_root = Some(workspace_root.into());
        self
    }

    pub fn with_workspace_instruction_files(
        mut self,
        files: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) -> Self {
        self.workspace_instruction_files = files.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_context_budget(mut self, context_budget: AgentContextBudget) -> Self {
        self.context_budget = context_budget;
        self
    }
}

fn default_workspace_instruction_files() -> Vec<PathBuf> {
    vec![PathBuf::from("AGENTS.md")]
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentRun {
    pub final_decision: AgentLoopDecision,
    pub stop_reason: AgentStopReason,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript: Vec<AgentTranscriptItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_turns: Vec<AgentModelTurn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<AgentToolOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<AgentItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_reports: Vec<ContextAssemblyReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_events: Vec<AgentModelEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compactions: Vec<AgentCompactionSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EventEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_evidence_bundle: Option<AgentCandidateEvidenceBundle>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentStopReason {
    FinalDecision,
    SilentAsyncUpdate,
    MaxStepsReached,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentModelTurn {
    pub step_index: usize,
    pub request: LanguageModelRequest,
    pub response: LanguageModelResponse,
    pub decision: AgentLoopDecision,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentModelEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentTranscriptItem {
    pub step_index: usize,
    pub role: AgentTranscriptRole,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<TaskId>,
}

impl AgentTranscriptItem {
    pub fn user(step_index: usize, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::User,
            content: content.into(),
            tool_call_id: None,
        }
    }

    pub fn assistant(step_index: usize, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::Assistant,
            content: content.into(),
            tool_call_id: None,
        }
    }

    pub fn tool(step_index: usize, tool_call_id: TaskId, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::Tool,
            content: content.into(),
            tool_call_id: Some(tool_call_id),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentTranscriptRole {
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentPolicyBundle {
    pub policy_id: String,
    pub role: AgentRole,
    #[serde(default)]
    pub tools: AgentToolPolicy,
}

impl AgentPolicyBundle {
    pub fn new(policy_id: impl Into<String>, role: AgentRole) -> Self {
        Self {
            policy_id: policy_id.into(),
            role,
            tools: AgentToolPolicy::default(),
        }
    }

    pub fn default_for_role(role: AgentRole) -> Self {
        Self::new(format!("{}-default", role.policy_label()), role)
    }

    pub fn with_allowed_tools(
        mut self,
        tools: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.tools.allowed_tools = tools.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_denied_tools(mut self, tools: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tools.denied_tools = tools.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_allowed_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = AgentToolCapability>,
    ) -> Self {
        self.tools.allowed_capabilities = capabilities.into_iter().collect();
        self
    }

    pub fn with_denied_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = AgentToolCapability>,
    ) -> Self {
        self.tools.denied_capabilities = capabilities.into_iter().collect();
        self
    }

    pub fn evaluate_descriptor(&self, descriptor: &AgentToolDescriptor) -> AgentToolPolicyDecision {
        self.tools.evaluate_descriptor(&self.policy_id, descriptor)
    }

    pub fn evaluate_tool_call(
        &self,
        descriptor: Option<&AgentToolDescriptor>,
        call: &AgentToolCall,
    ) -> AgentToolPolicyDecision {
        if let Some(descriptor) = descriptor {
            return self.evaluate_descriptor(descriptor);
        }
        if self
            .tools
            .denied_tools
            .iter()
            .any(|tool| tool == &call.tool)
        {
            return AgentToolPolicyDecision {
                allowed: false,
                reason: format!(
                    "tool `{}` is denied by agent policy `{}`",
                    call.tool, self.policy_id
                ),
            };
        }
        if !self.tools.allowed_tools.is_empty()
            && !self
                .tools
                .allowed_tools
                .iter()
                .any(|tool| tool == &call.tool)
        {
            return AgentToolPolicyDecision {
                allowed: false,
                reason: format!(
                    "tool `{}` is not in the allow-list for agent policy `{}`",
                    call.tool, self.policy_id
                ),
            };
        }
        AgentToolPolicyDecision {
            allowed: true,
            reason: format!(
                "tool `{}` has no descriptor and is available to reach the registry by agent policy `{}`",
                call.tool, self.policy_id
            ),
        }
    }

    pub fn filter_tool_descriptors(
        &self,
        descriptors: Vec<AgentToolDescriptor>,
    ) -> (Vec<AgentToolDescriptor>, Vec<ContextOmission>) {
        let mut visible = Vec::new();
        let mut omitted = Vec::new();
        for descriptor in descriptors {
            let decision = self.evaluate_descriptor(&descriptor);
            if decision.allowed {
                visible.push(descriptor);
            } else {
                omitted.push(ContextOmission {
                    kind: "tool_schema".to_string(),
                    label: descriptor.spec.name,
                    reason: decision.reason,
                });
            }
        }
        (visible, omitted)
    }
}

impl AgentRole {
    fn policy_label(&self) -> &'static str {
        match self {
            AgentRole::Main => "main",
            AgentRole::Maid => "maid",
            AgentRole::Deliberation => "deliberation",
            AgentRole::Evolution => "evolution",
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentToolPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_capabilities: Vec<AgentToolCapability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_capabilities: Vec<AgentToolCapability>,
}

impl AgentToolPolicy {
    fn evaluate_descriptor(
        &self,
        policy_id: &str,
        descriptor: &AgentToolDescriptor,
    ) -> AgentToolPolicyDecision {
        let tool_name = &descriptor.spec.name;
        if self.denied_tools.iter().any(|tool| tool == tool_name) {
            return AgentToolPolicyDecision {
                allowed: false,
                reason: format!("tool `{tool_name}` is denied by agent policy `{policy_id}`"),
            };
        }
        if !self.allowed_tools.is_empty()
            && !self.allowed_tools.iter().any(|tool| tool == tool_name)
        {
            return AgentToolPolicyDecision {
                allowed: false,
                reason: format!(
                    "tool `{tool_name}` is not in the allow-list for agent policy `{policy_id}`"
                ),
            };
        }
        if let Some(capability) = descriptor
            .capabilities
            .iter()
            .find(|capability| self.denied_capabilities.contains(capability))
        {
            return AgentToolPolicyDecision {
                allowed: false,
                reason: format!(
                    "tool `{tool_name}` has unavailable capability `{}` under agent policy `{policy_id}`",
                    capability.policy_label()
                ),
            };
        }
        if !self.allowed_capabilities.is_empty()
            && descriptor
                .capabilities
                .iter()
                .any(|capability| !self.allowed_capabilities.contains(capability))
        {
            return AgentToolPolicyDecision {
                allowed: false,
                reason: format!(
                    "tool `{tool_name}` has capabilities outside the available capability set for agent policy `{policy_id}`"
                ),
            };
        }
        AgentToolPolicyDecision {
            allowed: true,
            reason: format!("tool `{tool_name}` is available under agent policy `{policy_id}`"),
        }
    }
}

pub struct Agent<M> {
    model: M,
    config: AgentConfig,
    policy: AgentPolicyBundle,
    tools: AgentToolRegistry,
    context_assembler: ContextAssembler,
    compactor: AgentCompactor,
    trace_store: Box<dyn AgentTraceStore>,
    session_store: Box<dyn AgentSessionStore>,
}

impl<M: InteractionModel> Agent<M> {
    pub fn new(model: M, provider: impl Into<String>, model_id: impl Into<String>) -> Self {
        let provider = provider.into();
        let model_id = model_id.into();
        Self {
            model,
            config: AgentConfig::new(provider, model_id),
            policy: AgentPolicyBundle::default_for_role(AgentRole::Deliberation),
            tools: AgentToolRegistry::new(),
            context_assembler: ContextAssembler,
            compactor: AgentCompactor::default(),
            trace_store: Box::new(NoopAgentTraceStore),
            session_store: Box::new(NoopAgentSessionStore),
        }
    }

    pub fn with_config(mut self, config: AgentConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_tool_registry(mut self, tools: AgentToolRegistry) -> Self {
        self.tools = tools;
        self
    }

    pub fn tool_registry(&self) -> AgentToolRegistry {
        self.tools.clone()
    }

    pub fn register_tool<T: AgentTool + 'static>(&self, tool: T) -> Option<AgentToolDescriptor> {
        self.tools.register(tool)
    }

    pub fn remove_tool(&self, tool_name: &str) -> Option<AgentToolDescriptor> {
        self.tools.remove(tool_name)
    }

    pub fn with_policy(mut self, policy: AgentPolicyBundle) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_context_assembler(mut self, context_assembler: ContextAssembler) -> Self {
        self.context_assembler = context_assembler;
        self
    }

    pub fn with_compactor(mut self, compactor: AgentCompactor) -> Self {
        self.compactor = compactor;
        self
    }

    pub fn with_trace_store<T: AgentTraceStore + 'static>(mut self, trace_store: T) -> Self {
        self.trace_store = Box::new(trace_store);
        self
    }

    pub fn with_session_store<T: AgentSessionStore + 'static>(mut self, session_store: T) -> Self {
        self.session_store = Box::new(session_store);
        self
    }

    pub fn load_session(&self, session_id: &SessionId) -> Result<Option<AgentSession>, AgentError> {
        self.session_store
            .load_session(session_id)
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    pub fn save_session(&self, session: &AgentSession) -> Result<(), AgentError> {
        self.session_store
            .save_session(session)
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    fn trace(
        &self,
        session_id: &SessionId,
        record: AgentTraceRecordKind,
    ) -> Result<(), AgentError> {
        self.trace_store
            .append(&AgentTraceRecord::new(session_id.clone(), record))
            .map_err(|source| AgentError::TraceStore {
                detail: source.to_string(),
            })
    }

    fn session_store_record(
        &self,
        session_id: &SessionId,
        record: AgentSessionStoreRecordKind,
    ) -> Result<(), AgentError> {
        self.session_store
            .append_record(&AgentSessionStoreRecord::new(session_id.clone(), record))
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    fn session_store_items(
        &self,
        session_id: &SessionId,
        items: &[AgentItem],
    ) -> Result<(), AgentError> {
        self.session_store
            .append_items(session_id, items)
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    fn session_store_projection(
        &self,
        session_id: &SessionId,
        projection: &AgentSessionProjection,
    ) -> Result<(), AgentError> {
        self.session_store
            .append_projection(session_id, projection)
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    pub fn start_turn(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
    ) -> Result<AgentRun, AgentError> {
        if session.is_aborted() {
            return Ok(AgentRun {
                final_decision: AgentLoopDecision::silent(),
                stop_reason: AgentStopReason::Aborted,
                transcript: Vec::new(),
                model_turns: Vec::new(),
                tool_results: Vec::new(),
                items: Vec::new(),
                context_reports: Vec::new(),
                model_events: vec![AgentModelEvent::Aborted {
                    reason: session
                        .abort
                        .as_ref()
                        .map(|abort| abort.reason.clone())
                        .unwrap_or_else(|| "session aborted".to_string()),
                }],
                compactions: Vec::new(),
                events: Vec::new(),
                candidate_evidence_bundle: None,
            });
        }
        let transcript_prefix = project_session_transcript(
            &session.transcript,
            self.config.context_budget.retained_tail_items,
        )?;
        let steering = session.steering_queue.clone();
        let follow_ups = session.follow_up_queue.clone();
        let session_projection = session.projection.clone();
        self.session_store_record(
            &session.session_id,
            AgentSessionStoreRecordKind::TurnStarted {
                provider: self.config.provider.clone(),
                model: self.config.model.clone(),
                max_steps: self.config.max_steps,
            },
        )?;
        let run = self.run_with_session_context(
            input,
            transcript_prefix,
            session_projection,
            &steering,
            &follow_ups,
        )?;
        session.steering_queue.clear();
        session.follow_up_queue.clear();
        session.append_run(&run);
        self.session_store_items(&session.session_id, &run.items)?;
        for compaction in &run.compactions {
            self.session_store_projection(&session.session_id, &compaction.projection)?;
        }
        self.session_store_record(
            &session.session_id,
            AgentSessionStoreRecordKind::TurnCompleted {
                stop_reason: run.stop_reason.clone(),
                item_count: run.items.len(),
                event_count: run.events.len(),
            },
        )?;
        self.save_session(session)?;
        Ok(run)
    }

    pub fn continue_turn(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn(session, input)
    }

    pub fn steer(
        &self,
        session: &mut AgentSession,
        message: impl Into<String>,
    ) -> Result<AgentSteeringMessage, AgentError> {
        let steering = AgentSteeringMessage::new(message);
        session.steering_queue.push(steering.clone());
        self.session_store_record(
            &session.session_id,
            AgentSessionStoreRecordKind::SteeringQueued {
                steering: steering.clone(),
            },
        )?;
        self.save_session(session)?;
        Ok(steering)
    }

    pub fn follow_up(
        &self,
        session: &mut AgentSession,
        message: impl Into<String>,
    ) -> Result<AgentFollowUpMessage, AgentError> {
        let follow_up = AgentFollowUpMessage::new(message);
        session.follow_up_queue.push(follow_up.clone());
        self.session_store_record(
            &session.session_id,
            AgentSessionStoreRecordKind::FollowUpQueued {
                follow_up: follow_up.clone(),
            },
        )?;
        self.save_session(session)?;
        Ok(follow_up)
    }

    pub fn compact(
        &self,
        session: &mut AgentSession,
        reason: impl Into<String>,
    ) -> Result<AgentCompactionSummary, AgentError> {
        let source_transcript = session.compaction_source_transcript();
        let summary = self
            .compactor
            .compact_retaining_tail_count_with_session_state(
                &source_transcript,
                reason,
                source_transcript.items.len(),
                self.compactor
                    .retained_tail_count
                    .unwrap_or(source_transcript.items.len()),
                session.projection.as_ref(),
                &session.steering_queue,
                &session.follow_up_queue,
            );
        let compaction_item = AgentItem::CompactionSummary {
            item_id: TaskId::generate(),
            summary: summary.clone(),
        };
        session.raw_transcript.items.push(compaction_item.clone());
        session.projection = Some(summary.projection.clone());
        session.transcript.items = compacted_session_items(
            &source_transcript,
            compaction_item.clone(),
            summary.retained_tail_count,
        );
        self.session_store_items(&session.session_id, &[compaction_item])?;
        self.session_store_projection(&session.session_id, &summary.projection)?;
        self.session_store_record(
            &session.session_id,
            AgentSessionStoreRecordKind::Compacted {
                summary: summary.clone(),
                item_count: session.transcript.items.len(),
            },
        )?;
        self.save_session(session)?;
        Ok(summary)
    }

    pub fn abort(
        &self,
        session: &mut AgentSession,
        reason: impl Into<String>,
    ) -> Result<AgentAbortRecord, AgentError> {
        let abort = AgentAbortRecord::new(reason);
        session.abort = Some(abort.clone());
        self.session_store_record(
            &session.session_id,
            AgentSessionStoreRecordKind::Aborted {
                abort: abort.clone(),
            },
        )?;
        self.save_session(session)?;
        Ok(abort)
    }

    pub fn run(&self, input: &AgentLoopInput) -> Result<AgentRun, AgentError> {
        self.run_with_session_context(input, Vec::new(), None, &[], &[])
    }

    fn run_with_session_context(
        &self,
        input: &AgentLoopInput,
        transcript_prefix: Vec<AgentTranscriptItem>,
        mut session_projection: Option<AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
    ) -> Result<AgentRun, AgentError> {
        if let Some(update) = &input.async_update
            && !update.needs_user_attention
            && update.next_action_hint == AsyncTaskNextAction::ContinueSilently
        {
            let item = AgentItem::AsyncTaskUpdate {
                item_id: TaskId::generate(),
                update: update.clone(),
            };
            let event = caused_by_latest_event(
                agent_event(
                    &input.session,
                    "agent.loop.completed",
                    0.48,
                    serde_json::json!({
                        "stop_reason": AgentStopReason::SilentAsyncUpdate,
                        "silent_async_update": input.async_update,
                    }),
                ),
                input,
            );
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Item { item: item.clone() },
            )?;
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Event {
                    event: event.clone(),
                },
            )?;
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::RunCompleted {
                    stop_reason: AgentStopReason::SilentAsyncUpdate,
                    final_decision: AgentLoopDecision::silent(),
                },
            )?;
            return Ok(AgentRun {
                final_decision: AgentLoopDecision::silent(),
                stop_reason: AgentStopReason::SilentAsyncUpdate,
                transcript: Vec::new(),
                model_turns: Vec::new(),
                tool_results: Vec::new(),
                items: vec![item],
                context_reports: Vec::new(),
                model_events: Vec::new(),
                compactions: Vec::new(),
                events: vec![event],
                candidate_evidence_bundle: None,
            });
        }

        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::RunStarted {
                provider: self.config.provider.clone(),
                model: self.config.model.clone(),
                max_steps: self.config.max_steps,
            },
        )?;
        let initial_input = render_initial_agent_input(input)?;
        let mut transcript = transcript_prefix;
        let mut queued_agent_item = None;
        let queued_turn_context = render_queued_turn_context(steering, follow_ups)
            .map(|content| AgentTranscriptItem::user(0, content));
        if let Some(item) = queued_turn_context {
            let queued_item = AgentItem::UserMessage {
                item_id: TaskId::generate(),
                step_index: 0,
                content: item.content.clone(),
                event_id: None,
            };
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Item {
                    item: queued_item.clone(),
                },
            )?;
            queued_agent_item = Some(queued_item);
            transcript.push(item);
        }
        transcript.push(AgentTranscriptItem::user(0, initial_input.clone()));
        let mut model_turns = Vec::new();
        let mut tool_results = Vec::new();
        let initial_item = AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: initial_input,
            event_id: Some(input.latest_event.event_id.clone()),
        };
        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::Item {
                item: initial_item.clone(),
            },
        )?;
        let mut items = queued_agent_item.into_iter().collect::<Vec<_>>();
        items.push(initial_item);
        let mut context_reports = Vec::new();
        let mut model_events = Vec::new();
        let mut compactions = Vec::new();
        let start_event = caused_by_latest_event(
            agent_event(
                &input.session,
                "agent.loop.started",
                0.54,
                serde_json::json!({
                    "provider": self.config.provider,
                    "model": self.config.model,
                    "max_steps": self.config.max_steps,
                    "max_tool_calls_per_step": self.config.max_tool_calls_per_step,
                }),
            ),
            input,
        );
        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::Event {
                event: start_event.clone(),
            },
        )?;
        let mut events = vec![start_event];
        let mut final_decision = AgentLoopDecision::silent();
        let mut stop_reason = AgentStopReason::MaxStepsReached;
        let mut step_index = 0usize;

        loop {
            if self
                .config
                .max_steps
                .is_some_and(|max_steps| step_index >= max_steps)
            {
                break;
            }
            let mut turn_context = self.assemble_context_with_projection(
                input,
                &transcript,
                session_projection.as_ref(),
                step_index,
            )?;
            if self
                .config
                .context_budget
                .compaction_threshold_bytes()
                .is_some_and(|threshold_bytes| {
                    turn_context.budget_estimate.total_bytes > threshold_bytes
                })
            {
                let target_transcript_bytes =
                    compaction_target_transcript_bytes(&turn_context.budget_estimate);
                let retained_transcript = retained_transcript_tail_for_compaction(
                    &transcript,
                    self.compactor.retained_tail_count,
                    target_transcript_bytes,
                );
                if retained_transcript.len() < transcript.len() {
                    let mut typed_transcript = AgentTranscript::new();
                    typed_transcript.items.extend(items.iter().cloned());
                    let retained_tail_count = self
                        .compactor
                        .retained_tail_count
                        .unwrap_or(retained_transcript.len())
                        .min(typed_transcript.items.len());
                    let compaction = self
                        .compactor
                        .compact_retaining_tail_count_with_session_state(
                            &typed_transcript,
                            "context budget threshold reached",
                            turn_context.budget_estimate.total_bytes,
                            retained_tail_count,
                            session_projection.as_ref(),
                            steering,
                            follow_ups,
                        );
                    let compaction_item = AgentItem::CompactionSummary {
                        item_id: TaskId::generate(),
                        summary: compaction.clone(),
                    };
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Compaction {
                            summary: compaction.clone(),
                        },
                    )?;
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Item {
                            item: compaction_item.clone(),
                        },
                    )?;
                    items.push(compaction_item);
                    let compaction_event = caused_by_latest_event(
                        agent_event(
                            &input.session,
                            "agent.compaction.completed",
                            0.57,
                            serde_json::json!({
                                "step_index": step_index,
                                "compaction": compaction,
                            }),
                        ),
                        input,
                    );
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Event {
                            event: compaction_event.clone(),
                        },
                    )?;
                    events.push(compaction_event);
                    session_projection = Some(compaction.projection.clone());
                    transcript = compacted_transcript_view(&compaction, retained_transcript)?;
                    turn_context = self.assemble_context_with_projection(
                        input,
                        &transcript,
                        session_projection.as_ref(),
                        step_index,
                    )?;
                    compactions.push(compaction);
                }
            }
            context_reports.push(turn_context.report.clone());
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::ContextReport {
                    report: turn_context.report.clone(),
                },
            )?;
            let context_item = AgentItem::ContextInjection {
                item_id: TaskId::generate(),
                report: turn_context.report.clone(),
            };
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Item {
                    item: context_item.clone(),
                },
            )?;
            items.push(context_item);
            let context_event = caused_by_latest_event(
                agent_event(
                    &input.session,
                    "agent.context.assembled",
                    0.53,
                    serde_json::json!({
                        "step_index": step_index,
                        "report": turn_context.report.clone(),
                    }),
                ),
                input,
            );
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Event {
                    event: context_event.clone(),
                },
            )?;
            events.push(context_event);
            let mut request = self.request_from_context(input, &turn_context, step_index)?;
            let mut repair_attempt = 0;
            let (request, response, decision, step_model_events) = loop {
                let sampled_request = request.clone();
                let mut language_model_stream_events = Vec::new();
                let response =
                    self.model
                        .complete_streaming_events(&sampled_request, &mut |event| {
                            language_model_stream_events.push(event);
                        })?;
                let assistant_item = AgentItem::AssistantText {
                    item_id: TaskId::generate(),
                    step_index,
                    content: response.content.clone(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: assistant_item.clone(),
                    },
                )?;
                items.push(assistant_item);
                match parse_agent_model_decision(
                    &response,
                    &language_model_stream_events,
                    &turn_context.available_tool_specs,
                ) {
                    Ok(decision) => {
                        let step_model_events = AgentModelEvent::from_model_response(
                            step_index,
                            &response,
                            &decision,
                            language_model_stream_events,
                        );
                        break (sampled_request, response, decision, step_model_events);
                    }
                    Err(error) => {
                        let detail = model_repair_parse_error_detail(
                            repair_attempt,
                            self.config.max_model_repair_attempts,
                            &error,
                        );
                        let step_model_events = AgentModelEvent::from_unparsed_model_response(
                            step_index,
                            &response,
                            language_model_stream_events,
                            detail.clone(),
                        );
                        for event in &step_model_events {
                            self.trace(
                                &input.session.session_id,
                                AgentTraceRecordKind::ModelEvent {
                                    event: event.clone(),
                                },
                            )?;
                        }
                        model_events.extend(step_model_events);
                        let error_item = AgentItem::Error {
                            item_id: TaskId::generate(),
                            step_index,
                            detail: detail.clone(),
                        };
                        self.trace(
                            &input.session.session_id,
                            AgentTraceRecordKind::Item {
                                item: error_item.clone(),
                            },
                        )?;
                        items.push(error_item);
                        if self
                            .config
                            .max_model_repair_attempts
                            .is_some_and(|max_attempts| repair_attempt >= max_attempts)
                        {
                            return Err(AgentError::ModelResponse { detail });
                        }
                        repair_attempt += 1;
                        request = repair_request_for_model_response(
                            &sampled_request,
                            &response.content,
                            &detail,
                            repair_attempt,
                        )?;
                    }
                }
            };
            transcript.push(AgentTranscriptItem::assistant(
                step_index,
                response.content.clone(),
            ));
            for event in &step_model_events {
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::ModelEvent {
                        event: event.clone(),
                    },
                )?;
            }
            model_events.extend(step_model_events.iter().cloned());
            if response.usage.is_some() {
                let usage_item = AgentItem::ModelUsage {
                    item_id: TaskId::generate(),
                    step_index,
                    provider: response.provider.clone(),
                    model: response.model.clone(),
                    usage: response.usage.clone(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: usage_item.clone(),
                    },
                )?;
                items.push(usage_item);
            }
            let model_event = caused_by_latest_event(
                agent_event(
                    &input.session,
                    "agent.loop.model.completed",
                    0.58,
                    serde_json::json!({
                        "step_index": step_index,
                        "response_mode": decision.response_mode,
                        "tool_call_count": decision.tool_calls.len(),
                        "next_state": decision.next_state,
                    }),
                ),
                input,
            );
            self.trace(
                &input.session.session_id,
                AgentTraceRecordKind::Event {
                    event: model_event.clone(),
                },
            )?;
            events.push(model_event);
            model_turns.push(AgentModelTurn {
                step_index,
                request,
                response,
                decision: decision.clone(),
                events: step_model_events,
            });
            final_decision = decision.clone();

            if decision.tool_calls.is_empty() {
                stop_reason = AgentStopReason::FinalDecision;
                break;
            }

            let mut step_results = Vec::new();
            let tool_plan = self.tool_plan(input);
            for (call_index, call) in decision.tool_calls.iter().enumerate() {
                let tool_call_item = AgentItem::ToolCall {
                    item_id: TaskId::generate(),
                    step_index,
                    call_index,
                    call: call.clone(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: tool_call_item.clone(),
                    },
                )?;
                items.push(tool_call_item);
                let descriptor = tool_plan.descriptor(&call.tool);
                let policy_decision = self.policy.evaluate_tool_call(descriptor, call);
                let mut outcome = if !policy_decision.allowed {
                    AgentToolOutcome {
                        call_id: TaskId::generate(),
                        step_index,
                        call_index,
                        call: call.clone(),
                        status: AgentToolStatus::Blocked,
                        summary: policy_decision.reason.clone(),
                        output: serde_json::json!({
                            "error": "policy_denied",
                            "policy_id": self.policy.policy_id.clone(),
                            "tool": call.tool.clone(),
                            "reason": policy_decision.reason.clone(),
                        }),
                        event_id: None,
                        evidence: AgentToolEvidence::policy_only(
                            false,
                            policy_decision.reason.clone(),
                        ),
                    }
                } else if self
                    .config
                    .max_tool_calls_per_step
                    .is_some_and(|max_tool_calls| call_index >= max_tool_calls)
                {
                    let max_tool_calls_per_step = self.config.max_tool_calls_per_step.unwrap_or(0);
                    AgentToolOutcome {
                        call_id: TaskId::generate(),
                        step_index,
                        call_index,
                        call: call.clone(),
                        status: AgentToolStatus::Blocked,
                        summary: format!(
                            "tool call limit {} reached for step {step_index}",
                            max_tool_calls_per_step
                        ),
                        output: serde_json::json!({
                            "error": "tool_call_limit_reached",
                            "max_tool_calls_per_step": max_tool_calls_per_step,
                        }),
                        event_id: None,
                        evidence: AgentToolEvidence::policy_only(
                            false,
                            "blocked by per-step tool call limit",
                        ),
                    }
                } else if descriptor.is_some() && !tool_plan.is_executable(&call.tool) {
                    AgentToolOutcome {
                        call_id: TaskId::generate(),
                        step_index,
                        call_index,
                        call: call.clone(),
                        status: AgentToolStatus::Blocked,
                        summary: format!(
                            "tool `{}` is visible to the model but has no executor attached to this agent",
                            call.tool
                        ),
                        output: serde_json::json!({
                            "error": "missing_executor",
                            "tool": call.tool,
                        }),
                        event_id: None,
                        evidence: AgentToolEvidence::policy_only(
                            false,
                            "blocked because planned tool has no runtime executor",
                        ),
                    }
                } else {
                    self.tools
                        .execute(&input.session, step_index, call_index, call)
                };
                if policy_decision.allowed && outcome.evidence.policy.allowed {
                    outcome.evidence.policy = policy_decision;
                }
                let event = caused_by_latest_event(
                    agent_event(
                        &input.session,
                        "agent.loop.tool.completed",
                        0.62,
                        serde_json::json!({
                            "call_id": outcome.call_id,
                            "step_index": outcome.step_index,
                            "call_index": outcome.call_index,
                            "tool": outcome.call.tool,
                            "status": outcome.status,
                            "summary": outcome.summary,
                        }),
                    ),
                    input,
                );
                outcome.event_id = Some(event.event_id.clone());
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::ToolOutcome {
                        outcome: outcome.clone(),
                    },
                )?;
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Event {
                        event: event.clone(),
                    },
                )?;
                events.push(event);
                let tool_result_item = AgentItem::ToolResult {
                    item_id: TaskId::generate(),
                    step_index: outcome.step_index,
                    call_index: outcome.call_index,
                    call_id: outcome.call_id.clone(),
                    status: outcome.status.clone(),
                    summary: outcome.summary.clone(),
                    output: outcome.output.clone(),
                    evidence: outcome.evidence.clone(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: tool_result_item.clone(),
                    },
                )?;
                items.push(tool_result_item);
                if let Some(patch_snapshot_item) = patch_snapshot_item_from_tool_outcome(&outcome) {
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Item {
                            item: patch_snapshot_item.clone(),
                        },
                    )?;
                    items.push(patch_snapshot_item);
                }
                if let Some(verification_item) = verification_item_from_tool_outcome(&outcome) {
                    self.trace(
                        &input.session.session_id,
                        AgentTraceRecordKind::Item {
                            item: verification_item.clone(),
                        },
                    )?;
                    items.push(verification_item);
                }
                step_results.push(outcome.clone());
                tool_results.push(outcome);
            }
            let tool_message_id = step_results
                .first()
                .map(|result| result.call_id.clone())
                .unwrap_or_else(TaskId::generate);
            transcript.push(AgentTranscriptItem::tool(
                step_index,
                tool_message_id,
                render_tool_results(step_index, &step_results)?,
            ));
            step_index = step_index.saturating_add(1);
        }

        let completed_event = caused_by_latest_event(
            agent_event(
                &input.session,
                "agent.loop.completed",
                0.56,
                serde_json::json!({
                    "stop_reason": stop_reason,
                    "model_turn_count": model_turns.len(),
                    "tool_result_count": tool_results.len(),
                    "final_response_mode": final_decision.response_mode,
                }),
            ),
            input,
        );
        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::Event {
                event: completed_event.clone(),
            },
        )?;
        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::RunCompleted {
                stop_reason: stop_reason.clone(),
                final_decision: final_decision.clone(),
            },
        )?;
        events.push(completed_event);

        let candidate_evidence_bundle = build_candidate_evidence_bundle(
            input.evolution_context.as_ref(),
            &final_decision,
            &items,
        );

        Ok(AgentRun {
            final_decision,
            stop_reason,
            transcript,
            model_turns,
            tool_results,
            items,
            context_reports,
            model_events,
            compactions,
            events,
            candidate_evidence_bundle,
        })
    }

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

    fn assemble_context_with_projection(
        &self,
        input: &AgentLoopInput,
        transcript: &[AgentTranscriptItem],
        session_projection: Option<&AgentSessionProjection>,
        step_index: usize,
    ) -> Result<TurnContext, AgentError> {
        let all_tool_descriptors = self.tool_plan(input).descriptors();
        let (tool_descriptors, policy_omissions) =
            self.policy.filter_tool_descriptors(all_tool_descriptors);
        let mut context = self.context_assembler.assemble(
            input,
            AgentRole::Deliberation,
            &self.config,
            transcript,
            session_projection.cloned(),
            tool_descriptors,
            step_index,
        )?;
        let policy_bytes = serde_json::to_vec(&self.policy)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        context.report.included.push(ContextSource {
            kind: "agent_policy".to_string(),
            label: self.policy.policy_id.clone(),
            bytes: policy_bytes,
        });
        context.budget_estimate.lanes.push(ContextBudgetLane {
            kind: "fixed_prompt".to_string(),
            label: "agent policy".to_string(),
            bytes: policy_bytes,
            priority: 0,
        });
        context.report.omitted.extend(policy_omissions);
        context.budget_estimate.total_bytes += policy_bytes;
        context.report.budget_estimate = context.budget_estimate.clone();
        Ok(context)
    }

    fn request_from_context(
        &self,
        input: &AgentLoopInput,
        turn_context: &TurnContext,
        step_index: usize,
    ) -> Result<LanguageModelRequest, AgentError> {
        let mut messages = vec![LanguageModelMessage {
            role: LanguageModelRole::System,
            content: turn_context.system_instructions.clone(),
        }];
        messages.push(LanguageModelMessage {
            role: LanguageModelRole::User,
            content: render_turn_context_message(input, turn_context)?,
        });
        messages.extend(
            turn_context
                .transcript_view
                .iter()
                .map(|item| LanguageModelMessage {
                    role: match item.role {
                        AgentTranscriptRole::Assistant => LanguageModelRole::Assistant,
                        AgentTranscriptRole::User | AgentTranscriptRole::Tool => {
                            LanguageModelRole::User
                        }
                    },
                    content: item.content.clone(),
                }),
        );

        let items = messages
            .iter()
            .map(|message| {
                LanguageModelRequestItem::message(message.role.clone(), message.content.clone())
            })
            .collect();
        let tools = turn_context
            .available_tool_specs
            .iter()
            .map(language_model_tool_spec_from_agent_tool)
            .collect();

        Ok(LanguageModelRequest {
            provider: self.config.provider.clone(),
            model: self.config.model.clone(),
            messages,
            items,
            tools,
            tool_choice: Some(LanguageModelToolChoice::Auto),
            parallel_tool_calls: Some(false),
            temperature: self.config.temperature,
            max_output_tokens: self.config.max_output_tokens,
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::json!({
                "loop": "agent",
                "step_index": step_index,
                "session_id": input.session.session_id,
                "event_id": input.latest_event.event_id,
                "context_report_id": turn_context.report.report_id,
                "context_budget_estimate": turn_context.budget_estimate,
                "agent_policy_id": self.policy.policy_id.clone(),
            }),
        })
    }

    fn tool_plan(&self, input: &AgentLoopInput) -> AgentToolPlan {
        AgentToolPlanner::plan(
            self.tools.descriptors(),
            input.tool_specs.clone(),
            default_main_agent_tools(),
            input.tool_specs.is_empty(),
        )
    }
}

fn language_model_tool_spec_from_agent_tool(
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

fn language_model_provider_tool_spec_from_agent_tool(
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

fn language_model_custom_tool_spec_from_agent_tool(
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
    let tools_json = serde_json::to_string_pretty(tools).expect("tool descriptors serialize");
    format!(
        r#"You are Sinomo's deliberation agent model.

You operate inside Sinomo's coding-agent loop. The agent owns tool execution,
policy, event emission, and session state. Use tools when evidence or workspace
changes are needed, then continue after tool_result messages arrive.

Return only JSON shaped as:
{{
  "response_mode": "answer|acknowledge|clarify|defer|silent",
  "text": "short user-facing response or a concise working note",
  "tool_calls": [
    {{
      "tool": "tool name",
      "input": {{}},
      "async": false,
      "summary": "why this tool is being used"
    }}
  ],
  "maid_handoff": null,
  "next_state": "speaking|listening|waiting|delegating|recovering|idle"
}}

When the next step is clear and no more tools are needed, return an empty
tool_calls array. If a tool fails or is blocked, recover from the model-visible
tool_result instead of repeating the same call blindly.

Available agent tools:
```json
{tools_json}
```"#
    )
}

fn render_initial_agent_input(input: &AgentLoopInput) -> Result<String, AgentError> {
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

fn render_queued_turn_context(
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

fn project_session_transcript(
    transcript: &AgentTranscript,
    retained_tail_items: Option<usize>,
) -> Result<Vec<AgentTranscriptItem>, AgentError> {
    let items = compacted_projection_items(&transcript.items, retained_tail_items);
    items
        .into_iter()
        .filter_map(project_agent_item_to_transcript_item)
        .collect()
}

fn compacted_session_items(
    transcript: &AgentTranscript,
    compaction_item: AgentItem,
    retained_tail_count: usize,
) -> Vec<AgentItem> {
    let mut items = vec![compaction_item];
    items.extend(transcript.retained_tail(retained_tail_count));
    items
}

fn compacted_projection_items(
    items: &[AgentItem],
    retained_tail_items: Option<usize>,
) -> Vec<AgentItem> {
    let mut items = latest_compacted_projection_items(items);
    if let Some(retained_tail_items) = retained_tail_items {
        items = if items
            .first()
            .is_some_and(|item| matches!(item, AgentItem::CompactionSummary { .. }))
        {
            let mut retained = vec![items[0].clone()];
            retained.extend(agent_item_tail(&items[1..], retained_tail_items));
            retained
        } else {
            agent_item_tail(&items, retained_tail_items)
        };
    }
    items
}

fn latest_compacted_projection_items(items: &[AgentItem]) -> Vec<AgentItem> {
    let Some(compaction_index) = items
        .iter()
        .rposition(|item| matches!(item, AgentItem::CompactionSummary { .. }))
    else {
        return items.to_vec();
    };
    let AgentItem::CompactionSummary { summary, .. } = &items[compaction_index] else {
        return items.to_vec();
    };

    let mut compacted = vec![items[compaction_index].clone()];
    if let Some(first_retained_item) = &summary.first_retained_item
        && let Some(retained_start) = items
            .iter()
            .position(|item| agent_item_id(item) == *first_retained_item)
        && retained_start < compaction_index
    {
        compacted.extend(
            items[retained_start..compaction_index]
                .iter()
                .filter(|item| !matches!(item, AgentItem::CompactionSummary { .. }))
                .cloned(),
        );
    }
    compacted.extend(items[compaction_index + 1..].iter().cloned());
    compacted
}

fn agent_item_tail(items: &[AgentItem], retained_tail_items: usize) -> Vec<AgentItem> {
    if items.len() <= retained_tail_items {
        return items.to_vec();
    }
    items[items.len().saturating_sub(retained_tail_items)..].to_vec()
}

fn compaction_target_transcript_bytes(estimate: &ContextBudgetEstimate) -> Option<usize> {
    let threshold_bytes = estimate.compaction_threshold_bytes?;
    let non_transcript_bytes = estimate
        .total_bytes
        .saturating_sub(estimate.transcript_bytes);
    Some(threshold_bytes.saturating_sub(non_transcript_bytes))
}

fn retained_transcript_tail_for_compaction(
    transcript: &[AgentTranscriptItem],
    retained_tail_count: Option<usize>,
    target_transcript_bytes: Option<usize>,
) -> Vec<AgentTranscriptItem> {
    let candidates = retained_tail_count
        .map(|count| transcript_item_tail(transcript, count))
        .unwrap_or_else(|| transcript.to_vec());
    let Some(target_transcript_bytes) = target_transcript_bytes else {
        return candidates;
    };
    if transcript_items_bytes(&candidates) <= target_transcript_bytes {
        return candidates;
    }

    let mut retained = Vec::new();
    let mut retained_bytes = 0usize;
    let keep_latest_even_when_large = retained_tail_count != Some(0);
    for item in candidates.iter().rev() {
        let item_bytes = item.content.len();
        let would_fit = retained_bytes.saturating_add(item_bytes) <= target_transcript_bytes;
        if would_fit || (retained.is_empty() && keep_latest_even_when_large) {
            retained_bytes = retained_bytes.saturating_add(item_bytes);
            retained.push(item.clone());
        } else {
            break;
        }
    }
    retained.reverse();
    retained
}

fn transcript_item_tail(
    items: &[AgentTranscriptItem],
    retained_tail_items: usize,
) -> Vec<AgentTranscriptItem> {
    if items.len() <= retained_tail_items {
        return items.to_vec();
    }
    items[items.len().saturating_sub(retained_tail_items)..].to_vec()
}

fn transcript_items_bytes(items: &[AgentTranscriptItem]) -> usize {
    items.iter().map(|item| item.content.len()).sum()
}

fn compacted_transcript_view(
    summary: &AgentCompactionSummary,
    retained_tail: Vec<AgentTranscriptItem>,
) -> Result<Vec<AgentTranscriptItem>, AgentError> {
    let mut transcript = vec![AgentTranscriptItem::user(
        0,
        serde_json::to_string_pretty(&serde_json::json!({
            "kind": "prior_compaction_summary",
            "summary": summary,
        }))
        .map_err(|source| AgentError::ModelRequest {
            detail: source.to_string(),
        })?,
    )];
    transcript.extend(retained_tail);
    Ok(transcript)
}

fn project_agent_item_to_transcript_item(
    item: AgentItem,
) -> Option<Result<AgentTranscriptItem, AgentError>> {
    match item {
        AgentItem::UserMessage {
            step_index,
            content,
            ..
        } => Some(Ok(AgentTranscriptItem::user(step_index, content))),
        AgentItem::AssistantText {
            step_index,
            content,
            ..
        } => Some(Ok(AgentTranscriptItem::assistant(step_index, content))),
        AgentItem::ToolResult {
            item_id: _,
            step_index,
            call_id,
            status,
            summary,
            output,
            evidence,
            ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_tool_result",
                "status": status,
                "summary": summary,
                "output": model_visible_tool_output(None, &output),
                "evidence": model_visible_tool_evidence(&evidence),
            }))
            .map(|content| AgentTranscriptItem::tool(step_index, call_id, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::CompactionSummary { summary, .. } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_compaction_summary",
                "summary": summary,
            }))
            .map(|content| AgentTranscriptItem::user(0, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::Error {
            step_index, detail, ..
        } => Some(Ok(AgentTranscriptItem::user(
            step_index,
            serde_json::json!({
                "kind": "prior_agent_error",
                "detail": detail,
            })
            .to_string(),
        ))),
        AgentItem::PatchSnapshot {
            step_index,
            call_id,
            status,
            summary,
            patch_ref,
            changed_files,
            ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_patch_snapshot",
                "call_id": call_id,
                "status": status,
                "summary": summary,
                "patch_ref": patch_ref,
                "changed_files": changed_files,
            }))
            .map(|content| AgentTranscriptItem::user(step_index, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::VerificationResult {
            step_index,
            call_id,
            command,
            status,
            summary,
            ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_verification_result",
                "call_id": call_id,
                "command": command,
                "status": status,
                "summary": summary,
            }))
            .map(|content| AgentTranscriptItem::user(step_index, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::AssistantReasoningSummary { .. }
        | AgentItem::ToolCall { .. }
        | AgentItem::AsyncTaskUpdate { .. }
        | AgentItem::ContextInjection { .. }
        | AgentItem::ModelUsage { .. } => None,
    }
}

fn render_turn_context_message(
    input: &AgentLoopInput,
    turn_context: &TurnContext,
) -> Result<String, AgentError> {
    serde_json::to_string_pretty(&serde_json::json!({
        "kind": "turn_context",
        "step_index": turn_context.report.step_index,
        "session_projection": turn_context.session_projection,
        "current_work": turn_context
            .session_projection
            .as_ref()
            .map(|projection| &projection.working_state),
        "workspace_facts": turn_context.workspace_facts,
        "loaded_instruction_files": turn_context.loaded_instruction_files,
        "prompt_references": turn_context.prompt_references,
        "evolution_job_facts": turn_context.evolution_job_facts,
        "async_update": input.async_update,
        "context_budget_estimate": turn_context.budget_estimate,
        "context_report_id": turn_context.report.report_id,
    }))
    .map_err(|source| AgentError::ModelRequest {
        detail: source.to_string(),
    })
}

fn render_tool_results(
    step_index: usize,
    results: &[AgentToolOutcome],
) -> Result<String, AgentError> {
    serde_json::to_string_pretty(&serde_json::json!({
        "kind": "tool_results",
        "step_index": step_index,
        "results": results
            .iter()
            .map(model_visible_tool_outcome)
            .collect::<Vec<_>>(),
    }))
    .map_err(|source| AgentError::ModelRequest {
        detail: source.to_string(),
    })
}

fn model_visible_tool_outcome(outcome: &AgentToolOutcome) -> serde_json::Value {
    let mut value = serde_json::json!({
        "call_id": outcome.call_id,
        "step_index": outcome.step_index,
        "call_index": outcome.call_index,
        "call": outcome.call,
        "status": outcome.status,
        "summary": outcome.summary,
        "output": model_visible_tool_output(Some(outcome.call.tool.as_str()), &outcome.output),
        "evidence": model_visible_tool_evidence(&outcome.evidence),
    });
    if let Some(event_id) = &outcome.event_id
        && let Some(object) = value.as_object_mut()
    {
        object.insert("event_id".to_string(), serde_json::json!(event_id));
    }
    value
}

fn model_visible_tool_output(
    tool_name: Option<&str>,
    output: &serde_json::Value,
) -> serde_json::Value {
    let mut value = output.clone();
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    let looks_like_patch_output = tool_name == Some("apply_patch")
        || object.contains_key("diff") && object.contains_key("changed_files");
    if looks_like_patch_output && let Some(diff) = object.get_mut("diff") {
        *diff = model_visible_patch_diff(diff);
    }
    value
}

fn model_visible_tool_evidence(evidence: &AgentToolEvidence) -> serde_json::Value {
    let artifacts = evidence
        .artifacts
        .iter()
        .map(|artifact| {
            serde_json::json!({
                "kind": artifact.kind,
                "label": artifact.label,
                "media_type": artifact.media_type,
                "uri_omitted": true,
            })
        })
        .collect::<Vec<_>>();
    let records = evidence
        .records
        .iter()
        .map(|record| {
            serde_json::json!({
                "kind": record.kind,
                "data": model_visible_evidence_record_data(&record.kind, &record.data),
            })
        })
        .collect::<Vec<_>>();
    let execution = evidence.execution.as_ref().map(|execution| {
        serde_json::json!({
            "environment": execution.environment,
            "exit_code": execution.exit_code,
            "cwd_omitted": execution.cwd.is_some(),
        })
    });
    let mut value = serde_json::json!({
        "policy": evidence.policy,
        "output": evidence.output,
        "artifacts": artifacts,
        "execution": execution,
        "records": records,
    });
    if !evidence.metadata.is_null()
        && let Some(object) = value.as_object_mut()
    {
        object.insert(
            "metadata_omitted".to_string(),
            serde_json::json!({
                "reason": "tool evidence metadata is stored in the run trace and omitted from model-visible context"
            }),
        );
    }
    value
}

fn model_visible_evidence_record_data(kind: &str, data: &serde_json::Value) -> serde_json::Value {
    if kind == AGENT_CODING_EVIDENCE_KIND {
        let mut value = data.clone();
        if let Some(object) = value.as_object_mut() {
            let removed_patch_ref = object.remove("patch_ref").is_some();
            let removed_diff_ref = object.remove("diff_ref").is_some();
            let removed_refs = removed_patch_ref || removed_diff_ref;
            if removed_refs {
                object.insert("artifact_refs_omitted".to_string(), serde_json::json!(true));
            }
        }
        return value;
    }
    if kind == "patch_diff" {
        return serde_json::json!({
            "files": patch_diff_file_summaries(data),
            "content_omitted": true,
            "omitted_bytes": data.to_string().len(),
        });
    }
    data.clone()
}

fn model_visible_patch_diff(diff: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "files": patch_diff_file_summaries(diff),
        "content_omitted": true,
        "omitted_bytes": diff.to_string().len(),
    })
}

fn patch_diff_file_summaries(diff: &serde_json::Value) -> Vec<serde_json::Value> {
    diff.get("files")
        .and_then(serde_json::Value::as_array)
        .map(|files| {
            files
                .iter()
                .map(|file| {
                    serde_json::json!({
                        "status": file.get("status"),
                        "path": file.get("path"),
                        "added_lines": file.get("added_lines"),
                        "removed_lines": file.get("removed_lines"),
                        "diff_omitted_bytes": file
                            .get("diff")
                            .and_then(serde_json::Value::as_str)
                            .map(str::len)
                            .unwrap_or_default(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_agent_model_decision(
    response: &LanguageModelResponse,
    stream_events: &[LanguageModelStreamEvent],
    available_tool_specs: &[AgentToolDescriptor],
) -> Result<AgentLoopDecision, AgentError> {
    let tool_calls = native_agent_tool_calls(response, stream_events, available_tool_specs);
    if !tool_calls.is_empty() {
        return Ok(AgentLoopDecision {
            response_mode: crate::AgentLoopResponseMode::Defer,
            text: if response.content.trim().is_empty() {
                "The model requested tool execution.".to_string()
            } else {
                response.content.clone()
            },
            tool_calls,
            maid_handoff: None,
            next_state: crate::AgentInteractionState::Waiting,
        });
    }
    parse_agent_loop_decision_strict(&response.content)
}

fn native_agent_tool_calls(
    response: &LanguageModelResponse,
    stream_events: &[LanguageModelStreamEvent],
    available_tool_specs: &[AgentToolDescriptor],
) -> Vec<AgentToolCall> {
    let mut calls = Vec::new();
    for call in &response.tool_calls {
        push_native_agent_tool_call(&mut calls, call, available_tool_specs);
    }
    for item in &response.items {
        if let LanguageModelResponseItem::ToolCall { call } = item {
            push_native_agent_tool_call(&mut calls, call, available_tool_specs);
        }
    }
    for event in stream_events {
        if let LanguageModelStreamEvent::ToolCallCompleted {
            call_id,
            name,
            input,
        } = event
        {
            push_native_agent_tool_call(
                &mut calls,
                &LanguageModelToolCall {
                    call_id: call_id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                    summary: None,
                },
                available_tool_specs,
            );
        }
    }
    calls
}

fn push_native_agent_tool_call(
    calls: &mut Vec<AgentToolCall>,
    call: &LanguageModelToolCall,
    available_tool_specs: &[AgentToolDescriptor],
) {
    let input = normalize_native_tool_call_input(call, available_tool_specs);
    if calls
        .iter()
        .any(|existing| existing.tool == call.name && existing.input == input)
    {
        return;
    }
    calls.push(AgentToolCall {
        tool: call.name.clone(),
        input,
        r#async: false,
        summary: call
            .summary
            .clone()
            .unwrap_or_else(|| format!("Native model tool call `{}`.", call.name)),
    });
}

fn normalize_native_tool_call_input(
    call: &LanguageModelToolCall,
    available_tool_specs: &[AgentToolDescriptor],
) -> serde_json::Value {
    if !call.input.is_string() {
        return call.input.clone();
    }
    let Some(raw_input) = call.input.as_str() else {
        return call.input.clone();
    };
    let Some(raw_input_argument) = available_tool_specs
        .iter()
        .find(|descriptor| descriptor.spec.name == call.name)
        .and_then(native_tool_raw_input_argument)
    else {
        return call.input.clone();
    };
    serde_json::json!({ raw_input_argument: raw_input })
}

fn native_tool_raw_input_argument(descriptor: &AgentToolDescriptor) -> Option<&str> {
    descriptor
        .provider_specs
        .iter()
        .filter_map(|spec| spec.custom.as_ref())
        .find_map(|custom| custom.raw_input_argument.as_deref())
}

fn parse_agent_loop_decision_strict(raw: &str) -> Result<AgentLoopDecision, AgentError> {
    let trimmed = raw.trim();
    let json = trimmed
        .strip_prefix("```json")
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    let value = serde_json::from_str::<serde_json::Value>(json).map_err(|source| {
        AgentError::ModelResponse {
            detail: format!("expected JSON object decision, got invalid JSON: {source}: {raw}"),
        }
    })?;
    if let Ok(decision) = serde_json::from_value::<AgentLoopDecision>(value.clone()) {
        return Ok(decision);
    }
    let Some(text) = value.get("text").and_then(serde_json::Value::as_str) else {
        return Err(AgentError::ModelResponse {
            detail: format!("model decision JSON is missing required string field `text`: {raw}"),
        });
    };
    Ok(AgentLoopDecision {
        response_mode: value
            .get("response_mode")
            .and_then(serde_json::Value::as_str)
            .and_then(crate::parse_agent_loop_response_mode)
            .unwrap_or(crate::AgentLoopResponseMode::Answer),
        text: text.to_string(),
        tool_calls: value
            .get("tool_calls")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_default(),
        maid_handoff: value
            .get("maid_handoff")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok()),
        next_state: value
            .get("next_state")
            .and_then(serde_json::Value::as_str)
            .and_then(crate::parse_agent_interaction_state)
            .unwrap_or(crate::AgentInteractionState::Listening),
    })
}

fn model_repair_parse_error_detail(
    repair_attempt: usize,
    max_model_repair_attempts: Option<usize>,
    error: &AgentError,
) -> String {
    if let Some(max_model_repair_attempts) = max_model_repair_attempts {
        format!(
            "model response parse failed on attempt {} of {}: {error}",
            repair_attempt + 1,
            max_model_repair_attempts + 1,
        )
    } else {
        format!(
            "model response parse failed on attempt {} with no repair attempt limit: {error}",
            repair_attempt + 1,
        )
    }
}

fn repair_request_for_model_response(
    request: &LanguageModelRequest,
    previous_response: &str,
    parse_error: &str,
    repair_attempt: usize,
) -> Result<LanguageModelRequest, AgentError> {
    let mut request = request.clone();
    let previous_response = truncate_utf8_lossy(previous_response.as_bytes(), 8 * 1024);
    let content = serde_json::to_string_pretty(&serde_json::json!({
        "kind": "agent_model_response_repair",
        "repair_attempt": repair_attempt,
        "parse_error": parse_error,
        "previous_response": previous_response.text,
        "previous_response_truncated": previous_response.truncated,
        "required_response_shape": {
            "response_mode": "answer|acknowledge|clarify|defer|silent",
            "text": "short user-facing response",
            "tool_calls": [],
            "maid_handoff": null,
            "next_state": "speaking|listening|waiting|delegating|recovering|idle"
        },
        "instruction": "Return only a valid JSON object matching required_response_shape. Do not include markdown fences or explanatory text."
    }))
    .map_err(|source| AgentError::ModelRequest {
        detail: source.to_string(),
    })?;
    request.messages.push(LanguageModelMessage {
        role: LanguageModelRole::User,
        content,
    });
    if let Some(metadata) = request.metadata.as_object_mut() {
        metadata.insert(
            "model_response_repair_attempt".to_string(),
            serde_json::json!(repair_attempt),
        );
        metadata.insert(
            "model_response_parse_error".to_string(),
            serde_json::json!(parse_error),
        );
    } else {
        request.metadata = serde_json::json!({
            "model_response_repair_attempt": repair_attempt,
            "model_response_parse_error": parse_error,
        });
    }
    Ok(request)
}

fn patch_snapshot_item_from_tool_outcome(outcome: &AgentToolOutcome) -> Option<AgentItem> {
    let coding = coding_evidence_from_tool_outcome(outcome);
    if outcome.call.tool != "apply_patch"
        || (coding.patch_ref.is_none() && coding.changed_files.is_empty())
    {
        return None;
    }
    Some(AgentItem::PatchSnapshot {
        item_id: TaskId::generate(),
        step_index: outcome.step_index,
        call_id: outcome.call_id.clone(),
        status: outcome.status.clone(),
        summary: coding
            .diff_summary
            .clone()
            .unwrap_or_else(|| outcome.summary.clone()),
        patch_ref: coding.patch_ref.clone(),
        changed_files: coding.changed_files.clone(),
    })
}

fn verification_item_from_tool_outcome(outcome: &AgentToolOutcome) -> Option<AgentItem> {
    let coding = coding_evidence_from_tool_outcome(outcome);
    if coding.verification_state == AgentVerificationState::NotRun {
        return None;
    }
    Some(AgentItem::VerificationResult {
        item_id: TaskId::generate(),
        step_index: outcome.step_index,
        call_id: outcome.call_id.clone(),
        command: verification_command_from_tool_outcome(outcome),
        status: coding.verification_state,
        summary: outcome.summary.clone(),
    })
}

fn coding_evidence_from_tool_outcome(outcome: &AgentToolOutcome) -> AgentCodingToolEvidence {
    outcome
        .evidence
        .record(AGENT_CODING_EVIDENCE_KIND)
        .unwrap_or_default()
}

fn verification_command_from_tool_outcome(outcome: &AgentToolOutcome) -> String {
    if let Some(command) = outcome
        .call
        .input
        .get("command")
        .and_then(serde_json::Value::as_str)
    {
        return command.to_string();
    }
    let Some(program) = outcome
        .output
        .get("program")
        .and_then(serde_json::Value::as_str)
    else {
        return outcome.call.tool.clone();
    };
    let args = outcome
        .output
        .get("args")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if args.is_empty() {
        program.to_string()
    } else {
        format!("{program} {}", args.join(" "))
    }
}

fn build_candidate_evidence_bundle(
    evolution_context: Option<&AgentEvolutionContext>,
    final_decision: &AgentLoopDecision,
    items: &[AgentItem],
) -> Option<AgentCandidateEvidenceBundle> {
    let context = evolution_context?;
    let mut evidence = context.candidate_evidence.clone();
    evidence.extend(context.verification_results.iter().map(|result| {
        AgentCandidateEvidence::Verification {
            command: result.command.clone(),
            status: result.status.clone(),
            summary: result.summary.clone(),
        }
    }));
    for item in items {
        match item {
            AgentItem::PatchSnapshot {
                status,
                summary,
                patch_ref,
                changed_files,
                ..
            } => evidence.push(AgentCandidateEvidence::Patch {
                summary: format!("{status:?}: {summary}"),
                patch_ref: patch_ref.clone(),
                changed_files: changed_files.clone(),
            }),
            AgentItem::VerificationResult {
                command,
                status,
                summary,
                ..
            } => evidence.push(AgentCandidateEvidence::Verification {
                command: command.clone(),
                status: status.clone(),
                summary: summary.clone(),
            }),
            _ => {}
        }
    }

    Some(AgentCandidateEvidenceBundle {
        bundle_id: TaskId::generate(),
        job_id: context
            .job
            .as_ref()
            .map(|job| job.job_id.clone())
            .or_else(|| {
                context
                    .candidate_bundle
                    .as_ref()
                    .map(|manifest| manifest.job_id.clone())
            })
            .or_else(|| {
                context
                    .mutation_packet
                    .as_ref()
                    .map(|packet| packet.job_id.clone())
            }),
        mutation_packet_job_id: context
            .mutation_packet
            .as_ref()
            .map(|packet| packet.job_id.clone()),
        candidate_bundle_path: context
            .candidate_bundle
            .as_ref()
            .map(|manifest| manifest.bundle_path.clone()),
        final_response: final_decision.text.clone(),
        evidence,
    })
}

fn agent_event(
    session: &InteractionSession,
    kind: &str,
    salience: f32,
    payload: serde_json::Value,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        time::OffsetDateTime::now_utc(),
        session.node_id.clone(),
        Source::new("agent.loop").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(salience).expect("valid salience"),
        Confidence::new(0.95).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session.session_id.clone())
    .with_payload(payload)
}

fn caused_by_latest_event(mut event: EventEnvelope, input: &AgentLoopInput) -> EventEnvelope {
    event
        .links
        .caused_by
        .push(input.latest_event.event_id.clone());
    event
}

struct TruncatedText {
    text: String,
    truncated: bool,
}

fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> TruncatedText {
    let truncated = bytes.len() > max_bytes;
    TruncatedText {
        text: String::from_utf8_lossy(&bytes[..bytes.len().min(max_bytes)]).to_string(),
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentTool, AgentToolArtifactRef, AgentToolKind, AgentToolRequest, AgentToolResult,
        AgentToolSpec, InteractionSurface, InteractionSurfaceKind, LocalWorkspaceAgentConfig,
        default_agent_tool_descriptors, default_coding_agent_tools, user_interaction_input_event,
    };
    use sinomo_lm::LanguageModelError;
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::NodeId;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct SequencedModel {
        responses: Mutex<VecDeque<String>>,
        requests: Mutex<Vec<LanguageModelRequest>>,
    }

    impl SequencedModel {
        fn new(responses: impl IntoIterator<Item = impl Into<String>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().map(Into::into).collect()),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl InteractionModel for SequencedModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            let response = self
                .responses
                .lock()
                .expect("responses")
                .pop_front()
                .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                    provider: "fake".to_string(),
                    model: "fake".to_string(),
                    detail: "no fake response queued".to_string(),
                })?;
            Ok(LanguageModelResponse {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                content: response,
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }
    }

    struct StreamingModel {
        response: String,
        deltas: Vec<String>,
        requests: Mutex<Vec<LanguageModelRequest>>,
    }

    impl StreamingModel {
        fn new(
            response: impl Into<String>,
            deltas: impl IntoIterator<Item = impl Into<String>>,
        ) -> Self {
            Self {
                response: response.into(),
                deltas: deltas.into_iter().map(Into::into).collect(),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl InteractionModel for StreamingModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.complete_streaming(request, &mut |_| {})
        }

        fn complete_streaming(
            &self,
            request: &LanguageModelRequest,
            on_delta: &mut dyn FnMut(&str),
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            for delta in &self.deltas {
                on_delta(delta);
            }
            Ok(LanguageModelResponse {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                content: self.response.clone(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }
    }

    struct EventfulModel {
        turns: Mutex<VecDeque<(String, Vec<LanguageModelStreamEvent>)>>,
    }

    impl EventfulModel {
        fn new(
            turns: impl IntoIterator<Item = (impl Into<String>, Vec<LanguageModelStreamEvent>)>,
        ) -> Self {
            Self {
                turns: Mutex::new(
                    turns
                        .into_iter()
                        .map(|(response, events)| (response.into(), events))
                        .collect(),
                ),
            }
        }
    }

    impl InteractionModel for EventfulModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.complete_streaming_events(request, &mut |_| {})
        }

        fn complete_streaming_events(
            &self,
            _request: &LanguageModelRequest,
            on_event: &mut dyn FnMut(LanguageModelStreamEvent),
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            let (response, events) =
                self.turns
                    .lock()
                    .expect("turns")
                    .pop_front()
                    .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                        provider: "fake".to_string(),
                        model: "fake".to_string(),
                        detail: "no fake response queued".to_string(),
                    })?;
            for event in events {
                on_event(event);
            }
            Ok(LanguageModelResponse {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                content: response,
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }
    }

    struct EchoTool;

    impl AgentTool for EchoTool {
        fn descriptor(&self) -> AgentToolDescriptor {
            AgentToolDescriptor::new(AgentToolSpec::new(
                AgentToolKind::ReadFile,
                "read_file",
                "read a fake file",
                false,
            ))
            .with_capabilities([AgentToolCapability::ReadWorkspace])
        }

        fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
            AgentToolResult::succeeded(
                "fake file read",
                serde_json::json!({
                    "call_id": request.call_id,
                    "input": request.call.input,
                    "content": "workspace evidence"
                }),
            )
        }
    }

    struct ReplacementReadTool;

    impl AgentTool for ReplacementReadTool {
        fn descriptor(&self) -> AgentToolDescriptor {
            AgentToolDescriptor::new(AgentToolSpec::new(
                AgentToolKind::ReadFile,
                "read_file",
                "read a replacement fake file",
                false,
            ))
            .with_capabilities([AgentToolCapability::ReadWorkspace])
        }

        fn execute(&self, _request: AgentToolRequest<'_>) -> AgentToolResult {
            AgentToolResult::succeeded(
                "replacement file read",
                serde_json::json!({ "content": "replacement evidence" }),
            )
        }
    }

    fn agent_input(prompt: &str) -> AgentLoopInput {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_agent").expect("session"),
            NodeId::from_legacy_named("node_agent").expect("node"),
            InteractionSurface::new("surface.agent", InteractionSurfaceKind::CommandLine),
        );
        let latest_event = user_interaction_input_event(&session, prompt);
        AgentLoopInput {
            session,
            latest_event,
            context: crate::BoundedContext {
                session_id: SessionId::from_legacy_named("sess_agent").expect("session"),
                events: Vec::new(),
                packet: sinomo_runtime::core::ContextPacket::default(),
            },
            async_update: None,
            evolution_context: None,
            tool_specs: default_coding_agent_tools(),
        }
    }

    fn coding_evidence(evidence: &AgentToolEvidence) -> AgentCodingToolEvidence {
        evidence
            .record(AGENT_CODING_EVIDENCE_KIND)
            .unwrap_or_default()
    }

    #[test]
    fn agent_config_defaults_to_unlimited_runtime_limits() {
        let config = AgentConfig::new("fake", "fake");

        assert_eq!(config.max_steps, None);
        assert_eq!(config.max_tool_calls_per_step, None);
        assert_eq!(config.max_model_repair_attempts, None);
        assert_eq!(config.max_output_tokens, None);
        assert_eq!(config.prompt_reference_max_bytes, None);
        assert_eq!(config.prompt_reference_max_entries, None);
        assert_eq!(config.context_budget.max_context_bytes, None);
        assert_eq!(config.context_budget.compaction_threshold_bytes(), None);
    }

    #[test]
    fn context_budget_can_be_tightened_from_loaded_model_metadata() {
        let large_context =
            AgentContextBudget::for_loaded_model("openai", "custom-1m", Some(1_200));

        let max_context_bytes = large_context.max_context_bytes.expect("context bytes");
        assert!(max_context_bytes > 500 * 1024);
        assert_eq!(
            large_context.compaction_threshold_ratio,
            Some(DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO)
        );
        assert_eq!(
            large_context.compaction_threshold_bytes(),
            Some(
                (max_context_bytes as f32 * DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO).round()
                    as usize
            )
        );

        let custom = AgentContextBudget {
            max_context_bytes: Some(10_000),
            compaction_threshold_ratio: Some(0.5),
            retained_tail_items: Some(8),
        };
        assert_eq!(custom.compaction_threshold_bytes(), Some(5_000));
    }

    #[test]
    fn tool_evidence_records_support_generic_typed_payloads() {
        let evidence = AgentToolEvidence::policy_only(true, "available")
            .with_record(
                AGENT_CODING_EVIDENCE_KIND,
                &AgentCodingToolEvidence {
                    changed_files: vec!["hello.txt".to_string()],
                    ..AgentCodingToolEvidence::default()
                },
            )
            .with_record_value("quota", serde_json::json!({ "remaining": 3 }))
            .with_record_value("quota", serde_json::json!({ "remaining": 2 }));

        assert_eq!(evidence.records.len(), 2);
        assert_eq!(
            coding_evidence(&evidence).changed_files,
            vec!["hello.txt".to_string()]
        );
        assert_eq!(
            evidence
                .record_value("quota")
                .and_then(|record| record.get("remaining"))
                .and_then(serde_json::Value::as_i64),
            Some(2)
        );
    }

    #[test]
    fn agent_executes_tool_calls_and_continues_until_final_answer() {
        let model = SequencedModel::new([
            r#"{
                "response_mode": "defer",
                "text": "I will inspect the file.",
                "tool_calls": [{
                    "tool": "read_file",
                    "input": {"path": "Cargo.toml"},
                    "async": false,
                    "summary": "Inspect workspace metadata."
                }],
                "maid_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The workspace evidence is available.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let agent = Agent::new(model, "fake", "fake")
            .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

        let run = agent.run(&agent_input("inspect the repo")).expect("run");

        assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
        assert_eq!(
            run.final_decision.text,
            "The workspace evidence is available."
        );
        assert_eq!(run.model_turns.len(), 2);
        assert_eq!(run.tool_results.len(), 1);
        assert_eq!(run.tool_results[0].status, AgentToolStatus::Succeeded);
        assert!(
            agent.model.requests.lock().expect("requests")[1]
                .messages
                .iter()
                .any(|message| message.content.contains("tool_results"))
        );
    }

    #[test]
    fn agent_records_context_items_model_events_and_tool_evidence() {
        let model = SequencedModel::new([
            r#"{
                "response_mode": "defer",
                "text": "I will inspect the file.",
                "tool_calls": [{
                    "tool": "read_file",
                    "input": {"path": "Cargo.toml"},
                    "async": false,
                    "summary": "Inspect workspace metadata."
                }],
                "maid_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The workspace evidence is available.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let agent = Agent::new(model, "fake", "fake")
            .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

        let run = agent.run(&agent_input("inspect the repo")).expect("run");

        assert_eq!(run.context_reports.len(), 2);
        assert_eq!(run.model_turns[0].events.len(), 5);
        assert!(run.model_events.iter().any(|event| {
            matches!(
                event,
                AgentModelEvent::ToolCallCompleted { call, .. }
                    if call.tool == "read_file"
            )
        }));
        assert!(
            run.items
                .iter()
                .any(|item| matches!(item, AgentItem::ContextInjection { .. }))
        );
        assert!(run.items.iter().any(
            |item| matches!(item, AgentItem::ToolResult { evidence, .. } if evidence.policy.allowed)
        ));
        assert!(run.tool_results[0].evidence.policy.allowed);
        assert!(
            agent.model.requests.lock().expect("requests")[0].metadata["context_report_id"]
                .as_str()
                .is_some()
        );
    }

    #[test]
    fn agent_records_streaming_text_deltas_when_model_supports_them() {
        let response = r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "maid_handoff": null,
            "next_state": "listening"
        }"#;
        let agent = Agent::new(
            StreamingModel::new(response, ["chunk-one", "chunk-two"]),
            "fake",
            "fake",
        );

        let run = agent.run(&agent_input("stream")).expect("run");

        let deltas: Vec<_> = run
            .model_events
            .iter()
            .filter_map(|event| match event {
                AgentModelEvent::AssistantTextDelta { delta, .. } => Some(delta.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(deltas, vec!["chunk-one", "chunk-two"]);
        assert!(
            !deltas
                .iter()
                .any(|delta| delta.contains("\"response_mode\""))
        );
    }

    #[test]
    fn agent_records_provider_stream_reasoning_tool_usage_and_finish_events() {
        let first_response = r#"{
            "response_mode": "defer",
            "text": "I will inspect the file.",
            "tool_calls": [{
                "tool": "read_file",
                "input": {"path": "Cargo.toml"},
                "async": false,
                "summary": "Inspect workspace metadata."
            }],
            "maid_handoff": null,
            "next_state": "waiting"
        }"#;
        let final_response = r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "maid_handoff": null,
            "next_state": "listening"
        }"#;
        let model = EventfulModel::new([
            (
                first_response,
                vec![
                    LanguageModelStreamEvent::Started {
                        provider: "fake".to_string(),
                        model: "fake".to_string(),
                    },
                    LanguageModelStreamEvent::ReasoningSummary {
                        summary: "checking workspace".to_string(),
                    },
                    LanguageModelStreamEvent::ToolCallStarted {
                        call_id: Some("call_1".to_string()),
                        name: "read_file".to_string(),
                    },
                    LanguageModelStreamEvent::ToolCallDelta {
                        call_id: Some("call_1".to_string()),
                        name: Some("read_file".to_string()),
                        arguments_delta: "{\"path\":\"Cargo.toml\"}".to_string(),
                    },
                    LanguageModelStreamEvent::ToolCallCompleted {
                        call_id: Some("call_1".to_string()),
                        name: "read_file".to_string(),
                        input: serde_json::json!({"path": "Cargo.toml"}),
                    },
                    LanguageModelStreamEvent::Usage {
                        usage: sinomo_lm::LanguageModelUsage {
                            input_tokens: Some(5),
                            output_tokens: Some(7),
                        },
                    },
                    LanguageModelStreamEvent::Finished {
                        finish_reason: Some("tool_calls".to_string()),
                    },
                ],
            ),
            (
                final_response,
                vec![LanguageModelStreamEvent::Finished {
                    finish_reason: Some("stop".to_string()),
                }],
            ),
        ]);
        let agent = Agent::new(model, "fake", "fake")
            .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

        let run = agent.run(&agent_input("inspect")).expect("run");

        assert!(run.model_events.iter().any(|event| {
            matches!(
                event,
                AgentModelEvent::AssistantReasoningSummary { summary, .. }
                    if summary == "checking workspace"
            )
        }));
        assert!(run.model_events.iter().any(|event| {
            matches!(
                event,
                AgentModelEvent::ToolCallDelta { arguments_delta, .. }
                    if arguments_delta.contains("Cargo.toml")
            )
        }));
        assert!(run.model_events.iter().any(|event| {
            matches!(
                event,
                AgentModelEvent::Usage { usage: Some(usage), .. }
                    if usage.input_tokens == Some(5)
            )
        }));
        assert!(run.model_events.iter().any(|event| {
            matches!(
                event,
                AgentModelEvent::Finished {
                    finish_reason: Some(reason),
                    ..
                } if reason == "tool_calls"
            )
        }));
    }

    #[test]
    fn agent_consumes_native_stream_tool_calls_without_json_decision_text() {
        let final_response = r#"{
            "response_mode": "answer",
            "text": "The native tool call completed.",
            "tool_calls": [],
            "maid_handoff": null,
            "next_state": "listening"
        }"#;
        let model = EventfulModel::new([
            (
                "",
                vec![
                    LanguageModelStreamEvent::Started {
                        provider: "fake".to_string(),
                        model: "fake".to_string(),
                    },
                    LanguageModelStreamEvent::ToolCallCompleted {
                        call_id: Some("call_native_1".to_string()),
                        name: "read_file".to_string(),
                        input: serde_json::json!({"path": "Cargo.toml"}),
                    },
                    LanguageModelStreamEvent::Finished {
                        finish_reason: Some("tool_calls".to_string()),
                    },
                ],
            ),
            (
                final_response,
                vec![LanguageModelStreamEvent::Finished {
                    finish_reason: Some("stop".to_string()),
                }],
            ),
        ]);
        let agent = Agent::new(model, "fake", "fake")
            .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

        let run = agent.run(&agent_input("inspect natively")).expect("run");

        assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
        assert_eq!(run.tool_results.len(), 1);
        assert_eq!(run.tool_results[0].call.tool, "read_file");
        assert_eq!(run.tool_results[0].status, AgentToolStatus::Succeeded);
        assert_eq!(run.final_decision.text, "The native tool call completed.");
        assert!(run.model_events.iter().any(|event| {
            matches!(
                event,
                AgentModelEvent::ToolCallCompleted {
                    call_id: Some(call_id),
                    call,
                    ..
                } if call_id == "call_native_1" && call.tool == "read_file"
            )
        }));
    }

    #[test]
    fn file_trace_store_records_run_evidence_as_jsonl() {
        let trace_path =
            std::env::temp_dir().join(format!("sinomo-agent-trace-{}.jsonl", TaskId::generate()));
        let model = SequencedModel::new([
            r#"{
                "response_mode": "defer",
                "text": "I will inspect the file.",
                "tool_calls": [{
                    "tool": "read_file",
                    "input": {"path": "Cargo.toml"},
                    "async": false,
                    "summary": "Inspect workspace metadata."
                }],
                "maid_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "Done.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let agent = Agent::new(model, "fake", "fake")
            .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool))
            .with_trace_store(FileAgentTraceStore::new(&trace_path));

        agent.run(&agent_input("inspect the repo")).expect("run");

        let records: Vec<AgentTraceRecord> = fs::read_to_string(&trace_path)
            .expect("trace")
            .lines()
            .map(|line| serde_json::from_str(line).expect("trace record"))
            .collect();

        assert!(
            records
                .iter()
                .any(|record| { matches!(record.record, AgentTraceRecordKind::RunStarted { .. }) })
        );
        assert!(
            records.iter().any(|record| {
                matches!(record.record, AgentTraceRecordKind::ContextReport { .. })
            })
        );
        assert!(
            records
                .iter()
                .any(|record| { matches!(record.record, AgentTraceRecordKind::ModelEvent { .. }) })
        );
        assert!(
            records.iter().any(|record| {
                matches!(record.record, AgentTraceRecordKind::ToolOutcome { .. })
            })
        );
        assert!(records.iter().any(|record| {
            matches!(
                record.record,
                AgentTraceRecordKind::Item {
                    item: AgentItem::ToolResult { .. }
                }
            )
        }));
        assert!(
            records.iter().any(|record| {
                matches!(record.record, AgentTraceRecordKind::RunCompleted { .. })
            })
        );
    }

    #[test]
    fn agent_storage_paths_use_sinomo_data_dir_layout() {
        let data_dir = SinomoDataDir::new(
            std::env::temp_dir().join(format!("sinomo-agent-storage-{}", TaskId::generate())),
        );
        let session_id = SessionId::from_legacy_named("sess_agent_storage").expect("session");
        let paths = AgentStoragePaths::from_data_dir(&data_dir);

        assert_eq!(paths.root, data_dir.root().join("events").join("agent"));
        assert_eq!(paths.sessions_dir, paths.root.join("sessions"));
        assert_eq!(paths.traces_dir, paths.root.join("traces"));
        assert_eq!(paths.evidence_dir, paths.root.join("evidence"));
        assert_eq!(
            paths.session_dir_for_session(&session_id),
            paths.sessions_dir.join(session_id.as_str())
        );
        assert_eq!(
            paths.session_snapshot_path_for_session(&session_id),
            paths
                .sessions_dir
                .join(session_id.as_str())
                .join("snapshot.json")
        );
        assert_eq!(
            paths.session_events_path_for_session(&session_id),
            paths
                .sessions_dir
                .join(session_id.as_str())
                .join("events.jsonl")
        );
        assert_eq!(
            paths.session_items_path_for_session(&session_id),
            paths
                .sessions_dir
                .join(session_id.as_str())
                .join("items.jsonl")
        );
        assert_eq!(
            paths.session_projections_path_for_session(&session_id),
            paths
                .sessions_dir
                .join(session_id.as_str())
                .join("projections.jsonl")
        );
        assert_eq!(
            paths.trace_path_for_session(&session_id),
            paths
                .traces_dir
                .join(format!("{}.jsonl", session_id.as_str()))
        );
        assert_eq!(
            paths.evidence_dir_for_session(&session_id),
            paths.evidence_dir.join(session_id.as_str())
        );
    }

    #[test]
    fn file_trace_store_can_write_to_agent_data_dir_layout() {
        let data_dir = SinomoDataDir::new(
            std::env::temp_dir().join(format!("sinomo-agent-trace-layout-{}", TaskId::generate())),
        );
        let session_id = SessionId::from_legacy_named("sess_agent_trace").expect("session");
        let store = FileAgentTraceStore::for_data_dir_session(&data_dir, &session_id);

        store
            .append(&AgentTraceRecord::new(
                session_id.clone(),
                AgentTraceRecordKind::RunStarted {
                    provider: "fake".to_string(),
                    model: "fake".to_string(),
                    max_steps: Some(1),
                },
            ))
            .expect("append trace");

        let paths = AgentStoragePaths::from_data_dir(&data_dir);
        assert_eq!(
            store.path(),
            paths.trace_path_for_session(&session_id).as_path()
        );
        assert!(
            fs::read_to_string(paths.trace_path_for_session(&session_id))
                .expect("trace")
                .contains("\"run_started\"")
        );
    }

    #[test]
    fn file_session_store_persists_and_reloads_session_snapshot() {
        let data_dir = SinomoDataDir::new(
            std::env::temp_dir().join(format!("sinomo-agent-session-{}", TaskId::generate())),
        );
        let session_id = SessionId::from_legacy_named("sess_agent_store").expect("session");
        let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
        let mut session = AgentSession::new(session_id.clone(), AgentRole::Deliberation);
        session.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "hello".to_string(),
            event_id: None,
        });

        store.save_session(&session).expect("save session");
        let loaded = store
            .load_session(&session_id)
            .expect("load session")
            .expect("saved session");
        assert_eq!(loaded, session);

        session
            .steering_queue
            .push(AgentSteeringMessage::new("prefer concise answers"));
        store.save_session(&session).expect("replace session");
        let loaded = store
            .load_session(&session_id)
            .expect("load replaced session")
            .expect("saved session");
        assert_eq!(loaded.steering_queue, session.steering_queue);

        let paths = AgentStoragePaths::from_data_dir(&data_dir);
        let events = fs::read_to_string(paths.session_events_path_for_session(&session_id))
            .expect("session events");
        assert!(events.contains("\"snapshot_saved\""));
        assert!(
            fs::read_to_string(paths.session_snapshot_path_for_session(&session_id))
                .expect("snapshot")
                .contains("\"schema_version\"")
        );
    }

    #[test]
    fn agent_session_store_records_turns_and_queue_updates() {
        let data_dir = SinomoDataDir::new(
            std::env::temp_dir().join(format!("sinomo-agent-session-run-{}", TaskId::generate())),
        );
        let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "maid_handoff": null,
            "next_state": "listening"
        }"#]);
        let agent = Agent::new(model, "fake", "fake").with_session_store(store.clone());
        let input = agent_input("persist this turn");
        let mut session =
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

        let run = agent.start_turn(&mut session, &input).expect("run");
        assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
        agent
            .steer(&mut session, "carry this into the next turn")
            .expect("steer");

        let loaded = store
            .load_session(&input.session.session_id)
            .expect("load session")
            .expect("saved session");
        assert_eq!(loaded.transcript.items, session.transcript.items);
        assert_eq!(loaded.steering_queue, session.steering_queue);

        let paths = AgentStoragePaths::from_data_dir(&data_dir);
        let events =
            fs::read_to_string(paths.session_events_path_for_session(&input.session.session_id))
                .expect("session events");
        assert!(events.contains("\"turn_started\""));
        assert!(events.contains("\"turn_completed\""));
        assert!(events.contains("\"steering_queued\""));
        assert!(events.contains("\"snapshot_saved\""));
    }

    #[test]
    fn explicit_compaction_preserves_raw_history_and_records_working_state() {
        let model = SequencedModel::new(Vec::<String>::new());
        let agent = Agent::new(model, "fake", "fake").with_compactor(AgentCompactor {
            retained_tail_count: Some(1),
        });
        let session_id = SessionId::from_legacy_named("sess_agent_projection").expect("session");
        let mut session = AgentSession::new(session_id, AgentRole::Deliberation);
        session.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "old request".to_string(),
            event_id: None,
        });
        session.transcript.push(AgentItem::AssistantText {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "old answer".to_string(),
        });
        session.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 1,
            content: "current work request".to_string(),
            event_id: None,
        });

        let summary = agent
            .compact(&mut session, "trim old context")
            .expect("compact");

        assert_eq!(summary.projection.source_item_count, 3);
        assert_eq!(summary.projection.retained_item_ids.len(), 1);
        assert_eq!(
            summary
                .projection
                .working_state
                .current_user_request
                .as_deref(),
            Some("current work request")
        );
        assert_eq!(session.projection, Some(summary.projection.clone()));
        assert_eq!(session.raw_transcript.items.len(), 4);
        assert!(session.raw_transcript.items.iter().any(|item| {
            matches!(item, AgentItem::UserMessage { content, .. } if content == "old request")
        }));
        assert_eq!(session.transcript.items.len(), 2);
        assert!(
            session
                .transcript
                .items
                .first()
                .is_some_and(|item| matches!(item, AgentItem::CompactionSummary { .. }))
        );
    }

    #[test]
    fn compaction_carries_prior_projection_working_state_forward() {
        let compactor = AgentCompactor {
            retained_tail_count: Some(1),
        };
        let mut first_transcript = AgentTranscript::new();
        first_transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "long running objective".to_string(),
            event_id: None,
        });
        let first = compactor.compact_retaining_tail_count_with_session_state(
            &first_transcript,
            "first compaction",
            100,
            1,
            None,
            &[],
            &[],
        );
        let mut second_transcript = AgentTranscript::new();
        second_transcript.push(AgentItem::AssistantText {
            item_id: TaskId::generate(),
            step_index: 1,
            content: "still working".to_string(),
        });

        let second = compactor.compact_retaining_tail_count_with_session_state(
            &second_transcript,
            "second compaction",
            100,
            1,
            Some(&first.projection),
            &[],
            &[],
        );

        assert_eq!(
            second.projection.previous_projection_id,
            Some(first.projection.projection_id.clone())
        );
        assert_eq!(
            second.projection.working_state.active_objective.as_deref(),
            Some("long running objective")
        );
        assert_eq!(
            second
                .projection
                .working_state
                .current_user_request
                .as_deref(),
            Some("long running objective")
        );
        assert!(
            second
                .projection
                .working_state
                .decisions
                .contains(&"still working".to_string())
        );
    }

    #[test]
    fn file_session_store_records_items_and_projections_append_only() {
        let data_dir = SinomoDataDir::new(std::env::temp_dir().join(format!(
            "sinomo-agent-session-projection-{}",
            TaskId::generate()
        )));
        let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "maid_handoff": null,
            "next_state": "listening"
        }"#]);
        let agent = Agent::new(model, "fake", "fake")
            .with_session_store(store.clone())
            .with_compactor(AgentCompactor {
                retained_tail_count: Some(1),
            });
        let input = agent_input("persist projection");
        let mut session =
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

        agent.start_turn(&mut session, &input).expect("run");
        agent
            .compact(&mut session, "persist compacted projection")
            .expect("compact");

        let loaded = store
            .load_session(&input.session.session_id)
            .expect("load session")
            .expect("saved session");
        assert!(loaded.projection.is_some());
        assert!(
            loaded.raw_transcript.items.len() > loaded.transcript.items.len(),
            "raw transcript should retain pre-compaction history"
        );

        let paths = AgentStoragePaths::from_data_dir(&data_dir);
        let events =
            fs::read_to_string(paths.session_events_path_for_session(&input.session.session_id))
                .expect("session events");
        let items =
            fs::read_to_string(paths.session_items_path_for_session(&input.session.session_id))
                .expect("session items");
        let projections = fs::read_to_string(
            paths.session_projections_path_for_session(&input.session.session_id),
        )
        .expect("session projections");
        assert!(events.contains("\"items_appended\""));
        assert!(events.contains("\"projection_saved\""));
        assert!(items.contains("\"compaction_summary\""));
        assert!(projections.contains("\"working_state\""));
    }

    #[test]
    fn agent_storage_paths_are_registered_in_storage_contract() {
        let mut layout = sinomo_runtime::storage::sinomo_data_dir_layout().expect("layout");
        crate::register_storage_paths(&mut layout).expect("register agent storage");

        let owner = layout
            .owner_for(
                PathBuf::from("events")
                    .join("agent")
                    .join("evidence")
                    .join("session")
                    .join("read-file.txt"),
            )
            .expect("owner");
        match owner {
            sinomo_runtime::storage::StoragePathOwner::Escape(escape) => {
                assert_eq!(escape.module, "sinomo-agent");
                assert_eq!(escape.ring, sinomo_runtime::storage::StorageRing::R2);
            }
            sinomo_runtime::storage::StoragePathOwner::File(_) => {
                panic!("agent storage should be an escape path")
            }
        }
    }

    #[test]
    fn unknown_tool_is_returned_as_model_visible_failure() {
        let model = SequencedModel::new([
            r#"{
                "response_mode": "defer",
                "text": "I will call a tool.",
                "tool_calls": [{
                    "tool": "missing_tool",
                    "input": {},
                    "async": false,
                    "summary": "Try a missing tool."
                }],
                "maid_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The requested tool is not available.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let agent = Agent::new(model, "fake", "fake");

        let run = agent.run(&agent_input("use a tool")).expect("run");

        assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
        assert_eq!(run.tool_results[0].status, AgentToolStatus::Failed);
        assert!(run.tool_results[0].summary.contains("unknown tool"));
        assert!(
            agent.model.requests.lock().expect("requests")[1]
                .messages
                .iter()
                .any(|message| message.content.contains("unknown_tool"))
        );
    }

    #[test]
    fn agent_stops_at_configured_step_limit() {
        let model = SequencedModel::new([r#"{
            "response_mode": "defer",
            "text": "I need one more tool.",
            "tool_calls": [{
                "tool": "read_file",
                "input": {"path": "Cargo.toml"},
                "async": false,
                "summary": "Inspect workspace metadata."
            }],
            "maid_handoff": null,
            "next_state": "waiting"
        }"#]);
        let config = AgentConfig::new("fake", "fake").with_max_steps(1);
        let agent = Agent::new(model, "fake", "fake")
            .with_config(config)
            .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

        let run = agent.run(&agent_input("inspect once")).expect("run");

        assert_eq!(run.stop_reason, AgentStopReason::MaxStepsReached);
        assert_eq!(run.model_turns.len(), 1);
        assert_eq!(run.tool_results.len(), 1);
    }

    #[test]
    fn agent_repairs_malformed_model_response_once() {
        let model = SequencedModel::new([
            "not json",
            r#"{
                "response_mode": "answer",
                "text": "Recovered.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let agent = Agent::new(model, "fake", "fake");

        let run = agent.run(&agent_input("recover")).expect("run");

        assert_eq!(run.final_decision.text, "Recovered.");
        assert_eq!(agent.model.requests.lock().expect("requests").len(), 2);
        assert!(
            agent.model.requests.lock().expect("requests")[1]
                .messages
                .last()
                .expect("repair message")
                .content
                .contains("agent_model_response_repair")
        );
        assert!(
            run.items
                .iter()
                .any(|item| matches!(item, AgentItem::Error { detail, .. } if detail.contains("invalid JSON")))
        );
        assert!(
            run.model_events
                .iter()
                .any(|event| matches!(event, AgentModelEvent::Error { detail, .. } if detail.contains("invalid JSON")))
        );
    }

    #[test]
    fn agent_returns_error_after_model_repair_budget_is_exhausted() {
        let model = SequencedModel::new(["not json", "still not json"]);
        let config = AgentConfig::new("fake", "fake").with_max_model_repair_attempts(1);
        let agent = Agent::new(model, "fake", "fake").with_config(config);

        let error = agent.run(&agent_input("recover")).expect_err("parse error");

        match error {
            AgentError::ModelResponse { detail } => {
                assert!(detail.contains("attempt 2 of 2"));
                assert!(detail.contains("invalid JSON"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(agent.model.requests.lock().expect("requests").len(), 2);
    }

    #[test]
    fn descriptor_only_registry_blocks_execution_without_losing_contract() {
        let registry = AgentToolRegistry::descriptor_only(default_agent_tool_descriptors());
        let input = agent_input("read");
        let call = AgentToolCall {
            tool: "read_file".to_string(),
            input: serde_json::json!({"path": "Cargo.toml"}),
            r#async: false,
            summary: "read".to_string(),
        };

        let outcome = registry.execute(&input.session, 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Blocked);
        assert_eq!(
            outcome.output.get("error").and_then(|value| value.as_str()),
            Some("missing_executor")
        );
    }

    #[test]
    fn tool_registry_supports_runtime_registration_replacement_and_removal() {
        let registry = AgentToolRegistry::new();
        let input = agent_input("read");
        let call = AgentToolCall {
            tool: "read_file".to_string(),
            input: serde_json::json!({"path": "Cargo.toml"}),
            r#async: false,
            summary: "read".to_string(),
        };

        assert!(registry.is_empty());
        assert!(registry.register(EchoTool).is_none());
        assert!(registry.contains("read_file"));
        assert_eq!(registry.len(), 1);
        let first = registry.execute(&input.session, 0, 0, &call);
        assert_eq!(
            first
                .output
                .get("content")
                .and_then(serde_json::Value::as_str),
            Some("workspace evidence")
        );

        let replaced = registry
            .register(ReplacementReadTool)
            .expect("replaced descriptor");
        assert_eq!(replaced.spec.name, "read_file");
        assert_eq!(registry.len(), 1);
        let second = registry.execute(&input.session, 0, 0, &call);
        assert_eq!(
            second
                .output
                .get("content")
                .and_then(serde_json::Value::as_str),
            Some("replacement evidence")
        );

        let removed = registry.remove("read_file").expect("removed descriptor");
        assert_eq!(removed.spec.name, "read_file");
        assert!(!registry.contains("read_file"));
        let missing = registry.execute(&input.session, 0, 0, &call);
        assert_eq!(missing.status, AgentToolStatus::Failed);
        assert_eq!(
            missing
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("unknown_tool")
        );
    }

    #[test]
    fn render_tool_results_keeps_large_evidence_out_of_model_context() {
        let outcome = AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index: 3,
            call_index: 0,
            call: AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": "*** Begin Patch\n*** End Patch" }),
                r#async: false,
                summary: "patch".to_string(),
            },
            status: AgentToolStatus::Succeeded,
            summary: "Success. Updated the following files:\nM hello.txt".to_string(),
            output: serde_json::json!({
                "changed_files": ["hello.txt"],
                "diff": {
                    "files": [{
                        "status": "M",
                        "path": "hello.txt",
                        "added_lines": 1,
                        "removed_lines": 1,
                        "diff": "large diff body that should not return to the model"
                    }],
                    "unified": "large unified diff body that should not return to the model"
                }
            }),
            event_id: None,
            evidence: AgentToolEvidence::policy_only(true, "allowed")
                .with_artifact(AgentToolArtifactRef::new(
                    "diff",
                    "/tmp/sinomo-agent/evidence/apply-patch.diff",
                ))
                .with_record(
                    AGENT_CODING_EVIDENCE_KIND,
                    &AgentCodingToolEvidence {
                        patch_ref: Some("/tmp/sinomo-agent/evidence/apply-patch.patch".to_string()),
                        diff_ref: Some("/tmp/sinomo-agent/evidence/apply-patch.diff".to_string()),
                        diff_summary: Some("M hello.txt".to_string()),
                        changed_files: vec!["hello.txt".to_string()],
                        verification_state: AgentVerificationState::NotRun,
                    },
                )
                .with_record_value(
                    "patch_diff",
                    serde_json::json!({
                        "files": [{
                            "status": "M",
                            "path": "hello.txt",
                            "added_lines": 1,
                            "removed_lines": 1,
                            "diff": "large patch evidence diff that should not return to the model"
                        }]
                    }),
                )
                .with_metadata(serde_json::json!({
                    "workspace_root": "/tmp/sinomo-agent/workspace"
                })),
        };

        let rendered = render_tool_results(3, &[outcome]).expect("render");

        assert!(rendered.contains("\"hello.txt\""));
        assert!(rendered.contains("\"content_omitted\": true"));
        assert!(rendered.contains("\"uri_omitted\": true"));
        assert!(rendered.contains("\"metadata_omitted\""));
        assert!(
            !rendered.contains("/tmp/sinomo-agent/evidence"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("/tmp/sinomo-agent/workspace"),
            "{rendered}"
        );
        assert!(!rendered.contains("large diff body"));
        assert!(!rendered.contains("large unified diff body"));
        assert!(!rendered.contains("large patch evidence diff"));
    }

    #[test]
    fn tool_planner_composes_runtime_and_session_descriptors() {
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            "fake",
            "fake",
        )
        .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));
        let mut input = agent_input("inspect with composed tools");
        input.tool_specs = vec![AgentToolSpec::new(
            AgentToolKind::SearchFiles,
            "search",
            "session-visible search tool",
            false,
        )];

        let context = agent.assemble_context(&input, &[], 0).expect("context");
        let visible_tools: Vec<_> = context
            .available_tool_specs
            .iter()
            .map(|descriptor| descriptor.spec.name.as_str())
            .collect();

        assert!(visible_tools.contains(&"read_file"));
        assert!(visible_tools.contains(&"search"));
        assert_eq!(
            visible_tools
                .iter()
                .filter(|tool| **tool == "read_file")
                .count(),
            1
        );
    }

    #[test]
    fn native_custom_tool_text_is_wrapped_with_configured_argument() {
        let descriptor = AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ApplyPatch,
            "apply_patch",
            "apply a patch",
            false,
        ))
        .with_provider_specs([AgentToolProviderSpec::custom_grammar(
            "openai",
            "lark",
            "start: /.+/",
        )
        .with_raw_input_argument("patch")]);
        let raw_patch = "*** Begin Patch\n*** End Patch\n";
        let call = LanguageModelToolCall {
            call_id: Some("call_patch".to_string()),
            name: "apply_patch".to_string(),
            input: serde_json::json!(raw_patch),
            summary: None,
        };

        let mut calls = Vec::new();
        push_native_agent_tool_call(&mut calls, &call, &[descriptor]);

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].tool, "apply_patch");
        assert_eq!(calls[0].input["patch"], raw_patch);
    }

    #[test]
    fn planned_tool_without_runtime_executor_returns_missing_executor() {
        let model = SequencedModel::new([
            r#"{
                "response_mode": "defer",
                "text": "I will search files.",
                "tool_calls": [{
                    "tool": "search",
                    "input": {"query": "needle", "path": "."},
                    "async": false,
                    "summary": "Search the workspace."
                }],
                "maid_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The tool is visible but has no executor.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let agent = Agent::new(model, "fake", "fake");

        let run = agent.run(&agent_input("list files")).expect("run");

        assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
        assert_eq!(run.tool_results[0].status, AgentToolStatus::Blocked);
        assert_eq!(
            run.tool_results[0]
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("missing_executor")
        );
    }

    #[test]
    fn default_agent_tools_match_initial_architecture_surface() {
        let names: Vec<_> = default_agent_tool_descriptors()
            .into_iter()
            .map(|descriptor| descriptor.spec.name)
            .collect();

        for expected in [
            "read_file",
            "search",
            "apply_patch",
            "run_command",
            "start_command_session",
            "inspect_command_session",
            "write_command_stdin",
            "cancel_command_session",
            "request_user_input",
        ] {
            assert!(names.iter().any(|name| name == expected), "{expected}");
        }
    }

    #[test]
    fn policy_bundle_filters_model_visible_tool_schemas() {
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            "fake",
            "fake",
        )
        .with_tool_registry(AgentToolRegistry::descriptor_only(
            default_agent_tool_descriptors(),
        ))
        .with_policy(
            AgentPolicyBundle::default_for_role(AgentRole::Deliberation)
                .with_denied_capabilities([AgentToolCapability::ExecuteProcess]),
        );

        let context = agent
            .assemble_context(&agent_input("inspect"), &[], 0)
            .expect("context");
        let visible_tools: Vec<_> = context
            .available_tool_specs
            .iter()
            .map(|descriptor| descriptor.spec.name.as_str())
            .collect();

        assert!(!visible_tools.contains(&"run_command"));
        assert!(!visible_tools.contains(&"start_command_session"));
        assert!(!visible_tools.contains(&"inspect_command_session"));
        assert!(!visible_tools.contains(&"write_command_stdin"));
        assert!(!visible_tools.contains(&"cancel_command_session"));
        assert!(
            context
                .report
                .omitted
                .iter()
                .any(|omission| omission.label == "run_command"
                    && omission.reason.contains("unavailable capability"))
        );
        assert!(
            context
                .report
                .included
                .iter()
                .any(|source| source.kind == "agent_policy")
        );
    }

    #[test]
    fn context_assembler_expands_prompt_file_and_directory_references() {
        let root =
            std::env::temp_dir().join(format!("sinomo-agent-prompt-ref-{}", TaskId::generate()));
        fs::create_dir_all(root.join("docs")).expect("workspace");
        fs::write(root.join("note.txt"), "important context\n").expect("note");
        fs::write(root.join("docs").join("a.md"), "a").expect("doc");
        fs::write(root.join("docs").join("b.md"), "b").expect("doc");
        let input = agent_input("inspect @note.txt and @docs");
        let config = AgentConfig::new("fake", "fake").with_workspace_root(&root);
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            "fake",
            "fake",
        )
        .with_config(config);

        let context = agent.assemble_context(&input, &[], 0).expect("context");

        assert_eq!(context.prompt_references.len(), 2);
        assert!(context.prompt_references.iter().any(|reference| {
            reference.kind == PromptReferenceKind::File
                && reference.path == "note.txt"
                && reference
                    .content
                    .as_deref()
                    .expect("content")
                    .contains("important context")
        }));
        assert!(context.prompt_references.iter().any(|reference| {
            reference.kind == PromptReferenceKind::Directory
                && reference.path == "docs"
                && reference
                    .entries
                    .iter()
                    .any(|entry| entry.path == "docs/a.md")
        }));
        assert!(
            context
                .report
                .included
                .iter()
                .any(|source| source.kind == "prompt_reference_file")
        );

        let request = agent.request(&input, &[], 0).expect("request");
        assert!(
            request
                .messages
                .iter()
                .any(|message| message.content.contains("\"prompt_references\"")
                    && message.content.contains("important context"))
        );
    }

    #[test]
    fn context_assembler_reports_unresolved_prompt_references() {
        let input = agent_input("inspect @missing.txt");
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            "fake",
            "fake",
        );

        let context = agent.assemble_context(&input, &[], 0).expect("context");

        assert!(context.prompt_references.is_empty());
        assert!(context.report.omitted.iter().any(|omission| {
            omission.kind == "prompt_reference"
                && omission.label == "missing.txt"
                && omission.reason.contains("workspace root is not configured")
        }));
    }

    #[test]
    fn context_assembler_loads_workspace_instruction_files() {
        let root =
            std::env::temp_dir().join(format!("sinomo-agent-instructions-{}", TaskId::generate()));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(
            root.join("AGENTS.md"),
            "# Repo Instructions\n\nPrefer deterministic context.\n",
        )
        .expect("instructions");
        let input = agent_input("inspect");
        let config = AgentConfig::new("fake", "fake").with_workspace_root(&root);
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            "fake",
            "fake",
        )
        .with_config(config);

        let context = agent.assemble_context(&input, &[], 0).expect("context");

        assert_eq!(context.loaded_instruction_files.len(), 1);
        assert_eq!(context.loaded_instruction_files[0].path, "AGENTS.md");
        assert!(!context.loaded_instruction_files[0].truncated);
        assert!(
            context.loaded_instruction_files[0]
                .content
                .contains("Prefer deterministic context")
        );
        assert!(
            context
                .report
                .included
                .iter()
                .any(|source| source.kind == "instruction_file" && source.label == "AGENTS.md")
        );

        let request = agent.request(&input, &[], 0).expect("request");
        assert!(request.messages.iter().any(|message| {
            message.content.contains("\"loaded_instruction_files\"")
                && message.content.contains("Prefer deterministic context")
        }));
    }

    #[test]
    fn context_assembler_loads_large_instruction_files_without_default_truncation() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-large-instructions-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        let content = format!("# Repo Instructions\n\n{}\n", "x".repeat(500 * 1024));
        fs::write(root.join("AGENTS.md"), &content).expect("instructions");
        let input = agent_input("inspect");
        let config = AgentConfig::new("local", "custom-1m").with_workspace_root(&root);
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            "local",
            "custom-1m",
        )
        .with_config(config);

        let context = agent.assemble_context(&input, &[], 0).expect("context");

        assert_eq!(context.loaded_instruction_files.len(), 1);
        assert_eq!(context.loaded_instruction_files[0].bytes, content.len());
        assert!(!context.loaded_instruction_files[0].truncated);
        assert_eq!(context.loaded_instruction_files[0].content, content);
        assert_eq!(context.budget_estimate.max_context_bytes, None);
    }

    #[test]
    fn policy_bundle_blocks_denied_tool_before_registry_execution() {
        let model = SequencedModel::new([
            r#"{
                "response_mode": "defer",
                "text": "I will run a command.",
                "tool_calls": [{
                    "tool": "run_command",
                    "input": {"command": "echo should-not-run"},
                    "async": false,
                    "summary": "Try a denied command."
                }],
                "maid_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The command was blocked by policy.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let agent = Agent::new(model, "fake", "fake").with_policy(
            AgentPolicyBundle::default_for_role(AgentRole::Deliberation)
                .with_denied_tools(["run_command"]),
        );

        let run = agent.run(&agent_input("run a command")).expect("run");

        assert_eq!(run.tool_results.len(), 1);
        assert_eq!(run.tool_results[0].status, AgentToolStatus::Blocked);
        assert!(!run.tool_results[0].evidence.policy.allowed);
        assert_eq!(
            run.tool_results[0]
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("policy_denied")
        );
        assert!(
            run.items
                .iter()
                .any(|item| matches!(item, AgentItem::ToolResult { evidence, .. } if !evidence.policy.allowed))
        );
    }

    #[test]
    fn local_workspace_registry_reads_repo_files() {
        let registry =
            AgentToolRegistry::local_workspace(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
        let input = agent_input("read");
        let call = AgentToolCall {
            tool: "read_file".to_string(),
            input: serde_json::json!({
                "path": "Cargo.toml",
                "max_bytes": 512
            }),
            r#async: false,
            summary: "read manifest".to_string(),
        };

        let outcome = registry.execute(&input.session, 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert!(
            outcome
                .output
                .get("content")
                .and_then(serde_json::Value::as_str)
                .expect("content")
                .contains("name = \"sinomo-agent\"")
        );
    }

    #[test]
    fn local_workspace_read_file_supports_one_based_line_ranges() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-read-line-range-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("sample.txt"), "one\ntwo\nthree\nfour\n").expect("seed file");
        let registry = AgentToolRegistry::local_workspace(&root);
        let input = agent_input("read range");
        let call = AgentToolCall {
            tool: "read_file".to_string(),
            input: serde_json::json!({
                "path": "sample.txt",
                "start_line": 2,
                "end_line": 3
            }),
            r#async: false,
            summary: "read lines".to_string(),
        };

        let outcome = registry.execute(&input.session, 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            outcome
                .output
                .get("content")
                .and_then(serde_json::Value::as_str),
            Some("two\nthree\n")
        );
        assert_eq!(
            outcome
                .output
                .pointer("/line_range/returned_start_line")
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
        assert_eq!(
            outcome
                .output
                .pointer("/line_range/returned_end_line")
                .and_then(serde_json::Value::as_u64),
            Some(3)
        );
        assert_eq!(
            outcome
                .output
                .get("total_lines")
                .and_then(serde_json::Value::as_u64),
            Some(4)
        );
    }

    #[test]
    fn local_workspace_registry_preserves_truncated_read_evidence() {
        let data_dir = SinomoDataDir::new(
            std::env::temp_dir().join(format!("sinomo-agent-test-{}", TaskId::generate())),
        );
        let input = agent_input("read");
        let mut config = LocalWorkspaceAgentConfig::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")))
            .with_data_dir_evidence(&data_dir, &input.session.session_id);
        config.max_read_bytes = 16;
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let call = AgentToolCall {
            tool: "read_file".to_string(),
            input: serde_json::json!({
                "path": "Cargo.toml",
                "max_bytes": 16
            }),
            r#async: false,
            summary: "read manifest".to_string(),
        };

        let outcome = registry.execute(&input.session, 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert!(outcome.evidence.output.truncated);
        let evidence_ref = outcome.evidence.full_output_ref().expect("full output ref");
        assert!(evidence_ref.contains("events/agent/evidence"));
        assert!(
            fs::read_to_string(evidence_ref)
                .expect("evidence")
                .contains("name = \"sinomo-agent\"")
        );
    }

    #[test]
    fn local_workspace_registry_records_output_evidence_for_readonly_tools() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-readonly-evidence-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("a.txt"), "needle\n").expect("seed a");
        fs::write(root.join("b.txt"), "other\n").expect("seed b");
        let mut config = LocalWorkspaceAgentConfig::new(&root);
        config.max_search_matches = 10;
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("readonly evidence");

        let search = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "search".to_string(),
                input: serde_json::json!({
                    "query": "needle",
                    "path": ".",
                    "max_matches": 10
                }),
                r#async: false,
                summary: "search files".to_string(),
            },
        );

        assert_eq!(search.status, AgentToolStatus::Succeeded);
        assert!(search.evidence.output.model_visible_bytes > 0);
        assert!(!search.evidence.output.truncated);
    }

    #[test]
    fn local_workspace_registry_blocks_shell_commands_by_default() {
        let registry =
            AgentToolRegistry::local_workspace(std::env::current_dir().expect("current dir"));
        let input = agent_input("command");
        let call = AgentToolCall {
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": "echo hi" }),
            r#async: false,
            summary: "run command".to_string(),
        };

        let outcome = registry.execute(&input.session, 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Blocked);
        assert_eq!(
            outcome.output.get("error").and_then(|value| value.as_str()),
            Some("command_execution_disabled")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn local_workspace_run_command_honors_timeout_and_cwd() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-command-timeout-{}",
            TaskId::generate()
        ));
        let subdir = root.join("subdir");
        fs::create_dir_all(&subdir).expect("workspace");
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).allowing_shell_commands(true),
        );
        let input = agent_input("command timeout");

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "run_command".to_string(),
                input: serde_json::json!({
                    "command": "pwd; sleep 1",
                    "cwd": "subdir",
                    "timeout_ms": 40
                }),
                r#async: false,
                summary: "run timed command".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(
            outcome
                .output
                .get("timed_out")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            outcome
                .output
                .get("cwd")
                .and_then(serde_json::Value::as_str),
            Some("subdir")
        );
        assert!(outcome.summary.contains("timed out after 40 ms"));
    }

    #[cfg(not(windows))]
    #[test]
    fn local_workspace_run_command_uses_pipefail_when_bash_is_available() {
        if !Path::new("/bin/bash").exists() && !Path::new("/usr/bin/bash").exists() {
            return;
        }
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-command-pipefail-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).allowing_shell_commands(true),
        );
        let input = agent_input("command pipefail");

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "run_command".to_string(),
                input: serde_json::json!({ "command": "false | true" }),
                r#async: false,
                summary: "run pipeline".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(
            outcome
                .output
                .get("success")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn local_workspace_registry_blocks_apply_patch_by_default() {
        let registry =
            AgentToolRegistry::local_workspace(std::env::current_dir().expect("current dir"));
        let input = agent_input("patch");
        let call = AgentToolCall {
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": "--- a/file\n+++ b/file\n" }),
            r#async: false,
            summary: "patch".to_string(),
        };

        let outcome = registry.execute(&input.session, 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Blocked);
        assert_eq!(
            outcome.output.get("error").and_then(|value| value.as_str()),
            Some("file_writes_disabled")
        );
        assert!(!outcome.evidence.policy.allowed);
    }

    #[cfg(not(windows))]
    #[test]
    fn local_workspace_command_session_streams_stdin_and_incremental_output() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-command-session-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).allowing_shell_commands(true),
        );
        let input = agent_input("command session");
        let start = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "start_command_session".to_string(),
                input: serde_json::json!({
                    "command": "read line; echo got:$line; sleep 0.05; echo done",
                    "timeout_ms": 5_000
                }),
                r#async: true,
                summary: "start interactive command".to_string(),
            },
        );
        assert_eq!(start.status, AgentToolStatus::Succeeded);
        let session_id = start
            .output
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .expect("session id")
            .to_string();

        let write = registry.execute(
            &input.session,
            0,
            1,
            &AgentToolCall {
                tool: "write_command_stdin".to_string(),
                input: serde_json::json!({
                    "session_id": session_id,
                    "input": "hello\n",
                    "close": true
                }),
                r#async: false,
                summary: "write stdin".to_string(),
            },
        );
        assert_eq!(write.status, AgentToolStatus::Succeeded);

        let mut stdout_offset = 0usize;
        let mut stderr_offset = 0usize;
        let mut stdout = String::new();
        let mut final_status = String::new();
        for _ in 0..50 {
            let inspect = registry.execute(
                &input.session,
                0,
                2,
                &AgentToolCall {
                    tool: "inspect_command_session".to_string(),
                    input: serde_json::json!({
                        "session_id": session_id,
                        "stdout_offset": stdout_offset,
                        "stderr_offset": stderr_offset
                    }),
                    r#async: false,
                    summary: "inspect output".to_string(),
                },
            );
            assert_eq!(inspect.status, AgentToolStatus::Succeeded);
            stdout.push_str(
                inspect
                    .output
                    .get("stdout")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            );
            stdout_offset = inspect
                .output
                .get("stdout_offset")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .expect("stdout offset");
            stderr_offset = inspect
                .output
                .get("stderr_offset")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .expect("stderr offset");
            final_status = inspect
                .output
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            if final_status != "running" {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        assert_eq!(final_status, "completed");
        assert!(stdout.contains("got:hello"));
        assert!(stdout.contains("done"));
    }

    #[cfg(not(windows))]
    #[test]
    fn local_workspace_command_session_truncated_offsets_can_be_paged() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-command-session-paging-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        let mut config = LocalWorkspaceAgentConfig::new(&root).allowing_shell_commands(true);
        config.max_command_output_bytes = 4;
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("command paging");
        let start = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "start_command_session".to_string(),
                input: serde_json::json!({
                    "command": "printf abcdefgh",
                    "timeout_ms": 5_000
                }),
                r#async: true,
                summary: "start output command".to_string(),
            },
        );
        let session_id = start
            .output
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .expect("session id")
            .to_string();

        let mut first_page = None;
        for _ in 0..50 {
            let inspect = registry.execute(
                &input.session,
                0,
                1,
                &AgentToolCall {
                    tool: "inspect_command_session".to_string(),
                    input: serde_json::json!({ "session_id": session_id }),
                    r#async: false,
                    summary: "inspect first page".to_string(),
                },
            );
            if inspect
                .output
                .get("status")
                .and_then(serde_json::Value::as_str)
                != Some("running")
                && inspect
                    .output
                    .get("stdout_total_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default()
                    >= 8
            {
                first_page = Some(inspect);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let first_page = first_page.expect("first page");
        assert_eq!(
            first_page
                .output
                .get("stdout")
                .and_then(serde_json::Value::as_str),
            Some("abcd")
        );
        assert_eq!(
            first_page
                .output
                .get("stdout_offset")
                .and_then(serde_json::Value::as_u64),
            Some(4)
        );
        assert_eq!(
            first_page
                .output
                .get("stdout_truncated")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );

        let second_page = registry.execute(
            &input.session,
            0,
            2,
            &AgentToolCall {
                tool: "inspect_command_session".to_string(),
                input: serde_json::json!({
                    "session_id": session_id,
                    "stdout_offset": 4
                }),
                r#async: false,
                summary: "inspect second page".to_string(),
            },
        );
        assert_eq!(
            second_page
                .output
                .get("stdout")
                .and_then(serde_json::Value::as_str),
            Some("efgh")
        );
        assert_eq!(
            second_page
                .output
                .get("stdout_offset")
                .and_then(serde_json::Value::as_u64),
            Some(8)
        );
        assert_eq!(
            second_page
                .output
                .get("stdout_truncated")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn local_workspace_command_session_supports_timeout_and_cancellation() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-command-control-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).allowing_shell_commands(true),
        );
        let input = agent_input("command control");

        let timeout_start = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "start_command_session".to_string(),
                input: serde_json::json!({
                    "command": "sleep 1",
                    "timeout_ms": 40
                }),
                r#async: true,
                summary: "start timeout command".to_string(),
            },
        );
        let timeout_session_id = timeout_start
            .output
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .expect("timeout session id")
            .to_string();
        let mut timed_out = false;
        for _ in 0..50 {
            let inspect = registry.execute(
                &input.session,
                0,
                1,
                &AgentToolCall {
                    tool: "inspect_command_session".to_string(),
                    input: serde_json::json!({ "session_id": timeout_session_id }),
                    r#async: false,
                    summary: "inspect timeout".to_string(),
                },
            );
            timed_out = inspect
                .output
                .get("timed_out")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if timed_out {
                assert_eq!(
                    inspect
                        .output
                        .get("status")
                        .and_then(serde_json::Value::as_str),
                    Some("failed")
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(timed_out);

        let cancel_start = registry.execute(
            &input.session,
            0,
            2,
            &AgentToolCall {
                tool: "start_command_session".to_string(),
                input: serde_json::json!({ "command": "sleep 5" }),
                r#async: true,
                summary: "start cancellable command".to_string(),
            },
        );
        let cancel_session_id = cancel_start
            .output
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .expect("cancel session id")
            .to_string();
        let cancel = registry.execute(
            &input.session,
            0,
            3,
            &AgentToolCall {
                tool: "cancel_command_session".to_string(),
                input: serde_json::json!({ "session_id": cancel_session_id }),
                r#async: false,
                summary: "cancel command".to_string(),
            },
        );
        assert_eq!(cancel.status, AgentToolStatus::Succeeded);
        let inspect = registry.execute(
            &input.session,
            0,
            4,
            &AgentToolCall {
                tool: "inspect_command_session".to_string(),
                input: serde_json::json!({ "session_id": cancel_session_id }),
                r#async: false,
                summary: "inspect cancelled command".to_string(),
            },
        );
        assert_eq!(
            inspect
                .output
                .get("status")
                .and_then(serde_json::Value::as_str),
            Some("cancelled")
        );
        assert_eq!(
            inspect
                .output
                .get("cancel_requested")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn local_workspace_registry_rejects_unified_diff_patches() {
        let root = std::env::temp_dir().join(format!("sinomo-agent-patch-{}", TaskId::generate()));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("hello.txt"), "old\n").expect("seed file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("patch");
        let patch = "--- a/hello.txt\n+++ b/hello.txt\n@@ -1 +1 @@\n-old\n+new\n";
        let call = AgentToolCall {
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
            r#async: false,
            summary: "patch file".to_string(),
        };

        let outcome = registry.execute(&input.session, 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(
            fs::read_to_string(root.join("hello.txt")).expect("file"),
            "old\n"
        );
        assert_eq!(
            outcome
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("freeform_patch_parse_failed")
        );
    }

    #[test]
    fn local_workspace_registry_applies_freeform_patch_when_file_writes_enabled() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-freeform-patch-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("old.txt"), "old\n").expect("seed old file");
        fs::write(root.join("multi.txt"), "line1\nline2\nline3\nline4\n").expect("seed multi file");
        fs::write(root.join("move.txt"), "before\n").expect("seed move file");
        fs::write(root.join("delete.txt"), "delete me\n").expect("seed delete file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("freeform patch");
        let patch = r#"*** Begin Patch
*** Add File: nested/added.txt
+hello
+world
*** Update File: old.txt
@@
-old
+new
*** Update File: multi.txt
@@
-line2
+changed2
@@
-line4
+changed4
*** Update File: move.txt
*** Move to: moved/move.txt
@@
-before
+after
*** Delete File: delete.txt
*** End Patch"#;

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
                r#async: false,
                summary: "apply freeform patch".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            fs::read_to_string(root.join("nested/added.txt")).expect("added file"),
            "hello\nworld\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("old.txt")).expect("old file"),
            "new\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("multi.txt")).expect("multi file"),
            "line1\nchanged2\nline3\nchanged4\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("moved/move.txt")).expect("moved file"),
            "after\n"
        );
        assert!(!root.join("move.txt").exists());
        assert!(!root.join("delete.txt").exists());
        assert_eq!(
            coding_evidence(&outcome.evidence).changed_files,
            vec![
                "delete.txt".to_string(),
                "move.txt".to_string(),
                "moved/move.txt".to_string(),
                "multi.txt".to_string(),
                "nested/added.txt".to_string(),
                "old.txt".to_string(),
            ]
        );
        assert_eq!(
            outcome
                .output
                .get("format")
                .and_then(serde_json::Value::as_str),
            Some("freeform")
        );
        assert!(
            outcome
                .summary
                .starts_with("Success. Updated the following files:")
        );
        assert!(outcome.summary.contains("\nA nested/added.txt"));
        assert!(outcome.summary.contains("\nM old.txt"));
        assert!(outcome.summary.contains("\nD delete.txt"));
        assert!(
            outcome
                .output
                .get("diff")
                .and_then(|diff| diff.get("files"))
                .and_then(serde_json::Value::as_array)
                .is_some_and(|files| !files.is_empty())
        );
    }

    #[test]
    fn local_workspace_apply_patch_records_diff_artifact_metadata() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-patch-diff-evidence-{}",
            TaskId::generate()
        ));
        let evidence_dir = root.join("evidence");
        fs::create_dir_all(&evidence_dir).expect("workspace");
        fs::write(root.join("hello.txt"), "old\n").expect("seed file");
        let config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_file_writes(true)
            .with_evidence_dir(&evidence_dir);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("diff evidence");
        let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
                r#async: false,
                summary: "patch with diff evidence".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert!(outcome.evidence.artifacts.iter().any(|artifact| {
            artifact.kind == "diff"
                && fs::read_to_string(&artifact.uri)
                    .expect("diff artifact")
                    .contains("+new")
        }));
        let patch_diff = outcome
            .evidence
            .record_value("patch_diff")
            .expect("patch diff record");
        assert_eq!(patch_diff["files"][0]["path"], "hello.txt");
        assert_eq!(patch_diff["files"][0]["status"], "M");
        assert!(
            coding_evidence(&outcome.evidence)
                .diff_ref
                .as_ref()
                .is_some_and(|path| path.ends_with(".diff"))
        );
    }

    #[test]
    fn local_workspace_apply_patch_preflights_all_changes_before_mutating() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-preflight-patch-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("target.txt"), "actual\n").expect("seed file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("preflight patch");
        let patch = r#"*** Begin Patch
*** Add File: added.txt
+created
*** Update File: target.txt
@@
-expected
+new
*** End Patch"#;

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
                r#async: false,
                summary: "preflight patch".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert!(!root.join("added.txt").exists());
        assert_eq!(
            fs::read_to_string(root.join("target.txt")).expect("target"),
            "actual\n"
        );
        assert!(
            outcome
                .summary
                .starts_with("apply_patch verification failed:")
        );
        assert!(outcome.summary.contains("Closest candidate starts at line"));
    }

    #[test]
    fn local_workspace_apply_patch_preserves_crlf_and_missing_final_newline() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-line-ending-patch-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("crlf.txt"), "old\r\nsecond\r\n").expect("seed crlf");
        fs::write(root.join("nonewline.txt"), "old").expect("seed no newline");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("line endings");
        let patch = r#"*** Begin Patch
*** Update File: crlf.txt
@@
-old
+new
*** Update File: nonewline.txt
@@
-old
+new
*** End Patch"#;

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
                r#async: false,
                summary: "line ending patch".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            fs::read_to_string(root.join("crlf.txt")).expect("crlf"),
            "new\r\nsecond\r\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("nonewline.txt")).expect("no newline"),
            "new"
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_workspace_apply_patch_preserves_existing_file_mode() {
        use std::os::unix::fs::PermissionsExt;

        let root =
            std::env::temp_dir().join(format!("sinomo-agent-mode-patch-{}", TaskId::generate()));
        fs::create_dir_all(&root).expect("workspace");
        let script = root.join("script.sh");
        fs::write(&script, "old\n").expect("seed script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("mode patch");

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({
                    "changes": [{
                        "action": "replace_file",
                        "path": "script.sh",
                        "content": "new\n"
                    }]
                }),
                r#async: false,
                summary: "replace script".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            fs::metadata(&script)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_workspace_apply_patch_rejects_symlink_path_components() {
        use std::os::unix::fs::symlink;

        let root =
            std::env::temp_dir().join(format!("sinomo-agent-symlink-patch-{}", TaskId::generate()));
        fs::create_dir_all(root.join("real")).expect("workspace");
        symlink(root.join("real"), root.join("link")).expect("symlink");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("symlink patch");
        let patch = r#"*** Begin Patch
*** Add File: link/new.txt
+created
*** End Patch"#;

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
                r#async: false,
                summary: "reject symlink".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert!(!root.join("real/new.txt").exists());
        assert!(outcome.summary.contains("symlink component"));
    }

    #[test]
    fn local_workspace_apply_patch_honors_context_and_eof_anchors() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-anchored-patch-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(
            root.join("anchors.txt"),
            "section a\nvalue\nsection b\nvalue\nfooter\n",
        )
        .expect("seed file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("anchored patch");
        let patch = r#"*** Begin Patch
*** Update File: anchors.txt
@@ section b
-value
+changed
*** Update File: anchors.txt
@@
-footer
+tail
*** End of File
*** End Patch"#;

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
                r#async: false,
                summary: "apply anchored patch".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            fs::read_to_string(root.join("anchors.txt")).expect("anchors file"),
            "section a\nvalue\nsection b\nchanged\ntail\n"
        );
    }

    #[test]
    fn local_workspace_apply_patch_inserts_addition_only_hunk_after_context() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-context-add-patch-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("insert.txt"), "before\nanchor\nafter\n").expect("seed file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("context insertion");
        let patch = r#"*** Begin Patch
*** Update File: insert.txt
@@ anchor
+inserted
*** End Patch"#;

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
                r#async: false,
                summary: "insert after context".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            fs::read_to_string(root.join("insert.txt")).expect("insert file"),
            "before\nanchor\ninserted\nafter\n"
        );
    }

    #[test]
    fn local_workspace_apply_patch_rejects_malformed_update_without_mutating() {
        let root =
            std::env::temp_dir().join(format!("sinomo-agent-bad-patch-{}", TaskId::generate()));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("bad.txt"), "old\n").expect("seed file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("bad patch");
        let patch = r#"*** Begin Patch
*** Update File: bad.txt
@@
old
+new
*** End Patch"#;

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
                r#async: false,
                summary: "reject bad patch".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(
            fs::read_to_string(root.join("bad.txt")).expect("bad file"),
            "old\n"
        );
        assert_eq!(
            outcome
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("freeform_patch_parse_failed")
        );
    }

    #[test]
    fn local_workspace_registry_applies_structured_patch_changes() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-structured-patch-{}",
            TaskId::generate()
        ));
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("replace.txt"), "old\n").expect("seed replace file");
        fs::write(root.join("move.txt"), "move me\n").expect("seed move file");
        fs::write(root.join("delete.txt"), "delete me\n").expect("seed delete file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("structured patch");

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({
                    "changes": [
                        {
                            "action": "add_file",
                            "path": "added.txt",
                            "content": "created\n"
                        },
                        {
                            "action": "update_file",
                            "path": "replace.txt",
                            "content": "new\n"
                        },
                        {
                            "action": "move_file",
                            "path": "move.txt",
                            "new_path": "renamed/move.txt"
                        },
                        {
                            "action": "delete_file",
                            "path": "delete.txt"
                        }
                    ]
                }),
                r#async: false,
                summary: "apply structured patch".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            fs::read_to_string(root.join("added.txt")).expect("added file"),
            "created\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("replace.txt")).expect("replace file"),
            "new\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("renamed/move.txt")).expect("moved file"),
            "move me\n"
        );
        assert!(!root.join("move.txt").exists());
        assert!(!root.join("delete.txt").exists());
        assert_eq!(
            outcome
                .output
                .get("format")
                .and_then(serde_json::Value::as_str),
            Some("structured")
        );
    }

    #[test]
    fn agent_records_patch_snapshot_items_for_apply_patch() {
        let root =
            std::env::temp_dir().join(format!("sinomo-agent-loop-patch-{}", TaskId::generate()));
        let evidence_dir = root.join("evidence");
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("hello.txt"), "old\n").expect("seed file");
        let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";
        let tool_turn = serde_json::json!({
            "response_mode": "defer",
            "text": "I will patch the file.",
            "tool_calls": [{
                "tool": "apply_patch",
                "input": {"patch": patch},
                "async": false,
                "summary": "Patch hello.txt."
            }],
            "maid_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let model = SequencedModel::new([
            tool_turn,
            r#"{
                "response_mode": "answer",
                "text": "Patched.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#
            .to_string(),
        ]);
        let config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_file_writes(true)
            .with_evidence_dir(&evidence_dir);
        let agent = Agent::new(model, "fake", "fake")
            .with_tool_registry(AgentToolRegistry::local_workspace_with_config(config));

        let run = agent.run(&agent_input("patch file")).expect("run");

        assert_eq!(
            fs::read_to_string(root.join("hello.txt")).expect("file"),
            "new\n"
        );
        let patch_snapshot = run
            .items
            .iter()
            .find_map(|item| {
                if let AgentItem::PatchSnapshot {
                    status,
                    changed_files,
                    patch_ref,
                    ..
                } = item
                {
                    Some((status, changed_files, patch_ref))
                } else {
                    None
                }
            })
            .expect("patch snapshot");
        assert_eq!(patch_snapshot.0, &AgentToolStatus::Succeeded);
        assert_eq!(patch_snapshot.1, &vec!["hello.txt".to_string()]);
        let patch_ref = patch_snapshot.2.as_deref().expect("patch ref");
        assert!(
            fs::read_to_string(patch_ref)
                .expect("patch evidence")
                .contains("+new")
        );
    }

    #[test]
    fn turn_context_renders_evolution_context_for_model() {
        let mut input = agent_input("continue the evolution job");
        input.evolution_context = Some(AgentEvolutionContext {
            job: None,
            mutation_packet: None,
            candidate_bundle: None,
            verification_results: vec![AgentEvolutionVerificationResult {
                command: "cargo test -p sinomo-agent".to_string(),
                status: AgentVerificationState::Passed,
                summary: "baseline passed".to_string(),
            }],
            candidate_evidence: Vec::new(),
        });
        let agent = Agent::new(
            SequencedModel::new([r#"{
                "response_mode": "answer",
                "text": "Done.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#]),
            "fake",
            "fake",
        );

        let context = agent.assemble_context(&input, &[], 0).expect("context");
        let request = agent.request(&input, &[], 0).expect("request");

        assert!(context.evolution_job_facts.is_some());
        assert!(
            context
                .report
                .included
                .iter()
                .any(|source| source.kind == "evolution_context")
        );
        assert!(context.budget_estimate.evolution_context_bytes > 0);
        assert!(request.messages.iter().any(|message| {
            message.content.contains("evolution_job_facts")
                && message.content.contains("cargo test -p sinomo-agent")
        }));
    }

    #[test]
    fn agent_run_collects_candidate_evidence_bundle() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-agent-candidate-evidence-{}",
            TaskId::generate()
        ));
        let evidence_dir = root.join("evidence");
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("hello.txt"), "old\n").expect("seed file");
        let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";
        let tool_turn = serde_json::json!({
            "response_mode": "defer",
            "text": "I will patch the file.",
            "tool_calls": [
                {
                    "tool": "apply_patch",
                    "input": {"patch": patch},
                    "async": false,
                    "summary": "Patch hello.txt."
                }
            ],
            "maid_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let model = SequencedModel::new([
            tool_turn,
            r#"{
                "response_mode": "answer",
                "text": "Candidate is ready.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#
            .to_string(),
        ]);
        let config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_file_writes(true)
            .with_evidence_dir(&evidence_dir);
        let agent = Agent::new(model, "fake", "fake")
            .with_tool_registry(AgentToolRegistry::local_workspace_with_config(config));
        let mut input = agent_input("build a candidate");
        input.evolution_context = Some(AgentEvolutionContext {
            job: None,
            mutation_packet: None,
            candidate_bundle: None,
            verification_results: Vec::new(),
            candidate_evidence: Vec::new(),
        });

        let run = agent.run(&input).expect("run");

        let bundle = run
            .candidate_evidence_bundle
            .expect("candidate evidence bundle");
        assert_eq!(bundle.final_response, "Candidate is ready.");
        assert!(bundle.evidence.iter().any(|evidence| {
            matches!(
                evidence,
                AgentCandidateEvidence::Patch { changed_files, .. }
                    if changed_files == &vec!["hello.txt".to_string()]
            )
        }));
    }

    #[test]
    fn automatic_compaction_prunes_model_context_before_sampling() {
        let old_context = format!("OLD_CONTEXT_MARKER:{}", "x".repeat(8 * 1024));
        let model = SequencedModel::new([
            r#"{
                "response_mode": "answer",
                "text": "First compacted answer.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "Second compacted answer.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let config = AgentConfig::new("fake", "fake").with_context_budget(AgentContextBudget {
            max_context_bytes: Some(4 * 1024),
            compaction_threshold_ratio: Some(0.5),
            retained_tail_items: None,
        });
        let agent = Agent::new(model, "fake", "fake")
            .with_config(config)
            .with_compactor(AgentCompactor {
                retained_tail_count: Some(1),
            });
        let input = agent_input("new request");
        let mut session =
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
        session.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: old_context,
            event_id: None,
        });
        session.transcript.push(AgentItem::AssistantText {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "OLD_ASSISTANT_MARKER".to_string(),
        });

        let first_run = agent.start_turn(&mut session, &input).expect("first run");
        let second_input = agent_input("follow up");
        let second_run = agent
            .continue_turn(&mut session, &second_input)
            .expect("second run");

        assert_eq!(first_run.compactions.len(), 1);
        assert_eq!(second_run.final_decision.text, "Second compacted answer.");
        let requests = agent.model.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            let request_text = request
                .messages
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(!request_text.contains("OLD_CONTEXT_MARKER"));
            assert!(!request_text.contains("OLD_ASSISTANT_MARKER"));
            assert!(request_text.contains("prior_compaction_summary"));
        }
    }

    #[test]
    fn session_projection_is_rendered_as_current_work_budget_lane() {
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Projection-aware answer.",
            "tool_calls": [],
            "maid_handoff": null,
            "next_state": "listening"
        }"#]);
        let agent = Agent::new(model, "fake", "fake").with_compactor(AgentCompactor {
            retained_tail_count: Some(1),
        });
        let input = agent_input("continue after compaction");
        let mut session =
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
        session.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "remember this active objective".to_string(),
            event_id: None,
        });
        session.transcript.push(AgentItem::AssistantText {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "I will keep working on it.".to_string(),
        });
        agent
            .compact(&mut session, "test projection context")
            .expect("compact");

        let run = agent.start_turn(&mut session, &input).expect("run");

        let request_text = agent.model.requests.lock().expect("requests")[0]
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(request_text.contains("\"session_projection\""));
        assert!(request_text.contains("\"current_work\""));
        assert!(request_text.contains("\"lanes\""));
        assert!(request_text.contains("\"recent_tail\""));
        assert!(
            run.context_reports[0]
                .budget_estimate
                .lanes
                .iter()
                .any(|lane| lane.kind == "current_work" && lane.bytes > 0)
        );
    }

    #[test]
    fn agent_session_api_supports_steer_compact_and_abort() {
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "maid_handoff": null,
            "next_state": "listening"
        }"#]);
        let agent = Agent::new(model, "fake", "fake");
        let input = agent_input("hello");
        let mut session =
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

        let run = agent.start_turn(&mut session, &input).expect("run");
        assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
        assert!(!session.transcript.items.is_empty());

        let steering = agent
            .steer(&mut session, "prefer a concise answer")
            .expect("steer");
        assert_eq!(session.steering_queue[0], steering);

        let compaction = agent
            .compact(&mut session, "test compaction")
            .expect("compact");
        assert_eq!(compaction.reason, "test compaction");
        assert!(
            session
                .transcript
                .items
                .iter()
                .any(|item| matches!(item, AgentItem::CompactionSummary { .. }))
        );

        let abort = agent
            .abort(&mut session, "no longer needed")
            .expect("abort");
        assert_eq!(session.abort, Some(abort));
        let aborted_run = agent.continue_turn(&mut session, &input).expect("aborted");
        assert_eq!(aborted_run.stop_reason, AgentStopReason::Aborted);
    }

    #[test]
    fn agent_session_continuation_uses_history_and_consumes_queued_context() {
        let model = SequencedModel::new([
            r#"{
                "response_mode": "answer",
                "text": "First answer.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "Second answer.",
                "tool_calls": [],
                "maid_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let agent = Agent::new(model, "fake", "fake");
        let input = agent_input("first");
        let mut session =
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

        agent.start_turn(&mut session, &input).expect("first turn");
        agent
            .steer(&mut session, "prefer concise reasoning")
            .expect("steer");
        agent
            .follow_up(&mut session, "also mention tests")
            .expect("follow up");
        let second_input = agent_input("second");
        let run = agent
            .continue_turn(&mut session, &second_input)
            .expect("second turn");

        assert_eq!(run.final_decision.text, "Second answer.");
        assert!(session.steering_queue.is_empty());
        assert!(session.follow_up_queue.is_empty());
        let requests = agent.model.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        let second_request_text = requests[1]
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(second_request_text.contains("First answer."));
        assert!(second_request_text.contains("agent_queued_turn_context"));
        assert!(second_request_text.contains("prefer concise reasoning"));
        assert!(second_request_text.contains("also mention tests"));
    }
}
