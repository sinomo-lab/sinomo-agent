use crate::session_store::{
    AgentSessionStore, AgentSessionStoreError, AgentSessionStoreRecord,
    AgentSessionStoreRecordKind, NoopAgentSessionStore,
};
use crate::tools::{
    AGENT_CODING_EVIDENCE_KIND, AgentCodingToolEvidence, AgentModuleToolProjection, AgentTool,
    AgentToolCapability, AgentToolCustomFormat, AgentToolCustomSpec, AgentToolDescriptor,
    AgentToolEvidence, AgentToolOutcome, AgentToolPlan, AgentToolPlanner, AgentToolPolicyDecision,
    AgentToolProgressEvent, AgentToolProgressSink, AgentToolProviderSpec, AgentToolRegistry,
    AgentToolStatus, is_tool_disclosure_bridge_name,
};
use crate::{
    AgentCancellationToken, AgentCompactionEngine, AgentCompactionRequest, AgentDefinition,
    AgentError, AgentLoopDecision, AgentLoopInput, AgentOutputFormat, AgentToolCall,
    AsyncTaskNextAction, CodexStyleAgentCompactionEngine, InteractionModel, InteractionSession,
    TaskId, parse_agent_loop_response, render_agent_system_prompt_with_descriptors,
};
use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelCustomToolFormat, LanguageModelCustomToolSpec, LanguageModelErrorCategory,
    LanguageModelMessage, LanguageModelProviderToolSpec, LanguageModelRequest,
    LanguageModelRequestItem, LanguageModelResponse, LanguageModelRole, LanguageModelStreamEvent,
    LanguageModelToolCall, LanguageModelToolChoice, LanguageModelToolSpec,
    classify_language_model_error_detail, collect_language_model_tool_calls,
    inferred_model_context_window_tokens,
};
use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventKind, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source,
};
use sinomo_runtime::events::{EventId, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

const DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO: f32 = 0.80;
const CONTEXT_ESTIMATE_BYTES_PER_TOKEN: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRole {
    Main,
    Background,
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

fn is_false(value: &bool) -> bool {
    !*value
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentRunContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<AgentTaskRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<AgentPlacementMetadata>,
}

impl AgentRunContext {
    pub fn is_empty(&self) -> bool {
        self.task.is_none() && self.workspace.is_none() && self.placement.is_none()
    }

    pub fn with_task(mut self, task: AgentTaskRef) -> Self {
        self.task = Some(task);
        self
    }

    pub fn with_workspace(mut self, workspace: WorkspaceRef) -> Self {
        self.workspace = Some(workspace);
        self
    }

    pub fn with_placement(mut self, placement: AgentPlacementMetadata) -> Self {
        self.placement = Some(placement);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentTaskRef {
    pub task_id: TaskId,
    pub kind: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_role: Option<AgentRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<TaskId>,
}

impl AgentTaskRef {
    pub fn new(kind: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            task_id: TaskId::generate(),
            kind: kind.into(),
            summary: summary.into(),
            owner_role: None,
            parent_task_id: None,
        }
    }

    pub fn with_owner_role(mut self, owner_role: AgentRole) -> Self {
        self.owner_role = Some(owner_role);
        self
    }

    pub fn with_parent_task_id(mut self, parent_task_id: TaskId) -> Self {
        self.parent_task_id = Some(parent_task_id);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WorkspaceRef {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sifs_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_policy: Option<String>,
}

impl WorkspaceRef {
    pub fn local_path(path: impl Into<PathBuf>) -> Self {
        let local_root = path.into();
        Self {
            id: local_root.display().to_string(),
            local_root: Some(local_root),
            sifs_root: None,
            owner_node: None,
            write_policy: Some("local".to_string()),
        }
    }

    pub fn sifs(id: impl Into<String>, sifs_root: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            local_root: None,
            sifs_root: Some(sifs_root.into()),
            owner_node: None,
            write_policy: None,
        }
    }

    pub fn with_owner_node(mut self, owner_node: impl Into<String>) -> Self {
        self.owner_node = Some(owner_node.into());
        self
    }

    pub fn with_write_policy(mut self, write_policy: impl Into<String>) -> Self {
        self.write_policy = Some(write_policy.into());
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentPlacementMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_os: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_arch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_class: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub local_only: bool,
}

impl AgentPlacementMetadata {
    pub fn local_current_process() -> Self {
        Self {
            origin_node: None,
            target_node: None,
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            sandbox_class: None,
            local_only: true,
        }
    }

    pub fn with_origin_node(mut self, origin_node: impl Into<String>) -> Self {
        self.origin_node = Some(origin_node.into());
        self
    }

    pub fn with_target_node(mut self, target_node: impl Into<String>) -> Self {
        self.target_node = Some(target_node.into());
        self
    }

    pub fn with_sandbox_class(mut self, sandbox_class: impl Into<String>) -> Self {
        self.sandbox_class = Some(sandbox_class.into());
        self
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
        role: AgentRole,
        policy_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_steps: Option<usize>,
        #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
        run_context: AgentRunContext,
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

    pub fn sifs_evidence_path_for_session(
        &self,
        session_id: &SessionId,
        file_name: impl AsRef<str>,
    ) -> String {
        format!(
            "/events/agent/evidence/{}/{}",
            session_id.as_str(),
            file_name.as_ref().trim_start_matches('/')
        )
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_call_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
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
    CodingRunState {
        item_id: TaskId,
        step_index: usize,
        state: CodingRunState,
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CodingPatchStatus {
    NotStarted,
    Applied,
    Failed,
    Blocked,
}

impl Default for CodingPatchStatus {
    fn default() -> Self {
        Self::NotStarted
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CodingValidationStatus {
    NotRun,
    Passed,
    FailedPatchRelevant,
    FailedEnvironment,
    FailedCommandSetup,
    TimedOut,
    Inconclusive,
}

impl Default for CodingValidationStatus {
    fn default() -> Self {
        Self::NotRun
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CodingBlockedReason {
    PolicyDenied,
    ToolUnavailable,
    Environment,
    CommandSetup,
    NeedsUserInput,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CodingStopDecision {
    Continue,
    FinishWithPatch,
    FinishBlocked,
    RequestUserInput,
    Abort,
}

impl Default for CodingStopDecision {
    fn default() -> Self {
        Self::Continue
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodingValidationAttempt {
    pub step_index: usize,
    pub call_id: TaskId,
    pub command: String,
    pub status: CodingValidationStatus,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CodingRunState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_diff_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_diff_summary: Option<String>,
    #[serde(default)]
    pub patch_status: CodingPatchStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validation_attempts: Vec<CodingValidationAttempt>,
    #[serde(default)]
    pub validation_status: CodingValidationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<CodingBlockedReason>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outstanding_evidence: Vec<String>,
    #[serde(default)]
    pub stop_recommendation: CodingStopDecision,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_risks: Vec<String>,
}

impl CodingRunState {
    pub fn is_empty(&self) -> bool {
        self.changed_files.is_empty()
            && self.current_diff_summary.is_none()
            && self.patch_status == CodingPatchStatus::NotStarted
            && self.validation_attempts.is_empty()
            && self.validation_status == CodingValidationStatus::NotRun
            && self.blocked_reason.is_none()
            && self.open_risks.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentContextAttachment {
    pub kind: String,
    pub summary: String,
    #[serde(default)]
    pub payload_json: serde_json::Value,
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
    pub context_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_summary: Option<String>,
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
    pub context_attachment_bytes: usize,
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
        let system_instructions =
            render_agent_system_prompt_with_descriptors(&config.definition, &tool_descriptors);
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
        let instruction_file_bytes = serde_json::to_vec(&loaded_instruction_files)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let (prompt_references, prompt_reference_sources, prompt_reference_omissions) =
            resolve_prompt_references(input, config);
        let prompt_reference_bytes = serde_json::to_vec(&prompt_references)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        let context_attachment_bytes = serde_json::to_vec(&input.context_attachments)
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
        if context_attachment_bytes > 0 {
            lanes.push(ContextBudgetLane {
                kind: "context_attachments".to_string(),
                label: "typed context attachments".to_string(),
                bytes: context_attachment_bytes,
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
            context_attachment_bytes,
            total_bytes: system_instructions.len()
                + session_projection_bytes
                + transcript_bytes
                + tool_schema_bytes
                + workspace_facts_bytes
                + instruction_file_bytes
                + prompt_reference_bytes
                + context_attachment_bytes,
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
        if session_projection_bytes > 0 {
            included.push(ContextSource {
                kind: "session_projection".to_string(),
                label: "current work projection".to_string(),
                bytes: session_projection_bytes,
            });
        }
        included.extend(instruction_sources);
        included.extend(prompt_reference_sources);
        if !input.context_attachments.is_empty() {
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
            run_context: config.run_context.clone(),
            session_projection,
            transcript_view: transcript.to_vec(),
            workspace_facts,
            loaded_instruction_files,
            prompt_references,
            context_attachments: input.context_attachments.clone(),
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        category: Option<LanguageModelErrorCategory>,
    },
    Aborted {
        reason: String,
    },
}

impl AgentModelEvent {
    fn from_stream_event(
        step_index: usize,
        provider: &str,
        model: &str,
        stream_event: &LanguageModelStreamEvent,
    ) -> Option<Self> {
        match stream_event {
            LanguageModelStreamEvent::Started { provider, model } => Some(Self::Started {
                step_index,
                provider: provider.clone(),
                model: model.clone(),
            }),
            LanguageModelStreamEvent::TextDelta { delta } => {
                (!delta.is_empty()).then(|| Self::AssistantTextDelta {
                    step_index,
                    delta: delta.clone(),
                })
            }
            LanguageModelStreamEvent::ReasoningSummary { summary } => {
                Some(Self::AssistantReasoningSummary {
                    step_index,
                    summary: summary.clone(),
                })
            }
            LanguageModelStreamEvent::ToolCallStarted { call_id, name } => {
                Some(Self::ToolCallStarted {
                    step_index,
                    call_index: None,
                    call_id: call_id.clone(),
                    tool: name.clone(),
                })
            }
            LanguageModelStreamEvent::ToolCallDelta {
                call_id,
                name,
                arguments_delta,
            } => Some(Self::ToolCallDelta {
                step_index,
                call_id: call_id.clone(),
                tool: name.clone(),
                arguments_delta: arguments_delta.clone(),
            }),
            LanguageModelStreamEvent::ToolCallCompleted {
                call_id,
                name,
                input,
            } => Some(Self::ToolCallCompleted {
                step_index,
                call_index: None,
                call_id: call_id.clone(),
                call: AgentToolCall {
                    call_id: call_id.clone(),
                    tool: name.clone(),
                    input: input.clone(),
                    r#async: false,
                    summary: String::new(),
                },
            }),
            LanguageModelStreamEvent::Usage { usage } => Some(Self::Usage {
                step_index,
                provider: provider.to_string(),
                model: model.to_string(),
                usage: Some(usage.clone()),
            }),
            LanguageModelStreamEvent::Finished { .. } => None,
            LanguageModelStreamEvent::Error { detail } => Some(Self::Error {
                step_index,
                category: classify_language_model_error_detail(detail),
                detail: detail.clone(),
            }),
            LanguageModelStreamEvent::Aborted { reason } => Some(Self::Aborted {
                reason: reason.clone(),
            }),
        }
    }

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
                        call_id: call_id.clone(),
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
                    events.push(AgentModelEvent::Error {
                        step_index,
                        category: classify_language_model_error_detail(&detail),
                        detail,
                    });
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
                        call_id: call_id.clone(),
                        call: AgentToolCall {
                            call_id,
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
                    events.push(AgentModelEvent::Error {
                        step_index,
                        category: classify_language_model_error_detail(&detail),
                        detail,
                    });
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
        events.push(AgentModelEvent::Error {
            step_index,
            category: classify_language_model_error_detail(&detail)
                .or(Some(LanguageModelErrorCategory::RepairNeeded)),
            detail,
        });
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
    #[serde(default = "default_agent_compaction_summary_strategy_id")]
    pub strategy_id: String,
    #[serde(default)]
    pub trigger: AgentCompactionTrigger,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_model: Option<AgentCompactionModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
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

fn default_agent_compaction_summary_strategy_id() -> String {
    crate::DEFAULT_AGENT_COMPACTION_STRATEGY_ID.to_string()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentCompactionTrigger {
    #[default]
    Manual,
    PreSamplingBudget,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentCompactionModelRef {
    pub provider: String,
    pub model: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentCompactionBuildOptions {
    pub strategy_id: String,
    pub trigger: AgentCompactionTrigger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_model: Option<AgentCompactionModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
}

impl Default for AgentCompactionBuildOptions {
    fn default() -> Self {
        Self {
            strategy_id: crate::DEFAULT_AGENT_COMPACTION_STRATEGY_ID.to_string(),
            trigger: AgentCompactionTrigger::Manual,
            summary_override: None,
            summary_model: None,
            fallback_reason: None,
        }
    }
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
        self.compact_retaining_tail_count_with_options(
            transcript,
            reason,
            token_estimate_before,
            retained_tail_count,
            previous_projection,
            steering,
            follow_ups,
            AgentCompactionBuildOptions::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compact_retaining_tail_count_with_options(
        &self,
        transcript: &AgentTranscript,
        reason: impl Into<String>,
        token_estimate_before: usize,
        retained_tail_count: usize,
        previous_projection: Option<&AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
        options: AgentCompactionBuildOptions,
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
        let summary = options
            .summary_override
            .as_ref()
            .map(|summary| summary.trim())
            .filter(|summary| !summary.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| projection_summary.clone());
        let projection = AgentSessionProjection {
            projection_id: TaskId::generate(),
            previous_projection_id: previous_projection
                .map(|projection| projection.projection_id.clone()),
            created_at: time::OffsetDateTime::now_utc(),
            reason: reason.clone(),
            summary,
            source_item_count: transcript.items.len(),
            source_item_ids: transcript.items.iter().map(agent_item_id).collect(),
            retained_item_ids: retained.iter().map(agent_item_id).collect(),
            working_state: working_state.clone(),
        };
        AgentCompactionSummary {
            compaction_id: TaskId::generate(),
            reason,
            strategy_id: options.strategy_id,
            trigger: options.trigger,
            summary: projection.summary.clone(),
            summary_model: options.summary_model,
            fallback_reason: options.fallback_reason,
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
            AgentItem::CodingRunState {
                state: coding_state,
                ..
            } => {
                for changed_file in &coding_state.changed_files {
                    push_unique(&mut state.files_modified, changed_file.clone());
                }
                if let Some(blocked_reason) = &coding_state.blocked_reason {
                    push_unique(
                        &mut state.blockers,
                        format!("coding run blocked: {blocked_reason:?}"),
                    );
                }
                state.open_risks.extend(coding_state.open_risks.clone());
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

fn coding_state_from_items(items: &[AgentItem]) -> CodingRunState {
    let mut state = CodingRunState::default();
    for item in items {
        match item {
            AgentItem::UserMessage { content, .. } => {
                if !content.contains("\"kind\": \"agent_queued_turn_context\"")
                    && state.objective.is_none()
                {
                    state.objective = Some(compact_multiline(content, 1000));
                }
            }
            AgentItem::PatchSnapshot {
                status,
                summary,
                changed_files,
                ..
            } => {
                match status {
                    AgentToolStatus::Succeeded => state.patch_status = CodingPatchStatus::Applied,
                    AgentToolStatus::Blocked => state.patch_status = CodingPatchStatus::Blocked,
                    AgentToolStatus::Failed | AgentToolStatus::Cancelled => {
                        state.patch_status = CodingPatchStatus::Failed
                    }
                }
                for changed_file in changed_files {
                    push_unique(&mut state.changed_files, changed_file.clone());
                }
                if !changed_files.is_empty() {
                    state.current_diff_summary = Some(compact_multiline(summary, 1000));
                }
                if matches!(status, AgentToolStatus::Failed | AgentToolStatus::Blocked) {
                    push_unique(&mut state.open_risks, compact_multiline(summary, 500));
                }
            }
            AgentItem::ToolResult {
                step_index,
                call_id,
                status,
                summary,
                output,
                evidence,
                ..
            } => {
                if let Some(reason) = coding_blocked_reason_from_tool_result(status, output) {
                    state.blocked_reason = Some(reason);
                    push_unique(&mut state.open_risks, compact_multiline(summary, 500));
                }
                let coding = evidence
                    .record::<AgentCodingToolEvidence>(AGENT_CODING_EVIDENCE_KIND)
                    .unwrap_or_default();
                for changed_file in coding.changed_files {
                    push_unique(&mut state.changed_files, changed_file);
                }
                if let Some(attempt) = coding_validation_attempt_from_tool_result(
                    *step_index,
                    call_id,
                    status,
                    summary,
                    output,
                ) {
                    state.validation_status =
                        merge_coding_validation_status(&state.validation_status, &attempt.status);
                    state.validation_attempts.push(attempt);
                }
            }
            AgentItem::VerificationResult {
                step_index,
                call_id,
                command,
                status,
                summary,
                ..
            } => {
                let status = coding_validation_status_from_agent_verification(status);
                state.validation_status =
                    merge_coding_validation_status(&state.validation_status, &status);
                state.validation_attempts.push(CodingValidationAttempt {
                    step_index: *step_index,
                    call_id: call_id.clone(),
                    command: command.clone(),
                    status,
                    summary: summary.clone(),
                    exit_code: None,
                    timed_out: false,
                });
            }
            AgentItem::CodingRunState { .. }
            | AgentItem::AssistantText { .. }
            | AgentItem::AssistantReasoningSummary { .. }
            | AgentItem::ToolCall { .. }
            | AgentItem::AsyncTaskUpdate { .. }
            | AgentItem::ContextInjection { .. }
            | AgentItem::CompactionSummary { .. }
            | AgentItem::ModelUsage { .. }
            | AgentItem::Error { .. } => {}
        }
    }
    finalize_coding_state(state)
}

fn finalize_coding_state(mut state: CodingRunState) -> CodingRunState {
    if state.patch_status == CodingPatchStatus::Applied
        && state.validation_status == CodingValidationStatus::NotRun
    {
        push_unique(
            &mut state.outstanding_evidence,
            "validation has not run for the current patch".to_string(),
        );
    }
    if state.patch_status == CodingPatchStatus::NotStarted && state.blocked_reason.is_none() {
        push_unique(
            &mut state.outstanding_evidence,
            "no patch has been produced yet".to_string(),
        );
    }
    state.stop_recommendation = match (&state.patch_status, &state.validation_status) {
        (CodingPatchStatus::Applied, CodingValidationStatus::Passed)
        | (CodingPatchStatus::Applied, CodingValidationStatus::FailedEnvironment)
            if is_narrow_coding_patch(&state) =>
        {
            CodingStopDecision::FinishWithPatch
        }
        _ if state.blocked_reason == Some(CodingBlockedReason::NeedsUserInput) => {
            CodingStopDecision::RequestUserInput
        }
        _ if state.blocked_reason.is_some() && state.patch_status != CodingPatchStatus::Applied => {
            CodingStopDecision::FinishBlocked
        }
        _ => CodingStopDecision::Continue,
    };
    state
}

fn is_narrow_coding_patch(state: &CodingRunState) -> bool {
    !state.changed_files.is_empty() && state.changed_files.len() <= 3
}

fn coding_final_decision_from_state(state: &CodingRunState) -> AgentLoopDecision {
    let files = if state.changed_files.is_empty() {
        "no files recorded".to_string()
    } else {
        state.changed_files.join(", ")
    };
    let validation = match state.validation_status {
        CodingValidationStatus::Passed => "validation passed".to_string(),
        CodingValidationStatus::FailedEnvironment => {
            "validation was blocked by the local environment".to_string()
        }
        _ => format!("validation status: {:?}", state.validation_status),
    };
    let summary = state
        .current_diff_summary
        .as_deref()
        .map(|summary| format!(" Summary: {summary}"))
        .unwrap_or_default();
    AgentLoopDecision {
        response_mode: crate::AgentLoopResponseMode::Answer,
        text: format!("Patch is ready. Changed files: {files}. {validation}.{summary}"),
        tool_calls: Vec::new(),
        background_handoff: None,
        next_state: crate::AgentInteractionState::Listening,
    }
}

fn merge_coding_validation_status(
    current: &CodingValidationStatus,
    next: &CodingValidationStatus,
) -> CodingValidationStatus {
    use CodingValidationStatus::{
        FailedCommandSetup, FailedEnvironment, FailedPatchRelevant, Inconclusive, NotRun, Passed,
        TimedOut,
    };
    match (current, next) {
        (FailedPatchRelevant, _) | (_, FailedPatchRelevant) => FailedPatchRelevant,
        (TimedOut, _) | (_, TimedOut) => TimedOut,
        (FailedCommandSetup, _) | (_, FailedCommandSetup) => FailedCommandSetup,
        (FailedEnvironment, _) | (_, FailedEnvironment) => FailedEnvironment,
        (Inconclusive, _) | (_, Inconclusive) => Inconclusive,
        (Passed, _) | (_, Passed) => Passed,
        _ => NotRun,
    }
}

fn coding_validation_status_from_agent_verification(
    status: &AgentVerificationState,
) -> CodingValidationStatus {
    match status {
        AgentVerificationState::NotRun => CodingValidationStatus::NotRun,
        AgentVerificationState::Passed => CodingValidationStatus::Passed,
        AgentVerificationState::Failed => CodingValidationStatus::FailedPatchRelevant,
        AgentVerificationState::Blocked => CodingValidationStatus::FailedEnvironment,
    }
}

fn coding_validation_attempt_from_tool_result(
    step_index: usize,
    call_id: &TaskId,
    status: &AgentToolStatus,
    summary: &str,
    output: &serde_json::Value,
) -> Option<CodingValidationAttempt> {
    if !looks_like_command_result(output) {
        return None;
    }
    let command = verification_command_from_tool_output(output).unwrap_or_else(|| "command".into());
    let timed_out = output
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let exit_code = output
        .get("status")
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok());
    let validation_status = classify_command_validation(status, summary, output);
    Some(CodingValidationAttempt {
        step_index,
        call_id: call_id.clone(),
        command,
        status: validation_status,
        summary: compact_multiline(summary, 1000),
        exit_code,
        timed_out,
    })
}

fn looks_like_command_result(output: &serde_json::Value) -> bool {
    output.get("program").is_some()
        && output.get("args").is_some()
        && output.get("status").is_some()
        && output.get("stdout").is_some()
        && output.get("stderr").is_some()
}

fn classify_command_validation(
    status: &AgentToolStatus,
    summary: &str,
    output: &serde_json::Value,
) -> CodingValidationStatus {
    if output
        .get("success")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return CodingValidationStatus::Passed;
    }
    if output
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return CodingValidationStatus::TimedOut;
    }
    if matches!(status, AgentToolStatus::Blocked) {
        return CodingValidationStatus::FailedEnvironment;
    }
    let combined = command_failure_text(summary, output);
    if command_setup_failure(&combined) {
        return CodingValidationStatus::FailedCommandSetup;
    }
    if environment_failure(&combined) {
        return CodingValidationStatus::FailedEnvironment;
    }
    CodingValidationStatus::FailedPatchRelevant
}

fn command_failure_text(summary: &str, output: &serde_json::Value) -> String {
    let mut parts = vec![summary.to_string()];
    for key in ["stderr", "stdout", "aggregated_output", "error", "detail"] {
        if let Some(value) = output.get(key).and_then(serde_json::Value::as_str) {
            parts.push(value.to_string());
        }
    }
    if let Some(command_output) = output.get("command_output") {
        for key in ["stderr", "stdout", "aggregated_output"] {
            if let Some(value) = command_output.get(key).and_then(serde_json::Value::as_str) {
                parts.push(value.to_string());
            }
        }
    }
    parts.join("\n").to_ascii_lowercase()
}

fn command_setup_failure(text: &str) -> bool {
    [
        "missing_command",
        "requires a string command",
        "invalid option",
        "usage:",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

fn environment_failure(text: &str) -> bool {
    [
        "command not found",
        "no such file or directory",
        "permission denied",
        "missing dependency",
        "module not found",
        "no module named",
        "cannot find module",
        "failed to download",
        "network is unreachable",
        "temporary failure in name resolution",
        "connection refused",
        "could not resolve",
        "unable to locate package",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

fn coding_blocked_reason_from_tool_result(
    status: &AgentToolStatus,
    output: &serde_json::Value,
) -> Option<CodingBlockedReason> {
    if !matches!(status, AgentToolStatus::Blocked) {
        return None;
    }
    let error = output
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match error {
        "policy_denied" => Some(CodingBlockedReason::PolicyDenied),
        "tool_not_visible" | "missing_executor" | "unknown_tool" => {
            Some(CodingBlockedReason::ToolUnavailable)
        }
        "missing_command" => Some(CodingBlockedReason::CommandSetup),
        "request_user_input_unavailable" => Some(CodingBlockedReason::NeedsUserInput),
        "command_execution_disabled" | "workspace_root_not_allowed" => {
            Some(CodingBlockedReason::Environment)
        }
        _ => Some(CodingBlockedReason::Unknown),
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
        | AgentItem::VerificationResult { item_id, .. }
        | AgentItem::CodingRunState { item_id, .. } => item_id.clone(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentConfig {
    pub provider: String,
    pub model: String,
    #[serde(default = "default_agent_config_definition")]
    pub definition: AgentDefinition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls_per_step: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_model_repair_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
    pub run_context: AgentRunContext,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_toolsets: Vec<String>,
    #[serde(default, skip_serializing_if = "AgentModuleToolProjection::is_empty")]
    pub module_tool_projection: AgentModuleToolProjection,
    #[serde(default = "default_workspace_instruction_files")]
    pub workspace_instruction_files: Vec<PathBuf>,
    #[serde(default = "default_expand_prompt_references")]
    pub expand_prompt_references: bool,
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
        let definition = AgentDefinition::coding();
        let visible_toolsets = default_visible_toolsets_for_role(&definition.role);
        Self {
            provider,
            model,
            temperature: definition.temperature,
            max_output_tokens: definition.max_output_tokens,
            definition,
            max_steps: None,
            max_tool_calls_per_step: None,
            max_model_repair_attempts: None,
            workspace_root: None,
            run_context: AgentRunContext::default(),
            visible_toolsets,
            module_tool_projection: AgentModuleToolProjection::default(),
            workspace_instruction_files: default_workspace_instruction_files(),
            expand_prompt_references: default_expand_prompt_references(),
            prompt_reference_max_bytes: None,
            prompt_reference_max_entries: None,
            context_budget: AgentContextBudget::default(),
        }
    }

    pub fn with_agent_definition(mut self, definition: AgentDefinition) -> Self {
        let previous_default_toolsets = default_visible_toolsets_for_role(&self.definition.role);
        self.temperature = definition
            .temperature
            .or(definition.model_policy.temperature);
        self.max_output_tokens = definition
            .max_output_tokens
            .or(definition.model_policy.max_output_tokens);
        if !definition.tool_surface.visible_toolsets.is_empty() {
            self.visible_toolsets = definition.tool_surface.visible_toolsets.clone();
        } else if self.visible_toolsets == previous_default_toolsets {
            self.visible_toolsets = default_visible_toolsets_for_role(&definition.role);
        }
        if self.max_steps.is_none() {
            self.max_steps = definition.runtime_limits.max_steps;
        }
        if self.max_tool_calls_per_step.is_none() {
            self.max_tool_calls_per_step = definition.runtime_limits.max_tool_calls_per_step;
        }
        if self.max_model_repair_attempts.is_none() {
            self.max_model_repair_attempts = definition.runtime_limits.max_model_repair_attempts;
        }
        self.definition = definition;
        self
    }

    pub fn with_temperature(mut self, temperature: Option<f32>) -> Self {
        self.temperature = temperature;
        self
    }

    pub fn with_max_output_tokens(mut self, max_output_tokens: Option<u32>) -> Self {
        self.max_output_tokens = max_output_tokens;
        self
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
        let workspace_root = workspace_root.into();
        if self.run_context.workspace.is_none() {
            self.run_context.workspace = Some(WorkspaceRef::local_path(workspace_root.clone()));
        }
        self.workspace_root = Some(workspace_root);
        self
    }

    pub fn with_run_context(mut self, run_context: AgentRunContext) -> Self {
        self.run_context = run_context;
        self
    }

    pub fn with_task_ref(mut self, task: AgentTaskRef) -> Self {
        self.run_context.task = Some(task);
        self
    }

    pub fn with_workspace_ref(mut self, workspace: WorkspaceRef) -> Self {
        self.run_context.workspace = Some(workspace);
        self
    }

    pub fn with_prompt_reference_expansion(mut self, enabled: bool) -> Self {
        self.expand_prompt_references = enabled;
        self
    }

    pub fn with_visible_toolsets(
        mut self,
        toolsets: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.visible_toolsets = toolsets.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_module_tool_projection(mut self, projection: AgentModuleToolProjection) -> Self {
        self.module_tool_projection = projection;
        self
    }

    pub fn with_placement(mut self, placement: AgentPlacementMetadata) -> Self {
        self.run_context.placement = Some(placement);
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

fn default_agent_config_definition() -> AgentDefinition {
    AgentDefinition::coding()
}

fn default_visible_toolsets_for_role(role: &crate::AgentRole) -> Vec<String> {
    let toolsets: &[&str] = match role {
        crate::AgentRole::Main => &["core", "interactive", "config", "delegation", "perception"],
        crate::AgentRole::Background => &[
            "core",
            "interactive",
            "config",
            "delegation",
            "perception",
            "cluster",
            "sifs",
        ],
        crate::AgentRole::Deliberation | crate::AgentRole::Evolution => {
            &["core", "coding", "local", "interactive", "discovery"]
        }
    };
    toolsets.iter().copied().map(str::to_string).collect()
}

fn default_workspace_instruction_files() -> Vec<PathBuf> {
    vec![PathBuf::from("AGENTS.md")]
}

fn default_expand_prompt_references() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentRun {
    pub final_decision: AgentLoopDecision,
    pub stop_reason: AgentStopReason,
    #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
    pub run_context: AgentRunContext,
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
    pub coding_state: Option<CodingRunState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_evidence_bundle: Option<AgentCandidateEvidenceBundle>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentStopReason {
    FinalDecision,
    SilentAsyncUpdate,
    CodingStopRecommended,
    MaxStepsReached,
    Aborted,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentOp {
    pub op_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    pub action: AgentOpAction,
}

impl AgentOp {
    pub fn new(action: AgentOpAction) -> Self {
        Self {
            op_id: TaskId::generate(),
            timeout_ms: None,
            action,
        }
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn start_turn(input: AgentLoopInput) -> Self {
        Self::new(AgentOpAction::StartTurn { input })
    }

    pub fn continue_turn(input: AgentLoopInput) -> Self {
        Self::new(AgentOpAction::ContinueTurn { input })
    }

    pub fn steer(message: impl Into<String>) -> Self {
        Self::new(AgentOpAction::Steer {
            message: message.into(),
        })
    }

    pub fn follow_up(message: impl Into<String>) -> Self {
        Self::new(AgentOpAction::FollowUp {
            message: message.into(),
        })
    }

    pub fn compact(reason: impl Into<String>) -> Self {
        Self::new(AgentOpAction::Compact {
            reason: reason.into(),
        })
    }

    pub fn abort(reason: impl Into<String>) -> Self {
        Self::new(AgentOpAction::Abort {
            reason: reason.into(),
        })
    }

    pub fn cancel_active_turn(reason: impl Into<String>) -> Self {
        Self::new(AgentOpAction::CancelActiveTurn {
            reason: reason.into(),
        })
    }

    pub fn snapshot() -> Self {
        Self::new(AgentOpAction::Snapshot)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentOpAction {
    StartTurn { input: AgentLoopInput },
    ContinueTurn { input: AgentLoopInput },
    Steer { message: String },
    FollowUp { message: String },
    Compact { reason: String },
    Abort { reason: String },
    CancelActiveTurn { reason: String },
    Snapshot,
}

impl AgentOpAction {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::StartTurn { .. } => "start_turn",
            Self::ContinueTurn { .. } => "continue_turn",
            Self::Steer { .. } => "steer",
            Self::FollowUp { .. } => "follow_up",
            Self::Compact { .. } => "compact",
            Self::Abort { .. } => "abort",
            Self::CancelActiveTurn { .. } => "cancel_active_turn",
            Self::Snapshot => "snapshot",
        }
    }

    pub fn is_turn(&self) -> bool {
        matches!(self, Self::StartTurn { .. } | Self::ContinueTurn { .. })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentOpOutcome {
    pub op_id: TaskId,
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<AgentRun>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentEvent {
    pub event_id: TaskId,
    pub op_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "time::serde::rfc3339")]
    pub emitted_at: time::OffsetDateTime,
    pub payload: AgentEventPayload,
}

impl AgentEvent {
    pub fn new(op_id: TaskId, session_id: SessionId, payload: AgentEventPayload) -> Self {
        Self {
            event_id: TaskId::generate(),
            op_id,
            session_id,
            emitted_at: time::OffsetDateTime::now_utc(),
            payload,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentEventPayload {
    OperationQueued {
        sequence: u64,
        action: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
    OperationRejected {
        sequence: u64,
        action: String,
        reason: String,
    },
    TurnStarted {
        provider: String,
        model: String,
        role: AgentRole,
        policy_id: String,
        continuation: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_steps: Option<usize>,
        #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
        run_context: AgentRunContext,
    },
    TurnCompleted {
        stop_reason: AgentStopReason,
        item_count: usize,
        event_count: usize,
        model_turn_count: usize,
        tool_result_count: usize,
        #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
        run_context: AgentRunContext,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        emitted_event_ids: Vec<EventId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        candidate_evidence_bundle_id: Option<TaskId>,
    },
    SteeringQueued {
        steering: AgentSteeringMessage,
    },
    FollowUpQueued {
        follow_up: AgentFollowUpMessage,
    },
    Compacted {
        summary: AgentCompactionSummary,
        item_count: usize,
    },
    Aborted {
        abort: AgentAbortRecord,
    },
    CancellationRequested {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        active_op_id: Option<TaskId>,
    },
    TurnTimedOut {
        sequence: u64,
        active_op_id: TaskId,
        timeout_ms: u64,
        reason: String,
    },
    TaskStatusChanged {
        snapshot: AgentTaskSnapshot,
    },
    Snapshot {
        item_count: usize,
        steering_count: usize,
        follow_up_count: usize,
        aborted: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentTaskLifecycleStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTaskSnapshot {
    pub task: AgentTaskRef,
    pub status: AgentTaskLifecycleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_op_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<EventId>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
}

impl AgentTaskSnapshot {
    pub fn new(task: AgentTaskRef, status: AgentTaskLifecycleStatus) -> Self {
        Self {
            task,
            status,
            progress_summary: None,
            active_op_id: None,
            result_ref: None,
            event_ids: Vec::new(),
            updated_at: time::OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentActiveTurn {
    pub op_id: TaskId,
    pub sequence: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: time::OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub cancel_requested: bool,
}

impl AgentActiveTurn {
    pub fn new(
        op_id: TaskId,
        sequence: u64,
        timeout_ms: Option<u64>,
        task_id: Option<TaskId>,
    ) -> Self {
        Self {
            op_id,
            sequence,
            started_at: time::OffsetDateTime::now_utc(),
            timeout_ms,
            task_id,
            cancel_requested: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionControlSnapshot {
    pub session_id: SessionId,
    pub next_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn: Option<AgentActiveTurn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tasks: Vec<AgentTaskSnapshot>,
    pub buffered_event_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionControlResult {
    pub sequence: u64,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<AgentOpOutcome>,
    pub snapshot: AgentSessionControlSnapshot,
}

impl AgentSessionControlResult {
    pub fn terminal_results(&self) -> Vec<AgentTaskTerminalResult> {
        terminal_results_from_tasks(&self.snapshot.tasks)
    }

    pub fn persist_to_store(
        &self,
        store: &dyn AgentSessionStore,
    ) -> Result<(), AgentSessionStoreError> {
        for event in &self.events {
            append_control_event_to_store(store, event)?;
        }
        append_control_snapshot_to_store(store, &self.snapshot)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionControlEventCursor {
    pub offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionControlEventBatch {
    pub cursor: AgentSessionControlEventCursor,
    pub next_cursor: AgentSessionControlEventCursor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_updates: Vec<AgentTaskSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal_results: Vec<AgentTaskTerminalResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentRuntimeEventCursor {
    pub offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentRuntimeEventBatch {
    pub cursor: AgentRuntimeEventCursor,
    pub next_cursor: AgentRuntimeEventCursor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentRuntimeEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentRuntimeEvent {
    pub event_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "time::serde::rfc3339")]
    pub emitted_at: time::OffsetDateTime,
    pub payload: AgentRuntimeEventPayload,
}

impl AgentRuntimeEvent {
    pub fn new(session_id: SessionId, payload: AgentRuntimeEventPayload) -> Self {
        Self {
            event_id: TaskId::generate(),
            session_id,
            emitted_at: time::OffsetDateTime::now_utc(),
            payload,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentRuntimeEventPayload {
    WorkerStarted {
        sequence: u64,
        op_id: TaskId,
        action: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
    WorkerCompleted {
        sequence: u64,
        op_id: TaskId,
        action: String,
        stop_reason: AgentStopReason,
    },
    WorkerFailed {
        sequence: u64,
        op_id: TaskId,
        action: String,
        detail: String,
    },
    TurnFinalDecision {
        sequence: u64,
        op_id: TaskId,
        decision: AgentLoopDecision,
        stop_reason: AgentStopReason,
    },
    ModelProgress {
        sequence: u64,
        op_id: TaskId,
        event: AgentModelEvent,
    },
    ToolProgress {
        sequence: u64,
        op_id: TaskId,
        event: AgentToolProgressEvent,
    },
    ControlEvent {
        event: AgentEvent,
    },
}

#[derive(Clone, Default)]
pub struct AgentModelProgressSink {
    callback: Option<Arc<dyn Fn(AgentModelEvent) + Send + Sync>>,
}

impl AgentModelProgressSink {
    pub fn new(callback: impl Fn(AgentModelEvent) + Send + Sync + 'static) -> Self {
        Self {
            callback: Some(Arc::new(callback)),
        }
    }

    pub fn emit(&self, event: AgentModelEvent) {
        if let Some(callback) = &self.callback {
            callback(event);
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.callback.is_some()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTaskTerminalResult {
    pub task: AgentTaskRef,
    pub status: AgentTaskLifecycleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<EventId>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct AgentSessionControlPlane {
    session: AgentSession,
    next_sequence: u64,
    active_turn: Option<AgentActiveTurn>,
    active_turn_cancellation: Option<AgentCancellationToken>,
    tasks: Vec<AgentTaskSnapshot>,
    events: Vec<AgentEvent>,
}

impl AgentSessionControlPlane {
    pub fn new(session: AgentSession) -> Self {
        Self {
            session,
            next_sequence: 0,
            active_turn: None,
            active_turn_cancellation: None,
            tasks: Vec::new(),
            events: Vec::new(),
        }
    }

    pub fn from_snapshot(
        session: AgentSession,
        snapshot: AgentSessionControlSnapshot,
    ) -> Result<Self, AgentSessionControlRestoreError> {
        if snapshot.session_id != session.session_id {
            return Err(AgentSessionControlRestoreError::SessionMismatch {
                expected: session.session_id,
                actual: snapshot.session_id,
            });
        }
        Ok(Self {
            session,
            next_sequence: snapshot.next_sequence,
            active_turn: snapshot.active_turn,
            active_turn_cancellation: None,
            tasks: snapshot.tasks,
            events: Vec::new(),
        })
    }

    pub fn session(&self) -> &AgentSession {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut AgentSession {
        &mut self.session
    }

    pub fn active_turn(&self) -> Option<&AgentActiveTurn> {
        self.active_turn.as_ref()
    }

    pub fn events(&self) -> &[AgentEvent] {
        &self.events
    }

    pub fn events_since(&self, offset: usize) -> &[AgentEvent] {
        self.events.get(offset..).unwrap_or_default()
    }

    pub fn event_cursor(&self) -> AgentSessionControlEventCursor {
        AgentSessionControlEventCursor {
            offset: self.events.len(),
            last_event_id: self.events.last().map(|event| event.event_id.clone()),
        }
    }

    pub fn progress_since(
        &self,
        cursor: AgentSessionControlEventCursor,
    ) -> AgentSessionControlEventBatch {
        let start = cursor.offset.min(self.events.len());
        let events: Vec<_> = self.events[start..].to_vec();
        let task_updates = events
            .iter()
            .filter_map(|event| match &event.payload {
                AgentEventPayload::TaskStatusChanged { snapshot } => Some(snapshot.clone()),
                _ => None,
            })
            .collect();
        let terminal_results = events
            .iter()
            .filter_map(|event| match &event.payload {
                AgentEventPayload::TaskStatusChanged { snapshot } => {
                    AgentTaskTerminalResult::from_snapshot(snapshot)
                }
                _ => None,
            })
            .collect();
        AgentSessionControlEventBatch {
            cursor,
            next_cursor: self.event_cursor(),
            events,
            task_updates,
            terminal_results,
        }
    }

    pub fn drain_events(&mut self) -> Vec<AgentEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn tasks(&self) -> &[AgentTaskSnapshot] {
        &self.tasks
    }

    pub fn task_snapshot(&self, task_id: &TaskId) -> Option<&AgentTaskSnapshot> {
        self.tasks
            .iter()
            .find(|snapshot| &snapshot.task.task_id == task_id)
    }

    pub fn terminal_task_snapshots(&self) -> impl Iterator<Item = &AgentTaskSnapshot> {
        self.tasks.iter().filter(|snapshot| {
            matches!(
                snapshot.status,
                AgentTaskLifecycleStatus::Completed
                    | AgentTaskLifecycleStatus::Failed
                    | AgentTaskLifecycleStatus::Cancelled
                    | AgentTaskLifecycleStatus::TimedOut
            )
        })
    }

    pub fn terminal_result(&self, task_id: &TaskId) -> Option<AgentTaskTerminalResult> {
        self.task_snapshot(task_id)
            .and_then(AgentTaskTerminalResult::from_snapshot)
    }

    pub fn terminal_results(&self) -> Vec<AgentTaskTerminalResult> {
        terminal_results_from_tasks(&self.tasks)
    }

    pub fn persist_events_to_store(
        &self,
        store: &dyn AgentSessionStore,
        events: &[AgentEvent],
    ) -> Result<(), AgentSessionStoreError> {
        for event in events {
            append_control_event_to_store(store, event)?;
        }
        Ok(())
    }

    pub fn persist_snapshot_to_store(
        &self,
        store: &dyn AgentSessionStore,
    ) -> Result<(), AgentSessionStoreError> {
        append_control_snapshot_to_store(store, &self.snapshot())
    }

    pub fn persist_result_to_store(
        &self,
        store: &dyn AgentSessionStore,
        result: &AgentSessionControlResult,
    ) -> Result<(), AgentSessionStoreError> {
        result.persist_to_store(store)
    }

    pub fn snapshot(&self) -> AgentSessionControlSnapshot {
        AgentSessionControlSnapshot {
            session_id: self.session.session_id.clone(),
            next_sequence: self.next_sequence,
            active_turn: self.active_turn.clone(),
            tasks: self.tasks.clone(),
            buffered_event_count: self.events.len(),
        }
    }

    pub fn submit_op<M: InteractionModel>(
        &mut self,
        agent: &Agent<M>,
        op: AgentOp,
    ) -> Result<AgentSessionControlResult, AgentError> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        let op_id = op.op_id.clone();
        let action = op.action.kind().to_string();
        let is_turn = op.action.is_turn();
        let timeout_ms = op.timeout_ms;
        let cancellation_token = is_turn.then(AgentCancellationToken::new);
        let mut events = vec![agent_session_event(
            &op_id,
            &self.session.session_id,
            AgentEventPayload::OperationQueued {
                sequence,
                action: action.clone(),
                timeout_ms,
            },
        )];

        if matches!(&op.action, AgentOpAction::CancelActiveTurn { .. }) {
            let AgentOpAction::CancelActiveTurn { reason } = op.action else {
                unreachable!("cancel action already matched")
            };
            let result = self.cancel_active_turn(sequence, &op_id, &action, reason, events);
            let mut result = result;
            self.events.extend(result.events.iter().cloned());
            result.snapshot = self.snapshot();
            return Ok(result);
        }

        if is_turn && self.active_turn.is_some() {
            events.push(agent_session_event(
                &op_id,
                &self.session.session_id,
                AgentEventPayload::OperationRejected {
                    sequence,
                    action,
                    reason: "session already has an active turn".to_string(),
                },
            ));
            self.events.extend(events.iter().cloned());
            return Ok(AgentSessionControlResult {
                sequence,
                accepted: false,
                events,
                outcome: None,
                snapshot: self.snapshot(),
            });
        }

        let task = if is_turn {
            agent.config.run_context.task.clone()
        } else {
            None
        };
        if is_turn {
            self.active_turn = Some(AgentActiveTurn::new(
                op_id.clone(),
                sequence,
                timeout_ms,
                task.as_ref().map(|task| task.task_id.clone()),
            ));
            self.active_turn_cancellation = cancellation_token.clone();
            if let Some(task) = task.clone() {
                let event = self.set_task_status(
                    &op_id,
                    task,
                    AgentTaskLifecycleStatus::Running,
                    Some("turn running".to_string()),
                    Some(op_id.clone()),
                    None,
                    Vec::new(),
                );
                events.push(event);
            }
        }

        let started = std::time::Instant::now();
        let outcome = agent.handle_op_with_cancellation(
            &mut self.session,
            op,
            cancellation_token.unwrap_or_else(AgentCancellationToken::new),
        )?;
        events.extend(outcome.events.iter().cloned());

        if is_turn {
            self.active_turn = None;
            self.active_turn_cancellation = None;
            if let Some(task) = task {
                let timed_out = timeout_ms
                    .map(|timeout_ms| {
                        started.elapsed() >= std::time::Duration::from_millis(timeout_ms)
                    })
                    .unwrap_or(false);
                if timed_out {
                    if let Some(timeout_ms) = timeout_ms {
                        events.push(agent_session_event(
                            &op_id,
                            &self.session.session_id,
                            AgentEventPayload::TurnTimedOut {
                                sequence,
                                active_op_id: op_id.clone(),
                                timeout_ms,
                                reason: "turn exceeded configured timeout".to_string(),
                            },
                        ));
                    }
                }
                let status = if timed_out {
                    AgentTaskLifecycleStatus::TimedOut
                } else {
                    task_status_for_run(&outcome.run)
                };
                let progress_summary = outcome.run.as_ref().map(|run| {
                    format!(
                        "turn completed with {:?} after {} model turn(s) and {} tool result(s)",
                        run.stop_reason,
                        run.model_turns.len(),
                        run.tool_results.len()
                    )
                });
                let result_ref = outcome
                    .run
                    .as_ref()
                    .and_then(|run| run.candidate_evidence_bundle.as_ref())
                    .map(|bundle| bundle.bundle_id.as_str().to_string());
                let event_ids = outcome
                    .run
                    .as_ref()
                    .map(|run| {
                        run.events
                            .iter()
                            .map(|event| event.event_id.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                events.push(self.set_task_status(
                    &op_id,
                    task,
                    status,
                    progress_summary,
                    None,
                    result_ref,
                    event_ids,
                ));
            }
        }

        self.events.extend(events.iter().cloned());
        let result = AgentSessionControlResult {
            sequence,
            accepted: true,
            events,
            outcome: Some(outcome),
            snapshot: self.snapshot(),
        };
        Ok(result)
    }

    pub fn cleanup_timed_out_turn(
        &mut self,
        reason: impl Into<String>,
    ) -> Option<AgentSessionControlResult> {
        let active_turn = self.active_turn.clone()?;
        let timeout_ms = active_turn.timeout_ms?;
        let elapsed_ms = (time::OffsetDateTime::now_utc() - active_turn.started_at)
            .whole_milliseconds()
            .max(0) as u64;
        if elapsed_ms < timeout_ms {
            return None;
        }
        if let Some(token) = &self.active_turn_cancellation {
            token.cancel();
        }
        self.active_turn_cancellation = None;
        self.active_turn = None;
        let op_id = active_turn.op_id.clone();
        let reason = reason.into();
        let mut events = vec![agent_session_event(
            &op_id,
            &self.session.session_id,
            AgentEventPayload::TurnTimedOut {
                sequence: active_turn.sequence,
                active_op_id: active_turn.op_id.clone(),
                timeout_ms,
                reason: reason.clone(),
            },
        )];
        if let Some(task_id) = active_turn.task_id
            && let Some(task) = self
                .tasks
                .iter()
                .find(|snapshot| snapshot.task.task_id == task_id)
                .map(|snapshot| snapshot.task.clone())
        {
            events.push(self.set_task_status(
                &op_id,
                task,
                AgentTaskLifecycleStatus::TimedOut,
                Some(reason),
                None,
                None,
                Vec::new(),
            ));
        }
        self.events.extend(events.iter().cloned());
        let result = AgentSessionControlResult {
            sequence: active_turn.sequence,
            accepted: true,
            events,
            outcome: None,
            snapshot: self.snapshot(),
        };
        Some(result)
    }

    fn cancel_active_turn(
        &mut self,
        sequence: u64,
        op_id: &TaskId,
        action: &str,
        reason: String,
        mut events: Vec<AgentEvent>,
    ) -> AgentSessionControlResult {
        if let Some(token) = &self.active_turn_cancellation {
            token.cancel();
        }
        self.active_turn_cancellation = None;
        if let Some(active_turn) = self.active_turn.as_mut() {
            active_turn.cancel_requested = true;
        }
        let Some(active_turn) = self.active_turn.take() else {
            events.push(agent_session_event(
                op_id,
                &self.session.session_id,
                AgentEventPayload::OperationRejected {
                    sequence,
                    action: action.to_string(),
                    reason: "session has no active turn".to_string(),
                },
            ));
            return AgentSessionControlResult {
                sequence,
                accepted: false,
                events,
                outcome: None,
                snapshot: self.snapshot(),
            };
        };

        events.push(agent_session_event(
            op_id,
            &self.session.session_id,
            AgentEventPayload::CancellationRequested {
                reason: reason.clone(),
                active_op_id: Some(active_turn.op_id.clone()),
            },
        ));
        if let Some(task_id) = active_turn.task_id
            && let Some(task) = self
                .tasks
                .iter()
                .find(|snapshot| snapshot.task.task_id == task_id)
                .map(|snapshot| snapshot.task.clone())
        {
            events.push(self.set_task_status(
                op_id,
                task,
                AgentTaskLifecycleStatus::Cancelled,
                Some(reason),
                None,
                None,
                Vec::new(),
            ));
        }
        AgentSessionControlResult {
            sequence,
            accepted: true,
            events,
            outcome: None,
            snapshot: self.snapshot(),
        }
    }

    fn set_task_status(
        &mut self,
        op_id: &TaskId,
        task: AgentTaskRef,
        status: AgentTaskLifecycleStatus,
        progress_summary: Option<String>,
        active_op_id: Option<TaskId>,
        result_ref: Option<String>,
        event_ids: Vec<EventId>,
    ) -> AgentEvent {
        let snapshot = AgentTaskSnapshot {
            task,
            status,
            progress_summary,
            active_op_id,
            result_ref,
            event_ids,
            updated_at: time::OffsetDateTime::now_utc(),
        };
        if let Some(existing) = self
            .tasks
            .iter_mut()
            .find(|existing| existing.task.task_id == snapshot.task.task_id)
        {
            *existing = snapshot.clone();
        } else {
            self.tasks.push(snapshot.clone());
        }
        agent_session_event(
            op_id,
            &self.session.session_id,
            AgentEventPayload::TaskStatusChanged { snapshot },
        )
    }
}

pub struct AgentSessionRuntime<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    agent: Arc<Agent<M>>,
    state: Arc<Mutex<AgentSessionRuntimeState>>,
}

struct AgentSessionRuntimeState {
    control: AgentSessionControlPlane,
    runtime_events: Vec<AgentRuntimeEvent>,
    completed_turns: Vec<AgentOpOutcome>,
    worker: Option<thread::JoinHandle<()>>,
}

impl<M> AgentSessionRuntime<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    pub fn new(agent: Agent<M>, session: AgentSession) -> Self {
        Self::from_arc(Arc::new(agent), session)
    }

    pub fn from_arc(agent: Arc<Agent<M>>, session: AgentSession) -> Self {
        Self {
            agent,
            state: Arc::new(Mutex::new(AgentSessionRuntimeState {
                control: AgentSessionControlPlane::new(session),
                runtime_events: Vec::new(),
                completed_turns: Vec::new(),
                worker: None,
            })),
        }
    }

    pub fn submit_op(&self, op: AgentOp) -> Result<AgentSessionControlResult, AgentError> {
        if op.action.is_turn() {
            return self.submit_turn_op(op);
        }
        let mut state = self.lock_state()?;
        let result = state.control.submit_op(self.agent.as_ref(), op)?;
        state.append_control_runtime_events(&result.events);
        Ok(result)
    }

    pub fn interrupt_active_turn(
        &self,
        reason: impl Into<String>,
    ) -> Result<AgentSessionControlResult, AgentError> {
        self.submit_op(AgentOp::cancel_active_turn(reason))
    }

    pub fn join_active_turn(&self) -> Result<(), AgentError> {
        let worker = {
            let mut state = self.lock_state()?;
            state.worker.take()
        };
        if let Some(worker) = worker {
            join_agent_session_worker(worker)?;
        }
        Ok(())
    }

    pub fn session_snapshot(&self) -> Result<AgentSession, AgentError> {
        Ok(self.lock_state()?.control.session.clone())
    }

    pub fn replace_session(&self, session: AgentSession) -> Result<(), AgentError> {
        self.reap_finished_worker()?;
        let mut state = self.lock_state()?;
        if state.control.active_turn.is_some() {
            return Err(AgentError::Runtime {
                detail: "cannot replace agent session while a turn is active".to_string(),
            });
        }
        if state
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            return Err(AgentError::Runtime {
                detail: "cannot replace agent session while a worker is active".to_string(),
            });
        }
        state.control = AgentSessionControlPlane::new(session);
        state.runtime_events.clear();
        state.completed_turns.clear();
        Ok(())
    }

    pub fn control_snapshot(&self) -> Result<AgentSessionControlSnapshot, AgentError> {
        Ok(self.lock_state()?.control.snapshot())
    }

    pub fn active_turn(&self) -> Result<Option<AgentActiveTurn>, AgentError> {
        Ok(self.lock_state()?.control.active_turn.clone())
    }

    pub fn event_cursor(&self) -> Result<AgentSessionControlEventCursor, AgentError> {
        Ok(self.lock_state()?.control.event_cursor())
    }

    pub fn runtime_event_cursor(&self) -> Result<AgentRuntimeEventCursor, AgentError> {
        Ok(self.lock_state()?.runtime_event_cursor())
    }

    pub fn progress_since(
        &self,
        cursor: AgentSessionControlEventCursor,
    ) -> Result<AgentSessionControlEventBatch, AgentError> {
        Ok(self.lock_state()?.control.progress_since(cursor))
    }

    pub fn runtime_events_since(
        &self,
        cursor: AgentRuntimeEventCursor,
    ) -> Result<AgentRuntimeEventBatch, AgentError> {
        Ok(self.lock_state()?.runtime_events_since(cursor))
    }

    pub fn terminal_result(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<AgentTaskTerminalResult>, AgentError> {
        Ok(self.lock_state()?.control.terminal_result(task_id))
    }

    pub fn turn_result(&self, op_id: &TaskId) -> Result<Option<AgentOpOutcome>, AgentError> {
        Ok(self
            .lock_state()?
            .completed_turns
            .iter()
            .find(|outcome| &outcome.op_id == op_id)
            .cloned())
    }

    fn submit_turn_op(&self, op: AgentOp) -> Result<AgentSessionControlResult, AgentError> {
        self.reap_finished_worker()?;
        let mut state = self.lock_state()?;
        let sequence = state.control.next_sequence;
        state.control.next_sequence = state.control.next_sequence.saturating_add(1);
        let op_id = op.op_id.clone();
        let action = op.action.kind().to_string();
        let timeout_ms = op.timeout_ms;
        let mut events = vec![agent_session_event(
            &op_id,
            &state.control.session.session_id,
            AgentEventPayload::OperationQueued {
                sequence,
                action: action.clone(),
                timeout_ms,
            },
        )];

        if state.control.active_turn.is_some() {
            events.push(agent_session_event(
                &op_id,
                &state.control.session.session_id,
                AgentEventPayload::OperationRejected {
                    sequence,
                    action,
                    reason: "session already has an active turn".to_string(),
                },
            ));
            state.control.events.extend(events.iter().cloned());
            state.append_control_runtime_events(&events);
            return Ok(AgentSessionControlResult {
                sequence,
                accepted: false,
                events,
                outcome: None,
                snapshot: state.control.snapshot(),
            });
        }

        let task = self.agent.config.run_context.task.clone();
        let cancellation_token = AgentCancellationToken::new();
        state.control.active_turn = Some(AgentActiveTurn::new(
            op_id.clone(),
            sequence,
            timeout_ms,
            task.as_ref().map(|task| task.task_id.clone()),
        ));
        state.control.active_turn_cancellation = Some(cancellation_token.clone());
        if let Some(task) = task.clone() {
            let event = state.control.set_task_status(
                &op_id,
                task,
                AgentTaskLifecycleStatus::Running,
                Some("turn running".to_string()),
                Some(op_id.clone()),
                None,
                Vec::new(),
            );
            events.push(event);
        }
        let session_for_turn = state.control.session.clone();
        state.control.session.steering_queue.clear();
        state.control.session.follow_up_queue.clear();
        state.control.events.extend(events.iter().cloned());
        state.append_control_runtime_events(&events);
        state.push_runtime_event(AgentRuntimeEventPayload::WorkerStarted {
            sequence,
            op_id: op_id.clone(),
            action: action.clone(),
            timeout_ms,
        });
        let result = AgentSessionControlResult {
            sequence,
            accepted: true,
            events,
            outcome: None,
            snapshot: state.control.snapshot(),
        };

        let worker_state = Arc::clone(&self.state);
        let progress_state = Arc::clone(&self.state);
        let model_progress_state = Arc::clone(&self.state);
        let worker_agent = Arc::clone(&self.agent);
        let worker_op_id = op_id.clone();
        let progress_op_id = op_id.clone();
        let model_progress_op_id = op_id.clone();
        let worker_action = action;
        let worker_task = task;
        let worker_started = std::time::Instant::now();
        let progress_sink = AgentToolProgressSink::new(move |event| {
            if let Ok(mut state) = progress_state.lock() {
                state.push_runtime_event(AgentRuntimeEventPayload::ToolProgress {
                    sequence,
                    op_id: progress_op_id.clone(),
                    event,
                });
            }
        });
        let model_progress_sink = AgentModelProgressSink::new(move |event| {
            if let Ok(mut state) = model_progress_state.lock() {
                state.push_runtime_event(AgentRuntimeEventPayload::ModelProgress {
                    sequence,
                    op_id: model_progress_op_id.clone(),
                    event,
                });
            }
        });
        let worker = thread::spawn(move || {
            let mut completed_session = session_for_turn;
            let outcome = worker_agent.handle_op_with_cancellation_and_progress(
                &mut completed_session,
                op,
                cancellation_token,
                progress_sink,
                model_progress_sink,
            );
            let mut state = worker_state
                .lock()
                .expect("agent session runtime lock poisoned");
            state.finish_turn(AgentSessionRuntimeTurnCompletion {
                sequence,
                op_id: worker_op_id,
                action: worker_action,
                task: worker_task,
                timeout_ms,
                started: worker_started,
                completed_session,
                outcome,
            });
        });
        state.worker = Some(worker);
        Ok(result)
    }

    fn reap_finished_worker(&self) -> Result<(), AgentError> {
        let worker = {
            let mut state = self.lock_state()?;
            if state
                .worker
                .as_ref()
                .is_some_and(|worker| worker.is_finished())
            {
                state.worker.take()
            } else {
                None
            }
        };
        if let Some(worker) = worker {
            join_agent_session_worker(worker)?;
        }
        Ok(())
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, AgentSessionRuntimeState>, AgentError> {
        self.state.lock().map_err(|_| AgentError::Runtime {
            detail: "agent session runtime lock poisoned".to_string(),
        })
    }
}

struct AgentSessionRuntimeTurnCompletion {
    sequence: u64,
    op_id: TaskId,
    action: String,
    task: Option<AgentTaskRef>,
    timeout_ms: Option<u64>,
    started: std::time::Instant,
    completed_session: AgentSession,
    outcome: Result<AgentOpOutcome, AgentError>,
}

impl AgentSessionRuntimeState {
    fn runtime_event_cursor(&self) -> AgentRuntimeEventCursor {
        AgentRuntimeEventCursor {
            offset: self.runtime_events.len(),
            last_event_id: self
                .runtime_events
                .last()
                .map(|event| event.event_id.clone()),
        }
    }

    fn runtime_events_since(&self, cursor: AgentRuntimeEventCursor) -> AgentRuntimeEventBatch {
        let start = cursor.offset.min(self.runtime_events.len());
        let events = self.runtime_events[start..].to_vec();
        AgentRuntimeEventBatch {
            cursor,
            next_cursor: self.runtime_event_cursor(),
            events,
        }
    }

    fn push_runtime_event(&mut self, payload: AgentRuntimeEventPayload) {
        self.runtime_events.push(AgentRuntimeEvent::new(
            self.control.session.session_id.clone(),
            payload,
        ));
    }

    fn append_control_runtime_events(&mut self, events: &[AgentEvent]) {
        self.runtime_events
            .extend(events.iter().cloned().map(|event| {
                AgentRuntimeEvent::new(
                    event.session_id.clone(),
                    AgentRuntimeEventPayload::ControlEvent { event },
                )
            }));
    }

    fn finish_turn(&mut self, completion: AgentSessionRuntimeTurnCompletion) {
        self.control.active_turn = None;
        self.control.active_turn_cancellation = None;
        let mut events = Vec::new();
        match completion.outcome {
            Ok(outcome) => {
                self.replace_session_preserving_live_inputs(completion.completed_session);
                events.extend(outcome.events.iter().cloned());
                let stop_reason = outcome
                    .run
                    .as_ref()
                    .map(|run| run.stop_reason.clone())
                    .unwrap_or(AgentStopReason::Aborted);
                if let Some(run) = &outcome.run {
                    self.push_runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                        sequence: completion.sequence,
                        op_id: completion.op_id.clone(),
                        decision: run.final_decision.clone(),
                        stop_reason: stop_reason.clone(),
                    });
                }
                if let Some(task) = completion.task {
                    self.append_terminal_task_status(
                        &mut events,
                        &completion.op_id,
                        completion.sequence,
                        completion.timeout_ms,
                        completion.started,
                        task,
                        &outcome,
                    );
                }
                self.completed_turns.push(outcome.clone());
                self.push_runtime_event(AgentRuntimeEventPayload::WorkerCompleted {
                    sequence: completion.sequence,
                    op_id: completion.op_id.clone(),
                    action: completion.action.clone(),
                    stop_reason,
                });
            }
            Err(error) => {
                let detail = error.to_string();
                events.push(agent_session_event(
                    &completion.op_id,
                    &self.control.session.session_id,
                    AgentEventPayload::OperationRejected {
                        sequence: completion.sequence,
                        action: completion.action.clone(),
                        reason: format!("turn execution failed: {detail}"),
                    },
                ));
                if let Some(task) = completion.task {
                    events.push(self.control.set_task_status(
                        &completion.op_id,
                        task,
                        AgentTaskLifecycleStatus::Failed,
                        Some(detail.clone()),
                        None,
                        None,
                        Vec::new(),
                    ));
                }
                self.push_runtime_event(AgentRuntimeEventPayload::WorkerFailed {
                    sequence: completion.sequence,
                    op_id: completion.op_id.clone(),
                    action: completion.action.clone(),
                    detail,
                });
            }
        }
        self.control.events.extend(events.iter().cloned());
        self.append_control_runtime_events(&events);
    }

    fn replace_session_preserving_live_inputs(&mut self, mut completed_session: AgentSession) {
        let live_session = &mut self.control.session;
        let queued_steering = std::mem::take(&mut live_session.steering_queue);
        let queued_follow_ups = std::mem::take(&mut live_session.follow_up_queue);
        let live_abort = live_session.abort.clone();
        completed_session.steering_queue = queued_steering;
        completed_session.follow_up_queue = queued_follow_ups;
        if live_abort.is_some() {
            completed_session.abort = live_abort;
        }
        self.control.session = completed_session;
    }

    fn append_terminal_task_status(
        &mut self,
        events: &mut Vec<AgentEvent>,
        op_id: &TaskId,
        sequence: u64,
        timeout_ms: Option<u64>,
        started: std::time::Instant,
        task: AgentTaskRef,
        outcome: &AgentOpOutcome,
    ) {
        let timed_out = timeout_ms
            .map(|timeout_ms| started.elapsed() >= std::time::Duration::from_millis(timeout_ms))
            .unwrap_or(false);
        if timed_out && let Some(timeout_ms) = timeout_ms {
            events.push(agent_session_event(
                op_id,
                &self.control.session.session_id,
                AgentEventPayload::TurnTimedOut {
                    sequence,
                    active_op_id: op_id.clone(),
                    timeout_ms,
                    reason: "turn exceeded configured timeout".to_string(),
                },
            ));
        }
        let status = if timed_out {
            AgentTaskLifecycleStatus::TimedOut
        } else {
            task_status_for_run(&outcome.run)
        };
        let progress_summary = outcome.run.as_ref().map(|run| {
            format!(
                "turn completed with {:?} after {} model turn(s) and {} tool result(s)",
                run.stop_reason,
                run.model_turns.len(),
                run.tool_results.len()
            )
        });
        let result_ref = outcome
            .run
            .as_ref()
            .and_then(|run| run.candidate_evidence_bundle.as_ref())
            .map(|bundle| bundle.bundle_id.as_str().to_string());
        let event_ids = outcome
            .run
            .as_ref()
            .map(|run| {
                run.events
                    .iter()
                    .map(|event| event.event_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        events.push(self.control.set_task_status(
            op_id,
            task,
            status,
            progress_summary,
            None,
            result_ref,
            event_ids,
        ));
    }
}

fn join_agent_session_worker(worker: thread::JoinHandle<()>) -> Result<(), AgentError> {
    worker.join().map_err(|_| AgentError::Runtime {
        detail: "agent session runtime worker panicked".to_string(),
    })
}

impl AgentTaskTerminalResult {
    fn from_snapshot(snapshot: &AgentTaskSnapshot) -> Option<Self> {
        if !is_terminal_task_status(&snapshot.status) {
            return None;
        }
        Some(Self {
            task: snapshot.task.clone(),
            status: snapshot.status.clone(),
            progress_summary: snapshot.progress_summary.clone(),
            result_ref: snapshot.result_ref.clone(),
            event_ids: snapshot.event_ids.clone(),
            updated_at: snapshot.updated_at,
        })
    }
}

fn terminal_results_from_tasks(tasks: &[AgentTaskSnapshot]) -> Vec<AgentTaskTerminalResult> {
    tasks
        .iter()
        .filter_map(AgentTaskTerminalResult::from_snapshot)
        .collect()
}

fn is_terminal_task_status(status: &AgentTaskLifecycleStatus) -> bool {
    matches!(
        status,
        AgentTaskLifecycleStatus::Completed
            | AgentTaskLifecycleStatus::Failed
            | AgentTaskLifecycleStatus::Cancelled
            | AgentTaskLifecycleStatus::TimedOut
    )
}

fn append_control_event_to_store(
    store: &dyn AgentSessionStore,
    event: &AgentEvent,
) -> Result<(), AgentSessionStoreError> {
    store.append_record(&AgentSessionStoreRecord::new(
        event.session_id.clone(),
        AgentSessionStoreRecordKind::ControlEventPublished {
            event: event.clone(),
        },
    ))?;
    if let AgentEventPayload::TaskStatusChanged { snapshot } = &event.payload {
        store.append_record(&AgentSessionStoreRecord::new(
            event.session_id.clone(),
            AgentSessionStoreRecordKind::TaskStatusChanged {
                snapshot: snapshot.clone(),
            },
        ))?;
    }
    Ok(())
}

fn append_control_snapshot_to_store(
    store: &dyn AgentSessionStore,
    snapshot: &AgentSessionControlSnapshot,
) -> Result<(), AgentSessionStoreError> {
    store.append_record(&AgentSessionStoreRecord::new(
        snapshot.session_id.clone(),
        AgentSessionStoreRecordKind::ControlSnapshotSaved {
            snapshot: snapshot.clone(),
        },
    ))
}

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum AgentSessionControlRestoreError {
    #[error("session control snapshot belongs to {actual}, expected {expected}")]
    SessionMismatch {
        expected: SessionId,
        actual: SessionId,
    },
}

fn task_status_for_run(run: &Option<AgentRun>) -> AgentTaskLifecycleStatus {
    match run.as_ref().map(|run| &run.stop_reason) {
        Some(AgentStopReason::Aborted | AgentStopReason::Cancelled) => {
            AgentTaskLifecycleStatus::Cancelled
        }
        Some(AgentStopReason::MaxStepsReached) => AgentTaskLifecycleStatus::Failed,
        Some(
            AgentStopReason::FinalDecision
            | AgentStopReason::SilentAsyncUpdate
            | AgentStopReason::CodingStopRecommended,
        ) => AgentTaskLifecycleStatus::Completed,
        None => AgentTaskLifecycleStatus::Failed,
    }
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTranscriptItem {
    pub step_index: usize,
    pub role: AgentTranscriptRole,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_items: Vec<LanguageModelRequestItem>,
}

impl AgentTranscriptItem {
    pub fn user(step_index: usize, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::User,
            content: content.into(),
            tool_call_id: None,
            native_items: Vec::new(),
        }
    }

    pub fn assistant(step_index: usize, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::Assistant,
            content: content.into(),
            tool_call_id: None,
            native_items: Vec::new(),
        }
    }

    pub fn tool(step_index: usize, tool_call_id: TaskId, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::Tool,
            content: content.into(),
            tool_call_id: Some(tool_call_id),
            native_items: Vec::new(),
        }
    }

    pub fn tool_call(step_index: usize, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::ToolCall,
            content: content.into(),
            tool_call_id: None,
            native_items: Vec::new(),
        }
    }

    pub fn with_native_items(mut self, native_items: Vec<LanguageModelRequestItem>) -> Self {
        self.native_items = native_items;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentTranscriptRole {
    User,
    Assistant,
    ToolCall,
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
            AgentRole::Background => "background",
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolOrchestrationRecord {
    pub policy_id: String,
    pub policy_allowed: bool,
    pub policy_reason: String,
    pub descriptor_visible: bool,
    pub executor_available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls_per_step: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
    pub run_context: AgentRunContext,
}

pub struct AgentToolOrchestrator<'a> {
    policy: &'a AgentPolicyBundle,
    max_tool_calls_per_step: Option<usize>,
    run_context: &'a AgentRunContext,
}

impl<'a> AgentToolOrchestrator<'a> {
    pub fn new(
        policy: &'a AgentPolicyBundle,
        max_tool_calls_per_step: Option<usize>,
        run_context: &'a AgentRunContext,
    ) -> Self {
        Self {
            policy,
            max_tool_calls_per_step,
            run_context,
        }
    }

    pub fn execute(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        self.execute_with_cancellation(
            registry,
            session,
            tool_plan,
            step_index,
            call_index,
            call,
            AgentCancellationToken::new(),
        )
    }

    pub fn execute_with_cancellation(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
    ) -> AgentToolOutcome {
        self.execute_with_cancellation_and_progress(
            registry,
            session,
            tool_plan,
            step_index,
            call_index,
            call,
            cancellation_token,
            AgentToolProgressSink::default(),
        )
    }

    pub fn execute_with_cancellation_and_progress(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
    ) -> AgentToolOutcome {
        let descriptor = tool_plan.descriptor(&call.tool);
        let descriptor_visible = tool_plan.is_model_visible(&call.tool);
        let executor_available = tool_plan.is_executable(&call.tool);
        let policy_descriptor = descriptor.filter(|_| descriptor_visible);
        let policy_decision = self.policy.evaluate_tool_call(policy_descriptor, call);
        let requested_timeout_ms = call
            .input
            .get("timeout_ms")
            .and_then(serde_json::Value::as_u64);

        let (mut outcome, blocked_reason) = if !policy_decision.allowed {
            let reason = policy_decision.reason.clone();
            (
                AgentToolOutcome {
                    call_id: TaskId::generate(),
                    step_index,
                    call_index,
                    call: call.clone(),
                    status: AgentToolStatus::Blocked,
                    summary: reason.clone(),
                    output: serde_json::json!({
                        "error": "policy_denied",
                        "policy_id": self.policy.policy_id.clone(),
                        "tool": call.tool.clone(),
                        "reason": reason,
                    }),
                    event_id: None,
                    evidence: AgentToolEvidence::policy_only(false, policy_decision.reason.clone()),
                },
                Some("policy_denied".to_string()),
            )
        } else if self
            .max_tool_calls_per_step
            .is_some_and(|max_tool_calls| call_index >= max_tool_calls)
        {
            let max_tool_calls_per_step = self.max_tool_calls_per_step.unwrap_or(0);
            (
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
                },
                Some("tool_call_limit_reached".to_string()),
            )
        } else if descriptor.is_some() && !descriptor_visible {
            (
                AgentToolOutcome {
                    call_id: TaskId::generate(),
                    step_index,
                    call_index,
                    call: call.clone(),
                    status: AgentToolStatus::Blocked,
                    summary: format!(
                        "tool `{}` is not visible in the current agent tool projection",
                        call.tool
                    ),
                    output: serde_json::json!({
                        "error": "tool_not_visible",
                        "tool": call.tool.clone(),
                        "visible_toolsets": &tool_plan.visible_toolsets,
                    }),
                    event_id: None,
                    evidence: AgentToolEvidence::policy_only(
                        false,
                        "blocked because tool is outside the current model-visible projection",
                    ),
                },
                Some("tool_not_visible".to_string()),
            )
        } else if descriptor_visible && !executor_available {
            (
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
                        "tool": call.tool.clone(),
                    }),
                    event_id: None,
                    evidence: AgentToolEvidence::policy_only(
                        false,
                        "blocked because planned tool has no runtime executor",
                    ),
                },
                Some("missing_executor".to_string()),
            )
        } else if is_tool_disclosure_bridge_name(&call.tool) {
            (
                self.execute_tool_disclosure_bridge(
                    registry,
                    session,
                    tool_plan,
                    step_index,
                    call_index,
                    call,
                    cancellation_token.clone(),
                    progress_sink.clone(),
                ),
                None,
            )
        } else {
            (
                registry.execute_with_cancellation_and_progress(
                    session,
                    step_index,
                    call_index,
                    call,
                    cancellation_token,
                    progress_sink,
                ),
                None,
            )
        };

        if policy_decision.allowed && outcome.evidence.policy.allowed {
            outcome.evidence.policy = policy_decision.clone();
        }
        let record = AgentToolOrchestrationRecord {
            policy_id: self.policy.policy_id.clone(),
            policy_allowed: policy_decision.allowed,
            policy_reason: policy_decision.reason,
            descriptor_visible,
            executor_available,
            blocked_reason,
            max_tool_calls_per_step: self.max_tool_calls_per_step,
            requested_timeout_ms,
            run_context: self.run_context.clone(),
        };
        outcome.evidence = with_tool_orchestration_record(outcome.evidence, &record);
        outcome
    }

    fn execute_tool_disclosure_bridge(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
    ) -> AgentToolOutcome {
        match call.tool.as_str() {
            "tool_search" => {
                self.execute_tool_search(session, tool_plan, step_index, call_index, call)
            }
            "tool_describe" => {
                self.execute_tool_describe(session, tool_plan, step_index, call_index, call)
            }
            "tool_call" => self.execute_tool_call_bridge(
                registry,
                session,
                tool_plan,
                step_index,
                call_index,
                call,
                cancellation_token,
                progress_sink,
            ),
            _ => unreachable!("tool disclosure bridge names are matched by caller"),
        }
    }

    fn execute_tool_search(
        &self,
        _session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        let query = json_input_string(call, "query");
        let include_bridge_tools = json_input_bool(call, "include_bridge_tools").unwrap_or(false);
        let max_results = json_input_usize(call, "max_results").unwrap_or(20).min(50);
        let matches = tool_plan
            .descriptors_matching(query.as_deref(), include_bridge_tools)
            .into_iter()
            .take(max_results)
            .map(|entry| {
                let descriptor = &entry.descriptor;
                serde_json::json!({
                    "name": descriptor.spec.name,
                    "description": descriptor.spec.description,
                    "toolsets": descriptor.toolsets,
                    "capabilities": descriptor.capabilities,
                    "source": entry.source,
                    "executable": entry.executable,
                    "model_visible": tool_plan.is_model_visible(&descriptor.spec.name),
                })
            })
            .collect::<Vec<_>>();
        let match_count = matches.len();
        let output = serde_json::json!({
            "query": query,
            "max_results": max_results,
            "include_bridge_tools": include_bridge_tools,
            "count": match_count,
            "registry_generation": tool_plan.registry_generation,
            "visible_toolsets": &tool_plan.visible_toolsets,
            "matches": matches,
        });
        let evidence =
            AgentToolEvidence::policy_only(true, "inspected planned agent tool descriptors")
                .with_output_value(&output, false)
                .with_record_value(
                    "tool_disclosure",
                    serde_json::json!({
                        "operation": "search",
                        "query": output.get("query").cloned().unwrap_or(serde_json::Value::Null),
                        "count": output.get("count").cloned().unwrap_or(serde_json::Value::Null),
                    }),
                );
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: AgentToolStatus::Succeeded,
            summary: format!("found {match_count} planned tool(s)"),
            output,
            event_id: None,
            evidence,
        }
    }

    fn execute_tool_describe(
        &self,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        let Some(tool_name) = json_input_string(call, "tool") else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "tool_describe requires a string tool",
                serde_json::json!({ "error": "missing_tool" }),
            );
        };
        let Some(entry) = tool_plan.entry(&tool_name) else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("tool `{tool_name}` is not planned"),
                serde_json::json!({ "error": "unknown_planned_tool", "tool": tool_name }),
            );
        };
        let output = serde_json::json!({
            "tool": tool_name,
            "descriptor": entry.descriptor,
            "source": entry.source,
            "executable": entry.executable,
            "model_visible": tool_plan.is_model_visible(&entry.descriptor.spec.name),
            "registry_generation": tool_plan.registry_generation,
            "visible_toolsets": &tool_plan.visible_toolsets,
        });
        let evidence =
            AgentToolEvidence::policy_only(true, "inspected planned agent tool descriptor")
                .with_output_value(&output, false)
                .with_record_value(
                    "tool_disclosure",
                    serde_json::json!({
                        "operation": "describe",
                        "tool": entry.descriptor.spec.name,
                    }),
                );
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: AgentToolStatus::Succeeded,
            summary: format!("described tool `{}`", entry.descriptor.spec.name),
            output,
            event_id: None,
            evidence,
        }
    }

    fn execute_tool_call_bridge(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
    ) -> AgentToolOutcome {
        let Some(target_tool) = json_input_string(call, "tool") else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "tool_call requires a string tool",
                serde_json::json!({ "error": "missing_tool" }),
            );
        };
        if is_tool_disclosure_bridge_name(&target_tool) {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "tool_call cannot recursively call disclosure bridge tools",
                serde_json::json!({ "error": "recursive_tool_disclosure", "tool": target_tool }),
            );
        }
        let Some(entry) = tool_plan.entry(&target_tool) else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("tool `{target_tool}` is not planned"),
                serde_json::json!({ "error": "unknown_planned_tool", "tool": target_tool }),
            );
        };
        let target_input = call
            .input
            .get("input")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        if !target_input.is_object() {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "tool_call input must be an object",
                serde_json::json!({
                    "error": "invalid_target_input",
                    "tool": target_tool,
                }),
            );
        }
        if !entry.executable {
            return bridge_blocked_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("tool `{target_tool}` is planned but has no executor attached"),
                serde_json::json!({
                    "error": "missing_executor",
                    "tool": target_tool,
                    "source": entry.source,
                }),
            );
        }
        let target_call = AgentToolCall {
            call_id: None,
            tool: target_tool.clone(),
            input: target_input,
            r#async: json_input_bool(call, "async").unwrap_or(false),
            summary: json_input_string(call, "summary").unwrap_or_else(|| {
                format!("called through progressive tool disclosure for `{target_tool}`")
            }),
        };
        let target_policy = self
            .policy
            .evaluate_tool_call(Some(&entry.descriptor), &target_call);
        if !target_policy.allowed {
            return bridge_blocked_outcome(
                session,
                step_index,
                call_index,
                call,
                target_policy.reason.clone(),
                serde_json::json!({
                    "error": "policy_denied",
                    "policy_id": self.policy.policy_id,
                    "tool": target_tool,
                    "reason": target_policy.reason,
                }),
            );
        }
        let target_outcome = registry.execute_with_cancellation_and_progress(
            session,
            step_index,
            call_index,
            &target_call,
            cancellation_token,
            progress_sink,
        );
        let target_record = AgentToolOrchestrationRecord {
            policy_id: self.policy.policy_id.clone(),
            policy_allowed: target_policy.allowed,
            policy_reason: target_policy.reason,
            descriptor_visible: tool_plan.is_model_visible(&target_tool),
            executor_available: entry.executable,
            blocked_reason: (target_outcome.status == AgentToolStatus::Blocked)
                .then_some("target_blocked".to_string()),
            max_tool_calls_per_step: self.max_tool_calls_per_step,
            requested_timeout_ms: target_call
                .input
                .get("timeout_ms")
                .and_then(serde_json::Value::as_u64),
            run_context: self.run_context.clone(),
        };
        let mut evidence = target_outcome.evidence.clone();
        evidence = with_tool_disclosure_metadata(
            evidence,
            serde_json::json!({
                "operation": "call",
                "target_tool": target_tool,
                "target_source": entry.source,
                "target_model_visible": tool_plan.is_model_visible(&target_call.tool),
                "target_orchestration": target_record,
            }),
        );
        let output = tool_call_bridge_output(&target_outcome);
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: target_outcome.status,
            summary: format!(
                "tool_call executed `{}`: {}",
                target_call.tool, target_outcome.summary
            ),
            output,
            event_id: None,
            evidence,
        }
    }
}

fn json_input_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

fn json_input_bool(call: &AgentToolCall, key: &str) -> Option<bool> {
    call.input.get(key).and_then(serde_json::Value::as_bool)
}

fn json_input_usize(call: &AgentToolCall, key: &str) -> Option<usize> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn bridge_failed_outcome(
    _session: &InteractionSession,
    step_index: usize,
    call_index: usize,
    call: &AgentToolCall,
    summary: impl Into<String>,
    output: serde_json::Value,
) -> AgentToolOutcome {
    let summary = summary.into();
    AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index,
        call_index,
        call: call.clone(),
        status: AgentToolStatus::Failed,
        summary: summary.clone(),
        output: output.clone(),
        event_id: None,
        evidence: AgentToolEvidence::policy_only(true, "tool disclosure bridge failed")
            .with_output_value(&output, false),
    }
}

fn bridge_blocked_outcome(
    _session: &InteractionSession,
    step_index: usize,
    call_index: usize,
    call: &AgentToolCall,
    summary: impl Into<String>,
    output: serde_json::Value,
) -> AgentToolOutcome {
    let summary = summary.into();
    AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index,
        call_index,
        call: call.clone(),
        status: AgentToolStatus::Blocked,
        summary: summary.clone(),
        output: output.clone(),
        event_id: None,
        evidence: AgentToolEvidence::policy_only(false, summary).with_output_value(&output, false),
    }
}

fn cancelled_tool_outcome(
    step_index: usize,
    call_index: usize,
    call: &AgentToolCall,
    reason: impl Into<String>,
) -> AgentToolOutcome {
    let reason = reason.into();
    let output = serde_json::json!({
        "error": "tool_cancelled",
        "tool": call.tool.clone(),
        "reason": reason,
    });
    AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index,
        call_index,
        call: call.clone(),
        status: AgentToolStatus::Cancelled,
        summary: format!("tool `{}` cancelled", call.tool),
        output: output.clone(),
        event_id: None,
        evidence: AgentToolEvidence::policy_only(false, "turn cancellation requested")
            .with_output_value(&output, false),
    }
}

fn tool_call_bridge_output(target_outcome: &AgentToolOutcome) -> serde_json::Value {
    let marker = serde_json::json!({
        "target_tool": target_outcome.call.tool,
        "target_status": target_outcome.status,
        "target_summary": target_outcome.summary,
    });
    match target_outcome.output.clone() {
        serde_json::Value::Object(mut object) => {
            object.insert("_tool_call".to_string(), marker);
            serde_json::Value::Object(object)
        }
        output => serde_json::json!({
            "_tool_call": marker,
            "target_output": output,
        }),
    }
}

fn with_tool_disclosure_metadata(
    mut evidence: AgentToolEvidence,
    disclosure: serde_json::Value,
) -> AgentToolEvidence {
    let mut metadata = evidence
        .metadata
        .as_object()
        .cloned()
        .unwrap_or_else(serde_json::Map::new);
    metadata.insert("tool_disclosure".to_string(), disclosure.clone());
    evidence.metadata = serde_json::Value::Object(metadata);
    evidence.with_record_value("tool_disclosure", disclosure)
}

fn with_tool_orchestration_record(
    mut evidence: AgentToolEvidence,
    record: &AgentToolOrchestrationRecord,
) -> AgentToolEvidence {
    let record_value =
        serde_json::to_value(record).expect("tool orchestration record serializes to JSON");
    let mut metadata = evidence
        .metadata
        .as_object()
        .cloned()
        .unwrap_or_else(serde_json::Map::new);
    metadata.insert("tool_orchestration".to_string(), record_value);
    evidence.metadata = serde_json::Value::Object(metadata);
    evidence
}

pub struct Agent<M> {
    model: M,
    config: AgentConfig,
    policy: AgentPolicyBundle,
    tools: AgentToolRegistry,
    context_assembler: ContextAssembler,
    compactor: AgentCompactor,
    compaction_engine: Arc<RwLock<Box<dyn AgentCompactionEngine<M>>>>,
    trace_store: Box<dyn AgentTraceStore>,
    session_store: Box<dyn AgentSessionStore>,
}

impl<M: InteractionModel> Agent<M> {
    pub fn new(model: M, provider: impl Into<String>, model_id: impl Into<String>) -> Self {
        let provider = provider.into();
        let model_id = model_id.into();
        let config = AgentConfig::new(provider, model_id);
        let policy = AgentPolicyBundle::default_for_role(config.definition.role.clone());
        Self {
            model,
            config,
            policy,
            tools: AgentToolRegistry::new(),
            context_assembler: ContextAssembler,
            compactor: AgentCompactor::default(),
            compaction_engine: Arc::new(RwLock::new(Box::new(
                CodexStyleAgentCompactionEngine::default(),
            ))),
            trace_store: Box::new(NoopAgentTraceStore),
            session_store: Box::new(NoopAgentSessionStore),
        }
    }

    pub fn with_config(mut self, config: AgentConfig) -> Self {
        let previous_default_policy =
            AgentPolicyBundle::default_for_role(self.config.definition.role.clone());
        if self.policy == previous_default_policy {
            self.policy = AgentPolicyBundle::default_for_role(config.definition.role.clone());
        }
        self.config = config;
        self
    }

    pub fn with_agent_definition(mut self, definition: AgentDefinition) -> Self {
        let previous_default_policy =
            AgentPolicyBundle::default_for_role(self.config.definition.role.clone());
        if self.policy == previous_default_policy {
            self.policy = AgentPolicyBundle::default_for_role(definition.role.clone());
        }
        if self.compactor == AgentCompactor::default() {
            self.compactor.retained_tail_count = definition.compaction.retained_tail_items;
        }
        self.config = self.config.with_agent_definition(definition);
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

    pub fn with_compaction_engine<E>(mut self, engine: E) -> Self
    where
        E: AgentCompactionEngine<M> + 'static,
    {
        self.compaction_engine = Arc::new(RwLock::new(Box::new(engine)));
        self
    }

    pub fn replace_compaction_engine<E>(&self, engine: E) -> Result<(), AgentError>
    where
        E: AgentCompactionEngine<M> + 'static,
    {
        let mut slot = self
            .compaction_engine
            .write()
            .map_err(|_| AgentError::Runtime {
                detail: "compaction engine lock poisoned".to_string(),
            })?;
        *slot = Box::new(engine);
        Ok(())
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

    pub fn handle_op(
        &self,
        session: &mut AgentSession,
        op: AgentOp,
    ) -> Result<AgentOpOutcome, AgentError> {
        self.handle_op_with_cancellation(session, op, AgentCancellationToken::new())
    }

    pub fn handle_op_with_cancellation(
        &self,
        session: &mut AgentSession,
        op: AgentOp,
        cancellation_token: AgentCancellationToken,
    ) -> Result<AgentOpOutcome, AgentError> {
        self.handle_op_with_cancellation_and_progress(
            session,
            op,
            cancellation_token,
            AgentToolProgressSink::default(),
            AgentModelProgressSink::default(),
        )
    }

    pub fn handle_op_with_cancellation_and_progress(
        &self,
        session: &mut AgentSession,
        op: AgentOp,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
    ) -> Result<AgentOpOutcome, AgentError> {
        let op_id = op.op_id;
        let session_id = session.session_id.clone();
        let mut events = Vec::new();
        let mut run = None;

        match op.action {
            AgentOpAction::StartTurn { input } => {
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::TurnStarted {
                        provider: self.config.provider.clone(),
                        model: self.config.model.clone(),
                        role: self.config.definition.role.clone(),
                        policy_id: self.policy.policy_id.clone(),
                        continuation: false,
                        max_steps: self.config.max_steps,
                        run_context: self.config.run_context.clone(),
                    },
                ));
                let completed_run = self.start_turn_with_cancellation_and_progress(
                    session,
                    &input,
                    cancellation_token.clone(),
                    progress_sink.clone(),
                    model_progress_sink.clone(),
                )?;
                events.push(turn_completed_agent_event(
                    &op_id,
                    &session_id,
                    &completed_run,
                ));
                run = Some(completed_run);
            }
            AgentOpAction::ContinueTurn { input } => {
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::TurnStarted {
                        provider: self.config.provider.clone(),
                        model: self.config.model.clone(),
                        role: self.config.definition.role.clone(),
                        policy_id: self.policy.policy_id.clone(),
                        continuation: true,
                        max_steps: self.config.max_steps,
                        run_context: self.config.run_context.clone(),
                    },
                ));
                let completed_run = self.continue_turn_with_cancellation_and_progress(
                    session,
                    &input,
                    cancellation_token.clone(),
                    progress_sink.clone(),
                    model_progress_sink.clone(),
                )?;
                events.push(turn_completed_agent_event(
                    &op_id,
                    &session_id,
                    &completed_run,
                ));
                run = Some(completed_run);
            }
            AgentOpAction::Steer { message } => {
                let steering = self.steer(session, message)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::SteeringQueued { steering },
                ));
            }
            AgentOpAction::FollowUp { message } => {
                let follow_up = self.follow_up(session, message)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::FollowUpQueued { follow_up },
                ));
            }
            AgentOpAction::Compact { reason } => {
                let summary = self.compact(session, reason)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::Compacted {
                        summary,
                        item_count: session.transcript.items.len(),
                    },
                ));
            }
            AgentOpAction::Abort { reason } => {
                let abort = self.abort(session, reason)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::Aborted { abort },
                ));
            }
            AgentOpAction::CancelActiveTurn { reason } => {
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::CancellationRequested {
                        reason,
                        active_op_id: None,
                    },
                ));
            }
            AgentOpAction::Snapshot => {
                self.save_session(session)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::Snapshot {
                        item_count: session.transcript.items.len(),
                        steering_count: session.steering_queue.len(),
                        follow_up_count: session.follow_up_queue.len(),
                        aborted: session.is_aborted(),
                    },
                ));
            }
        }

        Ok(AgentOpOutcome {
            op_id,
            session_id,
            events,
            run,
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
        self.start_turn_with_cancellation(session, input, AgentCancellationToken::new())
    }

    pub fn start_turn_with_cancellation(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn_with_cancellation_and_progress(
            session,
            input,
            cancellation_token,
            AgentToolProgressSink::default(),
            AgentModelProgressSink::default(),
        )
    }

    pub fn start_turn_with_cancellation_and_progress(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
    ) -> Result<AgentRun, AgentError> {
        if session.is_aborted() {
            return Ok(AgentRun {
                final_decision: AgentLoopDecision::silent(),
                stop_reason: AgentStopReason::Aborted,
                run_context: self.config.run_context.clone(),
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
                coding_state: None,
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
                role: self.config.definition.role.clone(),
                policy_id: self.policy.policy_id.clone(),
                max_steps: self.config.max_steps,
                run_context: self.config.run_context.clone(),
            },
        )?;
        let run = self.run_with_session_context_and_cancellation(
            input,
            transcript_prefix,
            session_projection,
            &steering,
            &follow_ups,
            cancellation_token,
            progress_sink,
            model_progress_sink,
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
        self.continue_turn_with_cancellation(session, input, AgentCancellationToken::new())
    }

    pub fn continue_turn_with_cancellation(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn_with_cancellation(session, input, cancellation_token)
    }

    pub fn continue_turn_with_cancellation_and_progress(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn_with_cancellation_and_progress(
            session,
            input,
            cancellation_token,
            progress_sink,
            model_progress_sink,
        )
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
        let retained_tail_count = self
            .compactor
            .retained_tail_count
            .unwrap_or(source_transcript.items.len());
        let cancellation_token = AgentCancellationToken::new();
        let summary = self.compact_source_transcript(
            &source_transcript,
            reason.into(),
            source_transcript.items.len(),
            retained_tail_count,
            session.projection.as_ref(),
            &session.steering_queue,
            &session.follow_up_queue,
            AgentCompactionTrigger::Manual,
            &cancellation_token,
        )?;
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

    #[allow(clippy::too_many_arguments)]
    fn compact_source_transcript(
        &self,
        source_transcript: &AgentTranscript,
        reason: String,
        token_estimate_before: usize,
        retained_tail_count: usize,
        previous_projection: Option<&AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
        trigger: AgentCompactionTrigger,
        cancellation_token: &AgentCancellationToken,
    ) -> Result<AgentCompactionSummary, AgentError> {
        let engine = self
            .compaction_engine
            .read()
            .map_err(|_| AgentError::Runtime {
                detail: "compaction engine lock poisoned".to_string(),
            })?;
        engine.compact(
            &self.model,
            AgentCompactionRequest {
                config: &self.config,
                projection_builder: &self.compactor,
                source_transcript,
                reason,
                trigger,
                token_estimate_before,
                retained_tail_count,
                previous_projection,
                steering,
                follow_ups,
                cancellation_token,
            },
        )
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
        session_projection: Option<AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
    ) -> Result<AgentRun, AgentError> {
        self.run_with_session_context_and_cancellation(
            input,
            transcript_prefix,
            session_projection,
            steering,
            follow_ups,
            AgentCancellationToken::new(),
            AgentToolProgressSink::default(),
            AgentModelProgressSink::default(),
        )
    }

    fn run_with_session_context_and_cancellation(
        &self,
        input: &AgentLoopInput,
        transcript_prefix: Vec<AgentTranscriptItem>,
        mut session_projection: Option<AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
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
                        "run_context": &self.config.run_context,
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
                run_context: self.config.run_context.clone(),
                transcript: Vec::new(),
                model_turns: Vec::new(),
                tool_results: Vec::new(),
                items: vec![item],
                context_reports: Vec::new(),
                model_events: Vec::new(),
                compactions: Vec::new(),
                events: vec![event],
                coding_state: None,
                candidate_evidence_bundle: None,
            });
        }

        self.trace(
            &input.session.session_id,
            AgentTraceRecordKind::RunStarted {
                provider: self.config.provider.clone(),
                model: self.config.model.clone(),
                role: self.config.definition.role.clone(),
                policy_id: self.policy.policy_id.clone(),
                max_steps: self.config.max_steps,
                run_context: self.config.run_context.clone(),
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
                    "role": self.config.definition.role,
                    "policy_id": self.policy.policy_id,
                    "max_steps": self.config.max_steps,
                    "max_tool_calls_per_step": self.config.max_tool_calls_per_step,
                    "run_context": &self.config.run_context,
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
        let mut latest_coding_state: Option<CodingRunState> = None;

        'turn_loop: loop {
            if cancellation_token.is_cancelled() {
                stop_reason = AgentStopReason::Cancelled;
                let event = AgentModelEvent::Aborted {
                    reason: "turn cancellation requested".to_string(),
                };
                model_progress_sink.emit(event.clone());
                model_events.push(event);
                break;
            }
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
                    let compaction = self.compact_source_transcript(
                        &typed_transcript,
                        "context budget threshold reached".to_string(),
                        turn_context.budget_estimate.total_bytes,
                        retained_tail_count,
                        session_projection.as_ref(),
                        steering,
                        follow_ups,
                        AgentCompactionTrigger::PreSamplingBudget,
                        &cancellation_token,
                    )?;
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
                        "run_context": &turn_context.run_context,
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
                let response = match self.model.complete_streaming_events_with_cancellation(
                    &sampled_request,
                    &cancellation_token,
                    &mut |event| {
                        if let Some(model_event) = AgentModelEvent::from_stream_event(
                            step_index,
                            &sampled_request.provider,
                            &sampled_request.model,
                            &event,
                        ) {
                            model_progress_sink.emit(model_event);
                        }
                        language_model_stream_events.push(event);
                    },
                ) {
                    Ok(response) => response,
                    Err(error) if cancellation_token.is_cancelled() => {
                        stop_reason = AgentStopReason::Cancelled;
                        let event = AgentModelEvent::Aborted {
                            reason: format!(
                                "turn cancellation requested during model response: {error}"
                            ),
                        };
                        self.trace(
                            &input.session.session_id,
                            AgentTraceRecordKind::ModelEvent {
                                event: event.clone(),
                            },
                        )?;
                        model_progress_sink.emit(event.clone());
                        model_events.push(event);
                        break 'turn_loop;
                    }
                    Err(error) => return Err(AgentError::Model(error)),
                };
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
                    &self.config.definition.output_format,
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
            if cancellation_token.is_cancelled() {
                stop_reason = AgentStopReason::Cancelled;
                let event = AgentModelEvent::Aborted {
                    reason: "turn cancellation requested after model response".to_string(),
                };
                model_progress_sink.emit(event.clone());
                model_events.push(event);
                break;
            }
            transcript.push(
                AgentTranscriptItem::assistant(step_index, response.content.clone())
                    .with_native_items(native_tool_call_items_from_decision(&decision)),
            );
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
            let tool_orchestrator = AgentToolOrchestrator::new(
                &self.policy,
                self.config.max_tool_calls_per_step,
                &self.config.run_context,
            );
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
                let mut outcome = if cancellation_token.is_cancelled() {
                    cancelled_tool_outcome(
                        step_index,
                        call_index,
                        call,
                        "turn cancellation requested before tool execution",
                    )
                } else {
                    tool_orchestrator.execute_with_cancellation_and_progress(
                        &self.tools,
                        &input.session,
                        &tool_plan,
                        step_index,
                        call_index,
                        call,
                        cancellation_token.clone(),
                        progress_sink.clone(),
                    )
                };
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
                    native_call_id: outcome.call.call_id.clone(),
                    tool_name: Some(outcome.call.tool.clone()),
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
                if outcome.status == AgentToolStatus::Cancelled {
                    stop_reason = AgentStopReason::Cancelled;
                }
                tool_results.push(outcome);
            }
            let tool_message_id = step_results
                .first()
                .map(|result| result.call_id.clone())
                .unwrap_or_else(TaskId::generate);
            transcript.push(
                AgentTranscriptItem::tool(
                    step_index,
                    tool_message_id,
                    render_tool_results(step_index, &step_results)?,
                )
                .with_native_items(native_tool_output_items_from_outcomes(&step_results)),
            );
            let coding_state = coding_state_from_items(&items);
            if !coding_state.is_empty() {
                let coding_state_item = AgentItem::CodingRunState {
                    item_id: TaskId::generate(),
                    step_index,
                    state: coding_state.clone(),
                };
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Item {
                        item: coding_state_item.clone(),
                    },
                )?;
                if let Some(transcript_item) =
                    project_agent_item_to_transcript_item(coding_state_item.clone()).transpose()?
                {
                    transcript.push(transcript_item);
                }
                items.push(coding_state_item);
                let coding_state_event = caused_by_latest_event(
                    agent_event(
                        &input.session,
                        "agent.coding.state.updated",
                        0.59,
                        serde_json::json!({
                            "step_index": step_index,
                            "state": coding_state.clone(),
                        }),
                    ),
                    input,
                );
                self.trace(
                    &input.session.session_id,
                    AgentTraceRecordKind::Event {
                        event: coding_state_event.clone(),
                    },
                )?;
                events.push(coding_state_event);
                if coding_state.stop_recommendation == CodingStopDecision::FinishWithPatch {
                    final_decision = coding_final_decision_from_state(&coding_state);
                    stop_reason = AgentStopReason::CodingStopRecommended;
                    latest_coding_state = Some(coding_state);
                    break;
                }
                latest_coding_state = Some(coding_state);
            }
            if stop_reason == AgentStopReason::Cancelled {
                break;
            }
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
                    "run_context": &self.config.run_context,
                    "coding_state": &latest_coding_state,
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

        let candidate_evidence_bundle =
            build_candidate_evidence_bundle(&input.context_attachments, &final_decision, &items);

        Ok(AgentRun {
            final_decision,
            stop_reason,
            run_context: self.config.run_context.clone(),
            transcript,
            model_turns,
            tool_results,
            items,
            context_reports,
            model_events,
            compactions,
            events,
            coding_state: latest_coding_state,
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
        let tool_plan = self.tool_plan(input);
        let all_tool_descriptors = match self.config.definition.output_format {
            AgentOutputFormat::JsonDecision => tool_plan.model_visible_descriptors(),
            AgentOutputFormat::Text => Vec::new(),
        };
        let (tool_descriptors, policy_omissions) =
            self.policy.filter_tool_descriptors(all_tool_descriptors);
        let mut context = self.context_assembler.assemble(
            input,
            self.config.definition.role.clone(),
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
        let turn_context_message = render_turn_context_message(input, turn_context)?;
        let mut messages = vec![LanguageModelMessage {
            role: LanguageModelRole::System,
            content: model_protocol_system_instructions(
                &turn_context.provider,
                &turn_context.model,
                &turn_context.system_instructions,
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
            response_format: self
                .config
                .definition
                .output_format
                .language_model_response_format(),
            metadata: serde_json::json!({
                "loop": "agent",
                "agent_name": self.config.definition.name,
                "agent_role": self.config.definition.role,
                "agent_output_format": self.config.definition.output_format,
                "step_index": step_index,
                "session_id": input.session.session_id,
                "event_id": input.latest_event.event_id,
                "context_report_id": turn_context.report.report_id,
                "context_budget_estimate": turn_context.budget_estimate,
                "agent_policy_id": self.policy.policy_id.clone(),
                "run_context": &turn_context.run_context,
            }),
        })
    }

    fn tool_plan(&self, input: &AgentLoopInput) -> AgentToolPlan {
        AgentToolPlanner::plan(
            self.tools.descriptors(),
            &self.config.module_tool_projection,
            input.tool_specs.clone(),
            self.config.definition.tools.clone(),
            input.tool_specs.is_empty(),
            self.config.visible_toolsets.clone(),
            self.tools.generation(),
        )
    }
}

fn model_protocol_system_instructions(provider: &str, model: &str, base: &str) -> String {
    if !prefers_native_tool_protocol(provider, model) {
        return base.to_string();
    }
    format!(
        "{base}\n\nModel protocol hint: native tool calls are available for this provider/model. Prefer the provider-native tool protocol for tool use; use JSON `tool_calls` only as a fallback. After native tool results are returned, continue with the required final response format."
    )
}

fn prefers_native_tool_protocol(provider: &str, model: &str) -> bool {
    let provider = provider.to_ascii_lowercase();
    let model = model.to_ascii_lowercase();
    match provider.as_str() {
        "openai" | "openai-api" => model.contains("gpt") || model.starts_with('o'),
        "codex" => model.contains("gpt") || model.contains("codex"),
        "deepseek" | "deepseek-api" => model.contains("deepseek"),
        _ => false,
    }
}

fn language_model_message_from_transcript_item(
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

fn language_model_request_items_from_transcript_item(
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
    render_agent_system_prompt_with_descriptors(&AgentDefinition::coding(), tools)
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
            native_call_id,
            tool_name,
            status,
            summary,
            output,
            evidence,
            ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_tool_result",
                "status": &status,
                "summary": &summary,
                "output": model_visible_tool_output(None, &output),
                "evidence": model_visible_tool_evidence(&evidence),
            }))
            .map(|content| {
                let native_items = native_call_id
                    .zip(tool_name)
                    .map(|(native_call_id, tool_name)| {
                        vec![LanguageModelRequestItem::tool_call_output(
                            native_call_id,
                            tool_name,
                            serde_json::json!({
                                "status": &status,
                                "summary": &summary,
                                "output": model_visible_tool_output(None, &output),
                                "evidence": model_visible_tool_evidence(&evidence),
                            }),
                        )]
                    })
                    .unwrap_or_default();
                AgentTranscriptItem::tool(step_index, call_id, content)
                    .with_native_items(native_items)
            })
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::ToolCall {
            step_index, call, ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_tool_call",
                "tool": &call.tool,
                "input": &call.input,
                "summary": &call.summary,
                "call_id": &call.call_id,
            }))
            .map(|content| {
                AgentTranscriptItem::tool_call(step_index, content)
                    .with_native_items(native_tool_call_item_from_call(&call).into_iter().collect())
            })
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
        AgentItem::CodingRunState {
            step_index, state, ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "coding_run_state",
                "state": state,
            }))
            .map(|content| AgentTranscriptItem::user(step_index, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::AssistantReasoningSummary { .. }
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
        "run_context": &turn_context.run_context,
        "session_projection": turn_context.session_projection,
        "current_work": turn_context
            .session_projection
            .as_ref()
            .map(|projection| &projection.working_state),
        "workspace_facts": turn_context.workspace_facts,
        "loaded_instruction_files": turn_context.loaded_instruction_files,
        "prompt_references": turn_context.prompt_references,
        "context_attachments": turn_context.context_attachments,
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
        "call_id": &outcome.call_id,
        "native_call_id": &outcome.call.call_id,
        "step_index": outcome.step_index,
        "call_index": outcome.call_index,
        "call": &outcome.call,
        "status": &outcome.status,
        "summary": &outcome.summary,
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

fn native_tool_call_items_from_decision(
    decision: &AgentLoopDecision,
) -> Vec<LanguageModelRequestItem> {
    decision
        .tool_calls
        .iter()
        .filter_map(native_tool_call_item_from_call)
        .collect()
}

fn native_tool_call_item_from_call(call: &AgentToolCall) -> Option<LanguageModelRequestItem> {
    Some(LanguageModelRequestItem::tool_call(
        call.call_id.as_ref()?.clone(),
        call.tool.clone(),
        call.input.clone(),
    ))
}

fn native_tool_output_items_from_outcomes(
    outcomes: &[AgentToolOutcome],
) -> Vec<LanguageModelRequestItem> {
    outcomes
        .iter()
        .filter_map(native_tool_output_item_from_outcome)
        .collect()
}

fn native_tool_output_item_from_outcome(
    outcome: &AgentToolOutcome,
) -> Option<LanguageModelRequestItem> {
    Some(LanguageModelRequestItem::tool_call_output(
        outcome.call.call_id.as_ref()?.clone(),
        outcome.call.tool.clone(),
        model_visible_tool_outcome(outcome),
    ))
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
    if looks_like_command_output(tool_name, object)
        && let Some(diagnostics) = model_visible_command_output_diagnostics(object)
    {
        object.insert("command_output_diagnostics".to_string(), diagnostics);
    }
    value
}

fn looks_like_command_output(
    tool_name: Option<&str>,
    object: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    tool_name == Some("run_command")
        || object.contains_key("program")
            && object.contains_key("args")
            && object.contains_key("status")
            && object.contains_key("stdout")
            && object.contains_key("stderr")
}

fn model_visible_command_output_diagnostics(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    let success = object
        .get("success")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let timed_out = object
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if success && !timed_out {
        return None;
    }
    let stderr = object
        .get("stderr")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let stdout = object
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if stderr.is_empty() && stdout.is_empty() && !timed_out {
        return None;
    }
    Some(serde_json::json!({
        "kind": if timed_out { "command_timed_out" } else { "command_failed" },
        "status": object.get("status").cloned().unwrap_or(serde_json::Value::Null),
        "timed_out": timed_out,
        "stderr_excerpt": model_visible_text_excerpt(stderr, 4096),
        "stdout_excerpt": model_visible_text_excerpt(stdout, 2048),
        "stderr_truncated": object.get("stderr_truncated").cloned().unwrap_or(serde_json::Value::Bool(false)),
        "stdout_truncated": object.get("stdout_truncated").cloned().unwrap_or(serde_json::Value::Bool(false)),
    }))
}

fn model_visible_text_excerpt(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}\n[truncated]", &text[..end])
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
    output_format: &AgentOutputFormat,
) -> Result<AgentLoopDecision, AgentError> {
    let language_model_tools: Vec<_> = available_tool_specs
        .iter()
        .map(language_model_tool_spec_from_agent_tool)
        .collect();
    let tool_output = collect_language_model_tool_calls(
        &response.provider,
        &response.model,
        response,
        stream_events,
        &language_model_tools,
    );
    let tool_calls = agent_tool_calls_from_language_model(&tool_output.tool_calls);
    if !tool_calls.is_empty() {
        let text = tool_output
            .text
            .as_deref()
            .unwrap_or(&response.content)
            .trim();
        return Ok(AgentLoopDecision {
            response_mode: crate::AgentLoopResponseMode::Defer,
            text: if text.is_empty() {
                "The model requested tool execution.".to_string()
            } else {
                text.to_string()
            },
            tool_calls,
            background_handoff: None,
            next_state: crate::AgentInteractionState::Waiting,
        });
    }
    match output_format {
        AgentOutputFormat::JsonDecision => parse_agent_loop_decision_strict(&response.content),
        AgentOutputFormat::Text => parse_agent_loop_response(output_format, &response.content),
    }
}

fn agent_tool_calls_from_language_model(calls: &[LanguageModelToolCall]) -> Vec<AgentToolCall> {
    calls
        .iter()
        .map(|call| AgentToolCall {
            call_id: call.call_id.clone(),
            tool: call.name.clone(),
            input: call.input.clone(),
            r#async: false,
            summary: call
                .summary
                .clone()
                .unwrap_or_else(|| format!("Native model tool call `{}`.", call.name)),
        })
        .collect()
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
        background_handoff: value
            .get("background_handoff")
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
            "background_handoff": null,
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
    verification_command_from_tool_output(&outcome.output)
        .unwrap_or_else(|| outcome.call.tool.clone())
}

fn verification_command_from_tool_output(output: &serde_json::Value) -> Option<String> {
    let Some(program) = output.get("program").and_then(serde_json::Value::as_str) else {
        return None;
    };
    let args = output
        .get("args")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if matches!(program, "bash" | "sh" | "cmd") {
        for (index, arg) in args.iter().enumerate() {
            if matches!(*arg, "-lc" | "-c" | "/C")
                && let Some(command) = args.get(index + 1)
            {
                return Some(command.to_string());
            }
        }
    }
    if args.is_empty() {
        Some(program.to_string())
    } else {
        Some(format!("{program} {}", args.join(" ")))
    }
}

fn build_candidate_evidence_bundle(
    context_attachments: &[AgentContextAttachment],
    final_decision: &AgentLoopDecision,
    items: &[AgentItem],
) -> Option<AgentCandidateEvidenceBundle> {
    if context_attachments.is_empty() {
        return None;
    }
    let mut evidence = Vec::new();
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
        context_kind: context_attachments
            .first()
            .map(|context| context.kind.clone()),
        context_summary: context_attachments
            .first()
            .map(|context| context.summary.clone()),
        final_response: final_decision.text.clone(),
        evidence,
    })
}

fn agent_session_event(
    op_id: &TaskId,
    session_id: &SessionId,
    payload: AgentEventPayload,
) -> AgentEvent {
    AgentEvent::new(op_id.clone(), session_id.clone(), payload)
}

fn turn_completed_agent_event(
    op_id: &TaskId,
    session_id: &SessionId,
    run: &AgentRun,
) -> AgentEvent {
    agent_session_event(
        op_id,
        session_id,
        AgentEventPayload::TurnCompleted {
            stop_reason: run.stop_reason.clone(),
            item_count: run.items.len(),
            event_count: run.events.len(),
            model_turn_count: run.model_turns.len(),
            tool_result_count: run.tool_results.len(),
            run_context: run.run_context.clone(),
            emitted_event_ids: run
                .events
                .iter()
                .map(|event| event.event_id.clone())
                .collect(),
            candidate_evidence_bundle_id: run
                .candidate_evidence_bundle
                .as_ref()
                .map(|bundle| bundle.bundle_id.clone()),
        },
    )
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
    use sinomo_lm::{LanguageModelError, LanguageModelResponseFormat};
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
        requests: Mutex<Vec<LanguageModelRequest>>,
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
                requests: Mutex::new(Vec::new()),
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
            request: &LanguageModelRequest,
            on_event: &mut dyn FnMut(LanguageModelStreamEvent),
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
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

    #[derive(Clone, Default)]
    struct CancellationAwareModel {
        started: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl CancellationAwareModel {
        fn started(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
            self.started.clone()
        }
    }

    impl InteractionModel for CancellationAwareModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            Ok(LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content: r#"{
                    "response_mode": "answer",
                    "text": "not cancelled",
                    "tool_calls": [],
                    "background_handoff": null,
                    "next_state": "listening"
                }"#
                .to_string(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }

        fn complete_streaming_events_with_cancellation(
            &self,
            request: &LanguageModelRequest,
            cancellation_token: &AgentCancellationToken,
            on_event: &mut dyn FnMut(LanguageModelStreamEvent),
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.started
                .store(true, std::sync::atomic::Ordering::SeqCst);
            on_event(LanguageModelStreamEvent::Started {
                provider: request.provider.clone(),
                model: request.model.clone(),
            });
            while !cancellation_token.is_cancelled() {
                thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(LanguageModelError::ProviderInvocationFailed {
                provider: request.provider.clone(),
                model: request.model.clone(),
                detail: "cancelled by test token".to_string(),
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
            context_attachments: Vec::new(),
            tool_specs: default_coding_agent_tools(),
        }
    }

    fn coding_evidence(evidence: &AgentToolEvidence) -> AgentCodingToolEvidence {
        evidence
            .record(AGENT_CODING_EVIDENCE_KIND)
            .unwrap_or_default()
    }

    fn run_git_for_test<const N: usize>(root: &Path, args: [&str; N]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(root)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("git command");
        assert!(status.success(), "git command failed");
    }

    #[test]
    fn agent_config_defaults_to_unlimited_runtime_limits() {
        let config = AgentConfig::new("fake", "fake");

        assert_eq!(config.definition.name, "coding-agent");
        assert_eq!(config.definition.role, AgentRole::Deliberation);
        assert_eq!(config.max_steps, None);
        assert_eq!(config.max_tool_calls_per_step, None);
        assert_eq!(config.max_model_repair_attempts, None);
        assert_eq!(config.max_output_tokens, None);
        assert!(config.expand_prompt_references);
        assert_eq!(config.prompt_reference_max_bytes, None);
        assert_eq!(config.prompt_reference_max_entries, None);
        assert_eq!(config.context_budget.max_context_bytes, None);
        assert_eq!(config.context_budget.compaction_threshold_bytes(), None);
    }

    #[test]
    fn agent_config_workspace_root_populates_local_workspace_ref() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-workspace");

        let config = AgentConfig::new("fake", "fake").with_workspace_root(&root);

        assert_eq!(config.workspace_root.as_deref(), Some(root.path()));
        let workspace = config
            .run_context
            .workspace
            .as_ref()
            .expect("workspace ref");
        assert_eq!(workspace.local_root.as_deref(), Some(root.path()));
        assert_eq!(workspace.write_policy.as_deref(), Some("local"));
    }

    #[test]
    fn agent_run_context_reaches_model_metadata_and_events() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-run-context");
        let task = AgentTaskRef::new("coding", "fix run context propagation")
            .with_owner_role(AgentRole::Deliberation);
        let placement = AgentPlacementMetadata::local_current_process()
            .with_origin_node("node.origin")
            .with_sandbox_class("local-process");
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Context recorded.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]);
        let config = AgentConfig::new("fake", "fake")
            .with_workspace_root(&root)
            .with_task_ref(task.clone())
            .with_placement(placement);
        let agent = Agent::new(model, "fake", "fake").with_config(config);
        let input = agent_input("record context");
        let mut session =
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

        let outcome = agent
            .handle_op(&mut session, AgentOp::start_turn(input))
            .expect("op");
        let run = outcome.run.as_ref().expect("run");

        assert_eq!(
            run.run_context.task.as_ref().map(|task| &task.task_id),
            Some(&task.task_id)
        );
        assert!(matches!(
            &outcome.events[0].payload,
            AgentEventPayload::TurnStarted {
                role: AgentRole::Deliberation,
                policy_id,
                run_context,
                ..
            } if policy_id == "deliberation-default"
                && run_context.task.as_ref().map(|task| &task.task_id) == Some(&task.task_id)
        ));
        assert!(matches!(
            &outcome.events[1].payload,
            AgentEventPayload::TurnCompleted {
                run_context,
                emitted_event_ids,
                ..
            } if run_context.task.as_ref().map(|task| &task.task_id) == Some(&task.task_id)
                && !emitted_event_ids.is_empty()
        ));

        let request = agent.model.requests.lock().expect("requests")[0].clone();
        assert_eq!(request.metadata["run_context"]["task"]["kind"], "coding");
        assert_eq!(
            request.metadata["run_context"]["placement"]["origin_node"],
            "node.origin"
        );
        let request_text = request
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(request_text.contains("\"run_context\""));
        assert!(request_text.contains("fix run context propagation"));
        assert!(request_text.contains("local-process"));
        assert_eq!(
            run.events[0].payload["run_context"]["task"]["kind"],
            "coding"
        );
    }

    #[test]
    fn custom_agent_definition_controls_context_role_prompt_and_default_tools() {
        let mut input = agent_input("draft docs");
        input.tool_specs = Vec::new();
        let definition = AgentDefinition::custom(
            "docs-agent",
            AgentRole::Background,
            "Draft operator docs from visible evidence.",
        )
        .with_tool(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "draft_doc",
            "Draft a documentation note from current evidence.",
            false,
        ));
        let config = AgentConfig::new("fake", "fake").with_agent_definition(definition);
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            "fake",
            "fake",
        )
        .with_config(config);

        let context = agent.assemble_context(&input, &[], 0).expect("context");
        let request = agent.request(&input, &[], 0).expect("request");
        let visible_tools: Vec<_> = context
            .available_tool_specs
            .iter()
            .map(|descriptor| descriptor.spec.name.as_str())
            .collect();

        assert_eq!(context.role, AgentRole::Background);
        assert!(context.system_instructions.contains("Draft operator docs"));
        assert!(visible_tools.contains(&"draft_doc"));
        assert!(!visible_tools.contains(&"read_file"));
        assert_eq!(
            request.response_format,
            LanguageModelResponseFormat::JsonObject
        );
        assert!(
            request.messages[0]
                .content
                .contains("Draft operator docs from visible evidence")
        );
        assert_eq!(
            request
                .metadata
                .get("agent_name")
                .and_then(serde_json::Value::as_str),
            Some("docs-agent")
        );
        assert_eq!(
            request
                .metadata
                .get("agent_role")
                .and_then(serde_json::Value::as_str),
            Some("background")
        );
    }

    #[test]
    fn text_output_agent_definition_returns_plain_final_answer() {
        let mut input = agent_input("status");
        input.tool_specs = Vec::new();
        let definition =
            AgentDefinition::custom("status-agent", AgentRole::Main, "Answer status plainly.")
                .with_output_format(AgentOutputFormat::Text);
        let config = AgentConfig::new("fake", "fake").with_agent_definition(definition);
        let agent =
            Agent::new(SequencedModel::new(["Plain status."]), "fake", "fake").with_config(config);

        let run = agent.run(&input).expect("run");
        let request = agent.model.requests.lock().expect("requests")[0].clone();

        assert_eq!(run.stop_reason, AgentStopReason::FinalDecision);
        assert_eq!(
            run.final_decision.response_mode,
            crate::AgentLoopResponseMode::Answer
        );
        assert_eq!(run.final_decision.text, "Plain status.");
        assert!(run.tool_results.is_empty());
        assert_eq!(request.response_format, LanguageModelResponseFormat::Text);
        assert_eq!(request.tool_choice, None);
        assert!(
            request.messages[0]
                .content
                .contains("Respond directly in plain text")
        );
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
        let task = AgentTaskRef::new("coding", "inspect workspace")
            .with_owner_role(AgentRole::Deliberation);
        let model = SequencedModel::new([
            r#"{
                "response_mode": "defer",
                "text": "I will inspect the file.",
                "tool_calls": [{
                    "tool": "read_file",
                    "input": {"path": "Cargo.toml", "timeout_ms": 123},
                    "async": false,
                    "summary": "Inspect workspace metadata."
                }],
                "background_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The workspace evidence is available.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let config = AgentConfig::new("fake", "fake")
            .with_task_ref(task.clone())
            .with_placement(AgentPlacementMetadata::local_current_process());
        let agent = Agent::new(model, "fake", "fake")
            .with_config(config)
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
        let orchestration = run.tool_results[0]
            .evidence
            .metadata
            .get("tool_orchestration")
            .expect("tool orchestration metadata");
        assert_eq!(
            orchestration
                .get("policy_id")
                .and_then(serde_json::Value::as_str),
            Some("deliberation-default")
        );
        assert_eq!(
            orchestration
                .get("executor_available")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            orchestration
                .get("requested_timeout_ms")
                .and_then(serde_json::Value::as_u64),
            Some(123)
        );
        assert_eq!(
            orchestration
                .get("run_context")
                .and_then(|context| context.get("task"))
                .and_then(|task| task.get("task_id"))
                .and_then(serde_json::Value::as_str),
            Some(task.task_id.as_str())
        );
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
                "background_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The workspace evidence is available.",
                "tool_calls": [],
                "background_handoff": null,
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
            "background_handoff": null,
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
            "background_handoff": null,
            "next_state": "waiting"
        }"#;
        let final_response = r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "background_handoff": null,
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
        let requests = agent.model.requests.lock().expect("requests");
        let second_items = &requests[1].items;
        assert!(second_items.iter().any(|item| {
            matches!(
                item,
                LanguageModelRequestItem::ToolCall {
                    call_id,
                    name,
                    input,
                } if call_id == "call_1" && name == "read_file" && input["path"] == "Cargo.toml"
            )
        }));
        assert!(second_items.iter().any(|item| {
            matches!(
                item,
                LanguageModelRequestItem::ToolCallOutput {
                    call_id,
                    name,
                    output,
                } if call_id == "call_1"
                    && name == "read_file"
                    && output["native_call_id"] == "call_1"
                    && output["status"] == serde_json::json!(AgentToolStatus::Succeeded)
            )
        }));
    }

    #[test]
    fn agent_consumes_native_stream_tool_calls_without_json_decision_text() {
        let final_response = r#"{
            "response_mode": "answer",
            "text": "The native tool call completed.",
            "tool_calls": [],
            "background_handoff": null,
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
        let trace_root = sinomo_testdir::TestDir::new("sinomo-agent-trace");
        let trace_path = trace_root.join("trace.jsonl");
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
                "background_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "Done.",
                "tool_calls": [],
                "background_handoff": null,
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
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-storage");
        let data_dir = SinomoDataDir::new(&data_root);
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
        assert_eq!(
            paths.sifs_evidence_path_for_session(&session_id, "apply-patch.diff"),
            format!(
                "/events/agent/evidence/{}/apply-patch.diff",
                session_id.as_str()
            )
        );

        let artifact = AgentToolArtifactRef::sifs(
            "diff",
            paths.sifs_evidence_path_for_session(&session_id, "diff.patch"),
        );
        assert!(artifact.is_sifs());
        assert_eq!(
            artifact.uri,
            format!(
                "sifs://events/agent/evidence/{}/diff.patch",
                session_id.as_str()
            )
        );
    }

    #[test]
    fn file_trace_store_can_write_to_agent_data_dir_layout() {
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-trace-layout");
        let data_dir = SinomoDataDir::new(&data_root);
        let session_id = SessionId::from_legacy_named("sess_agent_trace").expect("session");
        let store = FileAgentTraceStore::for_data_dir_session(&data_dir, &session_id);

        store
            .append(&AgentTraceRecord::new(
                session_id.clone(),
                AgentTraceRecordKind::RunStarted {
                    provider: "fake".to_string(),
                    model: "fake".to_string(),
                    role: AgentRole::Deliberation,
                    policy_id: "test-policy".to_string(),
                    max_steps: Some(1),
                    run_context: AgentRunContext::default(),
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
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-session");
        let data_dir = SinomoDataDir::new(&data_root);
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
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-session-run");
        let data_dir = SinomoDataDir::new(&data_root);
        let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "background_handoff": null,
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
    fn codex_style_compaction_uses_model_checkpoint_summary() {
        let model = SequencedModel::new(["## Current Progress\n- Summary from compaction model."]);
        let agent = Agent::new(model, "fake", "fake").with_compactor(AgentCompactor {
            retained_tail_count: Some(1),
        });
        let session_id =
            SessionId::from_legacy_named("sess_agent_model_compaction").expect("session");
        let mut session = AgentSession::new(session_id, AgentRole::Deliberation);
        session.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "old objective with critical id abc-123".to_string(),
            event_id: None,
        });
        session.transcript.push(AgentItem::AssistantText {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "working notes".to_string(),
        });

        let summary = agent
            .compact(&mut session, "checkpoint old context")
            .expect("compact");

        assert_eq!(
            summary.summary,
            "## Current Progress\n- Summary from compaction model."
        );
        assert_eq!(
            summary.strategy_id,
            crate::DEFAULT_AGENT_COMPACTION_STRATEGY_ID
        );
        assert_eq!(summary.trigger, AgentCompactionTrigger::Manual);
        assert_eq!(
            summary.summary_model,
            Some(AgentCompactionModelRef {
                provider: "fake".to_string(),
                model: "fake".to_string(),
            })
        );
        let requests = agent.model.requests.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].response_format,
            LanguageModelResponseFormat::Text
        );
        assert_eq!(
            requests[0]
                .metadata
                .get("loop")
                .and_then(serde_json::Value::as_str),
            Some("agent_compaction")
        );
        let request_text = requests[0]
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(request_text.contains("CONTEXT CHECKPOINT COMPACTION"));
        assert!(request_text.contains("abc-123"));
    }

    #[test]
    fn compaction_engine_can_be_replaced_online() {
        struct ReplacementEngine;

        impl<M: InteractionModel> AgentCompactionEngine<M> for ReplacementEngine {
            fn compact(
                &self,
                _model: &M,
                request: AgentCompactionRequest<'_>,
            ) -> Result<AgentCompactionSummary, AgentError> {
                Ok(request
                    .projection_builder
                    .compact_retaining_tail_count_with_options(
                        request.source_transcript,
                        request.reason,
                        request.token_estimate_before,
                        request.retained_tail_count,
                        request.previous_projection,
                        request.steering,
                        request.follow_ups,
                        AgentCompactionBuildOptions {
                            strategy_id: "test.replacement.compaction".to_string(),
                            trigger: request.trigger,
                            summary_override: Some("replacement engine summary".to_string()),
                            summary_model: None,
                            fallback_reason: None,
                        },
                    ))
            }
        }

        let model = SequencedModel::new(Vec::<String>::new());
        let agent = Agent::new(model, "fake", "fake").with_compactor(AgentCompactor {
            retained_tail_count: Some(1),
        });
        agent
            .replace_compaction_engine(ReplacementEngine)
            .expect("replace engine");
        let session_id =
            SessionId::from_legacy_named("sess_agent_replace_compaction").expect("session");
        let mut session = AgentSession::new(session_id, AgentRole::Deliberation);
        session.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "replace this compaction implementation".to_string(),
            event_id: None,
        });

        let summary = agent.compact(&mut session, "online swap").expect("compact");

        assert_eq!(summary.strategy_id, "test.replacement.compaction");
        assert_eq!(summary.summary, "replacement engine summary");
        assert!(agent.model.requests.lock().expect("requests").is_empty());
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
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-session-projection");
        let data_dir = SinomoDataDir::new(&data_root);
        let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Done.",
            "tool_calls": [],
            "background_handoff": null,
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
                "background_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The requested tool is not available.",
                "tool_calls": [],
                "background_handoff": null,
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
            "background_handoff": null,
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
                "background_handoff": null,
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
            call_id: None,
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
            call_id: None,
            tool: "read_file".to_string(),
            input: serde_json::json!({"path": "Cargo.toml"}),
            r#async: false,
            summary: "read".to_string(),
        };

        assert!(registry.is_empty());
        assert_eq!(registry.generation(), 0);
        assert!(registry.register(EchoTool).is_none());
        assert_eq!(registry.generation(), 1);
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
        assert_eq!(registry.generation(), 2);
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
        assert_eq!(registry.generation(), 3);
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
    fn tool_plan_tracks_generation_and_scopes_model_visible_descriptors() {
        let runtime_descriptors = vec![
            AgentToolDescriptor::new(AgentToolSpec::new(
                AgentToolKind::ReadFile,
                "read_file",
                "read a fake file",
                false,
            )),
            AgentToolDescriptor::new(AgentToolSpec::new(
                AgentToolKind::ConfigGet,
                "config_get",
                "read config",
                false,
            )),
        ];

        let plan = AgentToolPlanner::plan(
            runtime_descriptors,
            &AgentModuleToolProjection::default(),
            std::iter::empty::<AgentToolSpec>(),
            std::iter::empty::<AgentToolSpec>(),
            false,
            ["config"],
            42,
        );
        let visible_tools: Vec<_> = plan
            .model_visible_descriptors()
            .into_iter()
            .map(|descriptor| descriptor.spec.name)
            .collect();

        assert_eq!(plan.registry_generation, 42);
        assert_eq!(plan.visible_toolsets, vec!["config"]);
        assert!(plan.is_executable("read_file"));
        assert!(!plan.is_model_visible("read_file"));
        assert!(plan.is_model_visible("config_get"));
        assert_eq!(visible_tools, vec!["config_get"]);
    }

    #[test]
    fn progressive_tool_disclosure_searches_describes_and_calls_hidden_tools() {
        let registry = AgentToolRegistry::new().with_tool(EchoTool);
        let plan = AgentToolPlanner::plan(
            registry.descriptors(),
            &AgentModuleToolProjection::default(),
            std::iter::empty::<AgentToolSpec>(),
            std::iter::empty::<AgentToolSpec>(),
            false,
            ["discovery"],
            registry.generation(),
        );
        let input = agent_input("use disclosure");
        let policy = AgentPolicyBundle::default_for_role(AgentRole::Deliberation);
        let run_context = AgentRunContext::default();
        let orchestrator = AgentToolOrchestrator::new(&policy, None, &run_context);

        assert!(!plan.is_model_visible("read_file"));
        assert!(plan.is_model_visible("tool_search"));

        let search = orchestrator.execute(
            &registry,
            &input.session,
            &plan,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "tool_search".to_string(),
                input: serde_json::json!({ "query": "read" }),
                r#async: false,
                summary: "find read tool".to_string(),
            },
        );
        assert_eq!(search.status, AgentToolStatus::Succeeded);
        assert!(
            search
                .output
                .get("matches")
                .and_then(serde_json::Value::as_array)
                .expect("matches")
                .iter()
                .any(
                    |entry| entry.get("name").and_then(serde_json::Value::as_str)
                        == Some("read_file")
                        && entry
                            .get("model_visible")
                            .and_then(serde_json::Value::as_bool)
                            == Some(false)
                )
        );

        let describe = orchestrator.execute(
            &registry,
            &input.session,
            &plan,
            0,
            1,
            &AgentToolCall {
                call_id: None,
                tool: "tool_describe".to_string(),
                input: serde_json::json!({ "tool": "read_file" }),
                r#async: false,
                summary: "describe read tool".to_string(),
            },
        );
        assert_eq!(describe.status, AgentToolStatus::Succeeded);
        assert_eq!(
            describe
                .output
                .pointer("/descriptor/spec/name")
                .and_then(serde_json::Value::as_str),
            Some("read_file")
        );

        let called = orchestrator.execute(
            &registry,
            &input.session,
            &plan,
            0,
            2,
            &AgentToolCall {
                call_id: None,
                tool: "tool_call".to_string(),
                input: serde_json::json!({
                    "tool": "read_file",
                    "input": { "path": "Cargo.toml" },
                    "summary": "read through bridge"
                }),
                r#async: false,
                summary: "call hidden read tool".to_string(),
            },
        );
        assert_eq!(called.status, AgentToolStatus::Succeeded);
        assert_eq!(
            called
                .output
                .get("content")
                .and_then(serde_json::Value::as_str),
            Some("workspace evidence")
        );
        assert_eq!(
            called
                .output
                .pointer("/_tool_call/target_tool")
                .and_then(serde_json::Value::as_str),
            Some("read_file")
        );
        assert!(called.evidence.record_value("tool_disclosure").is_some());
    }

    #[test]
    fn module_tool_projection_uses_active_loader_state() {
        let module_id = TaskId::generate();
        let registration = sinomo_runtime::module::ModuleRegistration::native_rust(
            module_id.clone(),
            "0.1.0",
            "module-test",
            "sinomo-module-test",
            "module-test-v1",
            sinomo_runtime::core::TrustRing::R1TrustedRuntime,
            "module.test",
        )
        .with_surfaces(
            sinomo_runtime::module::ModuleSurfaces::default().with_object_rpc_route(
                sinomo_runtime::module::ModuleObjectRpcRoute::new(
                    "/module/test/status",
                    "module-test",
                    "status",
                    sinomo_runtime::module::ModuleRouteAuth::LocalOrClusterMember,
                ),
            ),
        );
        let projection = AgentModuleToolProjection::new()
            .with_registration(registration)
            .with_loader_state(sinomo_runtime::core::ModuleLoaderState {
                module_id: module_id.clone(),
                active_artifact_id: TaskId::generate(),
                previous_artifact_id: None,
                shadow_artifact_id: None,
                candidate_artifact_id: None,
                hot_swap_state: sinomo_runtime::core::HotSwapState::Stable,
            });
        let plan = AgentToolPlanner::plan(
            std::iter::empty::<AgentToolDescriptor>(),
            &projection,
            std::iter::empty::<AgentToolSpec>(),
            std::iter::empty::<AgentToolSpec>(),
            false,
            ["discovery"],
            0,
        );

        let projected = plan
            .entries
            .iter()
            .find(|entry| entry.descriptor.spec.name == "module_rpc_module_test_status")
            .expect("module route descriptor");
        assert_eq!(projected.source, crate::tools::AgentToolPlanSource::Module);
        assert!(!projected.executable);
        assert!(!plan.is_model_visible("module_rpc_module_test_status"));

        let registry = AgentToolRegistry::new();
        let input = agent_input("find module route");
        let policy = AgentPolicyBundle::default_for_role(AgentRole::Deliberation);
        let run_context = AgentRunContext::default();
        let orchestrator = AgentToolOrchestrator::new(&policy, None, &run_context);
        let search = orchestrator.execute(
            &registry,
            &input.session,
            &plan,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "tool_search".to_string(),
                input: serde_json::json!({ "query": "status" }),
                r#async: false,
                summary: "search modules".to_string(),
            },
        );
        assert_eq!(search.status, AgentToolStatus::Succeeded);
        assert!(
            search
                .output
                .get("matches")
                .and_then(serde_json::Value::as_array)
                .expect("matches")
                .iter()
                .any(
                    |entry| entry.get("name").and_then(serde_json::Value::as_str)
                        == Some("module_rpc_module_test_status")
                )
        );
    }

    #[test]
    fn module_tool_projection_exposes_network_cluster_health_tool() {
        let registration = sinomo_runtime::network_module::network_module_registration();
        let module_id = registration.manifest.module_id.clone();
        let projection = AgentModuleToolProjection::new()
            .with_registration(registration)
            .with_loader_state(sinomo_runtime::core::ModuleLoaderState {
                module_id,
                active_artifact_id: TaskId::generate(),
                previous_artifact_id: None,
                shadow_artifact_id: None,
                candidate_artifact_id: None,
                hot_swap_state: sinomo_runtime::core::HotSwapState::Stable,
            });
        let plan = AgentToolPlanner::plan(
            std::iter::empty::<AgentToolDescriptor>(),
            &projection,
            std::iter::empty::<AgentToolSpec>(),
            std::iter::empty::<AgentToolSpec>(),
            false,
            ["object_rpc"],
            0,
        );

        let projected = plan
            .entries
            .iter()
            .find(|entry| {
                entry.descriptor.spec.name == "module_rpc_cluster_health_cluster_health_read"
            })
            .expect("network cluster health module route descriptor");
        assert_eq!(projected.source, crate::tools::AgentToolPlanSource::Module);
        assert!(!projected.executable);
        assert!(plan.is_model_visible("module_rpc_cluster_health_cluster_health_read"));
        assert!(
            projected
                .descriptor
                .spec
                .description
                .contains(sinomo_runtime::network::CLUSTER_HEALTH_ROUTE)
        );
        assert!(
            projected
                .descriptor
                .toolsets
                .contains(&sinomo_runtime::network::SINOMO_NETWORK_MODULE_OWNER.to_string())
        );
        assert!(
            projected
                .descriptor
                .capabilities
                .contains(&AgentToolCapability::CallModule)
        );
    }

    #[test]
    fn module_tool_projection_exposes_native_dylib_declared_tools() {
        let module_id =
            sinomo_runtime::module::ModuleId::from_legacy_named("module.dynamic-native")
                .expect("module id");
        let state = sinomo_runtime::module::NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: Some(
                sinomo_runtime::module::ArtifactId::from_legacy_named(
                    "artifact.dynamic-native.test",
                )
                .expect("artifact id"),
            ),
            content_hash: "blake3:abc".to_string(),
            cache_key: "module/dynamic-native".to_string(),
            library_name: "libdynamic_native_module.so".to_string(),
            platform_triple: "x86_64-linux-unix".to_string(),
            native_abi_version: sinomo_runtime::module_api::SINOMO_NATIVE_MODULE_ABI_VERSION,
            interface_version: "sinomo.dynamic-native.v1".to_string(),
            activated_at: "2026-06-11T00:00:00Z".to_string(),
            source: "test".to_string(),
        };
        let descriptor = sinomo_runtime::module_api::NativeModuleDescriptor {
            module_id: module_id.as_str().to_string(),
            module_version: "0.1.0".to_string(),
            owner: "dynamic-native".to_string(),
            native_abi_version: sinomo_runtime::module_api::SINOMO_NATIVE_MODULE_ABI_VERSION,
            backend: sinomo_runtime::module_api::NativeModuleBackend::NativeDylib,
            interface_version: "sinomo.dynamic-native.v1".to_string(),
            platform_triple: "x86_64-linux-unix".to_string(),
            supported_operations: vec![
                "dynamic.native.inspect".to_string(),
                "dynamic.native.apply".to_string(),
            ],
            capabilities: vec![
                "dynamic.native.inspect".to_string(),
                "dynamic.native.apply".to_string(),
            ],
            surfaces: sinomo_runtime::module_api::NativeModuleSurfaces {
                object_rpc_routes: vec![
                    sinomo_runtime::module_api::NativeModuleObjectRpcRoute {
                        route: "/dynamic-native/inspect".to_string(),
                        category: "dynamic.native".to_string(),
                        operation: "dynamic.native.inspect".to_string(),
                        auth: "admin".to_string(),
                    },
                    sinomo_runtime::module_api::NativeModuleObjectRpcRoute {
                        route: "/dynamic-native/apply".to_string(),
                        category: "dynamic.native".to_string(),
                        operation: "dynamic.native.apply".to_string(),
                        auth: "admin".to_string(),
                    },
                ],
                ..Default::default()
            },
        };
        let registration =
            sinomo_runtime::module::native_dylib_module_registration_from_state_and_descriptor(
                &state,
                &descriptor,
            )
            .expect("native dylib registration");
        let module_id = registration.manifest.module_id.clone();
        let projection = AgentModuleToolProjection::new()
            .with_registration(registration)
            .with_loader_state(sinomo_runtime::core::ModuleLoaderState {
                module_id,
                active_artifact_id: TaskId::generate(),
                previous_artifact_id: None,
                shadow_artifact_id: None,
                candidate_artifact_id: None,
                hot_swap_state: sinomo_runtime::core::HotSwapState::Stable,
            });
        let plan = AgentToolPlanner::plan(
            std::iter::empty::<AgentToolDescriptor>(),
            &projection,
            std::iter::empty::<AgentToolSpec>(),
            std::iter::empty::<AgentToolSpec>(),
            false,
            ["object_rpc"],
            0,
        );

        let system_info = plan
            .entries
            .iter()
            .find(|entry| {
                entry.descriptor.spec.name == "module_rpc_dynamic_native_dynamic_native_inspect"
            })
            .expect("native dylib inspect tool");
        let shell_run = plan
            .entries
            .iter()
            .find(|entry| {
                entry.descriptor.spec.name == "module_rpc_dynamic_native_dynamic_native_apply"
            })
            .expect("native dylib apply tool");

        assert_eq!(
            system_info.source,
            crate::tools::AgentToolPlanSource::Module
        );
        assert!(plan.is_model_visible(&system_info.descriptor.spec.name));
        assert!(plan.is_model_visible(&shell_run.descriptor.spec.name));
        assert!(
            system_info
                .descriptor
                .spec
                .description
                .contains("/dynamic-native/inspect")
        );
        assert!(
            system_info
                .descriptor
                .toolsets
                .contains(&"dynamic-native".to_string())
        );
    }

    #[test]
    fn render_tool_results_keeps_large_evidence_out_of_model_context() {
        let outcome = AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index: 3,
            call_index: 0,
            call: AgentToolCall {
                call_id: None,
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
    fn render_tool_results_surfaces_failed_command_stderr() {
        let outcome = AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index: 4,
            call_index: 0,
            call: AgentToolCall {
                call_id: None,
                tool: "run_command".to_string(),
                input: serde_json::json!({ "command": "make check" }),
                r#async: false,
                summary: "run focused check".to_string(),
            },
            status: AgentToolStatus::Failed,
            summary: "make check failed with exit status Some(2)".to_string(),
            output: serde_json::json!({
                "program": "bash",
                "args": ["-lc", "make check"],
                "cwd": ".",
                "status": 2,
                "success": false,
                "timed_out": false,
                "timeout_ms": null,
                "stdout": "running checks\n",
                "stderr": "checker reported the actual failure\n",
                "stdout_truncated": false,
                "stderr_truncated": false,
                "changed_files": [],
            }),
            event_id: None,
            evidence: AgentToolEvidence::policy_only(true, "allowed"),
        };

        let rendered = render_tool_results(4, &[outcome]).expect("render");

        assert!(rendered.contains("\"command_output_diagnostics\""));
        assert!(rendered.contains("\"kind\": \"command_failed\""));
        assert!(rendered.contains("checker reported the actual failure"));
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

        let response = LanguageModelResponse {
            provider: "openai".to_string(),
            model: "gpt-5.2-codex".to_string(),
            content: String::new(),
            items: Vec::new(),
            tool_calls: vec![call],
            usage: None,
        };
        let decision = parse_agent_model_decision(
            &response,
            &[],
            &[descriptor],
            &AgentOutputFormat::JsonDecision,
        )
        .expect("decision");

        assert_eq!(decision.tool_calls.len(), 1);
        assert_eq!(decision.tool_calls[0].tool, "apply_patch");
        assert_eq!(decision.tool_calls[0].input["patch"], raw_patch);
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
                "background_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The tool is visible but has no executor.",
                "tool_calls": [],
                "background_handoff": null,
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
    fn hidden_runtime_tool_is_blocked_if_model_guesses_it() {
        let model = SequencedModel::new([
            r#"{
                "response_mode": "defer",
                "text": "I will try a hidden tool.",
                "tool_calls": [{
                    "tool": "read_file",
                    "input": {"path": "Cargo.toml"},
                    "async": false,
                    "summary": "Read the workspace."
                }],
                "background_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The tool is outside the current projection.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#,
        ]);
        let config = AgentConfig::new("fake", "fake").with_visible_toolsets(["config"]);
        let agent = Agent::new(model, "fake", "fake")
            .with_config(config)
            .with_tool_registry(AgentToolRegistry::new().with_tool(EchoTool));

        let run = agent.run(&agent_input("try hidden tool")).expect("run");
        let orchestration = run.tool_results[0]
            .evidence
            .metadata
            .get("tool_orchestration")
            .expect("tool orchestration metadata");

        assert_eq!(run.tool_results[0].status, AgentToolStatus::Blocked);
        assert_eq!(
            run.tool_results[0]
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("tool_not_visible")
        );
        assert_eq!(
            orchestration
                .get("descriptor_visible")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            orchestration
                .get("executor_available")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn default_agent_tools_match_initial_architecture_surface() {
        let default_tool_names = default_coding_agent_tools()
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();
        assert!(!default_tool_names.contains(&"changed_files".to_string()));
        assert!(!default_tool_names.contains(&"git_diff".to_string()));
        assert!(!default_tool_names.contains(&"replace_exact".to_string()));
        assert!(!default_tool_names.contains(&"replace_range".to_string()));

        let descriptors = default_agent_tool_descriptors();
        let names: Vec<_> = descriptors
            .iter()
            .map(|descriptor| descriptor.spec.name.as_str())
            .collect();

        for expected in [
            "read_file",
            "list_files",
            "search",
            "apply_patch",
            "changed_files",
            "git_diff",
            "run_command",
            "request_user_input",
        ] {
            assert!(names.contains(&expected), "{expected}");
        }

        assert!(!names.contains(&"replace_exact"));
        assert!(!names.contains(&"replace_range"));

        for expected_hidden in ["changed_files", "git_diff"] {
            let descriptor = descriptors
                .iter()
                .find(|descriptor| descriptor.spec.name == expected_hidden)
                .expect(expected_hidden);
            assert_eq!(
                descriptor
                    .toolsets
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                vec!["version_control"]
            );
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-prompt-ref");
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
    fn context_assembler_can_disable_prompt_reference_expansion() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-no-prompt-ref");
        fs::write(root.join("note.txt"), "important context\n").expect("note");
        let input = agent_input("inspect @note.txt and @missing.txt");
        let config = AgentConfig::new("fake", "fake")
            .with_workspace_root(&root)
            .with_prompt_reference_expansion(false);
        let agent = Agent::new(
            SequencedModel::new(std::iter::empty::<String>()),
            "fake",
            "fake",
        )
        .with_config(config);

        let context = agent.assemble_context(&input, &[], 0).expect("context");

        assert!(context.prompt_references.is_empty());
        assert!(
            !context
                .report
                .included
                .iter()
                .any(|source| source.kind.starts_with("prompt_reference"))
        );
        assert!(
            !context
                .report
                .omitted
                .iter()
                .any(|omission| omission.kind == "prompt_reference")
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-instructions");
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-large-instructions");
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
                "background_handoff": null,
                "next_state": "waiting"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "The command was blocked by policy.",
                "tool_calls": [],
                "background_handoff": null,
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
            call_id: None,
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
    fn local_workspace_registry_lists_files_with_bounds_and_hidden_filter() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-list-files");
        fs::create_dir_all(root.join("src")).expect("workspace");
        fs::write(root.join("README.md"), "hello").expect("readme");
        fs::write(root.join(".secret"), "hidden").expect("hidden");
        fs::write(root.join("src").join("lib.rs"), "pub fn sample() {}").expect("source");
        let registry = AgentToolRegistry::local_workspace(&root);
        let input = agent_input("list files");

        let shallow = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "list_files".to_string(),
                input: serde_json::json!({ "path": "." }),
                r#async: false,
                summary: "list root".to_string(),
            },
        );
        assert_eq!(shallow.status, AgentToolStatus::Succeeded);
        let shallow_entries = shallow
            .output
            .get("entries")
            .and_then(serde_json::Value::as_array)
            .expect("entries");
        assert!(shallow_entries.iter().any(|entry| {
            entry.get("path").and_then(serde_json::Value::as_str) == Some("README.md")
        }));
        assert!(
            shallow_entries
                .iter()
                .any(|entry| entry.get("path").and_then(serde_json::Value::as_str) == Some("src"))
        );
        assert!(
            !shallow_entries.iter().any(|entry| entry
                .get("path")
                .and_then(serde_json::Value::as_str)
                == Some(".secret"))
        );
        assert!(!shallow_entries.iter().any(|entry| {
            entry.get("path").and_then(serde_json::Value::as_str) == Some("src/lib.rs")
        }));

        let recursive = registry.execute(
            &input.session,
            0,
            1,
            &AgentToolCall {
                call_id: None,
                tool: "list_files".to_string(),
                input: serde_json::json!({
                    "path": ".",
                    "recursive": true,
                    "include_hidden": true,
                    "max_entries": 2
                }),
                r#async: false,
                summary: "list recursively".to_string(),
            },
        );
        assert_eq!(recursive.status, AgentToolStatus::Succeeded);
        assert_eq!(
            recursive
                .output
                .get("truncated")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            recursive
                .output
                .get("count")
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
    }

    #[test]
    fn local_workspace_read_file_supports_one_based_line_ranges() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-read-line-range");
        fs::write(root.join("sample.txt"), "one\ntwo\nthree\nfour\n").expect("seed file");
        let registry = AgentToolRegistry::local_workspace(&root);
        let input = agent_input("read range");
        let call = AgentToolCall {
            call_id: None,
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
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-test");
        let data_dir = SinomoDataDir::new(&data_root);
        let input = agent_input("read");
        let mut config = LocalWorkspaceAgentConfig::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")))
            .with_data_dir_evidence(&data_dir, &input.session.session_id);
        config.max_read_bytes = 16;
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let call = AgentToolCall {
            call_id: None,
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
    fn local_workspace_registry_can_mirror_evidence_to_sifs_mount() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sifs-evidence");
        let mount_dir = root.join("sifs-mount");
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("large.txt"), "alpha\nbeta\ngamma\n").expect("seed file");
        let input = agent_input("read with sifs evidence");
        let mut config = LocalWorkspaceAgentConfig::new(&root).with_sifs_evidence_mount(
            &mount_dir,
            format!(
                "/events/agent/evidence/{}",
                input.session.session_id.as_str()
            ),
        );
        config.max_read_bytes = 5;
        let registry = AgentToolRegistry::local_workspace_with_config(config);

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "read_file".to_string(),
                input: serde_json::json!({
                    "path": "large.txt",
                    "max_bytes": 5
                }),
                r#async: false,
                summary: "read file".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        let evidence_ref = outcome.evidence.full_output_ref().expect("full output ref");
        assert!(evidence_ref.starts_with("sifs://events/agent/evidence/"));
        assert!(evidence_ref.contains(input.session.session_id.as_str()));
        let mirrored: Vec<_> = fs::read_dir(&mount_dir)
            .expect("sifs mirror dir")
            .map(|entry| entry.expect("mirror entry").path())
            .collect();
        assert_eq!(mirrored.len(), 1);
        assert_eq!(
            fs::read_to_string(&mirrored[0]).expect("sifs mirror"),
            "alpha\nbeta\ngamma\n"
        );
    }

    #[test]
    fn local_workspace_registry_records_output_evidence_for_readonly_tools() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-readonly-evidence");
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
                call_id: None,
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
    fn local_workspace_registry_reports_changed_files_and_git_diff() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-git-tools");
        let evidence_dir = root.join("evidence");
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("hello.txt"), "old\n").expect("seed file");
        run_git_for_test(&root, ["init"]);
        run_git_for_test(&root, ["config", "user.email", "sinomo@example.invalid"]);
        run_git_for_test(&root, ["config", "user.name", "Sinomo Test"]);
        run_git_for_test(&root, ["add", "hello.txt"]);
        run_git_for_test(&root, ["commit", "-m", "seed"]);
        fs::write(root.join("hello.txt"), "new\n").expect("modify file");
        fs::write(root.join("new.txt"), "untracked\n").expect("untracked file");
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).with_evidence_dir(&evidence_dir),
        );
        let input = agent_input("inspect git state");

        let changed = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "changed_files".to_string(),
                input: serde_json::json!({}),
                r#async: false,
                summary: "List changes.".to_string(),
            },
        );
        assert_eq!(changed.status, AgentToolStatus::Succeeded);
        let paths = changed
            .output
            .get("paths")
            .and_then(serde_json::Value::as_array)
            .expect("paths");
        assert!(paths.iter().any(|path| path == "hello.txt"));
        assert!(paths.iter().any(|path| path == "new.txt"));

        let diff = registry.execute(
            &input.session,
            0,
            1,
            &AgentToolCall {
                call_id: None,
                tool: "git_diff".to_string(),
                input: serde_json::json!({ "max_bytes": 4096 }),
                r#async: false,
                summary: "Read diff.".to_string(),
            },
        );
        assert_eq!(diff.status, AgentToolStatus::Succeeded);
        assert!(
            diff.output
                .get("diff")
                .and_then(serde_json::Value::as_str)
                .expect("diff")
                .contains("-old")
        );
        let diff_ref = diff
            .output
            .get("full_diff_ref")
            .and_then(serde_json::Value::as_str)
            .expect("full diff ref");
        assert!(Path::new(diff_ref).exists());
        let coding = coding_evidence(&diff.evidence);
        assert_eq!(coding.changed_files, vec!["hello.txt".to_string()]);
        assert!(
            coding
                .diff_ref
                .as_deref()
                .is_some_and(|path| Path::new(path).exists())
        );
    }

    #[test]
    fn local_workspace_registry_blocks_shell_commands_by_default() {
        let registry =
            AgentToolRegistry::local_workspace(std::env::current_dir().expect("current dir"));
        let input = agent_input("command");
        let call = AgentToolCall {
            call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-command-timeout");
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
                call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-command-pipefail");
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).allowing_shell_commands(true),
        );
        let input = agent_input("command pipefail");

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
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
        assert_eq!(
            outcome
                .output
                .get("cwd")
                .and_then(serde_json::Value::as_str),
            Some(".")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn local_workspace_run_command_records_structured_output_and_stream_refs() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-command-structured");
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-command-data");
        let data_dir = SinomoDataDir::new(&data_root);
        let input = agent_input("structured command output");
        let mut config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_shell_commands(true)
            .with_data_dir_evidence(&data_dir, &input.session.session_id);
        config.max_command_output_bytes = 4;
        let registry = AgentToolRegistry::local_workspace_with_config(config);

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "run_command".to_string(),
                input: serde_json::json!({
                    "command": "printf abcdef; printf uvwxyz >&2; exit 2"
                }),
                r#async: false,
                summary: "run structured command".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        let command_output = outcome
            .output
            .get("command_output")
            .expect("command output");
        assert_eq!(
            command_output
                .get("exit_code")
                .and_then(serde_json::Value::as_i64),
            Some(2)
        );
        assert_eq!(
            command_output
                .get("stdout")
                .and_then(serde_json::Value::as_str),
            Some("abcd")
        );
        assert_eq!(
            command_output
                .get("stderr")
                .and_then(serde_json::Value::as_str),
            Some("uvwx")
        );
        assert_eq!(
            command_output
                .get("truncated")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(
            command_output
                .get("duration_ms")
                .and_then(serde_json::Value::as_u64)
                .is_some()
        );
        assert!(
            command_output
                .get("aggregated_output")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .contains("stderr:")
        );
        let stdout_ref = command_output
            .get("stdout_ref")
            .and_then(|value| value.get("uri"))
            .and_then(serde_json::Value::as_str)
            .expect("stdout ref");
        let stderr_ref = command_output
            .get("stderr_ref")
            .and_then(|value| value.get("uri"))
            .and_then(serde_json::Value::as_str)
            .expect("stderr ref");
        assert!(
            fs::read_to_string(stdout_ref)
                .expect("stdout ref")
                .contains("abcdef")
        );
        assert!(
            fs::read_to_string(stderr_ref)
                .expect("stderr ref")
                .contains("uvwxyz")
        );
        assert!(
            outcome
                .evidence
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "stdout")
        );
        assert!(
            outcome
                .evidence
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "stderr")
        );
    }

    #[test]
    fn local_workspace_registry_blocks_apply_patch_by_default() {
        let registry =
            AgentToolRegistry::local_workspace(std::env::current_dir().expect("current dir"));
        let input = agent_input("patch");
        let call = AgentToolCall {
            call_id: None,
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

    #[test]
    fn local_workspace_registry_rejects_unified_diff_patches() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-patch");
        fs::write(root.join("hello.txt"), "old\n").expect("seed file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("patch");
        let patch = "--- a/hello.txt\n+++ b/hello.txt\n@@ -1 +1 @@\n-old\n+new\n";
        let call = AgentToolCall {
            call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-freeform-patch");
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
                call_id: None,
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
    fn local_workspace_registry_accepts_raw_string_apply_patch_input() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-raw-string-patch");
        fs::write(root.join("hello.txt"), "old\n").expect("seed file");
        let config = LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true);
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let input = agent_input("raw string patch");
        let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";

        let outcome = registry.execute(
            &input.session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "apply_patch".to_string(),
                input: serde_json::Value::String(patch.to_string()),
                r#async: false,
                summary: "apply raw string patch".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            fs::read_to_string(root.join("hello.txt")).expect("patched file"),
            "new\n"
        );
    }

    #[test]
    fn local_workspace_apply_patch_records_diff_artifact_metadata() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-patch-diff-evidence");
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
                call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-preflight-patch");
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
                call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-line-ending-patch");
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
                call_id: None,
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

        let root = sinomo_testdir::TestDir::new("sinomo-agent-mode-patch");
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
                call_id: None,
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

        let root = sinomo_testdir::TestDir::new("sinomo-agent-symlink-patch");
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
                call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-anchored-patch");
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
                call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-context-add-patch");
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
                call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-bad-patch");
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
                call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-structured-patch");
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
                call_id: None,
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
        let root = sinomo_testdir::TestDir::new("sinomo-agent-loop-patch");
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
            "background_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let model = SequencedModel::new([
            tool_turn,
            r#"{
                "response_mode": "answer",
                "text": "Patched.",
                "tool_calls": [],
                "background_handoff": null,
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
    fn coding_state_stops_after_patch_with_environment_blocked_validation() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-coding-stop-env");
        let evidence_dir = root.join("evidence");
        fs::create_dir_all(&root).expect("workspace");
        fs::write(root.join("hello.txt"), "old\n").expect("seed file");
        let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";
        let patch_turn = serde_json::json!({
            "response_mode": "defer",
            "text": "I will patch the file.",
            "tool_calls": [{
                "tool": "apply_patch",
                "input": {"patch": patch},
                "async": false,
                "summary": "Patch hello.txt."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let validation_turn = serde_json::json!({
            "response_mode": "defer",
            "text": "I will validate the patch.",
            "tool_calls": [{
                "tool": "run_command",
                "input": {"command": "missing-sinomo-test-validator"},
                "async": false,
                "summary": "Run validation."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let model = SequencedModel::new([patch_turn, validation_turn]);
        let config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_file_writes(true)
            .allowing_shell_commands(true)
            .with_evidence_dir(&evidence_dir);
        let agent = Agent::new(model, "fake", "fake")
            .with_tool_registry(AgentToolRegistry::local_workspace_with_config(config));

        let run = agent.run(&agent_input("patch and validate")).expect("run");
        let coding_state = run.coding_state.as_ref().expect("coding state");

        assert_eq!(run.stop_reason, AgentStopReason::CodingStopRecommended);
        assert_eq!(run.model_turns.len(), 2);
        assert_eq!(
            run.final_decision.response_mode,
            crate::AgentLoopResponseMode::Answer
        );
        assert!(run.final_decision.text.contains("Patch is ready"));
        assert_eq!(coding_state.patch_status, CodingPatchStatus::Applied);
        assert_eq!(
            coding_state.validation_status,
            CodingValidationStatus::FailedEnvironment
        );
        assert_eq!(
            coding_state.stop_recommendation,
            CodingStopDecision::FinishWithPatch
        );
        assert_eq!(coding_state.changed_files, vec!["hello.txt".to_string()]);
        assert!(
            run.items
                .iter()
                .any(|item| matches!(item, AgentItem::CodingRunState { .. }))
        );
    }

    #[test]
    fn turn_context_renders_context_attachments_for_model() {
        let mut input = agent_input("continue the attached job");
        input.context_attachments.push(AgentContextAttachment {
            kind: "sinomo.evolution.mutation".to_string(),
            summary: "candidate verification context".to_string(),
            payload_json: serde_json::json!({
                "verification_results": [
                    {
                        "command": "cargo test -p sinomo-agent",
                        "status": "passed",
                        "summary": "baseline passed"
                    }
                ]
            }),
        });
        let agent = Agent::new(
            SequencedModel::new([r#"{
                "response_mode": "answer",
                "text": "Done.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#]),
            "fake",
            "fake",
        );

        let context = agent.assemble_context(&input, &[], 0).expect("context");
        let request = agent.request(&input, &[], 0).expect("request");

        assert_eq!(context.context_attachments.len(), 1);
        assert!(
            context
                .report
                .included
                .iter()
                .any(|source| source.kind == "context_attachments")
        );
        assert!(context.budget_estimate.context_attachment_bytes > 0);
        assert!(request.messages.iter().any(|message| {
            message.content.contains("context_attachments")
                && message.content.contains("cargo test -p sinomo-agent")
        }));
    }

    #[test]
    fn agent_run_collects_candidate_evidence_bundle() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-candidate-evidence");
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
            "background_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let model = SequencedModel::new([
            tool_turn,
            r#"{
                "response_mode": "answer",
                "text": "Candidate is ready.",
                "tool_calls": [],
                "background_handoff": null,
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
        input.context_attachments.push(AgentContextAttachment {
            kind: "sinomo.evolution.mutation".to_string(),
            summary: "candidate mutation context".to_string(),
            payload_json: serde_json::json!({
                "objective": "build a candidate"
            }),
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
            "Checkpoint summary for old context.",
            r#"{
                "response_mode": "answer",
                "text": "First compacted answer.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#,
            "Checkpoint summary for follow-up context.",
            r#"{
                "response_mode": "answer",
                "text": "Second compacted answer.",
                "tool_calls": [],
                "background_handoff": null,
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
        assert_eq!(requests.len(), 4);
        let compaction_requests = requests
            .iter()
            .filter(|request| {
                request
                    .metadata
                    .get("loop")
                    .and_then(serde_json::Value::as_str)
                    == Some("agent_compaction")
            })
            .collect::<Vec<_>>();
        assert_eq!(compaction_requests.len(), 2);
        assert_eq!(
            compaction_requests[0].response_format,
            LanguageModelResponseFormat::Text
        );
        let agent_requests = requests
            .iter()
            .filter(|request| {
                request
                    .metadata
                    .get("loop")
                    .and_then(serde_json::Value::as_str)
                    == Some("agent")
            })
            .collect::<Vec<_>>();
        assert_eq!(agent_requests.len(), 2);
        for request in agent_requests {
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
        let model = SequencedModel::new([
            "Checkpoint summary for projection context.",
            r#"{
            "response_mode": "answer",
            "text": "Projection-aware answer.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
        ]);
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

        let requests = agent.model.requests.lock().expect("requests");
        let request_text = requests[1]
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
            "background_handoff": null,
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
    fn agent_op_surface_runs_turn_and_emits_lifecycle_events() {
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Done through op.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]);
        let agent = Agent::new(model, "fake", "fake");
        let input = agent_input("hello through op");
        let mut session =
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);

        let outcome = agent
            .handle_op(&mut session, AgentOp::start_turn(input))
            .expect("op");

        assert_eq!(outcome.session_id, session.session_id);
        assert_eq!(outcome.events.len(), 2);
        assert!(matches!(
            &outcome.events[0].payload,
            AgentEventPayload::TurnStarted {
                provider,
                model,
                continuation: false,
                ..
            } if provider == "fake" && model == "fake"
        ));
        assert!(matches!(
            &outcome.events[1].payload,
            AgentEventPayload::TurnCompleted {
                stop_reason: AgentStopReason::FinalDecision,
                model_turn_count: 1,
                tool_result_count: 0,
                ..
            }
        ));
        assert_eq!(
            outcome.run.expect("run").final_decision.text,
            "Done through op."
        );
        assert!(!session.transcript.items.is_empty());
    }

    #[test]
    fn agent_op_surface_wraps_session_control_methods() {
        let model = SequencedModel::new(Vec::<String>::new());
        let agent = Agent::new(model, "fake", "fake");
        let session_id = SessionId::from_legacy_named("sess_agent_ops").expect("session");
        let mut session = AgentSession::new(session_id.clone(), AgentRole::Deliberation);
        session.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "remember this before compacting".to_string(),
            event_id: None,
        });

        let steering = agent
            .handle_op(&mut session, AgentOp::steer("prefer precise output"))
            .expect("steer");
        assert!(matches!(
            &steering.events[0].payload,
            AgentEventPayload::SteeringQueued { steering }
                if steering.message == "prefer precise output"
        ));
        assert_eq!(session.steering_queue.len(), 1);

        let follow_up = agent
            .handle_op(&mut session, AgentOp::follow_up("also mention tests"))
            .expect("follow up");
        assert!(matches!(
            &follow_up.events[0].payload,
            AgentEventPayload::FollowUpQueued { follow_up }
                if follow_up.message == "also mention tests"
        ));
        assert_eq!(session.follow_up_queue.len(), 1);

        let compaction = agent
            .handle_op(&mut session, AgentOp::compact("op compaction"))
            .expect("compact");
        assert!(matches!(
            &compaction.events[0].payload,
            AgentEventPayload::Compacted { summary, item_count }
                if summary.reason == "op compaction"
                    && *item_count == session.transcript.items.len()
        ));

        let snapshot = agent
            .handle_op(&mut session, AgentOp::snapshot())
            .expect("snapshot");
        assert!(matches!(
            &snapshot.events[0].payload,
            AgentEventPayload::Snapshot {
                item_count,
                steering_count: 1,
                follow_up_count: 1,
                aborted: false,
            } if *item_count == session.transcript.items.len()
        ));

        let abort = agent
            .handle_op(&mut session, AgentOp::abort("op abort"))
            .expect("abort");
        assert!(matches!(
            &abort.events[0].payload,
            AgentEventPayload::Aborted { abort } if abort.reason == "op abort"
        ));
        assert!(session.is_aborted());
    }

    #[test]
    fn agent_session_control_plane_orders_ops_and_tracks_task_status() {
        let task = AgentTaskRef::new("coding", "control-plane coding task")
            .with_owner_role(AgentRole::Deliberation);
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Controlled turn done.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]);
        let config = AgentConfig::new("fake", "fake").with_task_ref(task.clone());
        let agent = Agent::new(model, "fake", "fake").with_config(config);
        let input = agent_input("run under control plane");
        let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
        let mut control = AgentSessionControlPlane::new(session);

        let result = control
            .submit_op(&agent, AgentOp::start_turn(input))
            .expect("submit op");

        assert_eq!(result.sequence, 0);
        assert!(result.accepted);
        assert!(
            result
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.run.as_ref())
                .is_some()
        );
        assert!(control.active_turn().is_none());
        assert_eq!(control.snapshot().next_sequence, 1);
        assert!(matches!(
            &result.events[0].payload,
            AgentEventPayload::OperationQueued {
                sequence: 0,
                action,
                ..
            } if action == "start_turn"
        ));
        assert!(result.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::TaskStatusChanged { snapshot }
                    if snapshot.task.task_id == task.task_id
                        && snapshot.status == AgentTaskLifecycleStatus::Running
                        && snapshot.active_op_id.is_some()
            )
        }));
        assert!(result.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::TurnCompleted {
                    stop_reason: AgentStopReason::FinalDecision,
                    ..
                }
            )
        }));
        let snapshot = control.snapshot();
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].task.task_id, task.task_id);
        assert_eq!(
            snapshot.tasks[0].status,
            AgentTaskLifecycleStatus::Completed
        );
        assert!(!snapshot.tasks[0].event_ids.is_empty());
        assert_eq!(control.drain_events().len(), result.events.len());
    }

    #[test]
    fn agent_session_control_plane_persists_progress_and_terminal_result() {
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-control-store");
        let data_dir = SinomoDataDir::new(&data_root);
        let store = crate::FileAgentSessionStore::for_data_dir(&data_dir);
        let task = AgentTaskRef::new("coding", "persisted control-plane task")
            .with_owner_role(AgentRole::Deliberation);
        let model = SequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "Persisted turn done.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]);
        let config = AgentConfig::new("fake", "fake").with_task_ref(task.clone());
        let agent = Agent::new(model, "fake", "fake").with_config(config);
        let input = agent_input("persist control progress");
        let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
        let mut control = AgentSessionControlPlane::new(session);
        let cursor = AgentSessionControlEventCursor {
            offset: 0,
            last_event_id: None,
        };

        let result = control
            .submit_op(&agent, AgentOp::start_turn(input.clone()))
            .expect("submit op");
        result.persist_to_store(&store).expect("persist control");

        let progress = control.progress_since(cursor);
        assert_eq!(progress.events.len(), result.events.len());
        assert!(progress.task_updates.iter().any(|snapshot| {
            snapshot.task.task_id == task.task_id
                && snapshot.status == AgentTaskLifecycleStatus::Completed
        }));
        assert_eq!(progress.terminal_results.len(), 1);
        assert_eq!(
            control
                .terminal_result(&task.task_id)
                .expect("terminal result")
                .status,
            AgentTaskLifecycleStatus::Completed
        );
        assert_eq!(
            result.terminal_results()[0].status,
            AgentTaskLifecycleStatus::Completed
        );

        let loaded_snapshot = store
            .load_control_snapshot(&input.session.session_id)
            .expect("load control snapshot")
            .expect("control snapshot");
        assert_eq!(loaded_snapshot.next_sequence, 1);
        assert_eq!(loaded_snapshot.tasks[0].task.task_id, task.task_id);
        assert_eq!(
            loaded_snapshot.tasks[0].status,
            AgentTaskLifecycleStatus::Completed
        );
        let session_events = fs::read_to_string(
            AgentStoragePaths::from_data_dir(&data_dir)
                .session_events_path_for_session(&input.session.session_id),
        )
        .expect("session events");
        assert!(session_events.contains("\"control_event_published\""));
        assert!(session_events.contains("\"task_status_changed\""));
        assert!(session_events.contains("\"control_snapshot_saved\""));
    }

    #[test]
    fn agent_session_control_snapshot_restores_task_status_for_resume() {
        let task =
            AgentTaskRef::new("coding", "resume task").with_owner_role(AgentRole::Deliberation);
        let session_id = SessionId::from_legacy_named("sess_resume_control").expect("session");
        let snapshot = AgentSessionControlSnapshot {
            session_id: session_id.clone(),
            next_sequence: 7,
            active_turn: None,
            tasks: vec![AgentTaskSnapshot::new(
                task.clone(),
                AgentTaskLifecycleStatus::Completed,
            )],
            buffered_event_count: 3,
        };
        let session = AgentSession::new(session_id.clone(), AgentRole::Deliberation);

        let control =
            AgentSessionControlPlane::from_snapshot(session, snapshot).expect("restore snapshot");

        assert_eq!(control.snapshot().next_sequence, 7);
        assert_eq!(control.tasks().len(), 1);
        assert_eq!(
            control
                .task_snapshot(&task.task_id)
                .map(|snapshot| snapshot.status.clone()),
            Some(AgentTaskLifecycleStatus::Completed)
        );
        assert_eq!(control.events_since(0), &[]);
        assert_eq!(control.terminal_task_snapshots().count(), 1);

        let wrong_session = AgentSession::new(
            SessionId::from_legacy_named("sess_wrong_control").expect("session"),
            AgentRole::Deliberation,
        );
        let wrong_snapshot = control.snapshot();
        let error = AgentSessionControlPlane::from_snapshot(wrong_session, wrong_snapshot)
            .expect_err("session mismatch");
        assert!(matches!(
            error,
            AgentSessionControlRestoreError::SessionMismatch { .. }
        ));
    }

    #[test]
    fn agent_session_control_plane_rejects_cancels_and_times_out_active_turns() {
        let model = SequencedModel::new(Vec::<String>::new());
        let task = AgentTaskRef::new("coding", "active turn guard")
            .with_owner_role(AgentRole::Deliberation);
        let config = AgentConfig::new("fake", "fake").with_task_ref(task.clone());
        let agent = Agent::new(model, "fake", "fake").with_config(config);
        let input = agent_input("should be rejected");
        let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
        let mut control = AgentSessionControlPlane::new(session);
        control.tasks.push(AgentTaskSnapshot::new(
            task.clone(),
            AgentTaskLifecycleStatus::Running,
        ));
        control.active_turn = Some(AgentActiveTurn::new(
            TaskId::generate(),
            99,
            Some(1_000),
            Some(task.task_id.clone()),
        ));

        let rejected = control
            .submit_op(&agent, AgentOp::start_turn(input))
            .expect("rejected op");
        assert!(!rejected.accepted);
        assert!(control.active_turn().is_some());
        assert!(rejected.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::OperationRejected { reason, .. }
                    if reason.contains("active turn")
            )
        }));

        let cancelled = control
            .submit_op(&agent, AgentOp::cancel_active_turn("user cancelled"))
            .expect("cancel active");
        assert!(cancelled.accepted);
        assert!(control.active_turn().is_none());
        assert!(cancelled.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::CancellationRequested { reason, active_op_id }
                    if reason == "user cancelled" && active_op_id.is_some()
            )
        }));
        assert_eq!(
            control.snapshot().tasks[0].status,
            AgentTaskLifecycleStatus::Cancelled
        );

        let timeout_task =
            AgentTaskRef::new("coding", "timeout guard").with_owner_role(AgentRole::Deliberation);
        control.tasks.clear();
        control.tasks.push(AgentTaskSnapshot::new(
            timeout_task.clone(),
            AgentTaskLifecycleStatus::Running,
        ));
        let mut active_turn = AgentActiveTurn::new(
            TaskId::generate(),
            7,
            Some(1),
            Some(timeout_task.task_id.clone()),
        );
        active_turn.started_at -= time::Duration::milliseconds(10);
        control.active_turn = Some(active_turn);

        let timed_out = control
            .cleanup_timed_out_turn("active turn exceeded timeout")
            .expect("timed out");

        assert!(control.active_turn().is_none());
        assert!(timed_out.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::TurnTimedOut { timeout_ms: 1, reason, .. }
                    if reason == "active turn exceeded timeout"
            )
        }));
        assert_eq!(
            control.snapshot().tasks[0].status,
            AgentTaskLifecycleStatus::TimedOut
        );
    }

    #[test]
    fn agent_session_runtime_replace_session_swaps_transcript() {
        let first_session_id =
            SessionId::from_legacy_named("sess_runtime_replace_first").expect("session");
        let second_session_id =
            SessionId::from_legacy_named("sess_runtime_replace_second").expect("session");
        let agent = Agent::new(SequencedModel::new(Vec::<String>::new()), "fake", "fake");
        let runtime =
            AgentSessionRuntime::new(agent, AgentSession::new(first_session_id, AgentRole::Main));
        let mut replacement = AgentSession::new(second_session_id.clone(), AgentRole::Main);
        replacement.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "remember this".to_string(),
            event_id: None,
        });
        replacement.raw_transcript = replacement.transcript.clone();

        runtime
            .replace_session(replacement.clone())
            .expect("replace session");

        let snapshot = runtime.session_snapshot().expect("snapshot");
        assert_eq!(snapshot.session_id, second_session_id);
        assert_eq!(snapshot.transcript, replacement.transcript);
        assert_eq!(snapshot.raw_transcript, replacement.raw_transcript);
        let events = runtime
            .runtime_events_since(AgentRuntimeEventCursor {
                offset: 0,
                last_event_id: None,
            })
            .expect("runtime events");
        assert!(events.events.is_empty());
    }

    #[cfg(not(windows))]
    #[test]
    fn agent_session_runtime_interrupts_active_command_turn() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-cancel");
        let task = AgentTaskRef::new("coding", "runtime cancellable command")
            .with_owner_role(AgentRole::Deliberation);
        let marker = root.join("started.txt");
        let command_turn = serde_json::json!({
            "response_mode": "defer",
            "text": "I will run a long command.",
            "tool_calls": [{
                "tool": "run_command",
                "input": {"command": "printf started > started.txt; sleep 5"},
                "async": false,
                "summary": "Run a long command."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let model = SequencedModel::new([command_turn]);
        let agent_config = AgentConfig::new("fake", "fake")
            .with_task_ref(task.clone())
            .with_max_steps(4);
        let workspace_config = LocalWorkspaceAgentConfig::new(&root).allowing_shell_commands(true);
        let agent = Agent::new(model, "fake", "fake")
            .with_config(agent_config)
            .with_tool_registry(AgentToolRegistry::local_workspace_with_config(
                workspace_config,
            ));
        let input = agent_input("run a cancellable command");
        let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
        let runtime = AgentSessionRuntime::new(agent, session);
        let cursor = runtime.event_cursor().expect("cursor");
        let runtime_cursor = runtime.runtime_event_cursor().expect("runtime cursor");

        let queued = runtime
            .submit_op(AgentOp::start_turn(input))
            .expect("submit turn");
        assert!(queued.accepted);
        assert!(queued.outcome.is_none());
        assert!(runtime.active_turn().expect("active turn").is_some());
        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(marker.exists());

        let cancelled = runtime
            .interrupt_active_turn("user stopped command")
            .expect("cancel turn");
        assert!(cancelled.accepted);
        runtime.join_active_turn().expect("join runtime worker");

        let progress = runtime.progress_since(cursor).expect("progress");
        let runtime_events = runtime
            .runtime_events_since(runtime_cursor)
            .expect("runtime events");
        assert!(runtime_events.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentRuntimeEventPayload::WorkerStarted { action, .. }
                    if action == "start_turn"
            )
        }));
        assert!(runtime_events.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentRuntimeEventPayload::WorkerCompleted {
                    stop_reason: AgentStopReason::Cancelled,
                    ..
                }
            )
        }));
        assert!(progress.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::TurnCompleted {
                    stop_reason: AgentStopReason::Cancelled,
                    ..
                }
            )
        }));
        assert!(progress.task_updates.iter().any(|snapshot| {
            snapshot.task.task_id == task.task_id
                && snapshot.status == AgentTaskLifecycleStatus::Cancelled
        }));
        assert_eq!(
            runtime
                .terminal_result(&task.task_id)
                .expect("terminal result lookup")
                .expect("terminal result")
                .status,
            AgentTaskLifecycleStatus::Cancelled
        );
        let session = runtime.session_snapshot().expect("session snapshot");
        assert!(session.raw_transcript.items.iter().any(|item| {
            matches!(
                item,
                AgentItem::ToolResult {
                    status: AgentToolStatus::Cancelled,
                    ..
                }
            )
        }));
    }

    #[test]
    fn agent_session_runtime_interrupts_active_model_turn() {
        let task = AgentTaskRef::new("coding", "runtime cancellable model")
            .with_owner_role(AgentRole::Deliberation);
        let model = CancellationAwareModel::default();
        let started = model.started();
        let agent_config = AgentConfig::new("fake", "fake").with_task_ref(task.clone());
        let agent = Agent::new(model, "fake", "fake").with_config(agent_config);
        let input = agent_input("run a cancellable model turn");
        let session = AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation);
        let runtime = AgentSessionRuntime::new(agent, session);
        let cursor = runtime.event_cursor().expect("cursor");
        let runtime_cursor = runtime.runtime_event_cursor().expect("runtime cursor");

        let queued = runtime
            .submit_op(AgentOp::start_turn(input))
            .expect("submit turn");
        assert!(queued.accepted);
        for _ in 0..100 {
            if started.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(started.load(std::sync::atomic::Ordering::SeqCst));

        let cancelled = runtime
            .interrupt_active_turn("user stopped model")
            .expect("cancel turn");
        assert!(cancelled.accepted);
        runtime.join_active_turn().expect("join runtime worker");

        let progress = runtime.progress_since(cursor).expect("progress");
        let runtime_events = runtime
            .runtime_events_since(runtime_cursor)
            .expect("runtime events");
        assert!(runtime_events.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentRuntimeEventPayload::WorkerCompleted {
                    stop_reason: AgentStopReason::Cancelled,
                    ..
                }
            )
        }));
        assert!(!runtime_events.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentRuntimeEventPayload::WorkerFailed { .. }
            )
        }));
        assert!(progress.events.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::TurnCompleted {
                    stop_reason: AgentStopReason::Cancelled,
                    ..
                }
            )
        }));
        assert_eq!(
            runtime
                .terminal_result(&task.task_id)
                .expect("terminal result lookup")
                .expect("terminal result")
                .status,
            AgentTaskLifecycleStatus::Cancelled
        );
    }

    #[test]
    fn agent_session_continuation_uses_history_and_consumes_queued_context() {
        let model = SequencedModel::new([
            r#"{
                "response_mode": "answer",
                "text": "First answer.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#,
            r#"{
                "response_mode": "answer",
                "text": "Second answer.",
                "tool_calls": [],
                "background_handoff": null,
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
