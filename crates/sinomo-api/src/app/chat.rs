//! Provider-neutral chat contracts shared by daemon-hosted execution and thin clients.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub const CHAT_RPC_CATEGORY: &str = "chat";
pub const CHAT_ROUTE_AUTH_SCOPE: &str = "local-command";
pub const CHAT_SESSION_OPEN_ROUTE: &str = "/chat/session/open";
pub const CHAT_HANDOFF_PROTOCOL_VERSION: u32 = 1;
pub const CHAT_SESSION_COMMAND_ROUTE: &str = "/chat/session/command";
pub const CHAT_SESSION_EVENTS_ROUTE: &str = "/chat/session/events";
pub const CHAT_SESSION_CLOSE_ROUTE: &str = "/chat/session/close";
pub const CHAT_HISTORY_LIST_ROUTE: &str = "/chat/history/list";
pub const CHAT_HISTORY_LOAD_ROUTE: &str = "/chat/history/load";
pub const CHAT_HISTORY_CHANNEL_ROUTE: &str = "/chat/history/channel";
pub const CHAT_HISTORY_CREATE_CHANNEL_ROUTE: &str = "/chat/history/create-channel";
pub const CHAT_HISTORY_RENAME_ROUTE: &str = "/chat/history/rename";
pub const CHAT_HISTORY_DELETE_ROUTE: &str = "/chat/history/delete";
pub const CHAT_HISTORY_PARTICIPANT_ROUTE: &str = "/chat/history/participant";
pub const CHAT_HISTORY_APPEND_AGENT_ROUTE: &str = "/chat/history/append-agent";
pub const CHAT_RPC_ROUTES: &[&str] = &[
    CHAT_SESSION_OPEN_ROUTE,
    CHAT_SESSION_COMMAND_ROUTE,
    CHAT_SESSION_EVENTS_ROUTE,
    CHAT_SESSION_CLOSE_ROUTE,
    CHAT_HISTORY_LIST_ROUTE,
    CHAT_HISTORY_LOAD_ROUTE,
    CHAT_HISTORY_CHANNEL_ROUTE,
    CHAT_HISTORY_CREATE_CHANNEL_ROUTE,
    CHAT_HISTORY_RENAME_ROUTE,
    CHAT_HISTORY_DELETE_ROUTE,
    CHAT_HISTORY_PARTICIPANT_ROUTE,
    CHAT_HISTORY_APPEND_AGENT_ROUTE,
];
pub const CHAT_READ_CAPABILITY: &str = "chat.session.read";
pub const CHAT_WRITE_CAPABILITY: &str = "chat.session.write";
pub const CHAT_HISTORY_READ_CAPABILITY: &str = "chat.history.read";
pub const CHAT_HISTORY_WRITE_CAPABILITY: &str = "chat.history.write";
pub const CHAT_EVENT_PAGE_MAX: usize = 512;
pub const CHAT_EVENT_WAIT_MAX_MS: u64 = 5_000;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatRuntimeInfo {
    pub agent: String,
    pub provider: String,
    pub model: String,
    pub account: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatToolRuntimeSelection {
    pub sandbox_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<PathBuf>,
    /// Read-only daemon projection. None identifies a peer without workspace-list support.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspaces: Option<Vec<ChatWorkspace>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_session_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatWorkspace {
    pub id: String,
    /// Human-readable local path or SIFS path; mutations use the stable id.
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChatWorkspaceChange {
    AddFolder { path: PathBuf },
    Remove { workspace_id: String },
    MakeDefault { workspace_id: String },
    Clear,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ProviderCapabilities {
    pub responses_http: bool,
    pub responses_websocket: bool,
    pub realtime_websocket: bool,
    pub realtime_webrtc: bool,
    pub web_search: bool,
    pub image_generation: bool,
    pub image_editing: bool,
    pub transcription: bool,
    pub local_model_management: bool,
    pub mcp: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ProviderSummary {
    pub id: String,
    pub account: String,
    pub name: String,
    pub models: Vec<String>,
    #[serde(default)]
    pub capabilities: ProviderCapabilities,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSummary {
    pub key: String,
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_version: Option<String>,
    pub tool_runtime: ChatToolRuntimeSelection,
}

pub fn fallback_agent_summaries() -> Vec<AgentSummary> {
    vec![
        AgentSummary {
            key: "coding-agent".to_string(),
            name: "coding-agent".to_string(),
            description: "general workspace task and verification agent".to_string(),
            module_id: None,
            module_version: None,
            tool_runtime: ChatToolRuntimeSelection::default(),
        },
        AgentSummary {
            key: "main-agent".to_string(),
            name: "main-agent".to_string(),
            description: "user-facing interactive agent".to_string(),
            module_id: None,
            module_version: None,
            tool_runtime: ChatToolRuntimeSelection::default(),
        },
    ]
}

pub fn reasoning_efforts() -> Vec<String> {
    [
        "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChatHistoryConversationKind {
    #[default]
    Chat,
    Channel,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistorySessionSummary {
    pub id: String,
    pub title: String,
    pub kind: ChatHistoryConversationKind,
    pub updated_unix: i64,
    pub participant_count: usize,
    pub participant_names: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryParticipant {
    pub participant_id: String,
    pub agent_key: String,
    pub agent_name: String,
    pub agent_session_id: String,
    pub enabled: bool,
    pub order: u32,
    pub sandbox_node_id: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryChannel {
    pub id: String,
    pub title: String,
    pub kind: ChatHistoryConversationKind,
    pub participants: Vec<ChatHistoryParticipant>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChatHistoryAuthor {
    User { display_name: String },
    Agent { agent_name: String },
    System,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChatHistoryMessageKind {
    #[default]
    Normal,
    Info,
    Error,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryAttachment {
    pub file_name: String,
    pub stored_path: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryStoredMessage {
    pub message_id: String,
    pub sequence: u64,
    pub author: ChatHistoryAuthor,
    pub message_kind: ChatHistoryMessageKind,
    pub content: String,
    pub created_at_unix: i64,
    pub attachments: Vec<ChatHistoryAttachment>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryPage {
    pub messages: Vec<ChatHistoryStoredMessage>,
    pub start_sequence: Option<u64>,
    pub end_sequence: Option<u64>,
    pub has_older: bool,
    pub has_newer: bool,
    pub total_messages: u64,
    pub chunk_size: u64,
    pub discovered_message_ids: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryTimelineSegment {
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub message_count: u64,
    pub first_created_unix: Option<i64>,
    pub last_created_unix: Option<i64>,
    pub preview: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryTimelineOverview {
    pub total_messages: u64,
    pub oldest_sequence: Option<u64>,
    pub newest_sequence: Option<u64>,
    pub chunk_size: u64,
    pub segments: Vec<ChatHistoryTimelineSegment>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryListRequest;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryListResponse {
    pub sessions: Vec<ChatHistorySessionSummary>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChatHistoryLoadMode {
    Initial,
    Before,
    After,
    ReconcileLatest,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryLoadRequest {
    pub session_id: String,
    pub mode: ChatHistoryLoadMode,
    pub sequence: Option<u64>,
    pub limit: usize,
    pub include_overview: bool,
    #[serde(default)]
    pub known_message_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryLoadResponse {
    pub page: ChatHistoryPage,
    pub overview: Option<ChatHistoryTimelineOverview>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryChannelRequest {
    pub session_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryChannelResponse {
    pub channel: Option<ChatHistoryChannel>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryCreateChannelRequest {
    pub session_id: String,
    pub title: String,
    pub agent_key: String,
    pub agent_name: String,
    pub sandbox_node_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryRenameRequest {
    pub session_id: String,
    pub title: String,
    pub kind: ChatHistoryConversationKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryDeleteRequest {
    pub session_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ChatHistoryParticipantRequest {
    Add {
        session_id: String,
        agent_key: String,
        agent_name: String,
        sandbox_node_id: Option<String>,
    },
    Remove {
        session_id: String,
        participant_id: String,
    },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryMutationResponse {
    pub accepted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryAppendAgentRequest {
    pub session_id: String,
    pub agent_key: String,
    pub agent_name: String,
    pub agent_session_id: String,
    pub message_id: String,
    pub created_at_unix_nanos: i128,
    pub content: String,
    pub source_adapter: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChatStreamEvent {
    Token(String),
    AssistantTextDelta {
        step_index: usize,
        text: String,
    },
    ReasoningDelta {
        step_index: usize,
        delta: String,
    },
    ReasoningSummary {
        step_index: usize,
        summary: String,
    },
    AgentToken {
        speaker: ChatSpeaker,
        text: String,
    },
    Notice(String),
    ChannelSystem {
        channel_id: String,
        message_id: String,
        kind: String,
        text: String,
    },
    Context(ChatContextSnapshot),
    ToolCallStarted(ChatToolCallSnapshot),
    ToolCallDelta {
        step_index: usize,
        call_id: Option<String>,
        tool: Option<String>,
        arguments_delta: String,
    },
    ToolCallCompleted(ChatToolCallSnapshot),
    Process(ChatProcessSnapshot),
    Usage(ChatUsageSnapshot),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatContextLane {
    pub kind: String,
    pub label: String,
    pub bytes: usize,
    pub priority: u8,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatContextSnapshot {
    pub step_index: usize,
    pub provider: String,
    pub model: String,
    pub total_bytes: usize,
    pub max_context_bytes: Option<usize>,
    pub compaction_threshold_bytes: Option<usize>,
    pub lanes: Vec<ChatContextLane>,
    pub included_count: usize,
    pub omitted_count: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatToolCallSnapshot {
    pub step_index: usize,
    pub call_index: Option<usize>,
    pub call_id: Option<String>,
    pub tool: String,
    pub input: Option<String>,
    pub input_preview: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatProcessSnapshot {
    pub process_id: String,
    pub command: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub stdout: String,
    pub stderr: String,
    pub stdout_tail: String,
    pub stderr_tail: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatUsageSnapshot {
    pub provider: String,
    pub model: String,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSpeaker {
    pub channel_id: String,
    pub participant_id: String,
    pub agent_key: String,
    pub agent_name: String,
    pub agent_session_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatParticipant {
    pub participant_id: String,
    pub agent_key: String,
    pub agent_name: String,
    pub agent_session_id: String,
    pub enabled: bool,
    pub order: u32,
    pub tool_runtime: ChatToolRuntimeSelection,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatTaskSnapshot {
    pub task_id: String,
    pub kind: String,
    pub summary: String,
    pub status: String,
    pub progress_summary: Option<String>,
    pub result_ref: Option<String>,
    pub active_op_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatHistoryMessage {
    pub role: String,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChatAttachmentSource {
    LocalPath(PathBuf),
    LocalSnapshot {
        provenance: ChatLocalAttachmentProvenance,
        bytes: Arc<[u8]>,
    },
    SifsSnapshot {
        provenance: ChatSifsAttachmentProvenance,
        bytes: Arc<[u8]>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChatLocalAttachmentProvenance {
    pub original_path: String,
    pub canonical_path: Option<String>,
    pub modified_at_unix_nanos: Option<u64>,
    pub unsaved_editor_buffer: bool,
}

impl ChatLocalAttachmentProvenance {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            original_path: path.into(),
            canonical_path: None,
            modified_at_unix_nanos: None,
            unsaved_editor_buffer: false,
        }
    }

    pub fn source_path(&self) -> &str {
        self.canonical_path
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .unwrap_or(&self.original_path)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChatSifsAttachmentProvenance {
    pub path: String,
    pub object_id: Option<String>,
    pub version: Option<String>,
    pub content_hash: Option<String>,
    pub policy: Option<String>,
    pub unsaved_editor_buffer: bool,
}

impl ChatSifsAttachmentProvenance {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            object_id: None,
            version: None,
            content_hash: None,
            policy: None,
            unsaved_editor_buffer: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChatAttachmentInput {
    pub file_name: String,
    pub source: ChatAttachmentSource,
}

impl ChatAttachmentInput {
    pub fn local(path: PathBuf) -> Self {
        Self {
            file_name: attachment_file_name(&path),
            source: ChatAttachmentSource::LocalPath(path),
        }
    }

    pub fn sifs_snapshot(
        file_name: impl Into<String>,
        path: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self::sifs_snapshot_with_provenance(
            file_name,
            ChatSifsAttachmentProvenance::new(path),
            bytes,
        )
    }

    pub fn local_snapshot(
        file_name: impl Into<String>,
        path: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self::local_snapshot_with_provenance(
            file_name,
            ChatLocalAttachmentProvenance::new(path),
            bytes,
        )
    }

    pub fn local_snapshot_with_provenance(
        file_name: impl Into<String>,
        provenance: ChatLocalAttachmentProvenance,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            file_name: file_name.into(),
            source: ChatAttachmentSource::LocalSnapshot {
                provenance,
                bytes: bytes.into(),
            },
        }
    }

    pub fn sifs_snapshot_with_provenance(
        file_name: impl Into<String>,
        provenance: ChatSifsAttachmentProvenance,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            file_name: file_name.into(),
            source: ChatAttachmentSource::SifsSnapshot {
                provenance,
                bytes: bytes.into(),
            },
        }
    }
}

fn attachment_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("attachment")
        .to_string()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatControlStatus {
    pub session_id: String,
    pub accepted: bool,
    pub runtime_attached: bool,
    pub sequence: u64,
    pub event_count: usize,
    pub active_turn: bool,
    pub task_count: usize,
    pub terminal_result_count: usize,
    pub tasks: Vec<ChatTaskSnapshot>,
    pub queued_input: Option<ChatQueuedTurnInput>,
    pub removed_input_id: Option<String>,
    pub warning: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChatQueuedTurnInputKind {
    Steering,
    FollowUp,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatQueuedTurnInput {
    pub input_id: String,
    pub kind: ChatQueuedTurnInputKind,
    pub message: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChatReattachState {
    #[default]
    RuntimeUnavailable,
    LiveRuntimeAttached,
    PersistedStateOnly,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatControlState {
    pub session_id: String,
    pub accepted: bool,
    pub runtime_attached: bool,
    pub reattach_state: ChatReattachState,
    pub reattach_active_turn_persisted: bool,
    pub reattach_running_task_count: usize,
    pub reattach_persisted_event_count: usize,
    pub active_turn: bool,
    pub next_sequence: u64,
    pub task_count: usize,
    pub tasks: Vec<ChatTaskSnapshot>,
    pub event_cursor_offset: usize,
    pub steering_count: usize,
    pub follow_up_count: usize,
    pub queued_inputs: Vec<ChatQueuedTurnInput>,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RuntimeInfo {
    pub agent: String,
    pub provider: String,
    pub model: String,
    pub account: Option<String>,
    pub effort: Option<String>,
}

impl From<ChatRuntimeInfo> for RuntimeInfo {
    fn from(info: ChatRuntimeInfo) -> Self {
        Self {
            agent: info.agent,
            provider: info.provider,
            model: info.model,
            account: info.account,
            effort: info.reasoning_effort,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChatSessionCommand {
    Send {
        session_id: String,
        text: String,
        attachments: Vec<ChatAttachmentInput>,
    },
    Steer {
        session_id: String,
        message: String,
    },
    FollowUp {
        session_id: String,
        message: String,
    },
    RemoveQueuedInput {
        session_id: String,
        input_id: String,
    },
    Interrupt {
        session_id: String,
        reason: String,
    },
    ControlState {
        session_id: String,
    },
    NewSession {
        session_id: String,
    },
    Resume {
        session_id: String,
        history: Vec<ChatHistoryMessage>,
    },
    SetModel {
        provider: String,
        model: String,
        account: String,
    },
    SetAgent {
        session_id: String,
        key: String,
        history: Vec<ChatHistoryMessage>,
    },
    SetToolRuntime {
        selection: ChatToolRuntimeSelection,
    },
    SetWorkspace {
        session_id: String,
        workspace_root: Option<PathBuf>,
    },
    UpdateWorkspaces {
        session_id: String,
        request_id: String,
        change: ChatWorkspaceChange,
    },
    FetchWorkspaces {
        session_id: String,
    },
    SetToolRuntimeNode {
        sandbox_node_id: Option<String>,
    },
    FetchModels {
        provider: String,
    },
    FetchAgents,
    RealtimeHandoff {
        request_id: String,
        session_id: String,
        input: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChatEvent {
    Reattached {
        replay_gap: bool,
    },
    Ready {
        session_id: String,
        info: RuntimeInfo,
    },
    Token {
        session_id: String,
        text: String,
    },
    AssistantTextDelta {
        session_id: String,
        step_index: usize,
        text: String,
    },
    ReasoningSummary {
        session_id: String,
        step_index: usize,
        summary: String,
    },
    ReasoningDelta {
        session_id: String,
        step_index: usize,
        delta: String,
    },
    AgentToken {
        session_id: String,
        speaker: ChatSpeaker,
        text: String,
    },
    Notice {
        session_id: String,
        notice: String,
    },
    SystemMessage {
        session_id: String,
        kind: String,
        text: String,
    },
    Context {
        session_id: String,
        context: ChatContextSnapshot,
    },
    ToolCallStarted {
        session_id: String,
        call: ChatToolCallSnapshot,
    },
    ToolCallDelta {
        session_id: String,
        step_index: usize,
        call_id: Option<String>,
        tool: Option<String>,
        arguments_delta: String,
    },
    ToolCallCompleted {
        session_id: String,
        call: ChatToolCallSnapshot,
    },
    Process {
        session_id: String,
        process: ChatProcessSnapshot,
    },
    Usage {
        session_id: String,
        usage: ChatUsageSnapshot,
    },
    TurnDone {
        session_id: String,
    },
    ControlStatus(ChatControlStatus),
    ControlState(ChatControlState),
    RuntimeInfo(RuntimeInfo),
    ToolRuntime(ChatToolRuntimeSelection),
    WorkspacesUpdated {
        session_id: String,
        request_id: String,
        /// The current runtime state is included even if persistence reports an error.
        selection: Option<ChatToolRuntimeSelection>,
        error: Option<String>,
    },
    SessionReset {
        session_id: String,
    },
    Models {
        provider: String,
        models: Vec<String>,
    },
    Agents {
        agents: Vec<AgentSummary>,
        warning: Option<String>,
    },
    RealtimeHandoffResult {
        request_id: String,
        result: Result<String, String>,
    },
    Error {
        session_id: Option<String>,
        error: String,
    },
}

impl ChatEvent {
    pub fn session_id(&self) -> Option<&str> {
        match self {
            Self::Token { session_id, .. }
            | Self::AssistantTextDelta { session_id, .. }
            | Self::ReasoningDelta { session_id, .. }
            | Self::ReasoningSummary { session_id, .. }
            | Self::AgentToken { session_id, .. }
            | Self::Notice { session_id, .. }
            | Self::SystemMessage { session_id, .. }
            | Self::Context { session_id, .. }
            | Self::ToolCallStarted { session_id, .. }
            | Self::ToolCallDelta { session_id, .. }
            | Self::ToolCallCompleted { session_id, .. }
            | Self::Process { session_id, .. }
            | Self::Usage { session_id, .. }
            | Self::WorkspacesUpdated { session_id, .. }
            | Self::TurnDone { session_id }
            | Self::SessionReset { session_id } => Some(session_id),
            Self::ControlStatus(status) => Some(&status.session_id),
            Self::ControlState(state) => Some(&state.session_id),
            Self::Error {
                session_id: Some(session_id),
                ..
            } => Some(session_id),
            Self::Reattached { .. }
            | Self::Ready { .. }
            | Self::RuntimeInfo(_)
            | Self::Models { .. }
            | Self::Agents { .. }
            | Self::RealtimeHandoffResult { .. }
            | Self::Error {
                session_id: None, ..
            } => None,
            Self::ToolRuntime(selection) => selection.workspace_session_id.as_deref(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SequencedChatEvent {
    pub sequence: u64,
    pub event: ChatEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSessionOpenRequest {
    pub client_id: String,
    /// Durable execution host identity, independent of this transport subscription.
    #[serde(default)]
    pub execution_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSessionOpenResponse {
    pub client_id: String,
    pub next_event_sequence: u64,
    #[serde(default)]
    pub attached: bool,
    #[serde(default)]
    pub generation: String,
    #[serde(default)]
    pub runtime_ready: bool,
    #[serde(default)]
    pub handoff_protocol: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSessionCommandRequest {
    pub client_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub command: ChatSessionCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSessionCommandResponse {
    pub accepted: bool,
    #[serde(default)]
    pub reattach_required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSessionEventsRequest {
    pub client_id: String,
    pub after_sequence: u64,
    pub max_events: usize,
    pub wait_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSessionEventsResponse {
    pub events: Vec<SequencedChatEvent>,
    pub next_sequence: u64,
    #[serde(default)]
    pub first_available_sequence: u64,
    #[serde(default)]
    pub generation: String,
    #[serde(default)]
    pub execution_available: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSessionCloseRequest {
    pub client_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatSessionCloseResponse {
    pub closed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_management_contract_round_trips_and_keeps_legacy_projection_readable() {
        let legacy: ChatToolRuntimeSelection =
            serde_json::from_str(r#"{"sandbox_node_id":null,"workspace_root":"C:/project"}"#)
                .unwrap();
        assert!(legacy.workspaces.is_none());
        let selection = ChatToolRuntimeSelection {
            workspace_root: Some("C:/project".into()),
            workspace_session_id: Some("conversation".into()),
            workspaces: Some(vec![
                ChatWorkspace {
                    id: "local-project".into(),
                    path: "C:/project".into(),
                },
                ChatWorkspace {
                    id: "shared".into(),
                    path: "sifs:/team/lib".into(),
                },
            ]),
            ..Default::default()
        };
        for change in [
            ChatWorkspaceChange::AddFolder {
                path: "C:/project".into(),
            },
            ChatWorkspaceChange::Remove {
                workspace_id: "shared".into(),
            },
            ChatWorkspaceChange::MakeDefault {
                workspace_id: "shared".into(),
            },
            ChatWorkspaceChange::Clear,
        ] {
            let command = ChatSessionCommand::UpdateWorkspaces {
                session_id: "conversation".into(),
                request_id: "operation".into(),
                change,
            };
            let bytes = serde_json::to_vec(&command).unwrap();
            assert_eq!(
                serde_json::from_slice::<ChatSessionCommand>(&bytes).unwrap(),
                command
            );
        }
        let event = ChatEvent::WorkspacesUpdated {
            session_id: "conversation".into(),
            request_id: "operation".into(),
            selection: Some(selection),
            error: None,
        };
        assert_eq!(event.session_id(), Some("conversation"));
        assert_eq!(
            serde_json::from_slice::<ChatEvent>(&serde_json::to_vec(&event).unwrap()).unwrap(),
            event
        );
    }

    #[test]
    fn attachment_snapshots_and_stream_events_round_trip() {
        let command = ChatSessionCommand::Send {
            session_id: "session-a".to_string(),
            text: "hello".to_string(),
            attachments: vec![ChatAttachmentInput::local_snapshot(
                "note.txt",
                "C:/notes/note.txt",
                Arc::<[u8]>::from(&b"content"[..]),
            )],
        };
        let encoded = serde_json::to_vec(&command).expect("encode command");
        let decoded: ChatSessionCommand = serde_json::from_slice(&encoded).expect("decode command");
        assert_eq!(decoded, command);

        let workspace = ChatSessionCommand::SetWorkspace {
            session_id: "session-a".to_string(),
            workspace_root: Some(PathBuf::from("C:/work/todo")),
        };
        let encoded = serde_json::to_vec(&workspace).expect("encode workspace command");
        let decoded: ChatSessionCommand =
            serde_json::from_slice(&encoded).expect("decode workspace command");
        assert_eq!(decoded, workspace);

        let event = ChatEvent::ReasoningDelta {
            session_id: "session-a".to_string(),
            step_index: 2,
            delta: "checking".to_string(),
        };
        assert_eq!(event.session_id(), Some("session-a"));
    }
}
